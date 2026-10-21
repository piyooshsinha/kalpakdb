//! Read-only physical segment inspection. All writers must be stopped.
//! Findings are diagnostic candidates, never instructions to rebuild indexes.
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use kalpak_core::BlockId;
use serde::{Deserialize, Serialize};

use crate::io::SegmentFile;
use crate::segment::{self, HEADER_LEN};
use crate::BLOCK_ALIGN;

#[derive(Debug, Serialize)]
pub struct ScrubFinding {
    pub segment: String,
    pub offset: u64,
    pub kind: &'static str,
    pub block: Option<BlockId>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct ScrubSummary {
    pub records_verified: u64,
    pub findings: u64,
    pub bytes_read: u64,
    pub complete: bool,
}

#[derive(Serialize, Deserialize)]
struct Progress {
    version: u32,
    manifest: String,
    segment: usize,
    offset: u64,
    summary: ScrubSummary,
}

#[derive(Serialize, Deserialize)]
struct SavedProgress {
    checksum: String,
    progress: Progress,
}

fn stamp(path: &Path) -> io::Result<(u64, String)> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(io::Error::other("segment is not a regular file"));
    }
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        format!("{}:{}", metadata.dev(), metadata.ino())
    };
    #[cfg(not(unix))]
    let identity = format!("{:?}", metadata.created()?);
    Ok((
        metadata.len(),
        format!("{identity}:{:?}", metadata.modified()?),
    ))
}

fn checkpoint(dir: &Path, progress: &Progress) -> io::Result<()> {
    let checksum = blake3::hash(&serde_json::to_vec(progress).map_err(io::Error::other)?)
        .to_hex()
        .to_string();
    let bytes =
        serde_json::to_vec(&serde_json::json!({"checksum": checksum, "progress": progress}))
            .map_err(io::Error::other)?;
    let pending = dir.join("scrub-progress.pending");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(pending, dir.join("scrub-progress.json"))?;
    File::open(dir)?.sync_all()
}

struct Reader {
    next: Instant,
    read: u64,
    rate: u64,
}

impl Reader {
    fn read(&mut self, file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        file.read_at(buffer, offset)?;
        self.read = self.read.saturating_add(buffer.len() as u64);
        // Charge page coverage, not just the 64 header bytes, so scanning
        // damaged candidate boundaries cannot evade the read budget.
        let charged =
            ((offset % BLOCK_ALIGN) + buffer.len() as u64).div_ceil(BLOCK_ALIGN) * BLOCK_ALIGN;
        self.next = self.next.max(Instant::now())
            + Duration::from_secs_f64(charged as f64 / self.rate as f64);
        std::thread::sleep(self.next.saturating_duration_since(Instant::now()));
        Ok(())
    }
}

/// Resume an offline scan. Payload memory is bounded to 64 KiB; a restart
/// replays unfinished records rather than serializing opaque hash state.
/// `restart` explicitly begins a new sweep, discarding diagnostic progress.
pub fn scrub(
    dir: &Path,
    bytes_per_second: u64,
    restart: bool,
    mut report: impl FnMut(&ScrubFinding) -> io::Result<()>,
) -> io::Result<ScrubSummary> {
    if !(1024 * 1024..=1024 * 1024 * 1024).contains(&bytes_per_second) {
        return Err(io::Error::other("scrub rate must be 1..=1024 MiB/s"));
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("seg-") && name.ends_with(".klpk") {
            let id = name
                .strip_prefix("seg-")
                .and_then(|n| n.strip_suffix(".klpk"))
                .and_then(|n| n.parse::<u32>().ok())
                .ok_or_else(|| io::Error::other("invalid segment filename"))?;
            if name != format!("seg-{id:08}.klpk") {
                return Err(io::Error::other("noncanonical segment filename"));
            }
            files.push((id, name));
            if files.len() > 65_536 {
                return Err(io::Error::other("scrub segment limit exceeded"));
            }
        }
    }
    files.sort();
    if files.is_empty() {
        return Err(io::Error::other("no segment files found"));
    }
    let layout: Vec<_> = files
        .iter()
        .map(|(_, name)| stamp(&dir.join(name)).map(|s| (name.clone(), s)))
        .collect::<io::Result<_>>()?;
    let manifest = blake3::hash(&serde_json::to_vec(&layout).map_err(io::Error::other)?)
        .to_hex()
        .to_string();
    let saved = if restart {
        None
    } else {
        match std::fs::read(dir.join("scrub-progress.json")) {
            Ok(bytes) => {
                let saved: SavedProgress =
                    serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                let encoded = serde_json::to_vec(&saved.progress).map_err(io::Error::other)?;
                if blake3::hash(&encoded).to_hex().as_str() != saved.checksum {
                    return Err(io::Error::other("scrub checkpoint checksum mismatch"));
                }
                Some(saved.progress)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        }
    };
    let mut progress = saved.unwrap_or(Progress {
        version: 1,
        manifest: manifest.clone(),
        segment: 0,
        offset: 0,
        summary: Default::default(),
    });
    if progress.version != 1
        || progress.manifest != manifest
        || progress.segment > files.len()
        || !progress.offset.is_multiple_of(BLOCK_ALIGN)
        || (progress.segment < files.len()
            && progress.offset > layout[progress.segment].1 .0.saturating_add(BLOCK_ALIGN))
        || (progress.summary.complete && (progress.segment != files.len() || progress.offset != 0))
    {
        return Err(io::Error::other(
            "scrub checkpoint/layout mismatch; stop writers and use --restart for a new sweep",
        ));
    }
    if progress.summary.complete {
        return Ok(progress.summary);
    }
    let initial_bytes = progress.summary.bytes_read;
    let mut reader = Reader {
        next: Instant::now(),
        read: 0,
        rate: bytes_per_second,
    };
    let mut buffer = [0u8; 64 * 1024];
    checkpoint(dir, &progress)?;
    while progress.segment < files.len() {
        let (_, name) = &files[progress.segment];
        let path = dir.join(name);
        let expected = &layout[progress.segment].1;
        let file = File::open(&path)?;
        let length = expected.0;
        while progress.offset < length {
            if stamp(&path)? != *expected {
                return Err(io::Error::other("segment changed during offline scrub"));
            }
            let offset = progress.offset;
            let mut header = [0u8; HEADER_LEN];
            let mut bad = None;
            let mut block = None;
            let mut next = offset.saturating_add(BLOCK_ALIGN);
            if length - offset < HEADER_LEN as u64 {
                let remaining = (length - offset) as usize;
                reader.read(&file, &mut buffer[..remaining], offset)?;
                bad = Some("truncated_header");
            } else {
                reader.read(&file, &mut header, offset)?;
                if let Some(record) = segment::parse_header(&header) {
                    block = Some(record.id);
                    let total = record
                        .payload_len
                        .checked_add(HEADER_LEN as u64)
                        .and_then(|n| n.checked_add(BLOCK_ALIGN - 1))
                        .map(|n| n / BLOCK_ALIGN * BLOCK_ALIGN);
                    if header[48..64].iter().any(|b| *b != 0) {
                        bad = Some("invalid_reserved_header");
                    } else if let Some(total) = total {
                        if total > length - offset {
                            bad = Some("truncated_or_invalid_length");
                        } else {
                            let mut hash = blake3::Hasher::new();
                            let mut read = 0;
                            while read < record.payload_len {
                                let count =
                                    (record.payload_len - read).min(buffer.len() as u64) as usize;
                                reader.read(
                                    &file,
                                    &mut buffer[..count],
                                    offset + HEADER_LEN as u64 + read,
                                )?;
                                hash.update(&buffer[..count]);
                                read += count as u64;
                            }
                            if hash.finalize().as_bytes() == record.id.as_bytes() {
                                progress.summary.records_verified =
                                    progress.summary.records_verified.saturating_add(1);
                                next = offset + total;
                            } else {
                                bad = Some("payload_hash_mismatch");
                            }
                        }
                    } else {
                        bad = Some("length_overflow");
                    }
                } else {
                    bad = Some("invalid_header");
                }
            }
            if stamp(&path)? != *expected {
                return Err(io::Error::other("segment changed during offline scrub"));
            }
            if let Some(kind) = bad {
                report(&ScrubFinding {
                    segment: name.clone(),
                    offset,
                    kind,
                    block,
                })?;
                progress.summary.findings = progress.summary.findings.saturating_add(1);
            }
            // Damaged headers/lengths are not trusted to locate the next
            // record: inspect subsequent aligned candidate boundaries.
            progress.offset = next;
            progress.summary.bytes_read = initial_bytes.saturating_add(reader.read);
            checkpoint(dir, &progress)?;
        }
        progress.segment += 1;
        progress.offset = 0;
        checkpoint(dir, &progress)?;
    }
    for (name, expected) in &layout {
        if stamp(&dir.join(name))? != *expected {
            return Err(io::Error::other("segment changed during offline scrub"));
        }
    }
    let mut segment_count = 0;
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("seg-") && name.ends_with(".klpk") {
            segment_count += 1;
        }
    }
    if segment_count != files.len() {
        return Err(io::Error::other("segment set changed during offline scrub"));
    }
    progress.summary.complete = true;
    checkpoint(dir, &progress)?;
    Ok(progress.summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    const FAST: u64 = 1024 * 1024 * 1024;

    #[test]
    fn read_throttle_does_not_exceed_configured_payload_rate() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::BlockStore::open(dir.path()).unwrap();
        store.put(&vec![1; 128 * 1024]).unwrap();
        drop(store);
        let start = Instant::now();
        let result = scrub(dir.path(), 1024 * 1024, false, |_| Ok(())).unwrap();
        assert_eq!(result.records_verified, 1);
        assert!(start.elapsed() >= Duration::from_millis(125));
    }

    #[test]
    fn damaged_headers_and_lengths_do_not_hide_later_record_candidates() {
        for damage in ["magic", "length", "reserved", "payload"] {
            let dir = tempfile::tempdir().unwrap();
            let store = crate::BlockStore::open(dir.path()).unwrap();
            store.put(b"first").unwrap();
            store.put(b"healthy-later-record").unwrap();
            drop(store);
            let path = dir.path().join("seg-00000000.klpk");
            let mut bytes = std::fs::read(&path).unwrap();
            match damage {
                "magic" => bytes[0] ^= 0xff,
                "length" => bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes()),
                "reserved" => bytes[48] = 1,
                _ => bytes[HEADER_LEN] ^= 0xff,
            }
            std::fs::write(&path, &bytes).unwrap();
            let mut found = Vec::new();
            let result = scrub(dir.path(), FAST, false, |finding| {
                found.push(finding.kind);
                Ok(())
            })
            .unwrap();
            assert_eq!(result.findings, 1, "{damage}: {found:?}");
            assert_eq!(result.records_verified, 1);
            assert!(result.complete);
            assert_eq!(
                std::fs::read(&path).unwrap(),
                bytes,
                "scrubbing modified segment bytes"
            );
            let resumed = scrub(dir.path(), FAST, false, |_| {
                panic!("completed scan emitted again")
            })
            .unwrap();
            assert_eq!(resumed.findings, 1, "resume forgot earlier damage");
        }
    }

    #[test]
    fn checkpoint_tampering_and_changed_layout_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::BlockStore::open(dir.path()).unwrap();
        store.put(b"original").unwrap();
        scrub(dir.path(), FAST, false, |_| Ok(())).unwrap();
        store.put(b"new-record").unwrap();
        assert!(scrub(dir.path(), FAST, false, |_| Ok(())).is_err());
        scrub(dir.path(), FAST, true, |_| Ok(())).unwrap();
        let path = dir.path().join("scrub-progress.json");
        let mut saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        saved["progress"]["summary"]["records_verified"] = serde_json::json!(100);
        std::fs::write(&path, serde_json::to_vec(&saved).unwrap()).unwrap();
        assert!(scrub(dir.path(), FAST, false, |_| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("checksum"));
    }
}
