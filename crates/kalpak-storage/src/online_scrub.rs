//! Generation-aware, bounded physical inspection of sealed segments only.
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use kalpak_core::BlockId;
use serde::{Deserialize, Serialize};

use crate::io::{IoBackend, SegmentFile};
use crate::segment::{self, HEADER_LEN};
use crate::{BlockStore, BLOCK_ALIGN};

#[derive(Default, Serialize, Deserialize)]
struct Cursor {
    version: u32,
    segment: u32,
    generation: Option<String>,
    offset: u64,
    sweeps: u64,
}

#[derive(Serialize, Deserialize)]
struct Saved {
    checksum: String,
    cursor: Cursor,
}

#[derive(Debug, Serialize)]
pub struct OnlineFinding {
    pub segment: u32,
    pub generation: String,
    pub offset: u64,
    pub block: Option<BlockId>,
    pub kind: &'static str,
}

#[derive(Default, Debug)]
pub struct ScrubStep {
    pub bytes_read: u64,
    pub page_budget_bytes: u64,
    pub verified: u64,
    pub findings: Vec<OnlineFinding>,
    pub generation_reset: bool,
    pub yielded: bool,
    pub idle: bool,
}

struct Partial<F> {
    file: Arc<F>,
    id: BlockId,
    length: u64,
    total: u64,
    read: u64,
    hash: blake3::Hasher,
}

/// One scanner per store directory. Callers must pace returned page coverage;
/// each step can also persist a small checkpoint. Findings are physical
/// candidates, not proof that the current logical block is corrupt.
pub struct OnlineScrubber<F: SegmentFile> {
    path: PathBuf,
    cursor: Cursor,
    partial: Option<Partial<F>>,
}

impl<F: SegmentFile> OnlineScrubber<F> {
    pub fn open(dir: &Path) -> io::Result<Self> {
        let path = dir.join("online-scrub-progress.json");
        let cursor = match File::open(&path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(16 * 1024 + 1).read_to_end(&mut bytes)?;
                if bytes.len() > 16 * 1024 {
                    return Err(io::Error::other("online scrub checkpoint too large"));
                }
                let saved: Saved = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                let encoded = serde_json::to_vec(&saved.cursor).map_err(io::Error::other)?;
                if saved.checksum != blake3::hash(&encoded).to_hex().as_str() {
                    return Err(io::Error::other(
                        "online scrub checkpoint checksum mismatch",
                    ));
                }
                saved.cursor
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Cursor {
                version: 1,
                ..Default::default()
            },
            Err(error) => return Err(error),
        };
        if cursor.version != 1
            || !cursor.offset.is_multiple_of(BLOCK_ALIGN)
            || cursor
                .generation
                .as_ref()
                .is_some_and(|generation| generation.len() > 512)
        {
            return Err(io::Error::other("invalid online scrub checkpoint"));
        }
        Ok(Self {
            path,
            cursor,
            partial: None,
        })
    }

    fn save(&self) -> io::Result<()> {
        let encoded = serde_json::to_vec(&self.cursor).map_err(io::Error::other)?;
        let checksum = blake3::hash(&encoded).to_hex().to_string();
        let bytes =
            serde_json::to_vec(&serde_json::json!({"checksum": checksum, "cursor": self.cursor}))
                .map_err(io::Error::other)?;
        let pending = self.path.with_extension("pending");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&pending)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(pending, &self.path)?;
        File::open(self.path.parent().unwrap())?.sync_all()
    }

    /// At most one 64 KiB payload read or one header read. Foreground
    /// activity causes a yield, without introducing any foreground locks.
    pub fn step<B: IoBackend<SegmentFile = F>>(
        &mut self,
        store: &BlockStore<B>,
    ) -> io::Result<ScrubStep> {
        let mut result = ScrubStep::default();
        if store.foreground_io_active() {
            result.yielded = true;
            return Ok(result);
        }
        let Some(view) = store.sealed_scrub_view(self.cursor.segment)? else {
            if self.cursor.segment != 0 {
                self.cursor.segment = 0;
                self.cursor.offset = 0;
                self.cursor.generation = None;
                self.cursor.sweeps = self.cursor.sweeps.saturating_add(1);
                self.partial = None;
                self.save()?;
            }
            result.idle = true;
            return Ok(result);
        };
        if self.cursor.generation.as_deref() != Some(view.generation.as_str()) {
            result.generation_reset = self.cursor.generation.is_some();
            self.cursor.generation = Some(view.generation.clone());
            self.cursor.offset = 0;
            self.partial = None;
            self.save()?;
        }
        if self.cursor.offset >= view.length {
            self.cursor.segment = self
                .cursor
                .segment
                .checked_add(1)
                .ok_or_else(|| io::Error::other("segment cursor overflow"))?;
            self.cursor.offset = 0;
            self.cursor.generation = None;
            self.partial = None;
            self.save()?;
            result.idle = true;
            return Ok(result);
        }
        let start = self.cursor.offset;
        let mut next = None;
        let mut bad = None;
        let mut block = None;
        if let Some(partial) = &mut self.partial {
            block = Some(partial.id);
            let count = (partial.length - partial.read).min(64 * 1024) as usize;
            let offset = start + HEADER_LEN as u64 + partial.read;
            let mut buffer = [0u8; 64 * 1024];
            partial.file.read_at(&mut buffer[..count], offset)?;
            partial.hash.update(&buffer[..count]);
            partial.read += count as u64;
            result.bytes_read = count as u64;
            result.page_budget_bytes =
                ((offset % BLOCK_ALIGN) + count as u64).div_ceil(BLOCK_ALIGN) * BLOCK_ALIGN;
            if partial.read == partial.length {
                if partial.hash.finalize().as_bytes() == partial.id.as_bytes() {
                    result.verified = 1;
                    next = Some(start + partial.total);
                } else {
                    bad = Some("payload_hash_mismatch");
                }
            }
        } else {
            let count = (view.length - start).min(HEADER_LEN as u64) as usize;
            let mut header = [0u8; HEADER_LEN];
            view.file.read_at(&mut header[..count], start)?;
            result.bytes_read = count as u64;
            result.page_budget_bytes = BLOCK_ALIGN;
            if count != HEADER_LEN {
                bad = Some("truncated_header");
            } else if let Some(record) = segment::parse_header(&header) {
                block = Some(record.id);
                let total = record
                    .payload_len
                    .checked_add(HEADER_LEN as u64)
                    .and_then(|n| n.checked_add(BLOCK_ALIGN - 1))
                    .map(|n| n / BLOCK_ALIGN * BLOCK_ALIGN);
                if header[48..64].iter().any(|byte| *byte != 0) {
                    bad = Some("invalid_reserved_header");
                } else if let Some(total) = total {
                    if total > view.length - start {
                        bad = Some("truncated_or_invalid_length");
                    } else if record.payload_len == 0 {
                        if record.id == BlockId::of(&[]) {
                            result.verified = 1;
                            next = Some(start + total);
                        } else {
                            bad = Some("payload_hash_mismatch");
                        }
                    } else {
                        self.partial = Some(Partial {
                            file: view.file.clone(),
                            id: record.id,
                            length: record.payload_len,
                            total,
                            read: 0,
                            hash: blake3::Hasher::new(),
                        });
                    }
                } else {
                    bad = Some("length_overflow");
                }
            } else {
                bad = Some("invalid_header");
            }
        }
        // A retired descriptor is safe to read, but its offsets and findings
        // must never publish as progress against the replacement generation.
        let current = store.sealed_scrub_view(self.cursor.segment)?;
        if current.as_ref().map(|current| &current.generation) != Some(&view.generation) {
            self.partial = None;
            self.cursor.generation = None;
            self.cursor.offset = 0;
            self.save()?;
            result.verified = 0;
            result.generation_reset = true;
            return Ok(result);
        }
        if let Some(kind) = bad {
            result.findings.push(OnlineFinding {
                segment: self.cursor.segment,
                generation: view.generation,
                offset: start,
                block,
                kind,
            });
            next = Some(start.saturating_add(BLOCK_ALIGN));
        }
        if let Some(next) = next {
            self.cursor.offset = next;
            self.partial = None;
            self.save()?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_steps_resume_and_ignore_active_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open_with_roll(dir.path(), 256 * 1024).unwrap();
        store.put(&vec![3; 200 * 1024]).unwrap();
        let mut scanner = OnlineScrubber::<File>::open(dir.path()).unwrap();
        assert!(scanner.step(&store).unwrap().idle);
        store.put(&vec![4; 200 * 1024]).unwrap();
        scanner.step(&store).unwrap(); // header
        scanner.step(&store).unwrap(); // partial payload, not checkpointed
        drop(scanner);
        let mut scanner = OnlineScrubber::<File>::open(dir.path()).unwrap();
        let mut verified = 0;
        for _ in 0..6 {
            let step = scanner.step(&store).unwrap();
            assert!(step.bytes_read <= 64 * 1024);
            assert!(step.page_budget_bytes <= 68 * 1024);
            assert!(step.findings.is_empty());
            verified += step.verified;
        }
        assert_eq!(verified, 1);
    }

    #[test]
    fn compaction_restarts_old_offsets_and_truncates_stale_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open_with_roll(dir.path(), 256 * 1024).unwrap();
        let dead = store.put(&[1; 3000]).unwrap();
        let kept = store.put(&vec![2; 200 * 1024]).unwrap();
        store.put(&vec![3; 200 * 1024]).unwrap();
        let mut scanner = OnlineScrubber::<File>::open(dir.path()).unwrap();
        for _ in 0..4 {
            scanner.step(&store).unwrap();
        }
        assert_eq!(scanner.cursor.offset, 4096);
        assert!(scanner.partial.is_some());
        std::fs::write(
            dir.path().join("seg-00000000.klpk.compact"),
            vec![99; 400 * 1024],
        )
        .unwrap();
        store.compact(|id| *id != dead).unwrap();
        drop(scanner);
        let mut scanner = OnlineScrubber::<File>::open(dir.path()).unwrap();
        assert!(scanner.step(&store).unwrap().generation_reset);
        assert_eq!(scanner.cursor.offset, 0);
        let mut verified = 0;
        for _ in 0..5 {
            let step = scanner.step(&store).unwrap();
            assert!(step.findings.is_empty());
            verified += step.verified;
        }
        assert_eq!(verified, 1);
        assert_eq!(store.get(&kept).unwrap(), vec![2; 200 * 1024]);
        drop(store);
        let reopened = BlockStore::open(dir.path()).unwrap();
        assert!(!reopened.contains(&dead));
        assert_eq!(reopened.get(&kept).unwrap(), vec![2; 200 * 1024]);
    }

    #[test]
    fn detects_payload_damage_and_rejects_bad_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::open_with_roll(dir.path(), 4096).unwrap();
        store.put(&[1; 100]).unwrap();
        store.put(&[2; 100]).unwrap();
        let mut scanner = OnlineScrubber::<File>::open(dir.path()).unwrap();
        let file = OpenOptions::new()
            .write(true)
            .open(dir.path().join("seg-00000000.klpk"))
            .unwrap();
        file.write_at(&[9], HEADER_LEN as u64).unwrap();
        scanner.step(&store).unwrap();
        let step = scanner.step(&store).unwrap();
        assert_eq!(step.findings.len(), 1);
        assert_eq!(step.findings[0].kind, "payload_hash_mismatch");
        file.write_at(&[0], 0).unwrap();
        let step = scanner.step(&store).unwrap();
        assert!(step.generation_reset);
        assert_eq!(step.findings[0].kind, "invalid_header");
        std::fs::write(dir.path().join("online-scrub-progress.json"), b"{}").unwrap();
        assert!(OnlineScrubber::<File>::open(dir.path()).is_err());
    }

    #[test]
    fn scans_while_uploads_continue() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(BlockStore::open_with_roll(dir.path(), 16 * 1024).unwrap());
        for n in 0..12u32 {
            store.put(&n.to_le_bytes().repeat(750)).unwrap();
        }
        let writer = {
            let store = store.clone();
            std::thread::spawn(move || {
                for n in 12..48u32 {
                    let data = n.to_le_bytes().repeat(750);
                    let id = store.put(&data).unwrap();
                    assert_eq!(store.get(&id).unwrap(), data);
                }
            })
        };
        let mut scanner = OnlineScrubber::<File>::open(dir.path()).unwrap();
        for _ in 0..150 {
            let step = scanner.step(&store).unwrap();
            assert!(step.bytes_read <= 64 * 1024);
            assert!(step.findings.is_empty());
            std::thread::yield_now();
        }
        writer.join().unwrap();
        assert_eq!(store.stats().blocks, 48);
    }
}
