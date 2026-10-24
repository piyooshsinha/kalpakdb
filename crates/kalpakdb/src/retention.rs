//! Durable, non-expiring upload guards. Release requires a future coordinated
//! GC protocol; wall-clock expiry and local binding snapshots are not safe.
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Mutex;

use kalpak_core::BlockId;

const HEADER: usize = 56;
const MAX_BATCH: usize = 4096;
const MAX_IDS: usize = 1_048_576;
const MAX_BYTES: u64 = 128 * 1024 * 1024;
const NAME: &str = "upload-retention.log";
const GC_REASON: &str = "compaction blocked: upload retention requires a coordinated GC watermark";

struct State {
    file: File,
    ids: HashSet<BlockId>,
    sequence: u64,
    bytes: u64,
    failed: bool,
}

pub(crate) struct Retention(Mutex<State>);

impl Retention {
    pub(crate) fn open(dir: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .append(true)
            .open(dir.join(NAME))?;
        if file.metadata()?.len() > MAX_BYTES {
            return Err(io::Error::other(
                "upload retention journal exceeds capacity",
            ));
        }
        let mut ids = HashSet::new();
        let mut sequence = 0u64;
        let mut bytes = 0u64;
        loop {
            let mut header = [0u8; HEADER];
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error),
            }
            if &header[..8] != b"KLPKUL01"
                || header[12..16] != [0; 4]
                || BlockId::of(&header[..24]).as_bytes() != &header[24..56]
            {
                return Err(io::Error::other("corrupt upload retention header"));
            }
            let count = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
            let next = u64::from_le_bytes(header[16..24].try_into().unwrap());
            if !(1..=MAX_BATCH).contains(&count) || next != sequence + 1 {
                return Err(io::Error::other("invalid upload retention record"));
            }
            let mut body = vec![0u8; count * 32 + 32];
            match file.read_exact(&mut body) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error),
            }
            if BlockId::of(&body[..count * 32]).as_bytes() != &body[count * 32..] {
                return Err(io::Error::other("corrupt upload retention payload"));
            }
            for chunk in body[..count * 32].as_chunks::<32>().0 {
                let id = BlockId(*chunk);
                ids.insert(id);
            }
            if ids.len() > MAX_IDS {
                return Err(io::Error::other("upload retention capacity exceeded"));
            }
            sequence = next;
            bytes += (HEADER + body.len()) as u64;
        }
        // A partial final record precedes its payload write, and therefore
        // cannot represent an acknowledged new upload under this protocol.
        file.set_len(bytes)?;
        file.sync_all()?;
        File::open(dir)?.sync_all()?;
        Ok(Self(Mutex::new(State {
            file,
            ids,
            sequence,
            bytes,
            failed: false,
        })))
    }

    /// Commit the guard before attempting any payload append. Failures leave
    /// conservative guards; there is deliberately no cancellation rollback.
    pub(crate) fn protect(&self, ids: &[BlockId]) -> io::Result<()> {
        if ids.len() > MAX_BATCH {
            return Err(io::Error::other("upload retention batch too large"));
        }
        let mut state = self.0.lock().unwrap();
        if state.failed {
            return Err(io::Error::other(
                "upload retention journal needs restart after I/O failure",
            ));
        }
        let fresh: HashSet<_> = ids
            .iter()
            .filter(|id| !state.ids.contains(id))
            .copied()
            .collect();
        if fresh.is_empty() {
            return Ok(());
        }
        let size = HEADER + fresh.len() * 32 + 32;
        if state.ids.len() + fresh.len() > MAX_IDS || state.bytes + size as u64 > MAX_BYTES {
            return Err(io::Error::other(
                "upload retention capacity exhausted; safe reclamation is not implemented",
            ));
        }
        let mut record = Vec::with_capacity(size);
        record.extend_from_slice(b"KLPKUL01");
        record.extend_from_slice(&(fresh.len() as u32).to_le_bytes());
        record.extend_from_slice(&[0; 4]);
        record.extend_from_slice(&(state.sequence + 1).to_le_bytes());
        record.extend_from_slice(BlockId::of(&record).as_bytes());
        for id in &fresh {
            record.extend_from_slice(id.as_bytes());
        }
        record.extend_from_slice(BlockId::of(&record[HEADER..]).as_bytes());
        if let Err(error) = state
            .file
            .write_all(&record)
            .and_then(|_| state.file.sync_all())
        {
            state.failed = true;
            return Err(error);
        }
        state.sequence += 1;
        state.bytes += size as u64;
        state.ids.extend(fresh);
        Ok(())
    }

    pub(crate) fn evidence(&self, id: &BlockId) -> serde_json::Value {
        let state = self.0.lock().unwrap();
        serde_json::json!({"guarded": state.ids.contains(id),
            "journal_sequence": state.sequence, "write_failed": state.failed})
    }

    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let state = self.0.lock().unwrap();
        serde_json::json!({"guarded_blocks": state.ids.len(), "journal_bytes": state.bytes,
            "max_guarded_blocks": MAX_IDS, "max_journal_bytes": MAX_BYTES,
            "write_failed": state.failed, "automatic_expiry": false,
            "gc_enabled": false, "gc_blocked_reason": GC_REASON})
    }

    pub(crate) fn archive<W: Write>(
        &self,
        builder: &mut tar::Builder<W>,
        dir: &Path,
    ) -> io::Result<()> {
        let _guard = self.0.lock().unwrap();
        builder.append_path_with_name(dir.join(NAME), NAME)
    }
}

pub(crate) fn gc_reason() -> &'static str {
    GC_REASON
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn guards_survive_restart_deduplicate_and_recover_torn_tail_before_new_append() {
        let dir = tempfile::tempdir().unwrap();
        let a = BlockId::of(b"a");
        let b = BlockId::of(b"b");
        let journal = Retention::open(dir.path()).unwrap();
        journal.protect(&[a, a]).unwrap();
        let length = journal.snapshot()["journal_bytes"].as_u64().unwrap();
        journal.protect(&[a]).unwrap();
        assert_eq!(journal.snapshot()["journal_bytes"], length);
        drop(journal);
        OpenOptions::new()
            .append(true)
            .open(dir.path().join(NAME))
            .unwrap()
            .write_all(b"KLPK")
            .unwrap();
        let journal = Retention::open(dir.path()).unwrap();
        assert_eq!(journal.snapshot()["guarded_blocks"], 1);
        journal.protect(&[b]).unwrap();
        drop(journal);
        assert_eq!(
            Retention::open(dir.path()).unwrap().snapshot()["guarded_blocks"],
            2
        );
    }

    #[test]
    fn corruption_and_capacity_fail_closed_without_partial_publication() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Retention::open(dir.path()).unwrap();
        journal.protect(&[BlockId::of(b"guarded")]).unwrap();
        assert!(journal
            .protect(&vec![BlockId::of(b"new"); MAX_BATCH + 1])
            .is_err());
        assert_eq!(journal.snapshot()["guarded_blocks"], 1);
        drop(journal);
        let path = dir.path().join(NAME);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER] ^= 1;
        std::fs::write(path, bytes).unwrap();
        assert!(Retention::open(dir.path()).is_err());
    }

    #[test]
    fn failed_journal_write_poisoning_prevents_even_deduplicated_writes() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Retention::open(dir.path()).unwrap();
        let id = BlockId::of(b"guarded");
        journal.protect(&[id]).unwrap();
        // Simulate an I/O error without filling or damaging a real volume.
        journal.0.lock().unwrap().file = File::open(dir.path().join(NAME)).unwrap();
        assert!(journal.protect(&[BlockId::of(b"new")]).is_err());
        assert!(journal.protect(&[id]).is_err());
        assert_eq!(journal.snapshot()["write_failed"], true);
        drop(journal);
        let journal = Retention::open(dir.path()).unwrap();
        assert_eq!(journal.snapshot()["guarded_blocks"], 1);
        journal.protect(&[BlockId::of(b"new")]).unwrap();
    }

    #[test]
    fn capacity_check_is_atomic_and_concurrent_uploads_deduplicate() {
        let dir = tempfile::tempdir().unwrap();
        let journal = std::sync::Arc::new(Retention::open(dir.path()).unwrap());
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let journal = journal.clone();
                std::thread::spawn(move || {
                    for n in 0..16u32 {
                        journal.protect(&[BlockId::of(&n.to_le_bytes())]).unwrap();
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(journal.snapshot()["guarded_blocks"], 16);
        journal.0.lock().unwrap().bytes = MAX_BYTES;
        assert!(journal.protect(&[BlockId::of(b"beyond capacity")]).is_err());
        assert_eq!(journal.snapshot()["guarded_blocks"], 16);
    }
}
