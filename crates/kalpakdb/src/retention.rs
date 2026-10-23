//! Durable, non-expiring upload guards. Release requires a future coordinated
//! GC protocol; wall-clock expiry and local binding snapshots are not safe.
use std::collections::{HashMap, HashSet};
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
    ids: HashMap<BlockId, u64>,
    sequence: u64,
    bytes: u64,
    failed: bool,
    active: HashMap<BlockId, usize>,
    release_checks: HashSet<BlockId>,
}

pub(crate) struct Retention(Mutex<State>);

/// Owned by the actual local write operation, not a cancellable caller.
#[must_use]
pub(crate) struct WriteGuard<'a> {
    retention: &'a Retention,
    ids: HashSet<BlockId>,
}

/// Local exclusion only, not authorization to release guards or delete bytes.
/// Kept internal and unexposed until cluster authorization is implemented.
#[allow(dead_code)]
#[must_use]
pub(crate) struct ReleaseCheck<'a> {
    retention: &'a Retention,
    id: BlockId,
}

impl Drop for ReleaseCheck<'_> {
    fn drop(&mut self) {
        self.retention
            .0
            .lock()
            .unwrap()
            .release_checks
            .remove(&self.id);
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.retention.0.lock().unwrap();
        for id in &self.ids {
            let count = state.active.get_mut(id).expect("registered write guard");
            *count -= 1;
            if *count == 0 {
                state.active.remove(id);
            }
        }
    }
}

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
        let mut ids = HashMap::new();
        let mut sequence = 0u64;
        let mut bytes = 0u64;
        loop {
            let mut header = [0u8; HEADER];
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error),
            }
            if (&header[..8] != b"KLPKUL01" && &header[..8] != b"KLPKUL02")
                || header[12..16] != [0; 4]
                || BlockId::of(&header[..24]).as_bytes() != &header[24..56]
            {
                return Err(io::Error::other("corrupt upload retention header"));
            }
            let count = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
            let next = u64::from_le_bytes(header[16..24].try_into().unwrap());
            if !(1..=MAX_BATCH).contains(&count) || sequence.checked_add(1) != Some(next) {
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
                ids.insert(id, next);
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
            active: HashMap::new(),
            release_checks: HashSet::new(),
        })))
    }

    /// Commit the guard before attempting any payload append. Failures leave
    /// conservative guards; there is deliberately no cancellation rollback.
    pub(crate) fn protect(&self, ids: &[BlockId]) -> io::Result<WriteGuard<'_>> {
        if ids.len() > MAX_BATCH {
            return Err(io::Error::other("upload retention batch too large"));
        }
        let mut state = self.0.lock().unwrap();
        if state.failed {
            return Err(io::Error::other(
                "upload retention journal needs restart after I/O failure",
            ));
        }
        // Collapse duplicates within a batch, but never across attempts:
        // reuse must invalidate any previously observed local fence.
        let requested: HashSet<_> = ids.iter().copied().collect();
        // Check the whole batch before journal I/O or publishing any writers.
        // The same lock serializes writer entry and release-check acquisition.
        if requested.iter().any(|id| state.release_checks.contains(id)) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "block reserved by local release check",
            ));
        }
        if requested.is_empty() {
            return Ok(WriteGuard {
                retention: self,
                ids: requested,
            });
        }
        let fresh = requested
            .iter()
            .filter(|id| !state.ids.contains_key(id))
            .count();
        let next = state
            .sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("upload retention sequence exhausted"))?;
        let size = HEADER + requested.len() * 32 + 32;
        if state.ids.len() + fresh > MAX_IDS || state.bytes + size as u64 > MAX_BYTES {
            return Err(io::Error::other(
                "upload retention capacity exhausted; safe reclamation is not implemented",
            ));
        }
        let mut record = Vec::with_capacity(size);
        record.extend_from_slice(b"KLPKUL02");
        record.extend_from_slice(&(requested.len() as u32).to_le_bytes());
        record.extend_from_slice(&[0; 4]);
        record.extend_from_slice(&next.to_le_bytes());
        record.extend_from_slice(BlockId::of(&record).as_bytes());
        for id in &requested {
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
        state.sequence = next;
        state.bytes += size as u64;
        state.ids.extend(requested.iter().map(|id| (*id, next)));
        for id in &requested {
            *state.active.entry(*id).or_default() += 1;
        }
        Ok(WriteGuard {
            retention: self,
            ids: requested,
        })
    }

    pub(crate) fn evidence(&self, id: &BlockId) -> serde_json::Value {
        let state = self.0.lock().unwrap();
        serde_json::json!({"guarded": state.ids.contains_key(id),
            "local_generation": state.ids.get(id),
            "active_local_writes": state.active.get(id).copied().unwrap_or(0),
            "local_release_check": state.release_checks.contains(id),
            "journal_sequence": state.sequence, "write_failed": state.failed})
    }

    /// Atomically validate a durable local generation and exclude new guarded
    /// writes. No network, Raft, guard removal, or payload deletion is performed.
    /// Intentionally not called by the observational HTTP dry-run endpoint.
    #[allow(dead_code)]
    pub(crate) fn try_release_check(
        &self,
        id: BlockId,
        expected_generation: u64,
    ) -> io::Result<ReleaseCheck<'_>> {
        let mut state = self.0.lock().unwrap();
        if state.failed {
            return Err(io::Error::other("retention journal failed"));
        }
        if expected_generation == 0 || state.ids.get(&id) != Some(&expected_generation) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown or stale local generation",
            ));
        }
        if state.active.contains_key(&id) || state.release_checks.contains(&id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "block has an active local operation",
            ));
        }
        state.release_checks.insert(id);
        Ok(ReleaseCheck {
            retention: self,
            id,
        })
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
// Existing journal-format tests intentionally drop write guards immediately.
#[allow(unused_must_use)]
mod tests {
    use super::*;
    #[test]
    fn release_check_rejects_busy_stale_unknown_and_failed_state() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Retention::open(dir.path()).unwrap();
        let a = BlockId::of(b"excluded");
        let b = BlockId::of(b"unrelated");
        let writer = journal.protect(&[a]).unwrap();
        assert!(journal.try_release_check(a, 1).is_err());
        drop(writer);
        assert!(journal.try_release_check(a, 0).is_err());
        assert!(journal.try_release_check(b, 1).is_err());
        let check = journal.try_release_check(a, 1).unwrap();
        assert!(journal.try_release_check(a, 1).is_err());
        let before = journal.snapshot();
        assert_eq!(
            journal.protect(&[a, b]).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(journal.snapshot(), before);
        assert_eq!(journal.evidence(&b)["guarded"], false);
        // An unrelated writer can proceed, without changing a's fence.
        drop(journal.protect(&[b]).unwrap());
        assert_eq!(journal.evidence(&a)["local_release_check"], true);
        drop(check);
        drop(journal.protect(&[a]).unwrap());
        assert!(journal.try_release_check(a, 1).is_err());
        let check = journal.try_release_check(a, 3).unwrap();
        drop(check);
        journal.0.lock().unwrap().failed = true;
        assert!(journal.try_release_check(a, 3).is_err());
    }

    #[test]
    fn competing_writer_and_release_check_cannot_both_enter() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Retention::open(dir.path()).unwrap();
        let id = BlockId::of(b"race");
        drop(journal.protect(&[id]).unwrap());
        for _ in 0..32 {
            let generation = journal.evidence(&id)["local_generation"].as_u64().unwrap();
            let start = std::sync::Barrier::new(2);
            let finish = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let writer = scope.spawn(|| {
                    start.wait();
                    let guard = journal.protect(&[id]);
                    let entered = guard.is_ok();
                    finish.wait(); // Hold winner until both attempts finish.
                    drop(guard);
                    entered
                });
                let checker = scope.spawn(|| {
                    start.wait();
                    let guard = journal.try_release_check(id, generation);
                    let entered = guard.is_ok();
                    finish.wait();
                    drop(guard);
                    entered
                });
                assert_ne!(writer.join().unwrap(), checker.join().unwrap());
            });
            assert_eq!(journal.evidence(&id)["active_local_writes"], 0);
            assert_eq!(journal.evidence(&id)["local_release_check"], false);
        }
    }

    #[test]
    fn release_check_is_ephemeral_and_never_removes_durable_guard() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Retention::open(dir.path()).unwrap();
        let id = BlockId::of(b"retained");
        drop(journal.protect(&[id]).unwrap());
        let before = journal.snapshot();
        let result = std::panic::catch_unwind(|| {
            let _check = journal.try_release_check(id, 1).unwrap();
            panic!("simulate failed release coordinator");
        });
        assert!(result.is_err());
        assert_eq!(journal.evidence(&id)["local_release_check"], false);
        assert_eq!(journal.snapshot(), before);
        drop(journal);
        let journal = Retention::open(dir.path()).unwrap();
        assert_eq!(journal.evidence(&id)["guarded"], true);
        assert_eq!(journal.evidence(&id)["local_generation"], 1);
        assert_eq!(journal.evidence(&id)["local_release_check"], false);
    }

    #[tokio::test]
    async fn cancelling_async_waiter_keeps_submitted_disk_write_visible() {
        let dir = tempfile::tempdir().unwrap();
        let journal = std::sync::Arc::new(Retention::open(dir.path()).unwrap());
        let id = BlockId::of(b"submitted write");
        let worker_journal = journal.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let waiter = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let guard = worker_journal.protect(&[id]).unwrap();
                started_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
                drop(guard);
                let _ = done_tx.send(());
            })
            .await
            .unwrap();
        });
        started_rx.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(journal.evidence(&id)["active_local_writes"], 1);
        release_tx.send(()).unwrap();
        done_rx.await.unwrap();
        assert_eq!(journal.evidence(&id)["active_local_writes"], 0);
        assert_eq!(journal.evidence(&id)["guarded"], true);
    }

    #[test]
    fn overlapping_write_guards_count_operations_and_release_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Retention::open(dir.path()).unwrap();
        let id = BlockId::of(b"overlapping");
        let first = journal.protect(&[id, id]).unwrap();
        assert_eq!(journal.evidence(&id)["active_local_writes"], 1);
        let second = journal.protect(&[id]).unwrap();
        assert_eq!(journal.evidence(&id)["active_local_writes"], 2);
        drop(first);
        assert_eq!(journal.evidence(&id)["active_local_writes"], 1);
        drop(second);
        let fail = || -> io::Result<()> {
            let _guard = journal.protect(&[id])?;
            assert_eq!(journal.evidence(&id)["active_local_writes"], 1);
            Err(io::Error::other("payload failed"))
        };
        assert!(fail().is_err());
        assert_eq!(journal.evidence(&id)["active_local_writes"], 0);
        drop(journal);
        let journal = Retention::open(dir.path()).unwrap();
        assert_eq!(journal.evidence(&id)["active_local_writes"], 0);
        assert_eq!(journal.evidence(&id)["guarded"], true);
    }

    #[test]
    fn reuse_fences_survive_restart_and_migrate_legacy_records() {
        let dir = tempfile::tempdir().unwrap();
        let a = BlockId::of(b"reused");
        let b = BlockId::of(b"untouched");
        let journal = Retention::open(dir.path()).unwrap();
        journal.protect(&[a, b]).unwrap();
        drop(journal);
        // V1 used the same framing but did not append duplicate attempts.
        let path = dir.path().join(NAME);
        let mut record = std::fs::read(&path).unwrap();
        record[..8].copy_from_slice(b"KLPKUL01");
        let checksum = BlockId::of(&record[..24]);
        record[24..56].copy_from_slice(checksum.as_bytes());
        std::fs::write(&path, record).unwrap();
        let journal = Retention::open(dir.path()).unwrap();
        assert_eq!(journal.evidence(&a)["local_generation"], 1);
        journal.protect(&[a, a]).unwrap();
        assert_eq!(journal.evidence(&a)["local_generation"], 2);
        assert_eq!(journal.evidence(&b)["local_generation"], 1);
        assert_eq!(journal.snapshot()["guarded_blocks"], 2);
        drop(journal);
        let journal = Retention::open(dir.path()).unwrap();
        assert_eq!(journal.evidence(&a)["local_generation"], 2);
        assert_eq!(journal.evidence(&b)["local_generation"], 1);
        assert!(journal.evidence(&BlockId::of(b"missing"))["local_generation"].is_null());
        journal.0.lock().unwrap().sequence = u64::MAX;
        assert!(journal.protect(&[a]).is_err());
        assert_eq!(journal.evidence(&a)["local_generation"], 2);
    }

    #[test]
    fn guards_survive_restart_deduplicate_and_recover_torn_tail_before_new_append() {
        let dir = tempfile::tempdir().unwrap();
        let a = BlockId::of(b"a");
        let b = BlockId::of(b"b");
        let journal = Retention::open(dir.path()).unwrap();
        journal.protect(&[a, a]).unwrap();
        let length = journal.snapshot()["journal_bytes"].as_u64().unwrap();
        journal.protect(&[a]).unwrap();
        assert!(journal.snapshot()["journal_bytes"].as_u64().unwrap() > length);
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
        assert_eq!(journal.0.lock().unwrap().sequence, 64);
        journal.0.lock().unwrap().bytes = MAX_BYTES;
        assert!(journal.protect(&[BlockId::of(b"beyond capacity")]).is_err());
        assert!(journal
            .protect(&[BlockId::of(&0u32.to_le_bytes())])
            .is_err());
        assert_eq!(journal.snapshot()["guarded_blocks"], 16);
    }
}
