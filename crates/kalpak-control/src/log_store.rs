//! Raft log storage.
//!
//! [`LogStore`] keeps the log in a `BTreeMap` and, when opened with a
//! directory, persists every append/vote durably:
//!
//! - `raft-log.jsonl` — one JSON line per entry, fsynced before the append
//!   callback fires (Raft's correctness depends on that ordering).
//! - `raft-meta.json` — vote and last-purged log id, written atomically
//!   (tmp + rename).
//!
//! Truncate/purge rewrite the log file through a tmp + rename, so a crash
//! mid-rewrite leaves either the old or the new file, never a torn one.

// The error type's size is dictated by openraft's storage traits.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{AnyError, ErrorSubject, ErrorVerb, RaftLogReader, StorageIOError, Vote};
use serde::{Deserialize, Serialize};

use crate::types::{Entry, LogId, NodeId, StorageError, TypeConfig};

const LOG_FILE: &str = "raft-log.jsonl";
const META_FILE: &str = "raft-meta.json";

#[derive(Default, Serialize, Deserialize)]
struct Meta {
    vote: Option<Vote<NodeId>>,
    last_purged: Option<LogId>,
    #[serde(default)]
    committed: Option<LogId>,
}

#[derive(Default)]
struct Inner {
    log: BTreeMap<u64, Entry>,
    last_purged: Option<LogId>,
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId>,
    /// Open append handle for the log file, present in durable mode.
    log_file: Option<File>,
    dir: Option<PathBuf>,
}

fn io_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e)).into()
}

impl Inner {
    fn persist_meta(&self) -> Result<(), StorageError> {
        let Some(dir) = &self.dir else { return Ok(()) };
        let meta = Meta {
            vote: self.vote,
            last_purged: self.last_purged,
            committed: self.committed,
        };
        let bytes = serde_json::to_vec(&meta).map_err(io_err)?;
        crate::durability::replace(&dir.join(META_FILE), &bytes).map_err(io_err)?;
        Ok(())
    }

    /// Rewrite the whole log file from the in-memory map (truncate/purge).
    fn rewrite_log(&mut self) -> Result<(), StorageError> {
        let Some(dir) = &self.dir else { return Ok(()) };
        let tmp = dir.join(format!("{LOG_FILE}.tmp"));
        {
            let mut f = File::create(&tmp).map_err(io_err)?;
            for entry in self.log.values() {
                serde_json::to_writer(&mut f, entry).map_err(io_err)?;
                f.write_all(b"\n").map_err(io_err)?;
            }
            f.sync_data().map_err(io_err)?;
        }
        std::fs::rename(&tmp, dir.join(LOG_FILE)).map_err(io_err)?;
        crate::durability::sync_directory(dir).map_err(io_err)?;
        self.log_file = Some(
            OpenOptions::new()
                .append(true)
                .open(dir.join(LOG_FILE))
                .map_err(io_err)?,
        );
        Ok(())
    }

    fn append_durable(&mut self, entry: &Entry) -> Result<(), StorageError> {
        if let Some(f) = &mut self.log_file {
            serde_json::to_writer(&mut *f, entry).map_err(io_err)?;
            f.write_all(b"\n").map_err(io_err)?;
        }
        Ok(())
    }

    fn sync_log(&mut self) -> Result<(), StorageError> {
        if let Some(f) = &mut self.log_file {
            f.sync_data().map_err(io_err)?;
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct LogStore {
    inner: Arc<Mutex<Inner>>,
}

impl LogStore {
    /// Volatile store (tests, ephemeral nodes).
    pub fn in_memory() -> Self {
        Self::default()
    }

    /// Durable store rooted at `dir`, replaying any existing log.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(io_err)?;

        let meta: Meta = match std::fs::read(dir.join(META_FILE)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io_err)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Meta::default(),
            Err(e) => return Err(io_err(e)),
        };

        let mut log = BTreeMap::new();
        let mut valid_len = 0;
        match std::fs::read(dir.join(LOG_FILE)) {
            Ok(content) => {
                for line in content.split_inclusive(|b| *b == b'\n') {
                    // Only an unterminated tail is a recoverable append.
                    // Complete malformed records are corruption: never hide
                    // later acknowledged entries by silently stopping.
                    if !line.ends_with(b"\n") {
                        break;
                    }
                    let entry: Entry = serde_json::from_slice(line).map_err(io_err)?;
                    log.insert(entry.log_id.index, entry);
                    valid_len += line.len() as u64;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err(e)),
        }

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(LOG_FILE))
            .map_err(io_err)?;
        // Remove the invalid suffix before any future append. Otherwise
        // every subsequent restart stops at the same torn record forever.
        log_file.set_len(valid_len).map_err(io_err)?;
        log_file.sync_all().map_err(io_err)?;
        crate::durability::sync_directory(&dir).map_err(io_err)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                log,
                last_purged: meta.last_purged,
                vote: meta.vote,
                committed: meta.committed,
                log_file: Some(log_file),
                dir: Some(dir),
            })),
        })
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let inner = self.inner.lock().unwrap();
        let last = inner
            .log
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id)
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap();
        inner.vote = Some(*vote);
        inner.persist_meta()
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap();
        inner.committed = committed;
        inner.persist_meta()
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        Ok(self.inner.lock().unwrap().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + Send,
    {
        {
            let mut inner = self.inner.lock().unwrap();
            for entry in entries {
                inner.append_durable(&entry)?;
                inner.log.insert(entry.log_id.index, entry);
            }
            inner.sync_log()?;
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap();
        inner.log.retain(|idx, _| *idx < log_id.index);
        inner.rewrite_log()
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap();
        inner.last_purged = Some(log_id);
        inner.log.retain(|idx, _| *idx > log_id.index);
        inner.persist_meta()?;
        inner.rewrite_log()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64) -> Entry {
        serde_json::from_value(serde_json::json!({
            "log_id": {"leader_id": {"term": 1, "node_id": 1}, "index": index},
            "payload": "Blank"
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn torn_tail_is_removed_before_later_appends_and_second_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open(dir.path()).unwrap();
        {
            let mut inner = store.inner.lock().unwrap();
            inner.append_durable(&entry(1)).unwrap();
            inner.sync_log().unwrap();
            inner
                .log_file
                .as_mut()
                .unwrap()
                .write_all(b"{\"torn\":\xff")
                .unwrap();
        }
        drop(store);
        let store = LogStore::open(dir.path()).unwrap();
        {
            let mut inner = store.inner.lock().unwrap();
            inner.append_durable(&entry(2)).unwrap();
            inner.sync_log().unwrap();
        }
        drop(store);
        let mut reopened = LogStore::open(dir.path()).unwrap();
        let entries = reopened.try_get_log_entries(0..3).await.unwrap();
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn complete_corrupt_record_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(LOG_FILE), b"not-json\n").unwrap();
        assert!(LogStore::open(dir.path()).is_err());
    }

    #[tokio::test]
    async fn vote_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let vote = Vote::new(7, 2);
        let mut store = LogStore::open(dir.path()).unwrap();
        store.save_vote(&vote).await.unwrap();
        drop(store);
        assert_eq!(
            LogStore::open(dir.path())
                .unwrap()
                .read_vote()
                .await
                .unwrap(),
            Some(vote)
        );
    }

    #[tokio::test]
    async fn committed_position_survives_reopen_and_old_metadata_remains_readable() {
        let dir = tempfile::tempdir().unwrap();
        // Older deployments have no committed field in raft-meta.json.
        std::fs::write(
            dir.path().join(META_FILE),
            br#"{"vote":null,"last_purged":null}"#,
        )
        .unwrap();
        let mut store = LogStore::open(dir.path()).unwrap();
        assert_eq!(store.read_committed().await.unwrap(), None);
        let committed = entry(5).log_id;
        store.save_committed(Some(committed)).await.unwrap();
        drop(store);
        let mut store = LogStore::open(dir.path()).unwrap();
        assert_eq!(store.read_committed().await.unwrap(), Some(committed));
    }
}
