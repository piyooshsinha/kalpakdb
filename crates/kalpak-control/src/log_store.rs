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
}

#[derive(Default)]
struct Inner {
    log: BTreeMap<u64, Entry>,
    last_purged: Option<LogId>,
    vote: Option<Vote<NodeId>>,
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
        };
        let tmp = dir.join(format!("{META_FILE}.tmp"));
        let bytes = serde_json::to_vec(&meta).map_err(io_err)?;
        std::fs::write(&tmp, bytes).map_err(io_err)?;
        std::fs::rename(&tmp, dir.join(META_FILE)).map_err(io_err)?;
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
        match std::fs::read_to_string(dir.join(LOG_FILE)) {
            Ok(content) => {
                for line in content.lines() {
                    if line.is_empty() {
                        continue;
                    }
                    // A torn tail line from a crash mid-append is dropped;
                    // Raft re-replicates anything that wasn't acknowledged.
                    let Ok(entry) = serde_json::from_str::<Entry>(line) else {
                        break;
                    };
                    log.insert(entry.log_id.index, entry);
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

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                log,
                last_purged: meta.last_purged,
                vote: meta.vote,
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
