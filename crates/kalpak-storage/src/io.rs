//! Pluggable I/O backends.
//!
//! The engine is written against [`IoBackend`] so the same segment logic runs
//! over portable positioned I/O today and `io_uring` + `O_DIRECT` on Linux
//! NVMe nodes later. Backends are intentionally synchronous at this layer;
//! concurrency lives above the store, not inside it.

use std::fs::{File, OpenOptions};
use std::io::Result;
use std::path::Path;

pub trait IoBackend: Send + Sync {
    type SegmentFile: SegmentFile;

    /// Open (creating if absent) a segment file for append + random reads.
    fn open(&self, path: &Path) -> Result<Self::SegmentFile>;
}

pub trait SegmentFile: Send + Sync {
    fn len(&self) -> Result<u64>;

    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Read exactly `buf.len()` bytes at `offset`.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;

    /// Write all of `buf` at `offset`.
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()>;

    /// Durably flush data to the device.
    fn sync(&self) -> Result<()>;
}

/// Portable backend: positioned reads/writes via the OS page cache.
/// Correct everywhere; the performance path on Linux is the `uring` backend.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdBackend;

impl IoBackend for StdBackend {
    type SegmentFile = File;

    fn open(&self, path: &Path) -> Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
    }
}

impl SegmentFile for File {
    fn len(&self) -> Result<u64> {
        Ok(self.metadata()?.len())
    }

    #[cfg(unix)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
    }

    #[cfg(unix)]
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        std::os::unix::fs::FileExt::write_all_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let mut done = 0;
        while done < buf.len() {
            let n = std::os::windows::fs::FileExt::seek_read(self, &mut buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            done += n;
        }
        Ok(())
    }

    #[cfg(windows)]
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        let mut done = 0;
        while done < buf.len() {
            let n = std::os::windows::fs::FileExt::seek_write(self, &buf[done..], offset + done as u64)?;
            done += n;
        }
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.sync_data()
    }
}
