//! Ordered publication of small durable control-plane files.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

/// Sync contents before rename, then sync the directory entry. Callers
/// serialize updates to a destination (and its temporary file).
pub(crate) fn replace(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("pending");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    sync_directory(
        path.parent()
            .ok_or_else(|| io::Error::other("missing parent"))?,
    )
}

pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}
