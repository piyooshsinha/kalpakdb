//! On-disk record format for segment files.
//!
//! Each record is a fixed 64-byte header followed by the payload, with the
//! whole record padded to [`BLOCK_ALIGN`]:
//!
//! ```text
//! offset  size  field
//! 0       4     magic "KLPK"
//! 4       4     format version (LE u32)
//! 8       8     payload length (LE u64)
//! 16      32    BLAKE3 hash of payload (= BlockId)
//! 48      16    reserved (zero)
//! 64      n     payload
//! ...           zero padding to the next 4 KiB boundary
//! ```
//!
//! The header hash makes every record self-verifying, so the index can be
//! rebuilt by a forward scan and torn tail-writes are detected and truncated.

use kalpak_core::{BlockId, Error};

use crate::io::SegmentFile;
use crate::BLOCK_ALIGN;

pub const MAGIC: [u8; 4] = *b"KLPK";
pub const FORMAT_VERSION: u32 = 1;
pub const HEADER_LEN: usize = 64;

pub fn record_len(payload_len: u64) -> u64 {
    let raw = HEADER_LEN as u64 + payload_len;
    raw.div_ceil(BLOCK_ALIGN) * BLOCK_ALIGN
}

pub fn encode_record(id: &BlockId, payload: &[u8]) -> Vec<u8> {
    let total = record_len(payload.len() as u64) as usize;
    let mut buf = vec![0u8; total];
    buf[0..4].copy_from_slice(&MAGIC);
    buf[4..8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf[8..16].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    buf[16..48].copy_from_slice(id.as_bytes());
    buf[HEADER_LEN..HEADER_LEN + payload.len()].copy_from_slice(payload);
    buf
}

pub struct RecordHeader {
    pub id: BlockId,
    pub payload_len: u64,
}

/// Parse a header, returning `None` for anything that isn't a valid record
/// start (zero padding, torn write, foreign bytes).
pub fn parse_header(buf: &[u8; HEADER_LEN]) -> Option<RecordHeader> {
    if buf[0..4] != MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if version != FORMAT_VERSION {
        return None;
    }
    let payload_len = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let id = BlockId(buf[16..48].try_into().unwrap());
    Some(RecordHeader { id, payload_len })
}

/// Where a block lives inside a segment.
#[derive(Clone, Copy, Debug)]
pub struct Location {
    pub segment: u32,
    /// Offset of the record header within the segment file.
    pub offset: u64,
    pub payload_len: u64,
}

/// Scan a segment from the start, yielding every valid record. Stops at the
/// first invalid header (end of data, or a torn write at the tail) and
/// reports the offset where valid data ends.
pub fn scan<F: SegmentFile>(
    file: &F,
    segment: u32,
    mut on_record: impl FnMut(BlockId, Location),
) -> Result<u64, Error> {
    let len = file.len()?;
    let mut offset = 0u64;
    let mut header = [0u8; HEADER_LEN];

    while offset + HEADER_LEN as u64 <= len {
        file.read_at(&mut header, offset)?;
        let Some(rec) = parse_header(&header) else {
            break;
        };
        let total = record_len(rec.payload_len);
        if offset + total > len {
            // Torn tail write: header landed, payload didn't. Valid data
            // ends here; the store overwrites from this offset.
            break;
        }
        on_record(
            rec.id,
            Location {
                segment,
                offset,
                payload_len: rec.payload_len,
            },
        );
        offset += total;
    }
    Ok(offset)
}
