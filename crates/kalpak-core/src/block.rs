use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::Error;

/// Content address of an immutable block: the BLAKE3 hash of its bytes.
///
/// Two blocks with the same bytes always have the same `BlockId`, which gives
/// the store deduplication and integrity verification for free.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlockId(#[serde(with = "hex_bytes")] pub [u8; 32]);

impl BlockId {
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Verify that `bytes` hash to this id.
    pub fn verify(&self, bytes: &[u8]) -> bool {
        Self::of(bytes) == *self
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockId({})", &hex::encode(self.0)[..12])
    }
}

impl FromStr for BlockId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let raw = hex::decode(s).map_err(|_| Error::InvalidId(s.to_string()))?;
        let arr: [u8; 32] = raw
            .try_into()
            .map_err(|_| Error::InvalidId(s.to_string()))?;
        Ok(Self(arr))
    }
}

mod hex_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8; 32], ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<[u8; 32], D::Error> {
        let s = String::deserialize(de)?;
        let raw = hex::decode(&s).map_err(serde::de::Error::custom)?;
        raw.try_into()
            .map_err(|_| serde::de::Error::custom("expected 32 bytes"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_bytes_same_id() {
        assert_eq!(BlockId::of(b"kalpak"), BlockId::of(b"kalpak"));
        assert_ne!(BlockId::of(b"kalpak"), BlockId::of(b"kalpa"));
    }

    #[test]
    fn display_roundtrip() {
        let id = BlockId::of(b"roundtrip");
        let parsed: BlockId = id.to_string().parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn verify_detects_corruption() {
        let id = BlockId::of(b"payload");
        assert!(id.verify(b"payload"));
        assert!(!id.verify(b"paylaod"));
    }

    #[test]
    fn serde_as_hex_string() {
        let id = BlockId::of(b"json");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{id}\""));
        let back: BlockId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }
}
