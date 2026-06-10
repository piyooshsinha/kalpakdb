use std::fmt;
use std::str::FromStr;

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::Error;

/// Canonical agent identifier: an Ed25519 public key.
///
/// Agents own their state through their keypair, not their network location.
/// All state mutations are attributable to an `AgentId`, and the id remains
/// stable across restarts, migrations, and infrastructure changes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(#[serde(with = "pubkey_hex")] pub [u8; 32]);

impl AgentId {
    pub fn from_verifying_key(key: &VerifyingKey) -> Self {
        Self(key.to_bytes())
    }

    /// Verify a signature made by this agent over `message`.
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<(), Error> {
        let key = VerifyingKey::from_bytes(&self.0).map_err(|_| Error::InvalidAgentKey)?;
        key.verify(message, signature)
            .map_err(|_| Error::BadSignature)
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AgentId({})", &hex::encode(self.0)[..12])
    }
}

impl FromStr for AgentId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let raw = hex::decode(s).map_err(|_| Error::InvalidId(s.to_string()))?;
        let arr: [u8; 32] = raw
            .try_into()
            .map_err(|_| Error::InvalidId(s.to_string()))?;
        Ok(Self(arr))
    }
}

mod pubkey_hex {
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
    use ed25519_dalek::{Signer, SigningKey};

    #[test]
    fn signature_roundtrip() {
        let signing = SigningKey::from_bytes(&[7u8; 32]);
        let agent = AgentId::from_verifying_key(&signing.verifying_key());
        let sig = signing.sign(b"memory write");
        assert!(agent.verify(b"memory write", &sig).is_ok());
        assert!(agent.verify(b"tampered", &sig).is_err());
    }

    #[test]
    fn display_roundtrip() {
        let signing = SigningKey::from_bytes(&[9u8; 32]);
        let agent = AgentId::from_verifying_key(&signing.verifying_key());
        let parsed: AgentId = agent.to_string().parse().unwrap();
        assert_eq!(agent, parsed);
    }
}
