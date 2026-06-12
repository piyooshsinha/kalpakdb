//! Canonical signed-write messages.
//!
//! Makes "state mutations are signed and attributable" literal: every
//! metadata mutation (register, bind, chain-bind) has a deterministic binary
//! encoding that the owning agent signs with its Ed25519 key and the server
//! verifies against the agent's public key before the mutation enters Raft.
//!
//! The encoding is raw bytes — never JSON — so every SDK can reproduce it
//! exactly: length-free fields are fixed-width, strings are NUL-terminated,
//! counts are little-endian u32. Domain-separation prefixes keep a signature
//! for one operation from ever validating another.
//!
//! Replay: signatures carry no nonce. All three mutations are idempotent
//! (re-registering or re-binding identical content is a no-op), so replaying
//! a captured signature reproduces state that already exists. Capture
//! resistance on the wire is TLS's job, not the signature's.

use ed25519_dalek::{Signature, Signer, SigningKey};

use crate::{AgentId, BlockId, CacheKey};

const REGISTER_PREFIX: &[u8] = b"KLPK/reg/v1\0";
const BIND_PREFIX: &[u8] = b"KLPK/bind/v1\0";
const CHAIN_PREFIX: &[u8] = b"KLPK/chain/v1\0";

fn push_key(buf: &mut Vec<u8>, key: &CacheKey) {
    buf.extend_from_slice(key.fingerprint.model_id.as_bytes());
    buf.push(0);
    buf.extend_from_slice(key.fingerprint.tokenizer_hash.as_bytes());
    buf.push(0);
    buf.extend_from_slice(key.fingerprint.kv_layout.as_bytes());
    buf.push(0);
    buf.extend_from_slice(key.prefix_hash.as_bytes());
}

fn push_link(buf: &mut Vec<u8>, key: &CacheKey, blocks: &[BlockId], parent: Option<&CacheKey>) {
    push_key(buf, key);
    buf.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
    for b in blocks {
        buf.extend_from_slice(b.as_bytes());
    }
    match parent {
        Some(p) => {
            buf.push(1);
            // Parent shares the fingerprint by construction; its prefix
            // hash is what distinguishes it.
            buf.extend_from_slice(p.prefix_hash.as_bytes());
        }
        None => buf.push(0),
    }
}

/// Message an agent signs to register (or re-register) itself.
pub fn register_message(agent: &AgentId, display_name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(REGISTER_PREFIX.len() + 32 + display_name.len());
    buf.extend_from_slice(REGISTER_PREFIX);
    buf.extend_from_slice(&agent.0);
    buf.extend_from_slice(display_name.as_bytes());
    buf
}

/// Message an agent signs to bind one prefix to its blocks.
pub fn bind_message(
    agent: &AgentId,
    key: &CacheKey,
    blocks: &[BlockId],
    parent: Option<&CacheKey>,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(BIND_PREFIX);
    buf.extend_from_slice(&agent.0);
    push_link(&mut buf, key, blocks, parent);
    buf
}

/// Message an agent signs to bind a whole chain atomically.
pub fn chain_message(
    agent: &AgentId,
    links: &[(CacheKey, Vec<BlockId>, Option<CacheKey>)],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256 * links.len().max(1));
    buf.extend_from_slice(CHAIN_PREFIX);
    buf.extend_from_slice(&agent.0);
    buf.extend_from_slice(&(links.len() as u32).to_le_bytes());
    for (key, blocks, parent) in links {
        push_link(&mut buf, key, blocks, parent.as_ref());
    }
    buf
}

/// Sign a canonical message, returning the signature as lowercase hex
/// (the wire form carried in request bodies).
pub fn sign_hex(key: &SigningKey, message: &[u8]) -> String {
    hex::encode(key.sign(message).to_bytes())
}

/// Parse a wire-form (hex) signature.
pub fn signature_from_hex(s: &str) -> Result<Signature, crate::Error> {
    let raw = hex::decode(s).map_err(|_| crate::Error::BadSignature)?;
    let bytes: [u8; 64] = raw.try_into().map_err(|_| crate::Error::BadSignature)?;
    Ok(Signature::from_bytes(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ModelFingerprint;

    fn agent_and_key() -> (SigningKey, AgentId) {
        let sk = SigningKey::from_bytes(&[42; 32]);
        let id = AgentId::from_verifying_key(&sk.verifying_key());
        (sk, id)
    }

    fn key(tokens: &[u32]) -> CacheKey {
        CacheKey::root(ModelFingerprint::new("m", "t", "l"), tokens)
    }

    #[test]
    fn sign_verify_roundtrip_all_messages() {
        let (sk, agent) = agent_and_key();
        let k = key(&[1, 2]);
        let child = k.extend(&[3]);
        let blocks = vec![BlockId::of(b"a"), BlockId::of(b"b")];

        for msg in [
            register_message(&agent, "tester"),
            bind_message(&agent, &k, &blocks, None),
            bind_message(&agent, &child, &blocks, Some(&k)),
            chain_message(
                &agent,
                &[
                    (k.clone(), blocks.clone(), None),
                    (child.clone(), blocks.clone(), Some(k.clone())),
                ],
            ),
        ] {
            let sig = signature_from_hex(&sign_hex(&sk, &msg)).unwrap();
            agent.verify(&msg, &sig).unwrap();
        }
    }

    #[test]
    fn domain_separation_and_tamper_rejection() {
        let (sk, agent) = agent_and_key();
        let k = key(&[7]);
        let blocks = vec![BlockId::of(b"x")];

        // A bind signature must not validate as a chain message (or vice
        // versa) even over the same fields.
        let bind_sig =
            signature_from_hex(&sign_hex(&sk, &bind_message(&agent, &k, &blocks, None))).unwrap();
        let chain_msg = chain_message(&agent, &[(k.clone(), blocks.clone(), None)]);
        assert!(agent.verify(&chain_msg, &bind_sig).is_err());

        // Changing any field invalidates: different blocks...
        let msg = bind_message(&agent, &k, &blocks, None);
        let sig = signature_from_hex(&sign_hex(&sk, &msg)).unwrap();
        let other = bind_message(&agent, &k, &[BlockId::of(b"y")], None);
        assert!(agent.verify(&other, &sig).is_err());
        // ...or a parent appearing.
        let with_parent = bind_message(&agent, &k, &blocks, Some(&key(&[8])));
        assert!(agent.verify(&with_parent, &sig).is_err());

        // A different agent's signature never validates.
        let intruder = SigningKey::from_bytes(&[43; 32]);
        let forged = signature_from_hex(&sign_hex(&intruder, &msg)).unwrap();
        assert!(agent.verify(&msg, &forged).is_err());
    }

    #[test]
    fn encoding_is_stable() {
        // The canonical bytes are a wire contract shared with non-Rust
        // SDKs; lock the layout so changes are deliberate.
        let (_, agent) = agent_and_key();
        let msg = register_message(&agent, "x");
        assert!(msg.starts_with(b"KLPK/reg/v1\0"));
        assert_eq!(msg.len(), 12 + 32 + 1);

        let k = key(&[1]);
        let b = bind_message(&agent, &k, &[BlockId::of(b"a")], None);
        // prefix + pubkey + "m\0t\0l\0" + hash(32) + count(4) + block(32) + flag(1)
        assert_eq!(b.len(), 13 + 32 + 6 + 32 + 4 + 32 + 1);
    }
}
