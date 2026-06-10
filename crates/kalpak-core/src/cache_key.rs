use serde::{Deserialize, Serialize};

use crate::BlockId;

/// Identifies the exact inference configuration a KV block is valid for.
///
/// KV caches are not portable: a block produced by one model, tokenizer,
/// quantization, or tensor layout is garbage to any other. Every cached
/// prefix is therefore keyed by this fingerprint — reuse across agents is
/// only possible when fingerprints match exactly.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelFingerprint {
    /// Canonical model identifier, e.g. "meta-llama/Llama-3.1-8B-Instruct".
    pub model_id: String,
    /// Hash of the tokenizer definition (vocab + merges + special tokens).
    pub tokenizer_hash: String,
    /// KV tensor layout descriptor, e.g. "fp16/paged-16" or "q4/contiguous".
    pub kv_layout: String,
}

impl ModelFingerprint {
    pub fn new(
        model_id: impl Into<String>,
        tokenizer_hash: impl Into<String>,
        kv_layout: impl Into<String>,
    ) -> Self {
        Self {
            model_id: model_id.into(),
            tokenizer_hash: tokenizer_hash.into(),
            kv_layout: kv_layout.into(),
        }
    }
}

/// Lookup key for a cached token-prefix: which model produced it and the
/// rolling hash of the token prefix it covers.
///
/// `prefix_hash` is the BLAKE3 hash of the token-id sequence (as little-endian
/// u32s), so extending a prefix by one chunk produces a new key without
/// rehashing history — callers chain hashes per chunk.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey {
    pub fingerprint: ModelFingerprint,
    pub prefix_hash: BlockId,
}

impl CacheKey {
    /// Key for the first chunk of a context: hash the token ids directly.
    pub fn root(fingerprint: ModelFingerprint, tokens: &[u32]) -> Self {
        Self {
            fingerprint,
            prefix_hash: hash_tokens(None, tokens),
        }
    }

    /// Key for the next chunk: chain the parent's prefix hash with the new
    /// token ids, so equal prefixes converge and diverging ones split.
    pub fn extend(&self, tokens: &[u32]) -> Self {
        Self {
            fingerprint: self.fingerprint.clone(),
            prefix_hash: hash_tokens(Some(&self.prefix_hash), tokens),
        }
    }
}

fn hash_tokens(parent: Option<&BlockId>, tokens: &[u32]) -> BlockId {
    let mut hasher = blake3::Hasher::new();
    if let Some(p) = parent {
        hasher.update(p.as_bytes());
    }
    for t in tokens {
        hasher.update(&t.to_le_bytes());
    }
    BlockId(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp() -> ModelFingerprint {
        ModelFingerprint::new("test/model", "tok-abc", "fp16/paged-16")
    }

    #[test]
    fn equal_prefixes_converge() {
        let a = CacheKey::root(fp(), &[1, 2, 3]).extend(&[4, 5]);
        let b = CacheKey::root(fp(), &[1, 2, 3]).extend(&[4, 5]);
        assert_eq!(a, b);
    }

    #[test]
    fn diverging_prefixes_split() {
        let root = CacheKey::root(fp(), &[1, 2, 3]);
        assert_ne!(root.extend(&[4]), root.extend(&[5]));
    }

    #[test]
    fn chunking_is_significant() {
        // [1,2]+[3] and [1]+[2,3] are different chains by design: a cached
        // block covers a specific chunk boundary, not just the token stream.
        let a = CacheKey::root(fp(), &[1, 2]).extend(&[3]);
        let b = CacheKey::root(fp(), &[1]).extend(&[2, 3]);
        assert_ne!(a, b);
    }

    #[test]
    fn different_models_never_collide() {
        let other = ModelFingerprint::new("test/model", "tok-abc", "q4/contiguous");
        let a = CacheKey::root(fp(), &[1, 2, 3]);
        let b = CacheKey::root(other, &[1, 2, 3]);
        assert_ne!(a, b);
    }
}
