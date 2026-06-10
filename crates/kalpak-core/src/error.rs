use crate::BlockId;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid identifier: {0}")]
    InvalidId(String),

    #[error("block not found: {0}")]
    BlockNotFound(BlockId),

    #[error("block {id} failed integrity check: stored bytes do not hash to their id")]
    Corrupt { id: BlockId },

    #[error("invalid agent public key")]
    InvalidAgentKey,

    #[error("signature verification failed")]
    BadSignature,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
