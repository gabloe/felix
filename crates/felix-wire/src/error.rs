// Wire-level error type shared by the framing and codec modules.

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid magic number")]
    InvalidMagic,
    #[error("unsupported version {0}")]
    UnsupportedVersion(u16),
    #[error("frame too large")]
    FrameTooLarge,
    #[error("incomplete frame")]
    Incomplete,
    #[error("failed to serialize message")]
    Serialize(serde_json::Error),
    #[error("failed to deserialize message")]
    Deserialize(serde_json::Error),
}
