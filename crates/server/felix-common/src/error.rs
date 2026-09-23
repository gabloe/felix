//! The error type shared by the id and config helpers in this crate.

/// Shorthand for results carrying this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// Why an id failed to parse or a config value was rejected.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid id: {0}")]
    InvalidId(String),
    #[error("config error: {0}")]
    Config(String),
}

#[cfg(test)]
mod tests;
