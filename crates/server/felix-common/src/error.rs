//! Why an id failed to parse.

/// Shorthand for results carrying this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// Why an id failed to parse.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid id: {0}")]
    InvalidId(String),
}

#[cfg(test)]
mod tests;
