//! Wire format for framing Felix protocol messages.
//!
//! # Purpose
//! Defines the frame header layout, message enums, and JSON/binary encoders
//! used by broker and client transports to communicate over QUIC.
//!
//! # Design notes
//! The format balances readability (JSON control frames) with throughput
//! (binary batch frames) while enforcing size limits for safety.
//!
//! # Module layout
//! - [`frame`]: protocol constants, [`FrameHeader`], and [`Frame`].
//! - [`message`]: the [`Message`] enum and its JSON codec.
//! - [`text`]: hand-rolled JSON writer for the publish-batch hot path.
//! - [`binary`]: binary batch codec for publish and event batches.
//!
//! Items from `frame`, `message`, and `error` are re-exported at the crate
//! root; `text` and `binary` are addressed through their module paths.
mod base64_serde;
mod error;
mod frame;
mod message;

pub mod binary;
pub mod text;

pub use error::{Error, Result};
pub use frame::{
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_BINARY_PUBLISH_BATCH, Frame,
    FrameHeader, MAGIC, VERSION,
};
pub use message::{AckMode, Message};

#[cfg(test)]
mod tests;
