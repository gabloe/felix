//! Wire format for framing Felix protocol messages.
//!
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
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_BINARY_PUBLISH_ACK,
    FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH, FLAG_EVENT_BATCH_OFFSETS, Frame,
    FrameHeader, KNOWN_FLAGS, MAGIC, ORIGINAL_V1_FLAGS, VERSION, has_unknown_flags, supports,
};
pub use message::{AckMode, CursorErrorReason, Message, StartPosition};

#[cfg(test)]
mod tests;
