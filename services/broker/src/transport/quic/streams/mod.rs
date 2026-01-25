// QUIC stream handlers for control and uni-directional publish streams.
// Split into modules so each loop (read, write, ack waiter) is easier to understand and test.

mod ack_waiter;
mod control;
mod frame_source;
mod handlers;
mod hooks;
#[cfg(test)]
mod tests;
mod uni;
mod writer;

// Public surface for the transport layer; the rest stays internal to this module tree.
pub(crate) use handlers::{handle_stream, handle_uni_stream};
