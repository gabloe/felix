//! Broker service library crate.
//!
//! # Purpose
//! Exposes broker subsystems (auth, config, control-plane sync, QUIC transport)
//! for use by the broker binary and integration tests.
//!
//! # Notes
//! The public surface here is intentionally minimal and organized by feature area.
pub mod auth;
pub mod auth_demo;
pub mod config;
pub mod controlplane;
pub mod quic;
pub mod timings;
pub mod transport;

#[cfg(test)]
// Test utilities live alongside the library for reuse in integration tests.
mod test_support;
