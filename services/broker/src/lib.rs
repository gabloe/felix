//! Broker service library crate.
//!
//! Exposes broker subsystems (auth, config, control-plane sync, QUIC transport)
//! for use by the broker binary and integration tests.
//!
//! The public surface here is intentionally minimal and organized by feature area.
pub mod auth;
pub mod auth_demo;
pub mod config;
pub mod controlplane;
pub mod core_shards;
pub mod durable_config;
pub mod membership;
pub mod membership_metrics;
pub mod quic;
pub mod shard_watch;
pub mod shard_watch_metrics;
pub mod timings;
pub mod transport;

#[cfg(test)]
// Test utilities live alongside the library for reuse in integration tests.
mod test_support;
