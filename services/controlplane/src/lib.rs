//! Control-plane service library crate.
//!
//! Exposes the control-plane API surface, auth helpers, configuration, and
//! storage implementations for use by the binary and tests.
//!
//! Module boundaries mirror the HTTP API and storage backends for clarity.
pub mod api;
pub mod app;
pub mod auth;
pub mod config;
pub mod membership;
pub mod membership_metrics;
pub mod migrate;
pub mod model;
pub mod observability;
pub mod placement;
pub mod raft;
pub mod readiness;
pub mod replica_positions;
pub mod store;
#[cfg(test)]
mod test_support;
pub mod tls;
