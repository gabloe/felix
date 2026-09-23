//! Serving clients.
//!
//! [`quic`] accepts connections, decodes frames and runs the per-message work.
//! [`auth`] checks the token on each action. `cache_routing` decides which
//! broker answers for a cache key, `group_ops` is the consumer-group work
//! behind the queue handlers, and [`core_shards`] pins stream work to cores.

pub mod auth;
pub(crate) mod cache_routing;
pub mod core_shards;
pub(crate) mod group_ops;
pub mod quic;
