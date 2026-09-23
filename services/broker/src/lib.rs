//! The broker process: everything around [`felix_broker::Broker`] that makes
//! it a node in a cluster.
//!
//! **The entry point is `main.rs`**, next door — `run_with_shutdown` there
//! starts the listeners and the background tasks and owns the shutdown order.
//! This crate is the subsystems it wires together, exposed as a library so the
//! integration tests in `tests/` can drive them without a process.
//!
//! `felix-broker` holds the semantics — streams, caches, queues, the publish
//! path. Nothing in that crate knows about sockets, the control plane, or
//! other brokers. Everything here is that missing half, and the modules group
//! into four jobs.
//!
//! # 1. Serving clients
//!
//! [`quic`] accepts connections and [`transport`] decodes frames and runs the
//! per-message work. [`auth`] checks the token on each action, and
//! [`shard_routing`] answers the question every publish asks first: is this
//! shard mine, should it be forwarded, or can nobody serve it right now?
//! [`cache_routing`] is the same question for a cache key, [`group_ops`] and
//! [`core_shards`] are the queue and shard operations behind the handlers, and
//! [`client_endpoints`] is what this broker tells a client about where to
//! connect.
//!
//! # 2. Belonging to a cluster
//!
//! [`membership`] registers this node and heartbeats; [`credential`] holds the
//! token it presents and refreshes it before expiry. [`controlplane`] syncs
//! the metadata catalog, [`node_catalog`] turns node ids into addresses, and
//! [`lease`] is the authority to serve at all — renewed by the same heartbeat,
//! checked cheaply on admission and against the clock again before any record
//! is committed.
//!
//! # 3. Owning shards
//!
//! [`shard_watch`] follows what the control plane assigned, by snapshot and
//! then by change feed. [`shard_lifecycle`] is what this broker has actually
//! *done* about that, which is deliberately separate state: a shard serves
//! only once its log is open, and only at the generation the control plane
//! currently names.
//!
//! # 4. Replicating
//!
//! [`replication`] ships committed records to the followers of every shard
//! this broker leads, over the broker-to-broker transport in [`peer`]. It is
//! also what tells the control plane which replicas hold the log, which is
//! what a failover and a planned move both read.
//!
//! [`config`] parses the environment, [`timings`] is the opt-in per-stage
//! latency instrumentation, and [`durable_config`] decides where logs live.
//!
//! Each module owns its own tests at `<module>/tests.rs` and its own metrics
//! at `<module>/metrics.rs`.

// --- 1. Serving clients -------------------------------------------------
pub mod auth;
pub mod cache_routing;
pub mod client_endpoints;
pub mod core_shards;
pub mod group_ops;
pub mod quic;
pub mod shard_routing;
pub mod transport;

// --- 2. Belonging to a cluster ------------------------------------------
pub mod controlplane;
pub mod credential;
pub mod lease;
pub mod membership;
pub mod node_catalog;

// --- 3. Owning shards ---------------------------------------------------
pub mod shard_lifecycle;
pub mod shard_watch;

// --- 4. Replicating -----------------------------------------------------
pub mod peer;
pub mod replication;

// --- Process-wide -------------------------------------------------------
pub mod config;
pub mod durable_config;
pub mod timings;

/// A worked example of the authorization rules, used by `demos/rbac-live`.
pub mod auth_demo;

#[cfg(test)]
// Test utilities live alongside the library for reuse in integration tests.
mod test_support;
