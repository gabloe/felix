//! The control plane: the cluster's metadata, and the decisions taken from it.
//!
//! **The entry point is `main.rs`**, next door. It builds an [`app::AppState`]
//! — a store, an auth validator, config — hands it to [`app::build_router`],
//! and starts the background loops that make decisions on a timer.
//!
//! The control plane carries no payload data. It owns *who and where*:
//! tenants, namespaces, streams, caches, the node catalog, and which broker
//! leads each shard. Brokers read it and act; it never reaches into a broker.
//!
//! # The HTTP surface
//!
//! [`api`] is the REST layer and [`app`] assembles it into a router. [`model`]
//! is what those endpoints read and write — and the place to look first, since
//! the rules about a shard assignment or a node lifecycle are stated on the
//! type rather than scattered through handlers. [`auth`] verifies Felix
//! tokens, mints them through token exchange, and publishes JWKS; [`tls`]
//! terminates the listener.
//!
//! # Storage, behind one trait
//!
//! [`store::ControlPlaneStore`] has three implementations: in memory, over
//! Postgres, and over an embedded Raft group ([`raft`], with
//! [`store::state_machine`] applying the log). All three are held to one
//! shared contract test, because a rule that holds only in memory is a rule
//! the deployed system does not have. [`migrate`] moves metadata between
//! backends.
//!
//! # The decisions, each on a timer
//!
//! - [`placement`] decides which broker leads each shard — by rendezvous
//!   hashing for a new one, by promotion when a leader is lost, and by a
//!   staged handoff when a live broker has to give one up. It is a pure
//!   function of a metadata snapshot, which is what lets two instances agree
//!   without coordinating.
//! - [`membership`] expires nodes that stopped heartbeating, since silence is
//!   the only signal that a broker is gone.
//! - [`replica_positions`] reads what leaders reported about their replicas,
//!   which is what gates a promotion and a cut-over.
//! - [`readiness`] answers `/ready`, gating traffic on the store being usable
//!   rather than on the process being up.
//!
//! Each module owns its own tests at `<module>/tests.rs`.

pub mod api;
pub mod app;
pub mod model;

pub mod auth;
pub mod tls;

pub mod raft;
pub mod store;

pub mod membership;
pub mod placement;
pub mod readiness;
pub mod replica_positions;

pub mod clock;
pub mod config;
pub mod migrate;
pub mod observability;

#[cfg(test)]
mod test_support;
