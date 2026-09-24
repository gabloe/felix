//! What a client sees of a cluster: discovering brokers, seed lists,
//! reconnecting after a broker dies, multiple listeners, and idempotent
//! producers.
//!
//! Every test here starts real broker processes; see `docs/cluster-harness.md`.
//! Run with `cargo test -p felix-cluster --test clients`, or one module with
//! `--test clients reconnect::`.

mod discovery;
mod idempotent;
mod listeners;
mod reconnect;
mod seed_endpoints;
