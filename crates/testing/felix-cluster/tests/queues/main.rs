//! Consumer groups across brokers, on one shard and across every shard of a
//! stream.
//!
//! Every test here starts real broker processes; see `docs/cluster-harness.md`.
//! Run with `cargo test -p felix-cluster --test queues`, or one module with
//! `--test queues consumer_groups::`.

mod consumer_groups;
mod sharded_group;
