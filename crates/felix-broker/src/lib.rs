// In-process pub/sub broker with a tiny cache hook.
// The broker enforces tenant/namespace/stream existence via local registries
// that are kept in sync by the control plane watcher.
//
// Module layout:
// - `telemetry`: cfg-gated sampling shims and the `t_histogram!` macro.
// - `error` / `config`: shared error type, capacity defaults, queue policy.
// - `keys`: map keys plus their borrowed lookup twins.
// - `delivery`: shared delivery batches and queue-depth accounting.
// - `commit_order`: one authoritative publish order per durable stream.
// - `stream_state`: per-stream subscriber registry, snapshot, and replay log.
// - `durable`: disk-backed logs for streams registered with `durable: true`.
// - `subscription`: subscriber-facing receive handles.
// - `broker` / `registry`: the `Broker` aggregate and its two impl blocks.
//
// Everything public is re-exported at the crate root; downstream crates and the
// docs site address these types as `felix_broker::<Name>`.

// Declared first so the `t_histogram!` macro is in scope for every module below.
#[macro_use]
mod telemetry;

mod broker;
mod commit_order;
mod config;
pub mod consumer_groups;

/// Which of a shard's logs a request is about.
///
/// A stream shard has two: the records themselves, and the consumer-group
/// cursors kept beside them. Both have to reach a replica, or a promoted leader
/// serves the records and has no idea where any group had got to — it starts
/// them at the beginning and redelivers everything already finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogKind {
    /// A stream's records.
    Stream,
    /// A cache's records.
    Cache,
    /// The consumer-group cursors belonging to a stream shard.
    GroupCursors,
}
pub mod dead_letters;
mod delivery;
pub mod durable;
mod error;
pub mod group_delivery;
pub mod group_reader;
mod keys;
mod registry;
pub mod replication;
mod stream_state;
mod subscription;

pub mod timings;

pub use broker::{
    Broker, CacheMetadata, ConsistencyLevel, HistoryRange, PublishOutcome, ResumedSubscription,
    StartPosition, StreamHandle, StreamMetadata,
};
pub use config::SubQueuePolicy;
pub use delivery::DeliveryEnvelope;
pub use durable::{DurableStorage, StreamLog};
pub use error::{BrokerError, Result};
pub use keys::{CacheKey, NamespaceKey, StreamKey, TopicKey};
pub use stream_state::Cursor;
pub use subscription::{Subscription, SubscriptionGuard, SubscriptionReceiver};

#[cfg(test)]
mod tests;
