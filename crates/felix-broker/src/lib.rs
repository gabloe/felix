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
pub mod cache_watch;
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
    /// The dead-letter list belonging to a stream shard: the offsets its
    /// groups gave up on. Beside the cursors for the same reason the cursors
    /// are beside the records — a promoted leader that serves the stream but
    /// has lost which records its groups abandoned would silently redrive
    /// nothing and list nothing.
    GroupDeadLetters,
    /// The counter log belonging to a cache shard: signed deltas folded into
    /// running sums. Rides the cache shard's replica set the way group state
    /// rides a stream shard's, so a promoted replica resumes the true sum.
    Counters,
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
pub use cache_watch::{CacheChangeEvent, CacheWatchFilter, CacheWatchHub, CacheWatchSubscription};
pub use config::SubQueuePolicy;
pub use delivery::DeliveryEnvelope;
pub use durable::{DurableStorage, StreamLog};
pub use error::{BrokerError, Result};
pub use keys::{CacheKey, NamespaceKey, StreamKey, TopicKey};
pub use stream_state::Cursor;
pub use subscription::{Subscription, SubscriptionGuard, SubscriptionReceiver};

#[cfg(test)]
mod tests;
