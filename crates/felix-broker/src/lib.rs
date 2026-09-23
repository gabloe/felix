//! The broker core: streams, caches and queues over one log.
//!
//! **Start at [`Broker`].** It owns every stream and cache this process
//! serves, and the publish path runs through it: [`Broker::claim_publish`]
//! takes offsets and a place in the commit order, [`Broker::complete_publish`]
//! makes the record durable and fans it out. A [`StreamHandle`] is the cached
//! lookup a hot publish path holds instead of resolving the stream again.
//!
//! This crate is the *logic*, not the process. It has no sockets and no
//! control-plane client: `services/broker` wires those around it. That split
//! is why the semantics can be tested without a network.
//!
//! # One log, three semantics
//!
//! A stream is the log read forward, a cache is a key-to-latest-value
//! projection of it, and a queue is a durable cursor over it. They are not
//! three subsystems:
//!
//! - [`Subscription`] and [`Cursor`] read the log forward.
//! - [`cache_watch`] delivers each applied write in the shard's write order.
//! - [`consumer_groups`], [`group_reader`], [`group_delivery`] and
//!   [`dead_letters`] are the queue: a claim, an acknowledgement, a
//!   redelivery after a visibility timeout, and an offset given up on.
//! - [`replication`] ships committed records to the followers of a shard this
//!   broker leads.
//! - [`durable`] is the disk-backed side, for streams created `durable: true`.
//!
//! Everything public is re-exported at the crate root, so downstream code
//! writes `felix_broker::<Name>` and never names an internal module.
//!
//! # Invariants worth knowing before changing anything here
//!
//! Offsets are taken *before* the durability wait, so a batch claims its place
//! in the stream's order the instant its offsets are consumed; `CommitSequencer`
//! in `felix-storage` then makes later publishes wait behind earlier ones
//! whether those succeed, fail, or are cancelled. Fanout happens *after*
//! durability. Reordering those steps is almost always a bug — see
//! `docs/architecture.md`.

// Declared first so the `t_histogram!` macro is in scope for every module below.
#[macro_use]
mod telemetry;

mod broker;
pub mod cache_watch;
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
mod producers;
mod registry;
pub mod replication;
mod stream_state;
mod subscription;

pub mod timings;

pub use broker::{
    Broker, CacheMetadata, ConsistencyLevel, HistoryRange, IdempotentOutcome, PublishOutcome,
    ResumedSubscription, StartPosition, StreamHandle, StreamMetadata,
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
