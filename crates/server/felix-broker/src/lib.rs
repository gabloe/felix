//! The broker core: streams, caches and queues over one log.
//!
//! **Start at [`Broker`].** It owns every stream and cache this process
//! serves, and the publish path runs through it: [`Broker::claim_publish`]
//! takes offsets and a place in the commit order, [`Broker::complete_publish`]
//! makes the record durable and fans it out. A [`StreamHandle`] is the cached
//! lookup a hot publish path holds instead of resolving the stream again.
//!
//! This crate is the *logic*, not the process. It has no sockets and no
//! control-plane client: `services/felix-broker-service` wires those around it. That split
//! is why the semantics can be tested without a network.
//!
//! # One log, three semantics
//!
//! A stream is the log read forward, a cache is a key-to-latest-value
//! projection of it, and a queue is a durable cursor over it. They are not
//! three subsystems:
//!
//! - [`Subscription`] and [`Cursor`] read the log forward.
//! - [`CacheWatchHub`] delivers each applied write in the shard's write order.
//! - [`GroupReader`], [`ConsumerGroups`] and [`DeadLetters`] are the queue: a
//!   claim, an acknowledgement, a redelivery after a visibility timeout, and an
//!   offset given up on.
//! - [`replication`] stores what a leader shipped, on a follower.
//! - [`DurableStorage`] is the disk-backed side, for streams created
//!   `durable: true`.
//!
//! Apart from [`replication`] and [`timings`], everything public is
//! re-exported at the crate root, so downstream code writes
//! `felix_broker::<Name>`.
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
mod cache;
mod durable;
mod error;
mod keys;
mod queue;
mod registry;
mod stream;

pub mod replication;
pub mod timings;

pub use broker::{
    Broker, CacheMetadata, ClaimedPublish, ConsistencyLevel, HistoryRange, IdempotentOutcome,
    JoinOffsets, PublishOutcome, ResumedSubscription, StartPosition, StreamHandle, StreamMetadata,
};
pub use error::{BrokerError, Result};
pub use keys::{CacheKey, NamespaceKey, StreamKey, TopicKey};

// Streams.
pub use stream::{
    Cursor, DeliveryEnvelope, SubQueuePolicy, Subscription, SubscriptionGuard, SubscriptionReceiver,
};

// Caches.
pub use cache::{CacheChangeEvent, CacheWatchFilter, CacheWatchHub, CacheWatchSubscription};

// Queues.
pub use queue::{Claimed, ConsumerGroups, DeadLetters, GroupKey, GroupReader};

// Durability.
pub use durable::{DurableStorage, StreamLog};

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

#[cfg(test)]
mod tests;
