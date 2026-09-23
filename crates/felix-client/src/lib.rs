//! The Rust client: how an application publishes, subscribes, and reads a
//! cache.
//!
//! **Start at [`Client`].** It connects to one broker over QUIC and is the
//! root of everything else: [`Client::publisher`] for a [`Publisher`],
//! [`Client::subscribe`] for a [`Subscription`], and the cache and counter
//! calls directly on it. Beyond that:
//!
//! - [`ClusterClient`] takes several broker addresses and rebuilds its
//!   connection from the rest when one fails, so an application outlives the
//!   broker it happened to reach.
//! - [`ShardedSubscription`] follows every shard of a stream at once and
//!   reports per-shard offsets, since one number cannot describe where a
//!   sharded consumer got to.
//! - [`IdempotentProducer`] re-sends across a reconnection without
//!   duplicating.
//! - [`InProcessClient`] talks to an embedded broker with no network at all.
//!   Behind the `in-process` feature, off by default, because it pulls in
//!   `felix-broker`, which is AGPL where the rest of this crate is Apache-2.0
//!   (see LICENSING.md).
//!
//! # The rule that shapes this crate
//!
//! This is a latency-oriented client, not a general-purpose "do anything
//! concurrently" QUIC client. Quinn's `SendStream` is effectively a
//! single-writer resource: concurrent writes from several tasks are
//! serialized by a mutex inside Quinn, which is a performance cliff under
//! load. So every stream here has one writer loop, fed through a bounded
//! queue, and parallelism comes from *more* connections or *more* streams —
//! never from writing to one stream from two tasks. The notes below record
//! how that is enforced.
//!
//! The frame codec and message types are `felix-wire`; the QUIC layer is
//! `felix-transport`. This crate is the application-facing shape over both.

/*
CLIENT DESIGN NOTES (felix-client)

This crate provides two client flavors:

1) InProcessClient (requires the `in-process` Cargo feature, off by default)
   - Thin wrapper around an in-memory `felix_broker::Broker`.
   - Useful for tests, benchmarks, and embedding Felix into a single process.
   - `felix-broker` is AGPL-3.0 (unlike the rest of this crate); gated behind
     an opt-in feature so the default build stays Apache-2.0. See LICENSING.md.
   - No transport concerns (no framing, flow control, backpressure across the network).

2) Client (QUIC network client)
   - Speaks `felix-wire` over QUIC.
   - QUIC multiplexing is powerful, but the write-side of a Quinn `SendStream` is
     effectively a single-writer resource: concurrent writes from multiple tasks
     create lock contention and can introduce expensive serialization.

Key decisions in this implementation:

A) Single-writer per QUIC stream
   - Cache: each cache worker owns exactly one bi-directional stream and performs
     strictly sequential request/response round-trips on that stream.
   - Publish: `Publisher` owns one bi-directional stream and a single writer task
     serializes publishes, optionally waiting for acks.
   - Subscribe: subscribe control happens on a short-lived bi stream; events arrive
     on a server-opened uni stream, which we route based on an initial
     `EventStreamHello { subscription_id }`.

B) Connection pooling to reduce HOL and improve concurrency
   - Cache ops are latency-sensitive and can become head-of-line blocked if a slow
     cache response sits ahead of faster ones on the same stream.
   - We pool cache connections, then open multiple streams per connection, and
     assign each stream a single-writer worker.
   - Subscriptions are round-robined across event connections; each subscription
     still gets its own server-opened uni stream for events.

C) Backpressure via bounded queues
   - Cache workers use bounded channels to apply pressure back to callers.
   - Publisher uses a bounded queue so we fail fast rather than buffer unboundedly.

D) Protocol invariants we rely on
   - Any acked publish (AckMode != None) must have a request_id.
   - Acks must match request_id (server is allowed to pipeline / reorder).
   - For subscriptions, the first frame on the uni stream must be EventStreamHello.
   - Subsequent shared event batches are bound by that stream and carry no subscription_id.
*/
#[macro_use]
mod macros;

mod client;
mod config;
mod counters;
mod wire;

pub mod timings;

pub use client::cache_watch::{CacheChange, CacheWatch, CacheWatchFilter, CacheWatchItem};
pub use client::client::Client;
pub use client::client::{NotLeaderError, SubscribeCursorError};
pub use client::cluster::{ClusterClient, ReconnectPolicy};
pub use client::idempotent::IdempotentProducer;
#[cfg(feature = "in-process")]
pub use client::inprocess::InProcessClient;
pub use client::publisher::Publisher;
pub use client::sharded::{ShardEvent, ShardOffsets, ShardedSubscription};
pub use client::sharded_group::{ShardedGroup, ShardedGroupRecord};
pub use client::sharding::PublishSharding;
pub use client::subscription::{Event, Subscription};
pub use config::{ClientConfig, ClientSubQueuePolicy};
pub use counters::{
    FrameCountersSnapshot, frame_counters_snapshot, publishes_forwarded, reset_frame_counters,
};
pub use felix_wire::{CursorErrorReason, PublishRefusalReason, StartPosition};

/// The broker would not append an idempotent publish, and said why.
///
/// Carried as a typed error so a producer can act on the reason rather than
/// parse the message: a sequence gap means stop, an unknown producer means
/// start again under a new id, and neither is a transport failure to retry.
/// Recover it from an `anyhow::Error` with `downcast_ref`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRefused {
    pub reason: PublishRefusalReason,
    pub message: String,
}

impl std::fmt::Display for PublishRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "publish refused ({:?}): {}", self.reason, self.message)
    }
}

impl std::error::Error for PublishRefused {}

pub(crate) use macros::{t_now_if, t_should_sample};

#[cfg(test)]
mod tests;
