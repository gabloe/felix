// High-level client for talking to a Felix broker.
// Provides both an in-process wrapper and a QUIC-based network client.
//
// IMPORTANT CLIENT-SIDE DESIGN INTENT
// ----------------------------------
// This crate is intentionally *not* a general-purpose “do anything concurrently”
// QUIC client. It is a latency-oriented client with explicit serialization
// points to avoid hidden contention:
//
// - Quinn `SendStream` is effectively a single-writer resource. If multiple tasks
//   write concurrently, Quinn (or our layers) must serialize those writes via
//   internal locking, which becomes a performance cliff under load. Quinn has a mutex
//   per `SendStream` for this purpose. We want to avoid that lock contention entirely.
//
// Therefore, for correctness + predictable performance, we build explicit
// single-writer loops and communicate via bounded queues.
//
// If we want more parallelism, we need to scale by increasing:
// - number of connections (pool size), and/or
// - number of independent streams (streams-per-connection),
// not by writing concurrently to the same `SendStream`.
/*
CLIENT DESIGN NOTES (felix-client)

This crate provides two client flavors:

1) InProcessClient
   - Thin wrapper around an in-memory `felix_broker::Broker`.
   - Useful for tests, benchmarks, and embedding Felix into a single process.
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
*/
#[macro_use]
mod macros;

mod client;
mod config;
mod counters;
mod wire;

pub mod timings;

pub use client::client::Client;
pub use client::inprocess::InProcessClient;
pub use client::publisher::Publisher;
pub use client::sharding::PublishSharding;
pub use client::subscription::{Event, Subscription};
pub use config::ClientConfig;
pub use counters::{FrameCountersSnapshot, frame_counters_snapshot, reset_frame_counters};

pub(crate) use macros::{t_now_if, t_should_sample};

#[cfg(test)]
mod tests;
