// QUIC transport adapter for the broker.
// Decodes felix-wire frames, enforces stream scope, and fans out events to subscribers.
/*
QUIC transport protocol overview:
- Bidirectional streams are the control plane: clients send Publish/PublishBatch, Subscribe,
  and Cache requests, and the broker returns acks/responses on the same stream.
- Subscribe on the control stream causes the broker to open a uni stream for event delivery;
  that uni stream starts with EventStreamHello and then carries Event/EventBatch frames.
- Client-initiated uni streams are publish-only (no acks); they accept Publish/PublishBatch
  and are treated as fire-and-forget ingress.
- Frames use felix-wire; high-throughput paths may carry binary batch frames to avoid JSON
  encode/decode overhead.
- Acked publishes require request_id; PublishOk/PublishError echo it and may arrive out of order.
*/
/*
DESIGN NOTES (why this file is structured this way)

High level goals
- Keep QUIC SendStream writes single-threaded: Quinn's SendStream is not safe/efficient under many
  concurrent writers (it can serialize internally and/or create heavy contention). We therefore funnel
  all outbound control-plane responses/acks through a single writer task per control stream.
- Keep broker mutation serialized per connection: a single publish worker per QUIC connection drains
  a bounded ingress queue. This avoids many tasks mutating broker state concurrently and reduces
  lock contention inside the broker.
- Make overload behavior explicit and observable: bounded queues + metrics + throttling.

Key queues
- Ingress publish queue (PUBLISH_QUEUE_DEPTH): work items sent to a per-connection publish worker.
- Outbound ack/response queue (ACK_QUEUE_DEPTH): Outgoing messages drained by the single writer.
- Ack waiter queue (ACK_WAITERS_MAX): only used when ack_on_commit is enabled; tracks acks that must
  wait until broker commit completes.

Ack modes & policies
- Wire-level ack mode is per-message/per-batch/none; server policy `ack_on_commit` optionally delays
  acks until the publish worker finishes.
- When ack_on_commit=true, the enqueue policy is Wait for acked publishes so we preserve the
  semantic that an ack implies the broker accepted work and (eventually) committed.
- When ack_on_commit=false, we can respond immediately after enqueue.

Potential issues / edge cases to be aware of
- Task lifecycle: writer + ack-waiter tasks must be joined or aborted on stream close. If one task
  finishes and the other is dropped without abort/join, it can continue running detached.
  (This is easy to accidentally introduce when using `tokio::select!` during shutdown.)
- Overload + acked publishes: if we accept a request that expects an ack, but later drop/skip the
  error ack because the outbound queue is full, clients may hang until their own timeout.
  Preferred policy is either "always respond" (critical enqueue / close on failure) or "hard close".
- Backpressure interactions: waiting to enqueue ingress (Wait policy) can propagate latency back to
  the control stream read loop; this is intentional for commit-acked publishes but must be bounded.
- Queue depth gauges are best-effort; under races they can drift. We track drift counters and reset
  local depths on teardown.
- Ordering: acks may be out-of-order relative to requests because publish jobs complete out-of-order
  (and ack waiters emit as they complete). This is allowed by protocol, but clients must treat
  request_id as the correlator.
- Cancellation: cancel signals are delivered via watch channels and are cooperative; code must check
  them in all long waits to avoid hanging tasks.
*/
pub use crate::transport::quic::*;
