---
title: "Internals: Subscribe & Fanout"
---

Picks up where [Internals: The Publish Path](/felix/development/internals-publish/) leaves
off — a `DeliveryEnvelope` has just been cloned into a subscriber's `mpsc`
channel. This page traces what happens from there through to bytes on the
wire, plus the subscribe handshake that set that channel up in the first
place.

## The cast of types

| Type | Where | What it is |
|---|---|---|
| `SubscriptionReceiver` | `crates/felix-broker/src/lib.rs` | The broker-core side of a subscriber's channel; yields `DeliveryEnvelope`s |
| `WriterLaneManager` | `services/broker/src/transport/quic/handlers/subscribe/lane.rs` | Owns a fixed set of writer lanes and the per-connection writer tasks they feed |
| `LaneCommand` | same | `Register` / `Delivery` / `Unregister`, sent from a subscription's feeder to its assigned lane |
| `ConnectionCommand` | same | Same three variants, one hop further — sent from a lane to the connection that owns the subscriber's QUIC stream |
| `run_lane_feeder` | same | One task per subscription; reads `DeliveryEnvelope`s, encodes (once), dispatches `LaneCommand`s |
| `run_writer_lane` | same | One task per lane; receives `LaneCommand`s, forwards to the right connection |
| `run_connection_writer` | same | One task per QUIC connection; owns the actual `SendStream`s, does the writing |

Three hops, deliberately: `SubscriptionReceiver` (broker-core, per
subscriber) → lane (shared across many subscribers, bounded parallelism) →
connection writer (one per physical connection, since a connection's QUIC
streams can't be written from multiple tasks concurrently without
coordination).

## Subscribe handshake

**File**: `services/broker/src/transport/quic/handlers/subscribe.rs`,
`handle_subscribe_message`

1. Client sends `Message::Subscribe` on the control (bi) stream.
2. Broker calls `Broker::subscribe(tenant, namespace, stream)` →
   `StreamState::register_subscriber()`, which allocates a slot in a `Slab`,
   creates the `mpsc::channel::<DeliveryEnvelope>(subscriber_queue_capacity)`,
   and rebuilds the lock-free `subscribers_snapshot` (see
   [Internals: The Publish Path](/felix/development/internals-publish/#broker-core-brokerpublish_batch_to_handle)
   for why that snapshot exists).
3. Broker opens a **new unidirectional stream** (`connection.open_uni()`) —
   this one *is* uni, unlike publish — and writes `Message::EventStreamHello
   { subscription_id }` as the first frame. This is the only frame that ever
   carries the subscription id; every event frame after it doesn't need to,
   because the client already knows which stream is bound to which
   subscription. This is what makes the shared-frame optimization below
   possible — see [Wire Protocol: Shared Binary EventBatch](/felix/architecture/wire-protocol/#shared-binary-eventbatch-encoding).
4. Broker computes `lane_idx = manager.select_lane(subscription_id,
   connection_id)` and sends `LaneCommand::Register` to that lane —
   registering the subscriber's `SendStream` with the writer-lane pipeline.
   If this fails (lane queue full), the broker replies `Message::Error` and
   stops here — the client never sees `Subscribed` for a registration that
   didn't actually take.
5. Only now does the broker reply `Message::Subscribed` on the control
   stream — confirming registration succeeded, not just that the request was
   received.
6. Broker spawns `run_lane_feeder`, the task that will pull
   `DeliveryEnvelope`s out of this subscriber's `SubscriptionReceiver` for
   the rest of the subscription's life. **If `core_shards` is enabled, this
   task is spawned on the shard owning the stream** (resolved via
   `resolve_stream_handle` + `shards.handle_for(handle.id())`), not on the
   default runtime — see
   [Internals: Backpressure & Core Sharding](/felix/development/internals-concurrency/#core-sharding).

## Lane assignment: `select_lane`

**File**: `subscribe/lane.rs`, `WriterLaneManager::select_lane` /
`lane_for_subscriber` / `lane_for_connection`

Controlled by `subscriber_lane_shard`:

- `subscriber_id_hash`: `hash64(subscriber_id) % lane_count` — independent of
  connection topology.
- `connection_id_hash`: `hash64(connection_id) % lane_count` — useful when
  many subscribers share one connection and you want them on the same lane.
- `round_robin_pin`: assigned once at subscribe time, pinned for the life of
  the subscription — preserves ordering, can skew under uneven churn.
- `auto` (default): connection-aware when a connection id is known
  (equivalent to `connection_id_hash`), else falls back to subscriber id.

`subscriber_single_writer_per_conn: true` forces every subscriber on a
connection onto the same lane regardless of the shard policy — the
latency-profile default, trading lane parallelism for strict per-connection
ordering.

## `run_lane_feeder`: where encode-once actually happens

**File**: `subscribe/feeder.rs`, `run_lane_feeder`

```rust
async fn run_lane_feeder(
    mut event_rx: SubscriptionReceiver,
    manager: Arc<WriterLaneManager>,
    lane_idx: usize,
    connection_id: Option<u64>,
    config: EventWriterConfig,
) {
    loop {
        let envelope = event_rx.recv().await; // blocks until broker core sends one
        // ... coalesce with more envelopes up to max_events / max_bytes ...
        let frame = envelope.shared_event_frame()?;   // <-- the encode-once call
        enqueue_lane_frame(&manager, lane_idx, config.subscription_id, frame, ..).await;
    }
}
```

`shared_event_frame()` (on `DeliveryEnvelope`, `crates/felix-broker/src/lib.rs`)
is a lazily-populated cache: the *first* subscriber's feeder to call it pays
the real encode cost (`felix_wire::binary::encode_shared_event_batch_bytes`)
and stores the result in `Mutex<Option<Bytes>>` inside the envelope; every
other subscriber calling it on the same envelope gets a cheap `Bytes::clone`
(a refcount bump, not a copy). Since publish fanout hands the *same*
`DeliveryEnvelope` (via `Arc`) to every subscriber
(see [Internals: The Publish Path](/felix/development/internals-publish/#broker-core-brokerpublish_batch_to_handle)),
one publish batch is encoded once, total, regardless of fanout — not once
per subscriber. This is the change that took fanout cost from O(fanout) to
O(1) per publish.

Coalescing here is governed by `EventWriterConfig`: `max_events`,
`max_bytes`, `flush_delay`, and `single_event_mode` (forced when
`fanout_batch_size <= 1`, i.e. the latency profile — one event per frame,
immediate flush, no batching delay).

## `run_writer_lane` → `run_connection_writer`

**File**: `subscribe/writer.rs` (lane routing in `subscribe/lane.rs`)

A lane's job is small: receive `LaneCommand`s and forward them as
`ConnectionCommand`s to whichever connection the subscriber belongs to
(`ensure_connection_writer`/`enqueue_connection`, which lazily spawns a
`run_connection_writer` task per connection the first time it's needed).
This hop exists because a QUIC connection's streams can't be written
concurrently from independent tasks without a single owner coordinating it.

`run_connection_writer` is where the actual `send.write_all()` happens, and
it's the part of this pipeline that changed most this session — worth
understanding in detail if you're touching write scheduling.

### The old design: a round barrier

Originally, each pass through the writer loop built **one write per
subscriber with pending data**, launched them all concurrently via
`FuturesUnordered`, then **waited for every one of them to complete** before
starting the next round. That's fine when every subscriber's stream is fast,
but one backpressured or slow QUIC stream would stall the *next* round for
every other subscriber sharing that connection — a straggler problem.

### The current design: continuous pipelining

```rust
let mut in_flight: HashSet<u64> = HashSet::new();
let mut writes = FuturesUnordered::new();
loop {
    // Start a write for every subscriber that has queued data AND isn't
    // already mid-write.
    let ready: Vec<u64> = deliveries.iter()
        .filter_map(|(id, q)| (!q.is_empty() && !in_flight.contains(id)).then_some(*id))
        .collect();
    for subscriber_id in ready {
        in_flight.insert(subscriber_id);
        writes.push(async move { /* coalesce + write */ });
    }
    let Some((subscriber_id, .., write_result)) = writes.next().await else {
        break; // nothing ready, nothing in flight — this connection is drained
    };
    in_flight.remove(&subscriber_id);
    // handle result; if Ok, this subscriber becomes eligible again next loop
}
```

The difference: as soon as *any* subscriber's write completes, the loop
immediately checks whether that subscriber has more queued data and — if
so — starts its next write right away, without waiting for other in-flight
writes to finish. A slow subscriber's write can still be in flight while
three other subscribers race ahead independently. This matters most when
multiple subscribers share one physical connection (`sub_conns` small
relative to fanout in the benchmark harness, or `subscriber_single_writer_per_conn:
true` in production) — see [Benchmarks](/felix/features/benchmarks/) for the
measured effect.

## Worked example

Three subscribers (A, B, C) on the same QUIC connection, one publish batch
lands as one `DeliveryEnvelope`:

1. Broker core sends the same envelope (3 `Arc` clones) to A's, B's, and C's
   `SubscriptionReceiver`s.
2. Three independent `run_lane_feeder` tasks wake up (possibly on different
   lanes, or the same lane if `subscriber_single_writer_per_conn` is set).
   Say A's feeder runs first: it calls `envelope.shared_event_frame()`,
   pays the encode cost, gets `Bytes`. B's and C's feeders call the same
   method microseconds later and get the cached `Bytes` for free.
3. Each feeder dispatches a `LaneCommand::Delivery` carrying that (shared)
   `Bytes` — cloning `Bytes` is cheap (refcount), so no re-serialization
   happens even though three separate lane commands now exist.
4. The lane(s) forward `ConnectionCommand::Delivery` to the one connection
   writer for this connection.
5. The connection writer's loop sees three subscribers ready, starts three
   concurrent writes. If A's QUIC stream is flow-control-blocked, B's and
   C's writes still complete and — if they have more queued data — start
   their next write immediately, not waiting on A.

## If you want to change...

| You want to... | Look at |
|---|---|
| Change event batching/coalescing thresholds | `EventWriterConfig` construction in `handle_subscribe_message`; the coalescing loop in `run_lane_feeder` |
| Change lane assignment policy | `SubscriberLaneShard` in `services/broker/src/config.rs`; `WriterLaneManager::select_lane` in `subscribe/lane.rs` |
| Change subscriber backpressure policy | `SubQueuePolicy` — two separate checkpoints: `subscriber_queue_policy` (broker-core, `Broker::publish_batch_to_handle`) and `subscriber_lane_queue_policy` (lane ingress, `WriterLaneManager::enqueue`/`enqueue_connection`). See [Internals: Backpressure](/felix/development/internals-concurrency/) |
| Change write scheduling/fairness across subscribers on one connection | `run_connection_writer`'s `in_flight`/`FuturesUnordered` loop, `subscribe/writer.rs` |
| Change the wire format for event delivery | `encode_shared_event_batch_bytes`/`decode_shared_event_batch`, `crates/felix-wire/src/lib.rs`; update [Wire Protocol](/felix/architecture/wire-protocol/) too |
| Add a new lane→connection routing mode | `WriterLaneManager::ensure_connection_writer`/`enqueue_connection`, `subscribe/lane.rs` |

Next: [Internals: Backpressure & Core Sharding](/felix/development/internals-concurrency/)
ties the publish-side and subscribe-side admission/queue layers together
into the full picture, and covers the `core_shards` thread-per-core design.
