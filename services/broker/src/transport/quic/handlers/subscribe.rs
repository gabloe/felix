//! QUIC subscribe handling and event-stream writer.
//!
//! A subscribe arrives on the bi-directional control stream; events go back on a
//! fresh uni-directional stream. The `EventStreamHello` written first is what
//! binds `subscription_id -> stream`, which is why event batches need no
//! per-subscriber identifier and can be encoded once and shared across every
//! subscriber.
//!
//! Fanout uses `try_send` against bounded per-subscriber queues, so a full queue
//! drops that batch (counted by `felix_subscribe_dropped_total`) rather than
//! stalling publish. Backpressure stays local to the slow subscriber.
//!
//! Delivery is sharded into independent writer lanes to cut write-path
//! contention at high fanout. Lane assignment is deterministic so per-subscriber
//! ordering holds. Batches coalesce until whichever comes first: `max_events`,
//! `max_bytes`, or `flush_delay`.

// Submodules:
// - `config`: event-writer tunables.
// - `lane`: writer-lane data model, manager, and routing.
// - `writer`: frame-writing primitives and the writer task loops.
// - `feeder`: broker subscription queue -> writer lane.
// - `conn_counts`: per-connection active-subscriber gauge bookkeeping.
// - `event_writer`: legacy direct-to-stream writer, test-only.
//
// `WriterLaneManager` and `handle_subscribe_message` are the only names this
// module exposes to the rest of the transport.

mod config;
mod conn_counts;
#[cfg(test)]
mod event_writer;
mod feeder;
mod lane;
mod writer;

#[cfg(test)]
mod tests;

pub(crate) use lane::{LaneCommand, WriterLaneManager};

// Re-exported so the test module (and its `use super::*`) sees the internals it
// exercises directly.
use config::EventWriterConfig;
#[cfg(test)]
use conn_counts::{
    ACTIVE_SUB_CONN_COUNTS, connection_subscriber_register, connection_subscriber_unregister,
};
#[cfg(test)]
use event_writer::run_event_writer;
use feeder::run_lane_feeder;
#[cfg(test)]
use lane::ConnectionCommand;
#[cfg(test)]
use writer::{run_connection_writer, write_parts_many, write_parts_to};

use anyhow::Result;
use felix_broker::Broker;
use felix_wire::{Message, StartPosition};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use super::publish::{Outgoing, SubscriptionLimiter, send_outgoing_critical};
use crate::transport::quic::SUBSCRIPTION_ID;
use crate::transport::quic::codec::write_message;
/// Report a failed subscribe on the control stream and keep the stream alive.
///
/// Extracted because the tail-only and resume paths fail identically, and
/// duplicating the ack-queue plumbing between them is how the two drift apart.
/// Turn a broker error into the most specific protocol message available.
///
/// A cursor rejection is machine-readable so the client can choose a remedy;
/// everything else stays a generic `Error`.
fn subscribe_error_message(err: felix_broker::BrokerError) -> Message {
    match err {
        felix_broker::BrokerError::CursorTooOld { oldest, requested } => {
            Message::SubscribeCursorError {
                reason: felix_wire::CursorErrorReason::TooOld,
                requested,
                available: oldest,
            }
        }
        felix_broker::BrokerError::CursorInFuture { requested, tail } => {
            Message::SubscribeCursorError {
                reason: felix_wire::CursorErrorReason::InFuture,
                requested,
                available: tail,
            }
        }
        other => Message::Error {
            message: other.to_string(),
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn subscribe_failed(
    message: Message,
    subscriptions: &Arc<SubscriptionLimiter>,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<std::sync::atomic::AtomicUsize>,
    ack_throttle_tx: &tokio::sync::watch::Sender<bool>,
    ack_timeout_state: &Arc<tokio::sync::Mutex<super::publish::AckTimeoutState>>,
    cancel_tx: &tokio::sync::watch::Sender<bool>,
) -> Result<bool> {
    subscriptions.release();
    t_counter!("felix_subscribe_requests_total", "result" => "error").increment(1);
    super::publish::handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(message),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    Ok(true)
}

/// Write a resumed subscription's stored history and ring backlog.
///
/// Disk history is *paged*, never collected: `read_durable` returns at most
/// `max_bytes` per call and this advances by the last offset it saw, so a client
/// resuming from the start of a large stream costs the broker one page of memory
/// at a time rather than the whole history. Each page is written before the next
/// is read, so backpressure from a slow client propagates naturally into slower
/// reading rather than unbounded buffering.
#[allow(clippy::too_many_arguments)]
async fn write_replay(
    event_send: &mut quinn::SendStream,
    broker: &Arc<Broker>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    subscription_id: u64,
    history: Option<felix_broker::HistoryRange>,
    backlog: Vec<(u64, bytes::Bytes)>,
    backlog_start: u64,
    subscription: &mut felix_broker::Subscription,
    max_events: usize,
    max_bytes: usize,
    offsets_enabled: bool,
) -> Result<()> {
    /// One page of history per read. Bounds broker memory for a resume that
    /// starts arbitrarily far back.
    const HISTORY_PAGE_BYTES: usize = 1024 * 1024;

    if let Some(range) = history {
        let mut at = range.from_offset;
        while at < range.until_offset {
            let records = broker
                .read_durable(tenant_id, namespace, stream, at, HISTORY_PAGE_BYTES)
                .await?;
            if records.is_empty() {
                break;
            }
            let mut batch = ReplayBatch::new(max_events, max_bytes);
            for record in records {
                if record.offset >= range.until_offset {
                    break;
                }
                at = record.offset + 1;
                if let Some(ready) = batch.push(record.offset, record.payload.clone()) {
                    write_replay_batch(event_send, subscription_id, &ready, offsets_enabled)
                        .await?;
                }
            }
            if let Some(ready) = batch.take() {
                write_replay_batch(event_send, subscription_id, &ready, offsets_enabled).await?;
            }
        }
    }

    let mut batch = ReplayBatch::new(max_events, max_bytes);
    let mut delivered_upto = backlog_start;
    for (offset, payload) in backlog {
        delivered_upto = offset + 1;
        if let Some(ready) = batch.push(offset, payload) {
            write_replay_batch(event_send, subscription_id, &ready, offsets_enabled).await?;
        }
    }
    if let Some(ready) = batch.take() {
        write_replay_batch(event_send, subscription_id, &ready, offsets_enabled).await?;
    }

    // Catch-up. The live subscription was registered before any of this ran, so
    // publishes have been queueing on it the whole time -- into the *ordinary*
    // bounded subscriber queue, which drops under `DropNew` once it is full.
    // Relying on that queue to carry the handoff means a long replay silently
    // loses live records, so instead: drain what is queued, and wherever the
    // offsets jump, fill the hole from disk. Disk is the authority; the queue is
    // only a shortcut for the part that has not been evicted.
    //
    // Repeated because draining takes time of its own, during which more can
    // arrive. It terminates because each pass only handles what was already
    // queued, and a pass that finds nothing ends it.
    for _ in 0..MAX_CATCH_UP_PASSES {
        let ready = subscription.drain_ready();
        if ready.is_empty() {
            break;
        }
        for envelope in ready {
            if let Some(base) = envelope.base_offset() {
                if base > delivered_upto {
                    // The queue dropped records. Page the gap from disk.
                    delivered_upto = write_history_range(
                        event_send,
                        broker,
                        tenant_id,
                        namespace,
                        stream,
                        subscription_id,
                        delivered_upto,
                        base,
                        max_events,
                        max_bytes,
                        offsets_enabled,
                    )
                    .await?;
                }
                if base + envelope.len() as u64 <= delivered_upto {
                    // Entirely covered by history already written.
                    continue;
                }
            }
            let mut batch = ReplayBatch::new(max_events, max_bytes);
            for (index, payload) in envelope.payloads().iter().enumerate() {
                let offset = envelope
                    .base_offset()
                    .map(|base| base + index as u64)
                    .unwrap_or(delivered_upto);
                if offset < delivered_upto {
                    continue;
                }
                delivered_upto = offset + 1;
                if let Some(chunk) = batch.push(offset, payload.clone()) {
                    write_replay_batch(event_send, subscription_id, &chunk, offsets_enabled)
                        .await?;
                }
            }
            if let Some(chunk) = batch.take() {
                write_replay_batch(event_send, subscription_id, &chunk, offsets_enabled).await?;
            }
        }
    }
    Ok(())
}

/// Bound on catch-up passes, so a stream being published to faster than it can
/// be written cannot keep a subscribe from completing. Reaching it hands over to
/// live delivery, which is correct: offsets are on the wire, so a client can see
/// any residual gap rather than being misled about it.
const MAX_CATCH_UP_PASSES: usize = 8;

/// Write `[from, until)` from disk, returning the offset reached.
#[allow(clippy::too_many_arguments)]
async fn write_history_range(
    event_send: &mut quinn::SendStream,
    broker: &Arc<Broker>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    subscription_id: u64,
    from: u64,
    until: u64,
    max_events: usize,
    max_bytes: usize,
    offsets_enabled: bool,
) -> Result<u64> {
    const HISTORY_PAGE_BYTES: usize = 1024 * 1024;
    let mut at = from;
    while at < until {
        let records = broker
            .read_durable(tenant_id, namespace, stream, at, HISTORY_PAGE_BYTES)
            .await?;
        if records.is_empty() {
            break;
        }
        let mut batch = ReplayBatch::new(max_events, max_bytes);
        for record in records {
            if record.offset >= until {
                break;
            }
            at = record.offset + 1;
            if let Some(ready) = batch.push(record.offset, record.payload.clone()) {
                write_replay_batch(event_send, subscription_id, &ready, offsets_enabled).await?;
            }
        }
        if let Some(ready) = batch.take() {
            write_replay_batch(event_send, subscription_id, &ready, offsets_enabled).await?;
        }
    }
    Ok(at.max(from))
}

/// Accumulates replay records into frames that are safe to send.
///
/// Three things force a flush, and all three are correctness rather than taste:
///
/// * **A gap in offsets.** One `base_offset` describes a batch only if its
///   records are contiguous, so a hole must start a new frame or every offset
///   after it is wrong.
/// * **The byte budget.** Chunking by record count alone lets a backlog of
///   large payloads build a frame past the configured delivery and client frame
///   limits, which fails the write after allocating the whole thing.
/// * **The record count**, matching live delivery's batching.
struct ReplayBatch {
    payloads: Vec<bytes::Bytes>,
    base_offset: u64,
    next_offset: u64,
    bytes: usize,
    max_events: usize,
    max_bytes: usize,
}

impl ReplayBatch {
    fn new(max_events: usize, max_bytes: usize) -> Self {
        Self {
            payloads: Vec::new(),
            base_offset: 0,
            next_offset: 0,
            bytes: 0,
            max_events: max_events.max(1),
            max_bytes: max_bytes.max(1),
        }
    }

    /// Add a record, returning a finished batch when this one had to be closed.
    fn push(&mut self, offset: u64, payload: bytes::Bytes) -> Option<Vec<(u64, bytes::Bytes)>> {
        let len = payload.len();
        let breaks_run = !self.payloads.is_empty() && offset != self.next_offset;
        let over_bytes = !self.payloads.is_empty() && self.bytes + len > self.max_bytes;
        let ready = if breaks_run || over_bytes {
            self.take()
        } else {
            None
        };
        if self.payloads.is_empty() {
            self.base_offset = offset;
        }
        self.payloads.push(payload);
        self.next_offset = offset + 1;
        self.bytes += len;
        if self.payloads.len() >= self.max_events {
            // Already at the count limit, so hand it over now. A batch closed
            // here and one closed above can never both be pending.
            return ready.or_else(|| self.take());
        }
        ready
    }

    fn take(&mut self) -> Option<Vec<(u64, bytes::Bytes)>> {
        if self.payloads.is_empty() {
            return None;
        }
        let base = self.base_offset;
        let payloads = std::mem::take(&mut self.payloads);
        self.bytes = 0;
        Some(
            payloads
                .into_iter()
                .enumerate()
                .map(|(index, payload)| (base + index as u64, payload))
                .collect(),
        )
    }
}

/// Encode and write one replay batch, with or without offsets as negotiated.
async fn write_replay_batch(
    event_send: &mut quinn::SendStream,
    subscription_id: u64,
    records: &[(u64, bytes::Bytes)],
    offsets_enabled: bool,
) -> Result<()> {
    let base_offset = match records.first() {
        Some((offset, _)) => *offset,
        None => return Ok(()),
    };
    let payloads: Vec<bytes::Bytes> = records.iter().map(|(_, payload)| payload.clone()).collect();
    let payloads = payloads.as_slice();
    let frame = if offsets_enabled {
        felix_wire::binary::encode_event_batch_bytes_with_offset(
            subscription_id,
            payloads,
            base_offset,
        )?
    } else {
        felix_wire::binary::encode_event_batch_bytes(subscription_id, payloads)?
    };
    event_send.write_all(&frame).await?;
    Ok(())
}

/// Handle a subscribe request received on the bi-directional control stream.
///
/// This function is invoked from the control stream read loop when a `Message::Subscribe`
/// is decoded.
///
/// Responsibilities:
/// - Allocate/derive a `subscription_id`.
/// - Ask the broker core for a subscription receiver.
/// - Acknowledge the subscribe on the control stream (`Message::Subscribed` or `Message::Error`).
/// - Open a uni stream for event delivery, send `EventStreamHello`, and spawn the event writer.
///
/// Return value semantics:
/// - `Ok(true)` means: “handled; keep the control stream alive / continue.”
///   (This is consistent with the caller’s pattern where subscribe is a control-plane operation.)
/// - `Err(_)` bubbles up unexpected failures (e.g., encoding errors).
///
/// Error handling strategy:
/// - If we can’t subscribe or can’t open the event stream, we **reply with `Message::Error`**
///   on the control stream (via the outbound ack queue) and return `Ok(true)` so the control
///   loop can continue / cleanly terminate based on higher-level policy.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_subscribe_message(
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: crate::config::BrokerConfig,
    subscriptions: &Arc<SubscriptionLimiter>,
    lane_manager: &Arc<WriterLaneManager>,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<std::sync::atomic::AtomicUsize>,
    ack_throttle_tx: &tokio::sync::watch::Sender<bool>,
    ack_timeout_state: &Arc<tokio::sync::Mutex<super::publish::AckTimeoutState>>,
    cancel_tx: &tokio::sync::watch::Sender<bool>,
    tenant_id: String,
    namespace: String,
    stream: String,
    subscription_id: Option<u64>,
    start: Option<StartPosition>,
    peer_flags: u16,
) -> Result<bool> {
    // Offsets ride the event batch only for a client that negotiated the bit.
    // One that did not gets exactly the frames it got before this existed.
    let offsets_enabled = felix_wire::supports(peer_flags, felix_wire::FLAG_EVENT_BATCH_OFFSETS);
    // Subscribe is a control-plane request: acknowledgements/metadata stay on this bi stream.
    // Actual event delivery happens on a fresh uni stream (broker -> client).
    let span = tracing::trace_span!(
        "subscribe",
        tenant_id = %tenant_id,
        namespace = %namespace,
        stream = %stream
    );
    let _enter = span.enter();

    // Client may provide an explicit subscription_id (useful for idempotency/testing).
    // If absent, we allocate one from a global atomic counter.
    let subscription_id = subscription_id
        .unwrap_or_else(|| SUBSCRIPTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

    // Enforce the per-connection subscription cap before asking the broker core for a
    // subscriber queue — no point allocating one just to reject it immediately after.
    if !subscriptions.try_reserve(config.max_subscriptions_per_conn) {
        t_counter!("felix_subscribe_requests_total", "result" => "error").increment(1);
        t_counter!("felix_broker_subscribe_conn_limit_rejected_total").increment(1);
        super::publish::handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::Error {
                    message: "max subscriptions per connection exceeded".to_string(),
                }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(true);
    }

    // Ask broker core for a managed subscriber queue.
    //
    // Without a start position this is the tail-only path every client used
    // before resume existed, and it stays byte-for-byte what it was. With one,
    // the broker registers the live subscription first and hands back the
    // history needed to reach it -- see `Broker::subscribe_from`.
    // On failure, respond on the control stream (through the ack queue) and keep the stream alive.
    let mut replay = None;
    let mut subscription = match start {
        None => match broker.subscribe(&tenant_id, &namespace, &stream).await {
            Ok(subscription) => subscription,
            Err(err) => {
                return subscribe_failed(
                    subscribe_error_message(err),
                    subscriptions,
                    out_ack_tx,
                    out_ack_depth,
                    ack_throttle_tx,
                    ack_timeout_state,
                    cancel_tx,
                )
                .await;
            }
        },
        Some(start) => match broker
            .subscribe_from(&tenant_id, &namespace, &stream, start)
            .await
        {
            Ok(resumed) => {
                replay = Some((resumed.history, resumed.backlog, resumed.backlog_start));
                resumed.subscription
            }
            Err(err) => {
                return subscribe_failed(
                    subscribe_error_message(err),
                    subscriptions,
                    out_ack_tx,
                    out_ack_depth,
                    ack_throttle_tx,
                    ack_timeout_state,
                    cancel_tx,
                )
                .await;
            }
        },
    };

    // Open a uni stream for event delivery. If this fails, respond with error on control stream.
    if matches!(
        config.sub_stream_mode,
        crate::config::SubStreamMode::HashedPool
    ) {
        t_counter!("broker_sub_stream_mode_fallback_total", "mode" => "hashed_pool").increment(1);
        tracing::debug!(
            requested_streams_per_conn = config.sub_streams_per_conn,
            "hashed_pool stream mode not enabled yet; using per_subscriber stream mode"
        );
    }
    let mut event_send = match connection.open_uni().await {
        Ok(send) => send,
        Err(err) => {
            subscriptions.release();
            t_counter!("felix_subscribe_requests_total", "result" => "error").increment(1);
            super::publish::handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::Error {
                        message: err.to_string(),
                    }),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            return Ok(true);
        }
    };

    // First write a hello on the uni stream so the client can bind:
    //   subscription_id -> this uni stream
    // before any events arrive. If this fails, we treat it as the subscriber being gone.
    if let Err(err) = write_message(
        &mut event_send,
        Message::EventStreamHello { subscription_id },
    )
    .await
    {
        subscriptions.release();
        tracing::info!(error = %err, "subscription event stream closed");
        return Ok(true);
    }

    // Acknowledge *before* streaming any history.
    //
    // The client waits for `Subscribed` before it registers the event stream
    // with its router and starts reading it. Writing replay first therefore
    // deadlocks as soon as the history exceeds the QUIC per-stream receive
    // window (64 MiB by default): the broker blocks for flow-control credit
    // that the client will not grant until it reads, and the client will not
    // read until it sees the acknowledgement the broker cannot send. Small
    // replays fit in the window and hide it, which is what makes it a
    // production bug rather than a test failure.
    t_counter!("felix_subscribe_requests_total", "result" => "ok").increment(1);
    super::publish::handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::Subscribed { subscription_id }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;

    // Replay goes out here, on the raw uni stream, before the lane manager is
    // registered for live delivery. That ordering is the delivery half of the
    // seam: the broker already registered the live subscription, so events are
    // queueing in `subscription` while these bytes are written, and they cannot
    // overtake replay because nothing drains that queue until registration
    // below. History, then backlog, then live -- contiguous.
    if let Some((history, backlog, backlog_start)) = replay
        && let Err(err) = write_replay(
            &mut event_send,
            &broker,
            &tenant_id,
            &namespace,
            &stream,
            subscription_id,
            history,
            backlog,
            backlog_start,
            &mut subscription,
            config.event_batch_max_events.max(1),
            config.event_batch_max_bytes.max(1),
            offsets_enabled,
        )
        .await
    {
        subscriptions.release();
        tracing::info!(error = %err, "subscription replay failed");
        return Ok(true);
    }

    // Batching configuration.
    // Note: `batch_size` is the “fanout batch size” (publisher -> broker internal),
    // but for subscriber delivery we compute and enforce independent limits.
    let max_events = config
        .event_batch_max_events
        .min(config.fanout_batch_size.max(1));
    let max_bytes = config.event_batch_max_bytes.max(1);
    let flush_delay = Duration::from_micros(config.event_batch_max_delay_us);

    let writer_config = EventWriterConfig {
        subscription_id,
        max_events,
        max_bytes,
        flush_delay,
        single_event_mode: config.fanout_batch_size <= 1,
        offsets_enabled,
        flush_max_items: config.subscriber_flush_max_items.max(1),
        flush_max_delay: Duration::from_micros(config.subscriber_flush_max_delay_us.max(1)),
        max_bytes_per_write: config.subscriber_max_bytes_per_write.max(1),
    };
    let connection_id = connection.info().id.0;
    let manager = Arc::clone(lane_manager);
    let lane_idx = manager.select_lane(subscription_id, Some(connection_id));
    let (event_rx, unsubscribe_guard) = subscription.into_parts();
    if manager
        .enqueue(
            lane_idx,
            LaneCommand::Register {
                subscriber_id: subscription_id,
                connection,
                connection_id: Some(connection_id),
                event_send,
                guard: unsubscribe_guard,
            },
        )
        .await
        .is_err()
    {
        metrics::counter!("felix_subscriber_lane_dropped_total").increment(1);
        manager.unregister_subscriber(subscription_id, Some(connection_id));
        subscriptions.release();
        tracing::warn!(
            lane = lane_idx,
            subscription_id,
            "subscriber lane queue full during register"
        );
        t_counter!("felix_subscribe_requests_total", "result" => "error").increment(1);
        super::publish::handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::Error {
                    message: "subscriber lane queue full during register".to_string(),
                }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(true);
    }

    let feeder_subscriptions = Arc::clone(subscriptions);
    let feeder_manager = Arc::downgrade(&manager);
    let feeder = async move {
        run_lane_feeder(
            event_rx,
            feeder_manager,
            lane_idx,
            Some(connection_id),
            writer_config,
            feeder_subscriptions,
        )
        .await;
    };
    // With core shards enabled, run this subscription's feeder on the shard
    // that owns its stream: the fanout enqueue (publish worker) and this
    // dequeue then happen on the same core, so the subscriber queue becomes a
    // core-local handoff instead of a cross-core wakeup. Mapping must match
    // `publish_worker_index` (handle id % shard count).
    let shard_runtime = match crate::core_shards::global_shards(&config) {
        Some(shards) => match broker
            .resolve_stream_handle(&tenant_id, &namespace, &stream)
            .await
        {
            Ok(handle) => Some(shards.handle_for(handle.id()).clone()),
            Err(err) => {
                tracing::warn!(error = %err, "stream handle unavailable; feeder on main runtime");
                None
            }
        },
        None => None,
    };
    match shard_runtime {
        Some(runtime) => {
            runtime.spawn(feeder);
        }
        None => {
            tokio::spawn(feeder);
        }
    }
    Ok(true)
}
