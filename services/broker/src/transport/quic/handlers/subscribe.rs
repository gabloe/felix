//! QUIC subscribe handling and event-stream writer.
//!
//! ## High-level flow
//! Subscriptions are requested on the **bi-directional control stream** (the same stream used
//! for publish/control-plane operations). Once accepted, the broker opens a **new uni-directional
//! stream** (broker → client) dedicated to delivering events for that subscription.
//!
//! This file is responsible for:
//! - Handling the `Message::Subscribe` request on the control stream.
//! - Sending `Message::Subscribed` back on the control stream as the acknowledgement.
//! - Opening a uni stream and sending a `Message::EventStreamHello` so the client can bind
//!   `subscription_id -> stream`.
//! - Handing the broker-provided subscriber queue directly to the writer task.
//! - Running lane-sharded event writers that preserve per-subscriber ordering.
//!
//! ## Buffering and drops
//! The broker core owns bounded per-subscriber queues and uses `try_send` for fanout:
//! - If a subscriber queue is full, that event is **dropped** and
//!   `felix_subscribe_dropped_total` is incremented in the broker.
//! - This keeps backpressure localized to the subscriber rather than stalling publish.
//!
//! ## Writer lanes and routing
//! Subscriber delivery is sharded into independent writer lanes to reduce write-path contention
//! at high fanout and large payload sizes.
//! - Each lane owns its own bounded command queue and write task.
//! - Lane assignment is deterministic (`auto`, `subscriber_id_hash`, `connection_id_hash`,
//!   `round_robin_pin`) so subscriber ordering remains stable.
//! - `auto` prefers connection-aware routing when a connection id is available, otherwise it
//!   falls back to subscriber-id hashing.
//!
//! ## Encoding / batching
//! Events are framed as binary `EventBatch` using `felix-wire` binary framing.
//!
//! In batch mode, we coalesce by whichever triggers first:
//! - max events (`max_events`)
//! - max bytes (`max_bytes`)
//! - deadline (`flush_delay`)
//!
//! ## Telemetry
//! Telemetry is compiled out unless `--features telemetry` is enabled. When enabled, we record:
//! - queue wait latency
//! - write latency
//! - end-to-end delivery latency (enqueue → write completion)
//! - frame/batch/item counters

// Subscribe path logic and event writer for subscription streams.
use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use felix_broker::Broker;
use felix_wire::Message;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
#[cfg(test)]
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::timings;

use super::publish::{Outgoing, send_outgoing_critical};
use crate::transport::quic::SUBSCRIPTION_ID;
use crate::transport::quic::codec::write_message;
use crate::transport::quic::telemetry::{t_now_if, t_should_sample};

static WRITER_LANE_MANAGER: OnceLock<Arc<WriterLaneManager>> = OnceLock::new();
#[cfg(all(feature = "telemetry", debug_assertions))]
static CONNECTION_LANE_ASSERT: OnceLock<DashMap<u64, usize>> = OnceLock::new();

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
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<std::sync::atomic::AtomicUsize>,
    ack_throttle_tx: &tokio::sync::watch::Sender<bool>,
    ack_timeout_state: &Arc<tokio::sync::Mutex<super::publish::AckTimeoutState>>,
    cancel_tx: &tokio::sync::watch::Sender<bool>,
    tenant_id: String,
    namespace: String,
    stream: String,
    subscription_id: Option<u64>,
) -> Result<bool> {
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

    // Ask broker core for a managed subscriber queue.
    // On failure, respond on the control stream (through the ack queue) and keep the stream alive.
    let subscription = match broker.subscribe(&tenant_id, &namespace, &stream).await {
        Ok(subscription) => subscription,
        Err(err) => {
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

    // Open a uni stream for event delivery. If this fails, respond with error on control stream.
    let mut event_send = match connection.open_uni().await {
        Ok(send) => send,
        Err(err) => {
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

    // Control-plane acknowledgement: subscriber is registered.
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

    // First write a hello on the uni stream so the client can bind:
    //   subscription_id -> this uni stream
    // before any events arrive. If this fails, we treat it as the subscriber being gone.
    if let Err(err) = write_message(
        &mut event_send,
        Message::EventStreamHello { subscription_id },
    )
    .await
    {
        tracing::info!(error = %err, "subscription event stream closed");
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
    };
    let manager = WriterLaneManager::init(&config);
    let connection_id = connection.info().id.0;
    let lane_idx = manager.select_lane(subscription_id, Some(connection_id));
    let (event_rx, unsubscribe_guard) = subscription.into_parts();
    if manager
        .try_send(
            lane_idx,
            LaneCommand::Register {
                subscriber_id: subscription_id,
                connection_id: Some(connection_id),
                event_send,
                guard: unsubscribe_guard,
            },
        )
        .is_err()
    {
        metrics::counter!("felix_subscriber_lane_dropped_total").increment(1);
        manager.unregister_subscriber(subscription_id, Some(connection_id));
        tracing::warn!(
            lane = lane_idx,
            subscription_id,
            "subscriber lane queue full during register"
        );
        return Ok(true);
    }
    tokio::spawn(async move {
        run_lane_feeder(
            event_rx,
            manager,
            lane_idx,
            Some(connection_id),
            writer_config,
        )
        .await;
    });
    Ok(true)
}

/// Configuration for the subscription event writer.
///
/// Fields are chosen to make the event writer a pure “I/O + framing” component:
/// it doesn’t need the broker, only identifiers and batching policy.
///
/// Batching behavior:
/// Writer always coalesces into a single binary EventBatch per flush.
/// Flush triggers:
/// - `max_events`
/// - `max_bytes`
/// - `flush_delay`
#[derive(Clone, Copy, Debug)]
pub(crate) struct EventWriterConfig {
    /// Stable identifier for this subscription; used by the client to route events.
    subscription_id: u64,

    /// Max number of events per flush in batch mode.
    max_events: usize,

    /// Max total payload bytes per flush in batch mode.
    max_bytes: usize,

    /// Deadline for flushing a partially-filled batch.
    flush_delay: Duration,

    /// If true, encode each payload as its own one-item EventBatch frame.
    single_event_mode: bool,
}

#[cfg(test)]
async fn write_parts_to<W>(
    writer: &mut W,
    parts: &felix_wire::binary::EncodedEventBatchParts,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    for segment in parts.segments() {
        writer
            .write_all(segment.as_ref())
            .await
            .context("write subscription frame segment")?;
    }
    Ok(())
}

async fn write_parts(
    send: &mut quinn::SendStream,
    parts: felix_wire::binary::EncodedEventBatchParts,
) -> Result<()> {
    // Perf note (measured with `latency-demo` + telemetry, 4096B payload, fanout=10):
    // publish-side fanout/enqueue is small, while subscriber write-await dominates the
    // broker hot path. Use a single `write_all_chunks` call per flush to cut await points.
    let mut segments = parts.into_segments();
    send.write_all_chunks(segments.as_mut_slice())
        .await
        .context("write subscription frame chunks")?;
    Ok(())
}

#[derive(Debug)]
struct LaneSubscriber {
    event_send: quinn::SendStream,
    _connection_id: Option<u64>,
    _unsubscribe_guard: felix_broker::SubscriptionGuard,
}

#[derive(Debug)]
enum LaneCommand {
    Register {
        subscriber_id: u64,
        connection_id: Option<u64>,
        event_send: quinn::SendStream,
        guard: felix_broker::SubscriptionGuard,
    },
    Delivery {
        subscriber_id: u64,
        frame_parts: felix_wire::binary::EncodedEventBatchParts,
        enqueue_at: Instant,
    },
    Unregister {
        subscriber_id: u64,
    },
}

#[derive(Debug)]
struct WriterLaneManager {
    lanes: Vec<mpsc::Sender<LaneCommand>>,
    lane_queue_capacity: usize,
    lane_queue_highwater: Vec<AtomicUsize>,
    connection_lanes: DashMap<u64, usize>,
    subscriber_pins: DashMap<u64, usize>,
    shard: crate::config::SubscriberLaneShard,
    rr_counter: AtomicUsize,
}

impl WriterLaneManager {
    fn init(config: &crate::config::BrokerConfig) -> Arc<Self> {
        WRITER_LANE_MANAGER
            .get_or_init(|| Arc::new(Self::new(config)))
            .clone()
    }

    fn new(config: &crate::config::BrokerConfig) -> Self {
        let requested_lanes = config.subscriber_writer_lanes.max(1);
        let max_lanes = config.max_subscriber_writer_lanes.max(1);
        let lane_count = requested_lanes.min(max_lanes);
        if requested_lanes > max_lanes {
            tracing::warn!(
                requested_lanes,
                max_lanes,
                "subscriber_writer_lanes exceeds max_subscriber_writer_lanes; clamping"
            );
        }
        let queue_depth = config.subscriber_lane_queue_depth.max(1);
        let mut lanes = Vec::with_capacity(lane_count);
        for lane_id in 0..lane_count {
            let (tx, rx) = mpsc::channel(queue_depth);
            tokio::spawn(run_writer_lane(lane_id, rx));
            lanes.push(tx);
        }
        let mut lane_queue_highwater = Vec::with_capacity(lane_count);
        for _ in 0..lane_count {
            lane_queue_highwater.push(AtomicUsize::new(0));
        }
        Self {
            lanes,
            lane_queue_capacity: queue_depth,
            lane_queue_highwater,
            connection_lanes: DashMap::new(),
            subscriber_pins: DashMap::new(),
            shard: config.subscriber_lane_shard,
            rr_counter: AtomicUsize::new(0),
        }
    }

    fn lane_for_subscriber(&self, subscriber_id: u64) -> usize {
        let lanes = self.lanes.len().max(1);
        hash64(subscriber_id) as usize % lanes
    }

    fn lane_for_connection(&self, connection_id: u64) -> usize {
        let lanes = self.lanes.len().max(1);
        *self
            .connection_lanes
            .entry(connection_id)
            .or_insert_with(|| hash64(connection_id) as usize % lanes)
    }

    fn select_lane(&self, subscriber_id: u64, connection_id: Option<u64>) -> usize {
        // Design intent:
        // - `Auto` defaults to connection ownership to avoid cross-lane contention on shared QUIC
        //   send state when multiple subscribers share one connection.
        // - `SubscriberIdHash` maximizes distribution independent of connection topology.
        // - `RoundRobinPin` assigns once at subscribe time (not per message) so ordering is stable.
        match self.shard {
            crate::config::SubscriberLaneShard::Auto => match connection_id {
                Some(connection_id) => self.lane_for_connection(connection_id),
                None => self.lane_for_subscriber(subscriber_id),
            },
            crate::config::SubscriberLaneShard::SubscriberIdHash => {
                self.lane_for_subscriber(subscriber_id)
            }
            crate::config::SubscriberLaneShard::ConnectionIdHash => match connection_id {
                Some(connection_id) => self.lane_for_connection(connection_id),
                None => self.lane_for_subscriber(subscriber_id),
            },
            crate::config::SubscriberLaneShard::RoundRobinPin => *self
                .subscriber_pins
                .entry(subscriber_id)
                .or_insert_with(|| {
                    self.rr_counter.fetch_add(1, Ordering::Relaxed) % self.lanes.len().max(1)
                }),
        }
    }

    fn unregister_subscriber(&self, subscriber_id: u64, connection_id: Option<u64>) {
        self.subscriber_pins.remove(&subscriber_id);
        if let Some(connection_id) = connection_id {
            self.connection_lanes.remove(&connection_id);
        }
    }

    fn update_lane_queue_highwater(&self, lane_idx: usize) {
        let sender = &self.lanes[lane_idx];
        let queue_len = self.lane_queue_capacity.saturating_sub(sender.capacity());
        loop {
            let current = self.lane_queue_highwater[lane_idx].load(Ordering::Relaxed);
            if queue_len <= current {
                break;
            }
            if self.lane_queue_highwater[lane_idx]
                .compare_exchange(current, queue_len, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                t_counter!(
                    "broker_sub_lane_queue_len_highwater",
                    "lane" => lane_idx.to_string()
                )
                .increment((queue_len - current) as u64);
                break;
            }
        }
    }

    fn try_send(&self, lane_idx: usize, cmd: LaneCommand) -> std::result::Result<(), LaneCommand> {
        match self.lanes[lane_idx].try_send(cmd) {
            Ok(()) => {
                t_counter!("broker_sub_lane_enqueued_total", "lane" => lane_idx.to_string())
                    .increment(1);
                self.update_lane_queue_highwater(lane_idx);
                Ok(())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(cmd))
            | Err(tokio::sync::mpsc::error::TrySendError::Closed(cmd)) => {
                t_counter!("broker_sub_lane_dropped_total", "lane" => lane_idx.to_string())
                    .increment(1);
                Err(cmd)
            }
        }
    }
}

async fn run_writer_lane(lane_id: usize, mut rx: mpsc::Receiver<LaneCommand>) {
    let mut subscribers: HashMap<u64, LaneSubscriber> = HashMap::new();
    while let Some(cmd) = rx.recv().await {
        match cmd {
            LaneCommand::Register {
                subscriber_id,
                connection_id,
                event_send,
                guard,
            } => {
                subscribers.insert(
                    subscriber_id,
                    LaneSubscriber {
                        event_send,
                        _connection_id: connection_id,
                        _unsubscribe_guard: guard,
                    },
                );
            }
            LaneCommand::Unregister { subscriber_id } => {
                subscribers.remove(&subscriber_id);
            }
            LaneCommand::Delivery {
                subscriber_id,
                frame_parts,
                enqueue_at,
            } => {
                let Some(subscriber) = subscribers.get_mut(&subscriber_id) else {
                    continue;
                };
                #[cfg(all(feature = "telemetry", debug_assertions))]
                if let Some(connection_id) = subscriber._connection_id {
                    debug_assert_connection_lane(connection_id, lane_id);
                }
                let sample = t_should_sample();
                let queue_wait_ns = enqueue_at.elapsed().as_nanos() as u64;
                timings::record_sub_queue_wait_ns(queue_wait_ns);
                t_histogram!("broker_sub_lane_queue_wait_ns", "lane" => lane_id.to_string())
                    .record(queue_wait_ns as f64);

                let write_start = t_now_if(sample);
                t_counter!("broker_sub_lane_write_calls_total", "lane" => lane_id.to_string())
                    .increment(1);
                match write_parts(&mut subscriber.event_send, frame_parts).await {
                    Ok(_) => {
                        if let Some(start) = write_start {
                            let write_ns = start.elapsed().as_nanos() as u64;
                            timings::record_sub_write_ns(write_ns);
                            timings::record_sub_write_await_ns(write_ns);
                            timings::record_quic_write_ns(write_ns);
                            t_histogram!("broker_sub_lane_write_ns", "lane" => lane_id.to_string())
                                .record(write_ns as f64);
                            t_histogram!(
                                "broker_sub_lane_write_await_ns",
                                "lane" => lane_id.to_string()
                            )
                            .record(write_ns as f64);
                        }
                    }
                    Err(err) => {
                        t_counter!(
                            "broker_sub_lane_write_errors_total",
                            "lane" => lane_id.to_string()
                        )
                        .increment(1);
                        metrics::counter!("felix_subscriber_disconnect_total").increment(1);
                        tracing::info!(
                            lane = lane_id,
                            subscriber_id,
                            error = %err,
                            "lane writer subscriber stream closed"
                        );
                        subscribers.remove(&subscriber_id);
                    }
                }
            }
        }
    }
}

#[cfg(all(feature = "telemetry", debug_assertions))]
fn debug_assert_connection_lane(connection_id: u64, lane_id: usize) {
    let map = CONNECTION_LANE_ASSERT.get_or_init(DashMap::new);
    let mut entry = map.entry(connection_id).or_insert(lane_id);
    debug_assert_eq!(
        *entry, lane_id,
        "connection_id {} routed to multiple lanes: {} and {}",
        connection_id, *entry, lane_id
    );
    *entry = lane_id;
}

fn hash64(value: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

async fn run_lane_feeder(
    mut event_rx: mpsc::Receiver<Bytes>,
    manager: Arc<WriterLaneManager>,
    lane_idx: usize,
    connection_id: Option<u64>,
    config: EventWriterConfig,
) {
    let max_events = config.max_events.max(1);
    let max_bytes = config.max_bytes.max(1);
    let mut pending: Option<Bytes> = None;

    loop {
        let first = match pending.take() {
            Some(payload) => payload,
            None => match event_rx.recv().await {
                Some(payload) => payload,
                None => break,
            },
        };

        let mut batch = Vec::with_capacity(max_events);
        let mut batch_bytes = first.len();
        batch.push(first);

        while batch.len() < max_events && batch_bytes < max_bytes {
            let next = if config.single_event_mode {
                None
            } else {
                match tokio::time::timeout(config.flush_delay, event_rx.recv()).await {
                    Ok(Some(payload)) => Some(payload),
                    Ok(None) | Err(_) => None,
                }
            };
            let Some(payload) = next else {
                break;
            };
            if batch_bytes.saturating_add(payload.len()) > max_bytes {
                pending = Some(payload);
                break;
            }
            batch_bytes += payload.len();
            batch.push(payload);
        }

        let sample = t_should_sample();
        let enqueue_start = t_now_if(sample);
        let prefix_start = t_now_if(sample);
        let parts =
            match felix_wire::binary::encode_event_batch_parts(config.subscription_id, &batch) {
                Ok(parts) => parts,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        subscriber_id = config.subscription_id,
                        "encode lane delivery failed"
                    );
                    continue;
                }
            };
        if let Some(start) = prefix_start {
            let prefix_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_prefix_ns(prefix_ns);
            t_histogram!("felix_broker_sub_prefix_build_ns").record(prefix_ns as f64);
        }

        let cmd = LaneCommand::Delivery {
            subscriber_id: config.subscription_id,
            frame_parts: parts,
            enqueue_at: Instant::now(),
        };
        if manager.try_send(lane_idx, cmd).is_err() {
            metrics::counter!("felix_subscriber_lane_dropped_total").increment(1);
        } else if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            t_histogram!("broker_sub_lane_enqueue_ns", "lane" => lane_idx.to_string())
                .record(enqueue_ns as f64);
        }
    }
    let _ = manager.try_send(
        lane_idx,
        LaneCommand::Unregister {
            subscriber_id: config.subscription_id,
        },
    );
    manager.unregister_subscriber(config.subscription_id, connection_id);
}

/// Drain subscriber events from an `mpsc` queue and write them onto a uni QUIC stream.
///
/// Internal detail: `pending`
/// In batch mode, we may read one event that would exceed `max_bytes` if appended. We keep that
/// event in `pending` so it becomes the first element of the next batch (instead of dropping it).
#[allow(unused_assignments)]
#[cfg(test)]
pub(crate) async fn run_event_writer(
    mut event_send: quinn::SendStream,
    mut rx: mpsc::Receiver<bytes::Bytes>,
    config: EventWriterConfig,
) -> Result<()> {
    // `pending` holds the first event of the next batch when the current batch is byte-limited.
    let mut pending: Option<bytes::Bytes> = None;
    let max_events = config.max_events.max(1);
    let max_bytes = config.max_bytes.max(1);

    if config.single_event_mode {
        loop {
            let sample = t_should_sample();
            let queue_start = t_now_if(sample);

            let first_payload = match rx.recv().await {
                Some(payload) => payload,
                None => break,
            };
            if let Some(start) = queue_start {
                let queue_ns = start.elapsed().as_nanos() as u64;
                timings::record_sub_queue_wait_ns(queue_ns);
                t_histogram!("felix_broker_sub_recv_wait_ns").record(queue_ns as f64);
            }

            let mut payloads = Vec::with_capacity(max_events);
            payloads.push(first_payload);
            while payloads.len() < max_events {
                match rx.try_recv() {
                    Ok(payload) => payloads.push(payload),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            let write_start = t_now_if(sample);
            #[cfg(not(feature = "telemetry"))]
            let _ = write_start;

            let mut payload_bytes = 0usize;
            let mut frames_written = 0u64;
            let mut bytes_written = 0u64;
            for payload in &payloads {
                payload_bytes += payload.len();
                let prefix_start = t_now_if(sample);
                let parts = felix_wire::binary::encode_event_batch_parts(
                    config.subscription_id,
                    std::slice::from_ref(payload),
                )
                .context("encode binary event batch parts")?;
                if let Some(start) = prefix_start {
                    let prefix_ns = start.elapsed().as_nanos() as u64;
                    timings::record_sub_prefix_ns(prefix_ns);
                    t_histogram!(
                        "felix_broker_sub_prefix_build_ns",
                        "payload_bytes" => payload.len().to_string(),
                        "batch_events" => "1"
                    )
                    .record(prefix_ns as f64);
                }
                t_histogram!("broker_sub_frame_bytes").record(parts.frame_len() as f64);
                bytes_written = bytes_written.saturating_add(parts.frame_len() as u64);
                let write_await_start = t_now_if(sample);
                write_parts(&mut event_send, parts).await?;
                if let Some(start) = write_await_start {
                    let write_await_ns = start.elapsed().as_nanos() as u64;
                    timings::record_sub_write_await_ns(write_await_ns);
                    t_histogram!(
                        "felix_broker_sub_write_await_ns",
                        "payload_bytes" => payload.len().to_string(),
                        "batch_events" => "1"
                    )
                    .record(write_await_ns as f64);
                }
                frames_written = frames_written.saturating_add(1);
            }
            t_counter!("felix_subscribe_bytes_total").increment(payload_bytes as u64);
            t_histogram!(
                "felix_broker_sub_flush_payload_bytes",
                "batch_events" => payloads.len().to_string()
            )
            .record(payload_bytes as f64);
            #[cfg(feature = "telemetry")]
            {
                let counters = crate::transport::quic::telemetry::frame_counters();
                counters
                    .frames_out_ok
                    .fetch_add(frames_written, std::sync::atomic::Ordering::Relaxed);
                counters
                    .bytes_out
                    .fetch_add(bytes_written, std::sync::atomic::Ordering::Relaxed);
                counters
                    .sub_frames_out_ok
                    .fetch_add(frames_written, std::sync::atomic::Ordering::Relaxed);
                counters
                    .sub_batches_out_ok
                    .fetch_add(frames_written, std::sync::atomic::Ordering::Relaxed);
                counters
                    .sub_items_out_ok
                    .fetch_add(payloads.len() as u64, std::sync::atomic::Ordering::Relaxed);
            }
            #[cfg(feature = "telemetry")]
            if let Some(start) = write_start {
                let write_end = Instant::now();
                let write_ns = write_end.duration_since(start).as_nanos() as u64;
                timings::record_sub_write_ns(write_ns);
                timings::record_quic_write_ns(write_ns);
                t_histogram!("felix_broker_sub_write_ns").record(write_ns as f64);
                t_histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
                t_histogram!("broker_sub_write_await_ns").record(write_ns as f64);
            }
        }
        let _ = event_send.finish();
        return Ok(());
    }

    // Coalesce events by count, bytes, or deadline; each flush writes exactly one binary EventBatch frame.
    loop {
        let sample = t_should_sample();
        let queue_start = t_now_if(sample);

        // Prefer `pending` (carried over when we hit `max_bytes` on the previous batch).
        let first = match pending.take() {
            Some(payload) => payload,
            None => match rx.recv().await {
                Some(payload) => payload,
                None => break,
            },
        };
        if let Some(start) = queue_start {
            let queue_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_queue_wait_ns(queue_ns);
            t_histogram!("felix_broker_sub_recv_wait_ns").record(queue_ns as f64);
        }

        let mut batch = Vec::with_capacity(max_events);
        let mut batch_bytes = 0usize;
        let mut closed = false;

        // Why we track `flush_reason`:
        // - helps diagnose which limiter is dominant under different workloads.
        #[cfg_attr(not(feature = "telemetry"), allow(unused_assignments))]
        #[allow(unused_assignments)]
        let mut flush_reason = "idle";

        // Seed the batch with the first element.
        batch_bytes += first.len();
        batch.push(first);

        // Deadline for flushing a partially full batch.
        let deadline = tokio::time::Instant::now() + config.flush_delay;
        let deadline_sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(deadline_sleep);

        // Immediate flush if already at a threshold.
        if batch.len() >= max_events {
            flush_reason = "count";
        } else if batch_bytes >= max_bytes {
            flush_reason = "bytes";
        }

        // Keep collecting until we hit a flush condition or the channel closes.
        while flush_reason == "idle" && !closed {
            // First, drain whatever is immediately available without awaiting:
            // this maximizes batching without introducing extra latency.
            while batch.len() < max_events && batch_bytes < max_bytes {
                match rx.try_recv() {
                    Ok(payload) => {
                        // If this payload would exceed the byte budget, hold it for next batch.
                        if batch_bytes.saturating_add(payload.len()) > max_bytes {
                            pending = Some(payload);
                            flush_reason = "bytes";
                            break;
                        }
                        batch_bytes += payload.len();
                        batch.push(payload);
                        if batch.len() >= max_events {
                            flush_reason = "count";
                            break;
                        }
                        if batch_bytes >= max_bytes {
                            flush_reason = "bytes";
                            break;
                        }
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        // No more senders: flush what we have, then exit after writing.
                        closed = true;
                        flush_reason = "idle";
                        break;
                    }
                }
            }

            if flush_reason != "idle" || closed {
                break;
            }

            // Otherwise, wait for either:
            // - next event
            // - deadline
            tokio::select! {
                recv = rx.recv() => {
                    match recv {
                        Some(payload) => {
                            if batch_bytes.saturating_add(payload.len()) > max_bytes {
                                pending = Some(payload);
                                flush_reason = "bytes";
                                break;
                            }
                            batch_bytes += payload.len();
                            batch.push(payload);
                            if batch.len() >= max_events {
                                flush_reason = "count";
                                break;
                            }
                            if batch_bytes >= max_bytes {
                                flush_reason = "bytes";
                                break;
                            }
                        }
                        None => {
                            closed = true;
                            flush_reason = "idle";
                            break;
                        }
                    }
                }
                _ = &mut deadline_sleep => {
                    flush_reason = "deadline";
                    break;
                }
            }
        }

        let write_start = t_now_if(sample);
        #[cfg(not(feature = "telemetry"))]
        let _ = write_start;
        t_counter!(
            "felix_broker_event_batch_flush_reason_total",
            "reason" => flush_reason
        )
        .increment(1);
        t_histogram!("felix_broker_event_batch_size_bytes").record(batch_bytes as f64);

        let prefix_start = t_now_if(sample);
        let frame_parts =
            felix_wire::binary::encode_event_batch_parts(config.subscription_id, &batch)
                .context("encode binary event batch parts")?;
        if let Some(start) = prefix_start {
            let prefix_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_prefix_ns(prefix_ns);
            t_histogram!(
                "felix_broker_sub_prefix_build_ns",
                "payload_bytes" => batch_bytes.to_string(),
                "batch_events" => batch.len().to_string()
            )
            .record(prefix_ns as f64);
        }

        t_counter!("felix_subscribe_bytes_total").increment(batch_bytes as u64);
        t_histogram!("broker_sub_frame_bytes").record(frame_parts.frame_len() as f64);
        t_histogram!(
            "felix_broker_sub_flush_payload_bytes",
            "batch_events" => batch.len().to_string()
        )
        .record(batch_bytes as f64);
        let write_await_start = t_now_if(sample);
        let frame_len = frame_parts.frame_len() as u64;
        write_parts(&mut event_send, frame_parts).await?;
        #[cfg(not(feature = "telemetry"))]
        let _ = frame_len;
        if let Some(start) = write_await_start {
            let write_await_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_write_await_ns(write_await_ns);
            t_histogram!(
                "felix_broker_sub_write_await_ns",
                "payload_bytes" => batch_bytes.to_string(),
                "batch_events" => batch.len().to_string()
            )
            .record(write_await_ns as f64);
        }
        #[cfg(feature = "telemetry")]
        {
            let counters = crate::transport::quic::telemetry::frame_counters();
            counters
                .frames_out_ok
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            counters
                .bytes_out
                .fetch_add(frame_len, std::sync::atomic::Ordering::Relaxed);
        }

        // Counters:
        // - one frame per flush
        // - one batch per flush
        // - items = batch.len()
        #[cfg(feature = "telemetry")]
        {
            let counters = crate::transport::quic::telemetry::frame_counters();
            counters
                .sub_frames_out_ok
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            counters
                .sub_batches_out_ok
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            counters
                .sub_items_out_ok
                .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        #[cfg(feature = "telemetry")]
        if let Some(start) = write_start {
            let write_end = Instant::now();
            let write_ns = write_end.duration_since(start).as_nanos() as u64;
            timings::record_sub_write_ns(write_ns);
            timings::record_quic_write_ns(write_ns);
            t_histogram!("felix_broker_sub_write_ns").record(write_ns as f64);
            t_histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
            t_histogram!("broker_sub_write_await_ns").record(write_ns as f64);
        }

        // If the channel was closed and we flushed the last batch, exit.
        if closed {
            break;
        }
    }
    let _ = event_send.finish();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::quic::handlers::publish::AckTimeoutState;
    use anyhow::Context;
    use bytes::{Bytes, BytesMut};
    use felix_storage::EphemeralCache;
    use felix_transport::{QuicClient, QuicServer, TransportConfig};
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use std::net::SocketAddr;
    use tokio::io::AsyncReadExt;

    fn make_server_config() -> anyhow::Result<(quinn::ServerConfig, CertificateDer<'static>)> {
        let cert = generate_simple_self_signed(vec!["localhost".into()])
            .context("generate self-signed cert")?;
        let cert_der = CertificateDer::from(cert.serialize_der()?);
        let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
        let server_config = quinn::ServerConfig::with_single_cert(
            vec![cert_der.clone()],
            PrivateKeyDer::Pkcs8(key_der),
        )?;
        Ok((server_config, cert_der))
    }

    fn make_client_config(cert: CertificateDer<'static>) -> anyhow::Result<quinn::ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(cert).context("add root cert")?;
        Ok(quinn::ClientConfig::with_root_certificates(
            std::sync::Arc::new(roots),
        )?)
    }

    fn test_config() -> crate::config::BrokerConfig {
        crate::config::BrokerConfig {
            quic_bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            metrics_bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            controlplane_url: None,
            controlplane_sync_interval_ms: 2000,
            ack_on_commit: false,
            max_frame_bytes: 16 * 1024 * 1024,
            publish_queue_wait_timeout_ms: 2000,
            ack_wait_timeout_ms: 2000,
            disable_timings: false,
            control_stream_drain_timeout_ms: 50,
            cache_conn_recv_window: 256 * 1024 * 1024,
            cache_stream_recv_window: 64 * 1024 * 1024,
            cache_send_window: 256 * 1024 * 1024,
            event_batch_max_events: 1,
            event_batch_max_bytes: 64 * 1024,
            event_batch_max_delay_us: 250,
            fanout_batch_size: 1,
            pub_workers_per_conn: 1,
            pub_queue_depth: 8,
            subscriber_queue_capacity: 8,
            subscriber_writer_lanes: 4,
            subscriber_lane_queue_depth: 8192,
            max_subscriber_writer_lanes: 8,
            subscriber_lane_shard: crate::config::SubscriberLaneShard::Auto,
        }
    }

    fn make_payload(payload: &[u8]) -> Bytes {
        Bytes::from(payload.to_vec())
    }

    async fn spawn_event_writer(
        rx: mpsc::Receiver<Bytes>,
        config: EventWriterConfig,
    ) -> Result<(
        tokio::task::JoinHandle<Result<()>>,
        felix_transport::QuicConnection,
    )> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let event_send = connection.open_uni().await?;
            run_event_writer(event_send, rx, config).await
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        Ok((server_task, connection))
    }

    #[tokio::test]
    async fn write_parts_preserves_order_and_bytes() -> Result<()> {
        let (mut tx, mut rx) = tokio::io::duplex(1024);
        let payloads = vec![Bytes::from_static(b"abc"), Bytes::from_static(b"defg")];
        let expected = felix_wire::binary::encode_event_batch_bytes(88, &payloads)?;
        let parts = felix_wire::binary::encode_event_batch_parts(88, &payloads)?;

        let write_task = tokio::spawn(async move { write_parts_to(&mut tx, &parts).await });

        let mut out = vec![0u8; expected.len()];
        rx.read_exact(&mut out).await?;
        assert_eq!(out, expected.as_ref());

        write_task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn write_parts_total_bytes_sanity() -> Result<()> {
        let (mut tx, mut rx) = tokio::io::duplex(64 * 1024);
        let payloads = (0..128)
            .map(|_| Bytes::from(vec![0xCD; 128]))
            .collect::<Vec<_>>();
        let parts = felix_wire::binary::encode_event_batch_parts(3, &payloads)?;
        let expected_bytes = parts.frame_len();

        let write_task = tokio::spawn(async move { write_parts_to(&mut tx, &parts).await });
        let mut read = 0usize;
        let mut buf = vec![0u8; 4096];
        while read < expected_bytes {
            let n = rx.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            read += n;
        }
        assert_eq!(read, expected_bytes);
        write_task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn writer_parts_match_legacy_encoded_bytes() -> Result<()> {
        let (mut tx, mut rx) = tokio::io::duplex(16 * 1024);
        let payloads = vec![
            Bytes::from(vec![0x11; 17]),
            Bytes::from(vec![0x22; 256]),
            Bytes::from(vec![0x33; 3]),
        ];
        let expected = felix_wire::binary::encode_event_batch_bytes(17, &payloads)?;
        let parts = felix_wire::binary::encode_event_batch_parts(17, &payloads)?;

        let write_task = tokio::spawn(async move { write_parts_to(&mut tx, &parts).await });
        let mut out = vec![0u8; expected.len()];
        rx.read_exact(&mut out).await?;

        write_task.await??;
        assert_eq!(out, expected.as_ref());
        Ok(())
    }

    #[tokio::test]
    async fn run_event_writer_single_closes_on_channel_close() -> Result<()> {
        crate::timings::enable_collection(1);
        crate::timings::set_enabled(true);

        let (tx, rx) = mpsc::channel(4);
        let config = EventWriterConfig {
            subscription_id: 1,
            max_events: 1,
            max_bytes: 1024,
            flush_delay: Duration::from_millis(10),
            single_event_mode: true,
        };

        let (server_task, connection) = spawn_event_writer(rx, config).await?;
        let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
        tx.send(make_payload(b"hello")).await?;
        let mut event_recv = accept_uni.await.context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("event frame");
        let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
        assert_eq!(batch.subscription_id, 1);
        assert_eq!(batch.payloads.len(), 1);
        assert_eq!(batch.payloads[0].as_ref(), b"hello");

        drop(tx);
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_event_writer_single_binary_uses_batch_encoding() -> Result<()> {
        crate::timings::enable_collection(1);
        crate::timings::set_enabled(true);

        let (tx, rx) = mpsc::channel(4);
        let config = EventWriterConfig {
            subscription_id: 9,
            max_events: 1,
            max_bytes: 1024,
            flush_delay: Duration::from_millis(10),
            single_event_mode: true,
        };

        let (server_task, connection) = spawn_event_writer(rx, config).await?;
        let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
        tx.send(make_payload(b"bin")).await?;
        let mut event_recv = accept_uni.await.context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("event frame");
        let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
        assert_eq!(batch.subscription_id, 9);
        assert_eq!(batch.payloads.len(), 1);
        assert_eq!(batch.payloads[0].as_ref(), b"bin");

        drop(tx);
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_event_writer_batches_with_pending_payload() -> Result<()> {
        crate::timings::enable_collection(1);
        crate::timings::set_enabled(true);

        let (tx, rx) = mpsc::channel(4);
        let config = EventWriterConfig {
            subscription_id: 7,
            max_events: 10,
            max_bytes: 5,
            flush_delay: Duration::from_millis(50),
            single_event_mode: false,
        };

        let (server_task, connection) = spawn_event_writer(rx, config).await?;
        let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
        tx.send(make_payload(b"aaaa")).await?;
        tx.send(make_payload(b"bbb")).await?;
        tx.send(make_payload(b"c")).await?;
        let mut event_recv = accept_uni.await.context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame1 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame1");
        let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
        assert_eq!(batch1.subscription_id, 7);
        assert_eq!(batch1.payloads.len(), 1);
        assert_eq!(batch1.payloads[0].as_ref(), b"aaaa");

        let frame2 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame2");
        let batch2 = felix_wire::binary::decode_event_batch(&frame2).context("decode batch2")?;
        assert_eq!(batch2.subscription_id, 7);
        assert_eq!(batch2.payloads.len(), 2);
        assert_eq!(batch2.payloads[0].as_ref(), b"bbb");
        assert_eq!(batch2.payloads[1].as_ref(), b"c");

        drop(tx);
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn handle_subscribe_message_sends_event_stream_binary_batch() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream(
                "t1",
                "default",
                "orders",
                felix_broker::StreamMetadata::default(),
            )
            .await?;

        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
        let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
        let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
            std::time::Instant::now(),
        )));
        let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

        let broker_for_server = broker.clone();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let handled = handle_subscribe_message(
                broker_for_server,
                connection,
                test_config(),
                &out_ack_tx,
                &out_ack_depth,
                &ack_throttle_tx,
                &ack_timeout_state,
                &cancel_tx,
                "t1".to_string(),
                "default".to_string(),
                "orders".to_string(),
                Some(7),
            )
            .await?;
            Result::<bool>::Ok(handled)
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;

        let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
            .await
            .context("ack timeout")?
            .context("ack missing")?;
        match ack {
            Outgoing::Message(Message::Subscribed { subscription_id }) => {
                assert_eq!(subscription_id, 7);
            }
            _ => panic!("unexpected ack"),
        }

        let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
            .await
            .context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let hello = crate::transport::quic::codec::read_message_limited(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("hello");
        match hello {
            Message::EventStreamHello { subscription_id } => {
                assert_eq!(subscription_id, 7);
            }
            other => panic!("unexpected hello: {other:?}"),
        }

        broker
            .publish(
                "t1",
                "default",
                "orders",
                bytes::Bytes::from_static(b"hello"),
            )
            .await?;

        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("event frame");
        let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
        assert_eq!(batch.subscription_id, 7);
        assert_eq!(batch.payloads.len(), 1);
        assert_eq!(batch.payloads[0].as_ref(), b"hello");

        let handled = server_task.await.context("server join")??;
        assert!(handled);
        Ok(())
    }

    #[tokio::test]
    async fn handle_subscribe_message_errors_when_stream_missing() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;

        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
        let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
        let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
            std::time::Instant::now(),
        )));
        let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

        let broker_for_server = broker.clone();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            handle_subscribe_message(
                broker_for_server,
                connection,
                test_config(),
                &out_ack_tx,
                &out_ack_depth,
                &ack_throttle_tx,
                &ack_timeout_state,
                &cancel_tx,
                "t1".to_string(),
                "default".to_string(),
                "missing".to_string(),
                Some(11),
            )
            .await
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let _connection = client.connect(addr, "localhost").await?;

        let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
            .await
            .context("ack timeout")?
            .context("ack missing")?;
        match ack {
            Outgoing::Message(Message::Error { message }) => {
                assert!(message.contains("stream not found"));
            }
            _ => panic!("unexpected ack"),
        }

        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn handle_subscribe_message_batches_by_bytes() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream(
                "t1",
                "default",
                "orders",
                felix_broker::StreamMetadata::default(),
            )
            .await?;

        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
        let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
        let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
            std::time::Instant::now(),
        )));
        let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

        let mut config = test_config();
        config.fanout_batch_size = 10;
        config.event_batch_max_events = 10;
        config.event_batch_max_bytes = 6;

        let broker_for_server = broker.clone();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            handle_subscribe_message(
                broker_for_server,
                connection,
                config,
                &out_ack_tx,
                &out_ack_depth,
                &ack_throttle_tx,
                &ack_timeout_state,
                &cancel_tx,
                "t1".to_string(),
                "default".to_string(),
                "orders".to_string(),
                Some(21),
            )
            .await
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;

        let _ = out_ack_rx.recv().await;
        let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
            .await
            .context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let _ = crate::transport::quic::codec::read_message_limited(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("hello");

        broker
            .publish("t1", "default", "orders", bytes::Bytes::from_static(b"aa"))
            .await?;
        broker
            .publish(
                "t1",
                "default",
                "orders",
                bytes::Bytes::from_static(b"bbbb"),
            )
            .await?;
        broker
            .publish(
                "t1",
                "default",
                "orders",
                bytes::Bytes::from_static(b"ccccc"),
            )
            .await?;

        let frame1 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame1");
        let batch1 = felix_wire::binary::decode_event_batch(&frame1).expect("batch1");
        assert_eq!(batch1.subscription_id, 21);
        assert_eq!(batch1.payloads.len(), 2);
        assert_eq!(batch1.payloads[0].as_ref(), b"aa");
        assert_eq!(batch1.payloads[1].as_ref(), b"bbbb");

        let frame2 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame2");
        let batch2 = felix_wire::binary::decode_event_batch(&frame2).expect("batch2");
        assert_eq!(batch2.subscription_id, 21);
        assert_eq!(batch2.payloads.len(), 1);
        assert_eq!(batch2.payloads[0].as_ref(), b"ccccc");

        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn lane_fanout_preserves_order_for_multiple_subscribers() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream(
                "t1",
                "default",
                "orders",
                felix_broker::StreamMetadata::default(),
            )
            .await?;

        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
        let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
        let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
            std::time::Instant::now(),
        )));
        let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

        let mut config = test_config();
        config.subscriber_writer_lanes = 4;
        config.subscriber_lane_shard = crate::config::SubscriberLaneShard::Auto;
        config.fanout_batch_size = 1;
        config.event_batch_max_events = 1;

        let broker_for_server = broker.clone();
        let server_task = tokio::spawn(async move {
            for sub_id in [31_u64, 32_u64] {
                let connection = server.accept().await?;
                handle_subscribe_message(
                    broker_for_server.clone(),
                    connection,
                    config.clone(),
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    "t1".to_string(),
                    "default".to_string(),
                    "orders".to_string(),
                    Some(sub_id),
                )
                .await?;
            }
            Result::<()>::Ok(())
        });

        let client1 = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            make_client_config(cert.clone())?,
            transport.clone(),
        )?;
        let client2 = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection1 = client1.connect(addr, "localhost").await?;
        let connection2 = client2.connect(addr, "localhost").await?;

        let mut subscribed = Vec::new();
        for _ in 0..2 {
            let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
                .await
                .context("ack timeout")?
                .context("ack missing")?;
            match ack {
                Outgoing::Message(Message::Subscribed { subscription_id }) => {
                    subscribed.push(subscription_id);
                }
                _ => panic!("unexpected ack"),
            }
        }
        subscribed.sort_unstable();
        assert_eq!(subscribed, vec![31, 32]);

        let mut event_recv_1 =
            tokio::time::timeout(Duration::from_secs(1), connection1.accept_uni())
                .await
                .context("accept uni timeout 1")??;
        let mut event_recv_2 =
            tokio::time::timeout(Duration::from_secs(1), connection2.accept_uni())
                .await
                .context("accept uni timeout 2")??;
        let mut scratch_1 = BytesMut::new();
        let mut scratch_2 = BytesMut::new();
        let _ = crate::transport::quic::codec::read_message_limited(
            &mut event_recv_1,
            16 * 1024,
            &mut scratch_1,
        )
        .await?
        .expect("hello1");
        let _ = crate::transport::quic::codec::read_message_limited(
            &mut event_recv_2,
            16 * 1024,
            &mut scratch_2,
        )
        .await?
        .expect("hello2");

        let expected = (0..20)
            .map(|i| format!("msg-{i}").into_bytes())
            .collect::<Vec<_>>();
        for payload in &expected {
            broker
                .publish(
                    "t1",
                    "default",
                    "orders",
                    bytes::Bytes::copy_from_slice(payload.as_slice()),
                )
                .await?;
        }

        let mut recv_1 = Vec::with_capacity(expected.len());
        let mut recv_2 = Vec::with_capacity(expected.len());
        for _ in 0..expected.len() {
            let frame = crate::transport::quic::codec::read_frame_limited_into(
                &mut event_recv_1,
                16 * 1024,
                &mut scratch_1,
            )
            .await?
            .expect("event frame 1");
            let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch 1")?;
            recv_1.push(batch.payloads[0].to_vec());

            let frame = crate::transport::quic::codec::read_frame_limited_into(
                &mut event_recv_2,
                16 * 1024,
                &mut scratch_2,
            )
            .await?
            .expect("event frame 2");
            let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch 2")?;
            recv_2.push(batch.payloads[0].to_vec());
        }

        assert_eq!(recv_1, expected);
        assert_eq!(recv_2, expected);

        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn lane_selection_auto_pins_shared_connection_to_one_lane() -> Result<()> {
        let mut config = test_config();
        config.subscriber_writer_lanes = 8;
        config.max_subscriber_writer_lanes = 8;
        config.subscriber_lane_shard = crate::config::SubscriberLaneShard::Auto;
        let manager = WriterLaneManager::new(&config);

        let lane_a = manager.select_lane(100, Some(7));
        let lane_b = manager.select_lane(200, Some(7));
        let lane_c = manager.select_lane(300, Some(7));
        assert_eq!(lane_a, lane_b);
        assert_eq!(lane_b, lane_c);

        Ok(())
    }

    #[tokio::test]
    async fn round_robin_pin_keeps_subscriber_sticky() -> Result<()> {
        let mut config = test_config();
        config.subscriber_writer_lanes = 4;
        config.max_subscriber_writer_lanes = 8;
        config.subscriber_lane_shard = crate::config::SubscriberLaneShard::RoundRobinPin;
        let manager = WriterLaneManager::new(&config);

        let first = manager.select_lane(42, None);
        let second = manager.select_lane(42, None);
        let third = manager.select_lane(42, None);
        assert_eq!(first, second);
        assert_eq!(second, third);

        Ok(())
    }
}
