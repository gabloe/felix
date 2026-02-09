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
use felix_broker::{Broker, SubscriptionReceiver};
use felix_wire::Message;
use std::collections::{HashMap, VecDeque};
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

static ACTIVE_SUB_CONN_COUNTS: OnceLock<DashMap<u64, usize>> = OnceLock::new();
static SUB_EGRESS_MANAGERS: OnceLock<DashMap<u64, Arc<WriterLaneManager>>> = OnceLock::new();

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
        flush_max_items: config.subscriber_flush_max_items.max(1),
        flush_max_delay: Duration::from_micros(config.subscriber_flush_max_delay_us.max(1)),
        max_bytes_per_write: config.subscriber_max_bytes_per_write.max(1),
    };
    let connection_id = connection.info().id.0;
    let manager = WriterLaneManager::init(&config, connection_id);
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

    // Control-plane acknowledgement: subscriber is fully wired for delivery.
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

    /// Max number of lane commands to gather per flush.
    flush_max_items: usize,

    /// Max delay while filling a lane flush buffer.
    flush_max_delay: Duration,

    /// Upper bound for coalesced bytes in one write.
    max_bytes_per_write: usize,
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

async fn write_parts_many(
    send: &mut quinn::SendStream,
    frames: Vec<felix_wire::binary::EncodedEventBatchParts>,
) -> Result<usize> {
    let mut total = 0usize;
    let mut segments = Vec::new();
    for frame in frames {
        total = total.saturating_add(frame.frame_len());
        segments.extend(frame.into_segments());
    }
    send.write_all_chunks(segments.as_mut_slice())
        .await
        .context("write subscription coalesced frame chunks")?;
    Ok(total)
}

#[derive(Debug)]
struct LaneSubscriber {
    event_send: quinn::SendStream,
    // Keep the connection alive as long as this subscriber is registered.
    _connection: felix_transport::QuicConnection,
    _connection_id: Option<u64>,
    _unsubscribe_guard: felix_broker::SubscriptionGuard,
}

#[derive(Debug)]
enum ConnectionCommand {
    Register {
        subscriber_id: u64,
        connection: felix_transport::QuicConnection,
        connection_id: Option<u64>,
        event_send: quinn::SendStream,
        guard: felix_broker::SubscriptionGuard,
    },
    Delivery {
        subscriber_id: u64,
        frame_parts: felix_wire::binary::EncodedEventBatchParts,
        item_count: usize,
        first_enqueued_at: Instant,
        enqueue_at: Instant,
    },
    Unregister {
        subscriber_id: u64,
    },
}

#[derive(Debug)]
enum LaneCommand {
    Register {
        subscriber_id: u64,
        connection_id: Option<u64>,
        connection: felix_transport::QuicConnection,
        event_send: quinn::SendStream,
        guard: felix_broker::SubscriptionGuard,
    },
    Delivery {
        subscriber_id: u64,
        frame_parts: felix_wire::binary::EncodedEventBatchParts,
        item_count: usize,
        first_enqueued_at: Instant,
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
    lane_queue_policy: felix_broker::SubQueuePolicy,
    lane_queue_highwater: Vec<AtomicUsize>,
    connection_writers: DashMap<u64, mpsc::Sender<ConnectionCommand>>,
    subscriber_connections: DashMap<u64, u64>,
    connection_queue_capacity: usize,
    max_bytes_per_write: usize,
    connection_lanes: DashMap<u64, usize>,
    subscriber_pins: DashMap<u64, usize>,
    shard: crate::config::SubscriberLaneShard,
    single_writer_per_conn: bool,
    rr_counter: AtomicUsize,
}

#[derive(Debug, Clone, Copy)]
struct LaneRuntimeConfig {
    flush_max_items: usize,
}

#[derive(Debug)]
struct LaneDelivery {
    subscriber_id: u64,
    frame_parts: felix_wire::binary::EncodedEventBatchParts,
    item_count: usize,
    first_enqueued_at: Instant,
    enqueue_at: Instant,
}

impl WriterLaneManager {
    fn init(config: &crate::config::BrokerConfig, scope_key: u64) -> Arc<Self> {
        let key = Self::config_key(config, scope_key);
        let managers = SUB_EGRESS_MANAGERS.get_or_init(DashMap::new);
        if let Some(existing) = managers.get(&key) {
            return existing.clone();
        }
        let manager = Self::new(config);
        managers.insert(key, Arc::clone(&manager));
        manager
    }

    fn config_key(config: &crate::config::BrokerConfig, scope_key: u64) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        scope_key.hash(&mut hasher);
        config.quic_bind.hash(&mut hasher);
        config.subscriber_writer_lanes.hash(&mut hasher);
        config.subscriber_lane_queue_depth.hash(&mut hasher);
        match config.subscriber_lane_queue_policy {
            felix_broker::SubQueuePolicy::Block => 0u8,
            felix_broker::SubQueuePolicy::DropNew => 1u8,
            felix_broker::SubQueuePolicy::DropOld => 2u8,
        }
        .hash(&mut hasher);
        match config.subscriber_lane_shard {
            crate::config::SubscriberLaneShard::Auto => 0u8,
            crate::config::SubscriberLaneShard::SubscriberIdHash => 1u8,
            crate::config::SubscriberLaneShard::ConnectionIdHash => 2u8,
            crate::config::SubscriberLaneShard::RoundRobinPin => 3u8,
        }
        .hash(&mut hasher);
        config.subscriber_single_writer_per_conn.hash(&mut hasher);
        config.subscriber_flush_max_items.hash(&mut hasher);
        config.subscriber_flush_max_delay_us.hash(&mut hasher);
        config.subscriber_max_bytes_per_write.hash(&mut hasher);
        hasher.finish()
    }

    fn new(config: &crate::config::BrokerConfig) -> Arc<Self> {
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
        let lane_cfg = LaneRuntimeConfig {
            flush_max_items: config.subscriber_flush_max_items.max(1),
        };
        let mut lanes = Vec::with_capacity(lane_count);
        let mut lane_receivers = Vec::with_capacity(lane_count);
        for _lane_id in 0..lane_count {
            let (tx, rx) = mpsc::channel(queue_depth);
            lanes.push(tx);
            lane_receivers.push(rx);
        }
        let mut lane_queue_highwater = Vec::with_capacity(lane_count);
        for _ in 0..lane_count {
            lane_queue_highwater.push(AtomicUsize::new(0));
        }
        let manager = Self {
            lanes,
            lane_queue_capacity: queue_depth,
            lane_queue_policy: config.subscriber_lane_queue_policy,
            lane_queue_highwater,
            connection_writers: DashMap::new(),
            subscriber_connections: DashMap::new(),
            connection_queue_capacity: queue_depth,
            max_bytes_per_write: config.subscriber_max_bytes_per_write.max(1),
            connection_lanes: DashMap::new(),
            subscriber_pins: DashMap::new(),
            shard: config.subscriber_lane_shard,
            single_writer_per_conn: config.subscriber_single_writer_per_conn,
            rr_counter: AtomicUsize::new(0),
        };
        let manager = Arc::new(manager);
        for (lane_id, rx) in lane_receivers.into_iter().enumerate() {
            tokio::spawn(run_writer_lane(lane_id, rx, lane_cfg, Arc::clone(&manager)));
        }
        manager
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
        if self.single_writer_per_conn
            && let Some(connection_id) = connection_id
        {
            return self.lane_for_connection(connection_id);
        }
        // Design intent:
        // - `Auto` defaults to connection ownership to avoid cross-lane contention on shared QUIC
        //   send state when multiple subscribers share one connection.
        // - `SubscriberIdHash` maximizes distribution independent of connection topology.
        // - `RoundRobinPin` assigns once at subscribe time (not per message) so ordering is stable.
        match self.shard {
            crate::config::SubscriberLaneShard::Auto => self.lane_for_subscriber(subscriber_id),
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
        self.subscriber_connections.remove(&subscriber_id);
        if let Some(connection_id) = connection_id {
            self.connection_lanes.remove(&connection_id);
        }
    }

    fn ensure_connection_writer(&self, connection_id: u64) -> mpsc::Sender<ConnectionCommand> {
        if let Some(existing) = self.connection_writers.get(&connection_id) {
            return existing.clone();
        }
        let (tx, rx) = mpsc::channel(self.connection_queue_capacity.max(1));
        tokio::spawn(run_connection_writer(
            connection_id,
            rx,
            self.max_bytes_per_write,
        ));
        self.connection_writers.insert(connection_id, tx.clone());
        tx
    }

    async fn enqueue_connection(
        &self,
        connection_id: u64,
        cmd: ConnectionCommand,
    ) -> std::result::Result<(), ()> {
        let sender = self.ensure_connection_writer(connection_id);
        let conn_label = connection_id.to_string();
        let wait_start = Instant::now();
        let result = match self.lane_queue_policy {
            felix_broker::SubQueuePolicy::Block => sender.send(cmd).await.map_err(|_| ()),
            felix_broker::SubQueuePolicy::DropNew | felix_broker::SubQueuePolicy::DropOld => {
                sender.try_send(cmd).map_err(|_| ())
            }
        };
        let wait_ns = wait_start.elapsed().as_nanos() as u64;
        timings::record_sub_queue_wait_ns(wait_ns);
        t_histogram!("felix_broker_sub_queue_wait_ns").record(wait_ns as f64);
        t_histogram!("broker_sub_conn_enqueue_wait_ns", "connection_id" => conn_label.clone())
            .record(wait_ns as f64);
        let depth = self
            .connection_queue_capacity
            .saturating_sub(sender.capacity());
        metrics::gauge!("felix_sub_conn_queue_len", "connection_id" => conn_label)
            .set(depth as f64);
        result
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

    async fn enqueue(&self, lane_idx: usize, cmd: LaneCommand) -> std::result::Result<(), ()> {
        let lane = lane_idx.to_string();
        let sender = &self.lanes[lane_idx];
        let enqueue_wait_start = Instant::now();
        let result = match self.lane_queue_policy {
            felix_broker::SubQueuePolicy::Block => sender.send(cmd).await.map_err(|_| ()),
            felix_broker::SubQueuePolicy::DropNew => sender.try_send(cmd).map_err(|err| {
                if matches!(err, tokio::sync::mpsc::error::TrySendError::Full(_)) {
                    t_counter!("broker_sub_lane_queue_full_total", "lane" => lane.clone())
                        .increment(1);
                }
            }),
            felix_broker::SubQueuePolicy::DropOld => sender.try_send(cmd).map_err(|err| {
                if matches!(err, tokio::sync::mpsc::error::TrySendError::Full(_)) {
                    t_counter!("broker_sub_lane_queue_full_total", "lane" => lane.clone())
                        .increment(1);
                    t_counter!(
                        "broker_sub_lane_drop_old_emulated_total",
                        "lane" => lane.clone()
                    )
                    .increment(1);
                }
            }),
        };
        let enqueue_wait_ns = enqueue_wait_start.elapsed().as_nanos() as u64;
        t_histogram!("broker_sub_lane_enqueue_block_ns", "lane" => lane.clone())
            .record(enqueue_wait_ns as f64);
        match result {
            Ok(()) => {
                t_counter!("broker_sub_lane_enqueued_total", "lane" => lane.clone()).increment(1);
                self.update_lane_queue_highwater(lane_idx);
                let queue_len = self.lane_queue_capacity.saturating_sub(sender.capacity());
                metrics::gauge!("felix_sub_lane_queue_len", "lane" => lane).set(queue_len as f64);
                Ok(())
            }
            Err(()) => {
                t_counter!("broker_sub_lane_dropped_total", "lane" => lane).increment(1);
                Err(())
            }
        }
    }
}

async fn run_writer_lane(
    lane_id: usize,
    mut rx: mpsc::Receiver<LaneCommand>,
    lane_cfg: LaneRuntimeConfig,
    manager: Arc<WriterLaneManager>,
) {
    let lane_label = lane_id.to_string();
    while let Some(first_cmd) = rx.recv().await {
        let mut pending = Vec::with_capacity(lane_cfg.flush_max_items.max(1));
        pending.push(first_cmd);

        while pending.len() < lane_cfg.flush_max_items {
            match rx.try_recv() {
                Ok(cmd) => pending.push(cmd),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        metrics::gauge!("felix_sub_lane_queue_len", "lane" => lane_label.clone())
            .set(rx.len() as f64);

        for cmd in pending {
            match cmd {
                LaneCommand::Register {
                    subscriber_id,
                    connection_id,
                    connection,
                    event_send,
                    guard,
                } => {
                    let Some(connection_id) = connection_id else {
                        continue;
                    };
                    manager
                        .subscriber_connections
                        .insert(subscriber_id, connection_id);
                    connection_subscriber_register(Some(connection_id));
                    let _ = manager
                        .enqueue_connection(
                            connection_id,
                            ConnectionCommand::Register {
                                subscriber_id,
                                connection,
                                connection_id: Some(connection_id),
                                event_send,
                                guard,
                            },
                        )
                        .await;
                }
                LaneCommand::Unregister { subscriber_id } => {
                    if let Some((_, connection_id)) =
                        manager.subscriber_connections.remove(&subscriber_id)
                    {
                        let _ = manager
                            .enqueue_connection(
                                connection_id,
                                ConnectionCommand::Unregister { subscriber_id },
                            )
                            .await;
                        connection_subscriber_unregister(Some(connection_id));
                    }
                }
                LaneCommand::Delivery {
                    subscriber_id,
                    frame_parts,
                    item_count,
                    first_enqueued_at,
                    enqueue_at,
                } => {
                    let Some(connection_id) = manager
                        .subscriber_connections
                        .get(&subscriber_id)
                        .map(|entry| *entry.value())
                    else {
                        continue;
                    };
                    let _ = manager
                        .enqueue_connection(
                            connection_id,
                            ConnectionCommand::Delivery {
                                subscriber_id,
                                frame_parts,
                                item_count,
                                first_enqueued_at,
                                enqueue_at,
                            },
                        )
                        .await;
                }
            }
        }
    }
}

async fn run_connection_writer(
    connection_id: u64,
    mut rx: mpsc::Receiver<ConnectionCommand>,
    max_bytes_per_write: usize,
) {
    let conn_label = connection_id.to_string();
    let mut debug_window_start = Instant::now();
    let mut debug_writes = 0u64;
    let mut debug_bytes = 0u64;
    let mut debug_dequeues = 0u64;
    let mut subscribers: HashMap<u64, LaneSubscriber> = HashMap::new();
    while let Some(first_cmd) = rx.recv().await {
        let mut pending = Vec::with_capacity(64);
        pending.push(first_cmd);
        while pending.len() < 64 {
            match rx.try_recv() {
                Ok(cmd) => pending.push(cmd),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        metrics::gauge!("felix_sub_conn_queue_len", "connection_id" => conn_label.clone())
            .set(rx.len() as f64);

        let mut deliveries: HashMap<u64, VecDeque<LaneDelivery>> = HashMap::new();
        for cmd in pending {
            match cmd {
                ConnectionCommand::Register {
                    subscriber_id,
                    connection,
                    connection_id,
                    event_send,
                    guard,
                } => {
                    subscribers.insert(
                        subscriber_id,
                        LaneSubscriber {
                            event_send,
                            _connection: connection,
                            _connection_id: connection_id,
                            _unsubscribe_guard: guard,
                        },
                    );
                }
                ConnectionCommand::Unregister { subscriber_id } => {
                    subscribers.remove(&subscriber_id);
                    deliveries.remove(&subscriber_id);
                }
                ConnectionCommand::Delivery {
                    subscriber_id,
                    frame_parts,
                    item_count,
                    first_enqueued_at,
                    enqueue_at,
                } => {
                    deliveries
                        .entry(subscriber_id)
                        .or_default()
                        .push_back(LaneDelivery {
                            subscriber_id,
                            frame_parts,
                            item_count,
                            first_enqueued_at,
                            enqueue_at,
                        });
                }
            }
        }

        let mut rr_order: VecDeque<u64> = deliveries.keys().copied().collect();
        while let Some(subscriber_id) = rr_order.pop_front() {
            let Some(queue) = deliveries.get_mut(&subscriber_id) else {
                continue;
            };
            let Some(first) = queue.pop_front() else {
                continue;
            };
            debug_dequeues = debug_dequeues.saturating_add(1);
            if !queue.is_empty() {
                rr_order.push_back(subscriber_id);
            }
            let Some(subscriber) = subscribers.get_mut(&subscriber_id) else {
                continue;
            };

            let mut frames = vec![first.frame_parts];
            let mut item_count = first.item_count;
            let mut coalesced_bytes = frames[0].frame_len();
            while let Some(next) = queue.front() {
                let next_len = next.frame_parts.frame_len();
                if coalesced_bytes.saturating_add(next_len) > max_bytes_per_write {
                    break;
                }
                if let Some(next_frame) = queue.pop_front() {
                    item_count = item_count.saturating_add(next_frame.item_count);
                    coalesced_bytes =
                        coalesced_bytes.saturating_add(next_frame.frame_parts.frame_len());
                    frames.push(next_frame.frame_parts);
                }
            }
            if !queue.is_empty() {
                rr_order.push_back(subscriber_id);
            }

            let sample = t_should_sample();
            let queue_wait_ns = first.enqueue_at.elapsed().as_nanos() as u64;
            let first_dequeue_ns = first.first_enqueued_at.elapsed().as_nanos() as u64;
            if sample {
                t_histogram!("broker_sub_lane_queue_wait_ns", "connection_id" => conn_label.clone())
                    .record(queue_wait_ns as f64);
                t_histogram!(
                    "broker_sub_lane_dequeue_to_write_start_ns",
                    "connection_id" => conn_label.clone()
                )
                .record(queue_wait_ns as f64);
                t_histogram!(
                    "broker_sub_time_to_first_dequeue_ns",
                    "connection_id" => conn_label.clone()
                )
                .record(first_dequeue_ns as f64);
            }

            let write_start = t_now_if(sample);
            if sample {
                t_counter!("broker_sub_conn_write_calls_total", "connection_id" => conn_label.clone())
                    .increment(1);
                t_histogram!(
                    "broker_sub_conn_writes_per_flush",
                    "connection_id" => conn_label.clone()
                )
                .record(frames.len() as f64);
            }
            let write_result = if frames.len() == 1 {
                write_parts(
                    &mut subscriber.event_send,
                    frames.pop().expect("single frame"),
                )
                .await
                .map(|_| coalesced_bytes)
            } else {
                write_parts_many(&mut subscriber.event_send, frames).await
            };
            match write_result {
                Ok(bytes_written) => {
                    debug_writes = debug_writes.saturating_add(1);
                    debug_bytes = debug_bytes.saturating_add(bytes_written as u64);
                    if sample {
                        t_histogram!(
                            "broker_sub_conn_avg_bytes_per_write",
                            "connection_id" => conn_label.clone()
                        )
                        .record(bytes_written as f64);
                        t_counter!(
                            "broker_sub_conn_bytes_written_total",
                            "connection_id" => conn_label.clone()
                        )
                        .increment(bytes_written as u64);
                    }
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = crate::transport::quic::telemetry::frame_counters();
                        counters
                            .frames_out_ok
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        counters
                            .bytes_out
                            .fetch_add(bytes_written as u64, std::sync::atomic::Ordering::Relaxed);
                        counters
                            .sub_frames_out_ok
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        counters
                            .sub_batches_out_ok
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        counters
                            .sub_items_out_ok
                            .fetch_add(item_count as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                    if let Some(start) = write_start {
                        let write_ns = start.elapsed().as_nanos() as u64;
                        timings::record_sub_write_ns(write_ns);
                        timings::record_sub_write_await_ns(write_ns);
                        timings::record_quic_write_ns(write_ns);
                        t_histogram!("broker_sub_write_blocked_ns").record(write_ns as f64);
                        t_histogram!("broker_sub_conn_write_ns", "connection_id" => conn_label.clone())
                            .record(write_ns as f64);
                        t_histogram!(
                            "broker_sub_conn_write_await_ns",
                            "connection_id" => conn_label.clone()
                        )
                        .record(write_ns as f64);
                    }
                }
                Err(err) => {
                    t_counter!(
                        "broker_sub_conn_write_errors_total",
                        "connection_id" => conn_label.clone()
                    )
                    .increment(1);
                    metrics::counter!("felix_subscriber_disconnect_total").increment(1);
                    tracing::info!(
                        connection_id,
                        subscriber_id = first.subscriber_id,
                        error = %err,
                        "connection writer subscriber stream closed"
                    );
                    subscribers.remove(&first.subscriber_id);
                }
            }
        }
        if debug_window_start.elapsed() >= Duration::from_secs(1) {
            let avg_bytes_per_write = if debug_writes == 0 {
                0.0
            } else {
                debug_bytes as f64 / debug_writes as f64
            };
            tracing::debug!(
                connection_id,
                queue_depth = rx.len(),
                dequeues_per_sec = debug_dequeues,
                writes_per_sec = debug_writes,
                avg_bytes_per_write,
                "subscriber connection throughput window"
            );
            debug_window_start = Instant::now();
            debug_writes = 0;
            debug_bytes = 0;
            debug_dequeues = 0;
        }
    }
}

fn hash64(value: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn connection_subscriber_register(connection_id: Option<u64>) {
    let Some(connection_id) = connection_id else {
        return;
    };
    let map = ACTIVE_SUB_CONN_COUNTS.get_or_init(DashMap::new);
    let new_count = if let Some(mut entry) = map.get_mut(&connection_id) {
        *entry += 1;
        *entry
    } else {
        map.insert(connection_id, 1);
        1
    };
    metrics::gauge!("felix_sub_active_connections").set(map.len() as f64);
    metrics::gauge!("felix_sub_connection_subscribers", "connection_id" => connection_id.to_string())
        .set(new_count as f64);
}

fn connection_subscriber_unregister(connection_id: Option<u64>) {
    let Some(connection_id) = connection_id else {
        return;
    };
    let Some(map) = ACTIVE_SUB_CONN_COUNTS.get() else {
        return;
    };
    if let Some(mut entry) = map.get_mut(&connection_id) {
        if *entry > 1 {
            *entry -= 1;
            metrics::gauge!(
                "felix_sub_connection_subscribers",
                "connection_id" => connection_id.to_string()
            )
            .set(*entry as f64);
        } else {
            drop(entry);
            map.remove(&connection_id);
            metrics::gauge!(
                "felix_sub_connection_subscribers",
                "connection_id" => connection_id.to_string()
            )
            .set(0.0);
        }
    }
    metrics::gauge!("felix_sub_active_connections").set(map.len() as f64);
}

async fn run_lane_feeder(
    mut event_rx: SubscriptionReceiver,
    manager: Arc<WriterLaneManager>,
    lane_idx: usize,
    connection_id: Option<u64>,
    config: EventWriterConfig,
) {
    let max_events = config.max_events.max(1);
    let max_bytes = config.max_bytes.max(1);
    let _lane_flush_hints = (
        config.flush_max_items,
        config.flush_max_delay,
        config.max_bytes_per_write,
    );
    let mut pending: Option<(Bytes, Instant)> = None;

    loop {
        let (first, first_enqueued_at) = match pending.take() {
            Some((payload, enqueued_at)) => (payload, enqueued_at),
            None => match event_rx.recv().await {
                Some(envelope) => {
                    let enqueued_at = envelope.enqueued_at();
                    (envelope.into_payload(), enqueued_at)
                }
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
                    Ok(Some(envelope)) => {
                        let enqueued_at = envelope.enqueued_at();
                        Some((envelope.into_payload(), enqueued_at))
                    }
                    Ok(None) | Err(_) => None,
                }
            };
            let Some((payload, enqueued_at)) = next else {
                break;
            };
            if batch_bytes.saturating_add(payload.len()) > max_bytes {
                pending = Some((payload, enqueued_at));
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
            item_count: batch.len(),
            first_enqueued_at,
            enqueue_at: Instant::now(),
        };
        if manager.enqueue(lane_idx, cmd).await.is_err() {
            metrics::counter!("felix_subscriber_lane_dropped_total").increment(1);
        } else if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            t_histogram!("broker_sub_lane_enqueue_ns", "lane" => lane_idx.to_string())
                .record(enqueue_ns as f64);
        }
    }
    let _ = manager
        .enqueue(
            lane_idx,
            LaneCommand::Unregister {
                subscriber_id: config.subscription_id,
            },
        )
        .await;
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
                    t_histogram!("broker_sub_write_blocked_ns").record(write_await_ns as f64);
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
                t_histogram!("broker_sub_write_blocked_ns").record(write_ns as f64);
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
            t_histogram!("broker_sub_write_blocked_ns").record(write_await_ns as f64);
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
            t_histogram!("broker_sub_write_blocked_ns").record(write_ns as f64);
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
        let rcgen::CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec!["localhost".into()])
                .context("generate self-signed cert")?;
        let cert_der = cert.der().clone();
        let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());
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
            subscriber_queue_policy: felix_broker::SubQueuePolicy::DropNew,
            subscriber_writer_lanes: 4,
            subscriber_lane_queue_depth: 8192,
            subscriber_lane_queue_policy: felix_broker::SubQueuePolicy::Block,
            max_subscriber_writer_lanes: 8,
            subscriber_lane_shard: crate::config::SubscriberLaneShard::Auto,
            subscriber_single_writer_per_conn: true,
            subscriber_flush_max_items: 64,
            subscriber_flush_max_delay_us: 200,
            subscriber_max_bytes_per_write: 256 * 1024,
            sub_streams_per_conn: 4,
            sub_stream_mode: crate::config::SubStreamMode::PerSubscriber,
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
            flush_max_items: 64,
            flush_max_delay: Duration::from_micros(200),
            max_bytes_per_write: 256 * 1024,
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
            flush_max_items: 64,
            flush_max_delay: Duration::from_micros(200),
            max_bytes_per_write: 256 * 1024,
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
            flush_max_items: 64,
            flush_max_delay: Duration::from_micros(200),
            max_bytes_per_write: 256 * 1024,
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

        let _ = felix_broker::timings::take_samples();
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

        let _ = felix_broker::timings::take_samples();
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

        let _ = felix_broker::timings::take_samples();
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn lane_selection_auto_pins_shared_connection_to_one_lane() -> Result<()> {
        let mut config = test_config();
        config.subscriber_writer_lanes = 8;
        config.max_subscriber_writer_lanes = 8;
        config.subscriber_lane_shard = crate::config::SubscriberLaneShard::Auto;
        config.subscriber_single_writer_per_conn = true;
        let manager = WriterLaneManager::new(&config);

        let lane_a = manager.select_lane(100, Some(7));
        let lane_b = manager.select_lane(200, Some(7));
        let lane_c = manager.select_lane(300, Some(7));
        assert_eq!(lane_a, lane_b);
        assert_eq!(lane_b, lane_c);

        Ok(())
    }

    #[tokio::test]
    async fn lane_selection_auto_uses_subscriber_hash_when_conn_pin_disabled() -> Result<()> {
        let mut config = test_config();
        config.subscriber_writer_lanes = 8;
        config.max_subscriber_writer_lanes = 8;
        config.subscriber_lane_shard = crate::config::SubscriberLaneShard::Auto;
        config.subscriber_single_writer_per_conn = false;
        let manager = WriterLaneManager::new(&config);

        let lane_a = manager.select_lane(100, Some(7));
        let lane_b = manager.select_lane(200, Some(7));
        let lane_c = manager.select_lane(100, Some(7));
        assert_eq!(lane_a, lane_c);
        assert_ne!(lane_a, lane_b);

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

    #[tokio::test]
    async fn handle_subscribe_message_hashed_pool_with_generated_id() -> Result<()> {
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
        config.sub_stream_mode = crate::config::SubStreamMode::HashedPool;
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
                None,
            )
            .await
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;

        let subscription_id = match out_ack_rx.recv().await.context("missing ack")? {
            Outgoing::Message(Message::Subscribed { subscription_id }) => subscription_id,
            _ => panic!("unexpected ack"),
        };
        assert!(subscription_id > 0);

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
            Message::EventStreamHello {
                subscription_id: hello_id,
            } => assert_eq!(hello_id, subscription_id),
            other => panic!("unexpected hello: {other:?}"),
        }

        broker
            .publish("t1", "default", "orders", bytes::Bytes::from_static(b"ok"))
            .await?;
        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("event frame");
        let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
        assert_eq!(batch.payloads[0].as_ref(), b"ok");

        let _ = felix_broker::timings::take_samples();
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn handle_subscribe_message_open_uni_failure_sends_error_ack() -> Result<()> {
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
            tokio::time::sleep(Duration::from_millis(50)).await;
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
                "orders".to_string(),
                Some(900),
            )
            .await
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        drop(connection);

        let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
            .await
            .context("ack timeout")?
            .context("missing ack")?;
        match ack {
            Outgoing::Message(Message::Error { message }) => {
                assert!(!message.is_empty());
            }
            _ => panic!("expected error ack"),
        }
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn write_parts_many_writes_two_frames_in_order() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let mut send = connection.open_uni().await?;
            let _ = opened_tx.send(());
            let frames = vec![
                felix_wire::binary::encode_event_batch_parts(1, &[Bytes::from_static(b"aa")])?,
                felix_wire::binary::encode_event_batch_parts(2, &[Bytes::from_static(b"bbb")])?,
            ];
            let result = write_parts_many(&mut send, frames).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            result
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        opened_rx.await.context("uni stream not opened")?;
        let mut recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
            .await
            .context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame1 = crate::transport::quic::codec::read_frame_limited_into(
            &mut recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame1");
        let batch1 = felix_wire::binary::decode_event_batch(&frame1).expect("decode frame1");
        assert_eq!(batch1.subscription_id, 1);
        assert_eq!(batch1.payloads[0].as_ref(), b"aa");

        let frame2 = crate::transport::quic::codec::read_frame_limited_into(
            &mut recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame2");
        let batch2 = felix_wire::binary::decode_event_batch(&frame2).expect("decode frame2");
        assert_eq!(batch2.subscription_id, 2);
        assert_eq!(batch2.payloads[0].as_ref(), b"bbb");

        let total = server_task.await.context("server join")??;
        assert!(total > 0);
        Ok(())
    }

    #[tokio::test]
    async fn run_event_writer_flushes_by_count_and_deadline() -> Result<()> {
        let (tx, rx) = mpsc::channel(8);
        let config = EventWriterConfig {
            subscription_id: 44,
            max_events: 2,
            max_bytes: 1024,
            flush_delay: Duration::from_millis(20),
            single_event_mode: false,
            flush_max_items: 64,
            flush_max_delay: Duration::from_micros(200),
            max_bytes_per_write: 256 * 1024,
        };
        let (server_task, connection) = spawn_event_writer(rx, config).await?;
        tx.send(make_payload(b"a")).await?;
        tx.send(make_payload(b"b")).await?;
        let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
            .await
            .context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("count-based frame");
        let batch = felix_wire::binary::decode_event_batch(&frame).expect("decode count batch");
        assert_eq!(batch.payloads.len(), 2);
        assert_eq!(batch.payloads[0].as_ref(), b"a");
        assert_eq!(batch.payloads[1].as_ref(), b"b");

        tx.send(make_payload(b"deadline")).await?;
        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("deadline frame");
        let batch = felix_wire::binary::decode_event_batch(&frame).expect("decode deadline batch");
        assert_eq!(batch.payloads.len(), 1);
        assert_eq!(batch.payloads[0].as_ref(), b"deadline");

        drop(tx);
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_event_writer_flushes_on_channel_close() -> Result<()> {
        let (tx, rx) = mpsc::channel(4);
        let config = EventWriterConfig {
            subscription_id: 55,
            max_events: 8,
            max_bytes: 1024,
            flush_delay: Duration::from_secs(5),
            single_event_mode: false,
            flush_max_items: 64,
            flush_max_delay: Duration::from_micros(200),
            max_bytes_per_write: 256 * 1024,
        };

        let (server_task, connection) = spawn_event_writer(rx, config).await?;
        tx.send(make_payload(b"closed")).await?;
        drop(tx);
        drop(connection);
        server_task.await.context("server join")??;
        Ok(())
    }

    #[tokio::test]
    async fn run_event_writer_single_event_mode_writes_multiple_frames() -> Result<()> {
        let (tx, rx) = mpsc::channel(4);
        let config = EventWriterConfig {
            subscription_id: 66,
            max_events: 2,
            max_bytes: 1024,
            flush_delay: Duration::from_millis(10),
            single_event_mode: true,
            flush_max_items: 64,
            flush_max_delay: Duration::from_micros(200),
            max_bytes_per_write: 256 * 1024,
        };

        let (server_task, connection) = spawn_event_writer(rx, config).await?;
        let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
        tx.send(make_payload(b"one")).await?;
        tx.send(make_payload(b"two")).await?;

        let mut event_recv = accept_uni.await.context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame1 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame1");
        let batch1 = felix_wire::binary::decode_event_batch(&frame1).expect("decode batch1");
        assert_eq!(batch1.payloads.len(), 1);
        assert_eq!(batch1.payloads[0].as_ref(), b"one");

        let frame2 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame2");
        let batch2 = felix_wire::binary::decode_event_batch(&frame2).expect("decode batch2");
        assert_eq!(batch2.payloads.len(), 1);
        assert_eq!(batch2.payloads[0].as_ref(), b"two");

        drop(tx);
        server_task.await.context("server join")??;
        Ok(())
    }

    #[test]
    fn connection_subscriber_register_unregister_tracks_counts() {
        let connection_id = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
            & u128::from(u64::MAX)) as u64;

        connection_subscriber_unregister(None);
        connection_subscriber_register(Some(connection_id));
        connection_subscriber_register(Some(connection_id));
        let map = ACTIVE_SUB_CONN_COUNTS
            .get()
            .expect("counts map should be initialized");
        let count = map
            .get(&connection_id)
            .expect("connection count should exist");
        assert_eq!(*count, 2);
        drop(count);

        connection_subscriber_unregister(Some(connection_id));
        let count = map
            .get(&connection_id)
            .expect("connection count should still exist");
        assert_eq!(*count, 1);
        drop(count);

        connection_subscriber_unregister(Some(connection_id));
        assert!(map.get(&connection_id).is_none());
    }

    #[test]
    fn connection_subscriber_unregister_no_map_is_noop() {
        let connection_id = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
            & u128::from(u64::MAX)) as u64;
        connection_subscriber_unregister(Some(connection_id));
    }

    #[tokio::test]
    async fn connection_id_hash_falls_back_to_subscriber_hash_without_connection() {
        let mut config = test_config();
        config.subscriber_writer_lanes = 8;
        config.max_subscriber_writer_lanes = 8;
        config.subscriber_lane_shard = crate::config::SubscriberLaneShard::ConnectionIdHash;
        config.subscriber_single_writer_per_conn = false;
        let manager = WriterLaneManager::new(&config);

        let without_conn = manager.select_lane(555, None);
        let expected = manager.lane_for_subscriber(555);
        assert_eq!(without_conn, expected);

        let with_conn_a = manager.select_lane(555, Some(42));
        let with_conn_b = manager.select_lane(999, Some(42));
        assert_eq!(with_conn_a, with_conn_b);
    }

    #[tokio::test]
    async fn unregister_subscriber_clears_internal_maps() {
        let mut config = test_config();
        config.subscriber_lane_shard = crate::config::SubscriberLaneShard::RoundRobinPin;
        let manager = WriterLaneManager::new(&config);
        manager.subscriber_pins.insert(7, 2);
        manager.subscriber_connections.insert(7, 88);
        manager.connection_lanes.insert(88, 1);

        manager.unregister_subscriber(7, Some(88));

        assert!(manager.subscriber_pins.get(&7).is_none());
        assert!(manager.subscriber_connections.get(&7).is_none());
        assert!(manager.connection_lanes.get(&88).is_none());
    }

    #[tokio::test]
    async fn run_connection_writer_coalesces_multiple_deliveries() -> Result<()> {
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
        let subscription = broker.subscribe("t1", "default", "orders").await?;
        let (_rx, guard) = subscription.into_parts();

        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move { server.accept().await });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let client_conn = client.connect(addr, "localhost").await?;

        let connection = server_task.await.context("server join")??;
        let connection_id = connection.info().id.0;
        let event_send = connection.open_uni().await?;

        let (tx, rx) = mpsc::channel(8);
        let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, 64 * 1024));

        tx.send(ConnectionCommand::Register {
            subscriber_id: 1,
            connection: connection.clone(),
            connection_id: Some(connection_id),
            event_send,
            guard,
        })
        .await
        .context("register")?;

        let frame_a = felix_wire::binary::encode_event_batch_parts(1, &[Bytes::from_static(b"a")])?;
        let frame_b =
            felix_wire::binary::encode_event_batch_parts(1, &[Bytes::from_static(b"bb")])?;
        let now = Instant::now();
        tx.send(ConnectionCommand::Delivery {
            subscriber_id: 1,
            frame_parts: frame_a,
            item_count: 1,
            first_enqueued_at: now,
            enqueue_at: now,
        })
        .await
        .context("delivery a")?;
        tx.send(ConnectionCommand::Delivery {
            subscriber_id: 1,
            frame_parts: frame_b,
            item_count: 1,
            first_enqueued_at: now,
            enqueue_at: now,
        })
        .await
        .context("delivery b")?;

        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
            .await
            .context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame1 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame1");
        let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
        assert_eq!(batch1.payloads[0].as_ref(), b"a");
        let frame2 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame2");
        let batch2 = felix_wire::binary::decode_event_batch(&frame2).context("decode batch2")?;
        assert_eq!(batch2.payloads[0].as_ref(), b"bb");

        drop(tx);
        writer_task.await.context("writer join")?;
        let _ = crate::timings::take_samples();
        Ok(())
    }

    #[tokio::test]
    async fn run_connection_writer_handles_write_error() -> Result<()> {
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
        let subscription = broker.subscribe("t1", "default", "orders").await?;
        let (_rx, guard) = subscription.into_parts();

        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move { server.accept().await });
        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let client_conn = client.connect(addr, "localhost").await?;

        let connection = server_task.await.context("server join")??;
        let connection_id = connection.info().id.0;
        let event_send = connection.open_uni().await?;
        drop(client_conn);

        let (tx, rx) = mpsc::channel(8);
        let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, 64 * 1024));

        tx.send(ConnectionCommand::Register {
            subscriber_id: 1,
            connection: connection.clone(),
            connection_id: Some(connection_id),
            event_send,
            guard,
        })
        .await
        .context("register")?;

        let frame = felix_wire::binary::encode_event_batch_parts(1, &[Bytes::from_static(b"a")])?;
        let now = Instant::now();
        tx.send(ConnectionCommand::Delivery {
            subscriber_id: 1,
            frame_parts: frame,
            item_count: 1,
            first_enqueued_at: now,
            enqueue_at: now,
        })
        .await
        .context("delivery")?;

        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(tx);
        writer_task.await.context("writer join")?;
        let _ = crate::timings::take_samples();
        Ok(())
    }

    #[tokio::test]
    async fn run_connection_writer_unregister_drops_late_deliveries() -> Result<()> {
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
        let subscription = broker.subscribe("t1", "default", "orders").await?;
        let (_rx, guard) = subscription.into_parts();

        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move { server.accept().await });
        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let client_conn = client.connect(addr, "localhost").await?;

        let connection = server_task.await.context("server join")??;
        let connection_id = connection.info().id.0;
        let event_send = connection.open_uni().await?;

        let (tx, rx) = mpsc::channel(8);
        let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, 64 * 1024));

        tx.send(ConnectionCommand::Register {
            subscriber_id: 1,
            connection: connection.clone(),
            connection_id: Some(connection_id),
            event_send,
            guard,
        })
        .await
        .context("register")?;

        let now = Instant::now();
        let frame = felix_wire::binary::encode_event_batch_parts(1, &[Bytes::from_static(b"a")])?;
        tx.send(ConnectionCommand::Delivery {
            subscriber_id: 1,
            frame_parts: frame,
            item_count: 1,
            first_enqueued_at: now,
            enqueue_at: now,
        })
        .await
        .context("delivery")?;

        let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
            .await
            .context("accept uni timeout")??;
        let mut scratch = BytesMut::new();
        let frame1 = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        )
        .await?
        .expect("frame1");
        let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
        assert_eq!(batch1.payloads[0].as_ref(), b"a");

        tx.send(ConnectionCommand::Unregister { subscriber_id: 1 })
            .await
            .context("unregister")?;

        let late = felix_wire::binary::encode_event_batch_parts(1, &[Bytes::from_static(b"late")])?;
        tx.send(ConnectionCommand::Delivery {
            subscriber_id: 1,
            frame_parts: late,
            item_count: 1,
            first_enqueued_at: now,
            enqueue_at: now,
        })
        .await
        .context("late delivery")?;

        let no_frame = tokio::time::timeout(
            Duration::from_millis(150),
            crate::transport::quic::codec::read_frame_limited_into(
                &mut event_recv,
                16 * 1024,
                &mut scratch,
            ),
        )
        .await;
        match no_frame {
            Err(_) => {}
            Ok(Ok(None)) => {}
            Ok(Ok(Some(_))) => panic!("unexpected late frame"),
            Ok(Err(err)) => return Err(err),
        }

        drop(tx);
        writer_task.await.context("writer join")?;
        let _ = crate::timings::take_samples();
        Ok(())
    }
}
