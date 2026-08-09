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
//! - Handing the broker-provided shared batch queue directly to the writer task.
//! - Running lane-sharded event writers that preserve per-subscriber ordering.
//!
//! ## Buffering and drops
//! The broker core owns bounded per-subscriber queues and uses `try_send` for fanout:
//! - If a subscriber queue is full, that delivery batch is **dropped** and
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
//! Multi-event publishes are encoded once as subscriber-independent binary `EventBatch` frames.
//! `EventStreamHello` provides the subscription binding, so the same frame bytes can be shared
//! across every subscriber.
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
//
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
use felix_wire::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use super::publish::{Outgoing, SubscriptionLimiter, send_outgoing_critical};
use crate::transport::quic::SUBSCRIPTION_ID;
use crate::transport::quic::codec::write_message;

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
    // On failure, respond on the control stream (through the ack queue) and keep the stream alive.
    let subscription = match broker.subscribe(&tenant_id, &namespace, &stream).await {
        Ok(subscription) => subscription,
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
