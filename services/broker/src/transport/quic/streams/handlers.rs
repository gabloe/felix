//! This module owns stream lifecycle orchestration for QUIC streams at the broker.
//!
//! The control stream is bi-directional and multiplexes control-plane requests,
//! request/response cache operations, and publish acknowledgments.
//!
//! Uni streams are data-plane publish streams (ingress-only) used for high-throughput publishing.
//!
//! Key invariants maintained here include:
//! - Single-writer for each SendStream to avoid concurrent writes.
//! - Bounded queues and backpressure signaling to prevent resource exhaustion.
//! - Cooperative cancellation propagation across tasks.
//! - Graceful drain behavior on stream shutdown.
//!
//! The heavy logic lives in the `writer` task (serializes all writes),
//! the `control` loop (handles control-plane messages),
//! the `ack_waiter` (manages delayed acknowledgments), and the `uni` loop (handles uni-directional publish streams).

// Import dependencies necessary for wiring and orchestration of stream tasks.
// These imports support spawning background tasks, managing concurrency primitives,
// and buffering data for efficient stream processing. Protocol-specific logic
// lives elsewhere; this module focuses on composing those pieces together.
use anyhow::Result;
use bytes::BytesMut;
use felix_broker::Broker;
use quinn::{RecvStream, SendStream};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore, mpsc, watch};

use crate::config::BrokerConfig;
use crate::transport::quic::handlers::publish::{
    AckTimeoutState, AckWaiterMessage, Outgoing, PublishContext, reset_local_depth_only,
};
use crate::transport::quic::telemetry::t_counter;
#[cfg(feature = "telemetry")]
use crate::transport::quic::telemetry::t_histogram;
use crate::transport::quic::{ACK_QUEUE_DEPTH, ACK_WAITERS_MAX, GLOBAL_ACK_DEPTH};

use super::ack_waiter::run_ack_waiter_loop;
use super::control::run_control_loop;
use super::uni::run_uni_loop;
use super::writer::run_writer_loop;

#[cfg(test)]
use super::hooks::test_hooks;

/// Handles a bi-directional control stream for a QUIC connection.
///
/// This handler is responsible for managing the lifecycle of the control stream,
/// which multiplexes control-plane requests, request/response cache operations,
/// and publish acknowledgments.
///
/// Concurrency model:
/// - Spawns a dedicated writer task to serialize all outbound writes to the SendStream.
/// - Optionally spawns an ack waiter task to handle delayed publish acknowledgments.
/// - Runs the control loop in the current task to process incoming frames.
///
/// Cancellation and drain semantics:
/// - Cancellation signals propagate via watch channels to all spawned tasks.
/// - On stream closure or error, tasks are given a drain period to finish cleanly,
///   after which they are aborted if still running.
///
/// "Graceful close" means the remote peer cleanly finished the stream (EOF).
/// Any other outcome (error, protocol violation) is considered an abnormal close.
///
/// The single-writer design ensures safety and performance by avoiding concurrent
/// SendStream writes, which are not supported by Quinn's API.
pub(crate) async fn handle_stream(
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: BrokerConfig,
    publish_ctx: PublishContext,
    send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    // Outbound ack/response queue:
    // We never write directly to the SendStream from multiple tasks to avoid concurrent writes.
    // Instead, all outbound messages are enqueued here and drained by a single writer task.
    let (out_ack_tx, out_ack_rx) = mpsc::channel::<Outgoing>(ACK_QUEUE_DEPTH);

    // Tracks the current depth of the outbound ack/response queue.
    // Used to apply backpressure and update telemetry gauges.
    // This local depth is reconciled with the global ACK depth gauge.
    let out_ack_depth = Arc::new(AtomicUsize::new(0));

    // Per-connection cache used by publish handlers to avoid repeated lookups.
    // This cache stores stream existence or metadata for hot-path optimization.
    let stream_cache = HashMap::new();
    let stream_cache_key = String::new();

    // Cancellation channel to propagate shutdown signals to all tasks (writer, waiter, control loop).
    // Sending `true` signals cancellation; all tasks watch this channel.
    let (cancel_tx, cancel_rx) = watch::channel(false);

    // Semaphore bounding the number of in-flight ack-on-commit waiters.
    // Prevents unbounded memory usage by limiting concurrent ack waiters.
    let ack_waiters = Arc::new(Semaphore::new(ACK_WAITERS_MAX));

    // Watch channel for writer-driven backpressure signaling.
    // When the outbound queue crosses watermarks, the writer signals the control loop
    // to throttle sending new messages.
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);

    // Shared mutable state tracking ack enqueue failures and timeouts.
    // Used by the ack waiter to decide when to throttle or cancel the connection.
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));

    // Reusable buffer for encoding/decoding frames.
    // Sized defensively up to the configured max frame size or 64KiB.
    // Reduces allocation overhead on the hot path.
    let mut frame_scratch = BytesMut::with_capacity(config.max_frame_bytes.min(64 * 1024));

    // Channel for sending ack waiter messages.
    // Only meaningful when ack_on_commit is enabled, but safe to always use.
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel::<AckWaiterMessage>(ACK_WAITERS_MAX);

    // Configured ack wait timeout duration.
    let ack_wait_timeout = Duration::from_millis(config.ack_wait_timeout_ms);

    // Clone Arcs and watch channels before spawning tasks to move ownership into
    // the spawned futures and avoid borrow checker issues.
    // This also keeps ownership clear and explicit across async boundaries.
    let out_ack_depth_worker = Arc::clone(&out_ack_depth);
    let cancel_tx_writer = cancel_tx.clone();
    let cancel_rx_writer = cancel_rx.clone();
    let ack_throttle_tx_writer = ack_throttle_tx.clone();

    let out_ack_tx_waiter = out_ack_tx.clone();
    let out_ack_depth_waiter = Arc::clone(&out_ack_depth);
    let ack_throttle_tx_waiter = ack_throttle_tx.clone();
    let ack_timeout_state_waiter = Arc::clone(&ack_timeout_state);
    let cancel_tx_waiter = cancel_tx.clone();
    let cancel_rx_waiter = cancel_rx.clone();

    // Spawn the single writer task:
    // - Owns the SendStream and performs *all* writes, ensuring ordered, non-overlapping writes.
    // - Applies backpressure signaling via ack_throttle when the outbound queue crosses watermarks.
    // - On any write or encode error, cancels the control stream (cooperative shutdown) and exits.
    // Correctness: if the writer exits, any pending Outgoing messages in the channel are dropped;
    // teardown must reconcile depth gauges via `reset_local_depth_only`.
    let writer_handle = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        out_ack_depth_worker,
        ack_throttle_tx_writer,
        cancel_tx_writer,
        cancel_rx_writer,
    ));

    // Spawn the ack waiter task:
    // - Only relevant when ack_on_commit mode is enabled.
    // - Decouples publish commit completion from the read loop.
    // - Allows out-of-order acknowledgment emission to improve throughput.
    let ack_waiter_handle = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx_waiter,
        out_ack_depth_waiter,
        ack_throttle_tx_waiter,
        ack_timeout_state_waiter,
        cancel_tx_waiter,
        cancel_rx_waiter,
        ack_wait_timeout,
    ));

    // Run the control loop in the current task:
    // - Processes incoming control frames from the RecvStream.
    // - Handles control-plane requests, publish operations, and manages the outbound queue.
    // - Returns Ok(true) on graceful close (EOF from remote),
    //   Ok(false) on protocol violation or handler-initiated close,
    //   Err on fatal decode or protocol errors.
    let result = run_control_loop(
        &mut recv,
        Arc::clone(&broker),
        connection.clone(),
        config.clone(),
        publish_ctx,
        stream_cache,
        stream_cache_key,
        out_ack_tx.clone(),
        Arc::clone(&out_ack_depth),
        ack_throttle_rx.clone(),
        ack_throttle_tx.clone(),
        Arc::clone(&ack_timeout_state),
        cancel_tx.clone(),
        cancel_rx.clone(),
        Arc::clone(&ack_waiters),
        ack_waiter_tx.clone(),
        ack_wait_timeout,
        &mut frame_scratch,
    )
    .await;

    // Determine if the close was graceful (EOF from remote).
    let graceful_close = matches!(result, Ok(true));
    if !graceful_close {
        // Signal cancellation to all spawned tasks on abnormal close.
        let _ = cancel_tx.send(true);
    }

    // Drop outbound sender channels to allow background tasks to exit gracefully.
    drop(out_ack_tx);
    drop(ack_waiter_tx);

    #[cfg(feature = "telemetry")]
    let drain_start = Instant::now();

    // Drain timeout duration for background task shutdown.
    let drain_timeout = Duration::from_millis(config.control_stream_drain_timeout_ms);

    let mut writer_handle = writer_handle;
    let mut ack_waiter_handle = ack_waiter_handle;
    let mut timed_out = false;

    // Wait for background tasks to finish or drain timeout.
    tokio::select! {
        _ = tokio::time::sleep(drain_timeout) => {
            timed_out = true;
        }
        _ = &mut writer_handle => {}
        _ = &mut ack_waiter_handle => {}
    }

    #[cfg(feature = "telemetry")]
    {
        // Record the drain duration in telemetry histogram.
        t_histogram!("felix_broker_control_stream_drain_ms")
            .record(drain_start.elapsed().as_secs_f64() * 1000.0);
    }

    #[cfg(test)]
    if test_hooks::force_drain_timeout() {
        // In tests, optionally force a drain timeout to exercise abort paths.
        timed_out = true;
    }

    if timed_out {
        // Increment telemetry counter for drain timeouts.
        t_counter!("felix_broker_control_stream_drain_timeout_total").increment(1);
        // Abort background tasks if they failed to exit in time.
        writer_handle.abort();
        ack_waiter_handle.abort();
    }

    // Reset local depth gauges to avoid leaks, even on abort.
    reset_local_depth_only(
        &out_ack_depth,
        &GLOBAL_ACK_DEPTH,
        "felix_broker_out_ack_depth",
    );

    result.map(|_| ())
}

/// Handles an ingress-only uni-directional publish stream.
///
/// This handler processes uni-directional streams that contain only publish messages or batches.
/// There is no response path on uni streams; protocol violations result in stream closure.
///
/// Uses a reusable scratch buffer similarly to the control stream handler.
/// The `run_uni_loop` drives reads until EOF or protocol violation.
pub(crate) async fn handle_uni_stream(
    broker: Arc<Broker>,
    config: BrokerConfig,
    publish_ctx: PublishContext,
    mut recv: RecvStream,
) -> Result<()> {
    // Reusable scratch buffer to reduce allocations on the hot path.
    let mut frame_scratch = BytesMut::with_capacity(config.max_frame_bytes.min(64 * 1024));

    // Run the uni-directional publish loop, which reads until EOF or violation.
    run_uni_loop(
        &mut recv,
        broker,
        config,
        publish_ctx,
        HashMap::new(),
        String::new(),
        &mut frame_scratch,
    )
    .await
}
