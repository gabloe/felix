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
use crate::transport::quic::telemetry::{t_counter, t_histogram};
use crate::transport::quic::{ACK_QUEUE_DEPTH, ACK_WAITERS_MAX, GLOBAL_ACK_DEPTH};

use super::ack_waiter::run_ack_waiter_loop;
use super::control::run_control_loop;
use super::uni::run_uni_loop;
use super::writer::run_writer_loop;

#[cfg(test)]
use super::hooks::test_hooks;

pub(crate) async fn handle_stream(
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: BrokerConfig,
    publish_ctx: PublishContext,
    send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    // Bi-directional stream: carries control-plane requests and their acks/responses.
    // Payload fanout to subscribers happens on separate uni streams opened per subscription.
    // Outbound response queue:
    // We never write to `send` directly from the read loop. All responses go through this queue and
    // are drained by a single writer task to avoid concurrent SendStream writes.
    // Single-writer response path: enqueue Outgoing messages, one task serializes writes.
    let (out_ack_tx, out_ack_rx) = mpsc::channel::<Outgoing>(ACK_QUEUE_DEPTH);
    let out_ack_depth = Arc::new(AtomicUsize::new(0));
    let stream_cache = HashMap::new();
    let stream_cache_key = String::new();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(ACK_WAITERS_MAX));
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let mut frame_scratch = BytesMut::with_capacity(config.max_frame_bytes.min(64 * 1024));
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel::<AckWaiterMessage>(ACK_WAITERS_MAX);
    let ack_wait_timeout = Duration::from_millis(config.ack_wait_timeout_ms);

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
    // Single writer task:
    // - Owns the SendStream and performs *all* writes, ensuring ordered, non-overlapping writes.
    // - Applies backpressure signaling via ack_throttle when the outbound queue crosses watermarks.
    // - On any write/encode error, we cancel the control stream (cooperative shutdown) and finish.
    // Correctness note: if the writer exits, any pending Outgoing messages in the channel are dropped;
    // teardown must reconcile depth gauges via `reset_local_depth_only`.
    let writer_handle = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        out_ack_depth_worker,
        ack_throttle_tx_writer,
        cancel_tx_writer,
        cancel_rx_writer,
    ));

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
    let graceful_close = matches!(result, Ok(true));
    if !graceful_close {
        let _ = cancel_tx.send(true);
    }
    drop(out_ack_tx);
    drop(ack_waiter_tx);
    #[cfg(feature = "telemetry")]
    let drain_start = Instant::now();
    let drain_timeout = Duration::from_millis(config.control_stream_drain_timeout_ms);
    let mut writer_handle = writer_handle;
    let mut ack_waiter_handle = ack_waiter_handle;
    let mut timed_out = false;
    tokio::select! {
        _ = tokio::time::sleep(drain_timeout) => {
            timed_out = true;
        }
        _ = &mut writer_handle => {}
        _ = &mut ack_waiter_handle => {}
    }
    #[cfg(feature = "telemetry")]
    {
        t_histogram!("felix_broker_control_stream_drain_ms")
            .record(drain_start.elapsed().as_secs_f64() * 1000.0);
    }
    #[cfg(test)]
    if test_hooks::force_drain_timeout() {
        timed_out = true;
    }
    if timed_out {
        t_counter!("felix_broker_control_stream_drain_timeout_total").increment(1);
        writer_handle.abort();
        ack_waiter_handle.abort();
    }
    reset_local_depth_only(
        &out_ack_depth,
        &GLOBAL_ACK_DEPTH,
        "felix_broker_out_ack_depth",
    );
    result.map(|_| ())
}

pub(crate) async fn handle_uni_stream(
    broker: Arc<Broker>,
    config: BrokerConfig,
    publish_ctx: PublishContext,
    mut recv: RecvStream,
) -> Result<()> {
    let mut frame_scratch = BytesMut::with_capacity(config.max_frame_bytes.min(64 * 1024));
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
