// QUIC stream handlers for control and uni-directional publish streams.
use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_broker::Broker;
use felix_wire::Message;
use futures::{StreamExt, stream::FuturesUnordered};
use quinn::{RecvStream, SendStream};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore, mpsc, watch};

use crate::config::BrokerConfig;
use crate::timings;

use super::codec::read_frame_limited_into;
use super::errors::{AckEnqueueError, record_ack_enqueue_failure};
use super::handlers::publish::{
    AckTimeoutState, AckWaiterMessage, AckWaiterResult, Outgoing, PublishContext, decrement_depth,
    handle_ack_enqueue_result, handle_binary_publish_batch_control,
    handle_binary_publish_batch_uni, handle_publish_batch_message,
    handle_publish_batch_message_uni, handle_publish_message, handle_publish_message_uni,
    reset_local_depth_only, send_outgoing_best_effort, send_outgoing_critical,
};
use super::handlers::subscribe::handle_subscribe_message;
use super::telemetry::{t_counter, t_histogram, t_now_if, t_should_sample};
use super::{ACK_HI_WATER, ACK_LO_WATER, ACK_QUEUE_DEPTH, ACK_WAITERS_MAX, GLOBAL_ACK_DEPTH};

pub(crate) async fn handle_stream(
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: BrokerConfig,
    publish_ctx: PublishContext,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    // Bi-directional stream: carries control-plane requests and their acks/responses.
    // Payload fanout to subscribers happens on separate uni streams opened per subscription.
    // Outbound response queue:
    // We never write to `send` directly from the read loop. All responses go through this queue and
    // are drained by a single writer task to avoid concurrent SendStream writes.
    // Single-writer response path: enqueue Outgoing messages, one task serializes writes.
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel::<Outgoing>(ACK_QUEUE_DEPTH);
    let out_ack_depth = Arc::new(AtomicUsize::new(0));
    let ack_on_commit = config.ack_on_commit;
    let mut stream_cache = HashMap::new();
    let mut stream_cache_key = String::new();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(ACK_WAITERS_MAX));
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let mut frame_scratch = BytesMut::with_capacity(config.max_frame_bytes.min(64 * 1024));
    let (ack_waiter_tx, mut ack_waiter_rx) = mpsc::channel::<AckWaiterMessage>(ACK_WAITERS_MAX);
    let ack_wait_timeout = Duration::from_millis(config.ack_wait_timeout_ms);

    let out_ack_depth_worker = Arc::clone(&out_ack_depth);
    let cancel_tx_writer = cancel_tx.clone();
    let mut cancel_rx_writer = cancel_rx.clone();
    let ack_throttle_tx_writer = ack_throttle_tx.clone();
    let out_ack_tx_waiter = out_ack_tx.clone();
    let out_ack_depth_waiter = Arc::clone(&out_ack_depth);
    let ack_throttle_tx_waiter = ack_throttle_tx.clone();
    let ack_timeout_state_waiter = Arc::clone(&ack_timeout_state);
    let cancel_tx_waiter = cancel_tx.clone();
    let mut cancel_rx_waiter = cancel_rx.clone();
    // Single writer task:
    // - Owns the SendStream and performs *all* writes, ensuring ordered, non-overlapping writes.
    // - Applies backpressure signaling via ack_throttle when the outbound queue crosses watermarks.
    // - On any write/encode error, we cancel the control stream (cooperative shutdown) and finish.
    // Correctness note: if the writer exits, any pending Outgoing messages in the channel are dropped;
    // teardown must reconcile depth gauges via `reset_local_depth_only`.
    let writer_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = cancel_rx_writer.changed() => {
                    if changed.is_err() || *cancel_rx_writer.borrow() {
                        break;
                    }
                }
                outgoing = out_ack_rx.recv() => {
                    let Some(outgoing) = outgoing else { break };
                    match outgoing {
                        Outgoing::Message(message) => {
                            let sample = t_should_sample();
                            let is_publish_ack = matches!(
                                message,
                                Message::PublishOk { .. } | Message::PublishError { .. }
                            );
                            #[cfg(not(feature = "telemetry"))]
                            let _ = is_publish_ack;
                            let write_start = t_now_if(sample);
                            if let Err(err) = super::codec::write_message(&mut send, message).await {
                                let _ = decrement_depth(
                                    &out_ack_depth_worker,
                                    &GLOBAL_ACK_DEPTH,
                                    "felix_broker_out_ack_depth",
                                );
                                tracing::info!(error = %err, "quic response stream closed");
                                let _ = ack_throttle_tx_writer.send(false);
                                let _ = cancel_tx_writer.send(true);
                                break;
                            }
                            let depth_update = decrement_depth(
                                &out_ack_depth_worker,
                                &GLOBAL_ACK_DEPTH,
                                "felix_broker_out_ack_depth",
                            );
                            if let Some((prev, cur)) = depth_update
                                && prev >= ACK_HI_WATER
                                && cur < ACK_LO_WATER
                            {
                                let _ = ack_throttle_tx_writer.send(false);
                            }
                            if let Some(start) = write_start {
                                let write_ns = start.elapsed().as_nanos() as u64;
                                timings::record_quic_write_ns(write_ns);
                                t_histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
                            }
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = super::telemetry::frame_counters();
                                counters.ack_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                if is_publish_ack {
                                    counters.ack_items_out_ok.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Outgoing::CacheMessage(message) => {
                            let sample = t_should_sample();
                            let encode_start = t_now_if(sample);
                            let frame = match message.encode().context("encode message") {
                                Ok(frame) => frame,
                                Err(err) => {
                                    // Dequeue happened; decrement depth even if encode fails.
                                    let _ = decrement_depth(
                                        &out_ack_depth_worker,
                                        &GLOBAL_ACK_DEPTH,
                                        "felix_broker_out_ack_depth",
                                    );
                                    tracing::info!(error = %err, "encode cache response failed");
                                    let _ = ack_throttle_tx_writer.send(false);
                                    let _ = cancel_tx_writer.send(true);
                                    break;
                                }
                            };
                            if let Some(start) = encode_start {
                                let encode_ns = start.elapsed().as_nanos() as u64;
                                timings::record_cache_encode_ns(encode_ns);
                            }
                            let write_start = t_now_if(sample);
                            if let Err(err) = super::codec::write_frame(&mut send, &frame).await {
                                let _ = decrement_depth(
                                    &out_ack_depth_worker,
                                    &GLOBAL_ACK_DEPTH,
                                    "felix_broker_out_ack_depth",
                                );
                                tracing::info!(error = %err, "quic response stream closed");
                                let _ = ack_throttle_tx_writer.send(false);
                                let _ = cancel_tx_writer.send(true);
                                break;
                            }
                            let depth_update = decrement_depth(
                                &out_ack_depth_worker,
                                &GLOBAL_ACK_DEPTH,
                                "felix_broker_out_ack_depth",
                            );
                            if let Some((prev, cur)) = depth_update
                                && prev >= ACK_HI_WATER
                                && cur < ACK_LO_WATER
                            {
                                let _ = ack_throttle_tx_writer.send(false);
                            }
                            if let Some(start) = write_start {
                                let write_ns = start.elapsed().as_nanos() as u64;
                                timings::record_cache_write_ns(write_ns);
                            }
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = super::telemetry::frame_counters();
                                counters.ack_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }
        let _ = send.finish();
    });

    let ack_waiter_handle = tokio::spawn(async move {
        // Ack waiter task (only meaningful when ack_on_commit=true):
        // We decouple "publish committed" from the control stream read loop.
        // - Each acked publish creates a oneshot receiver that completes when the publish worker
        //   finishes the broker call.
        // - FuturesUnordered allows acks to be emitted as commits complete (out-of-order allowed).
        // - A semaphore bounds the number of in-flight waiters to avoid unbounded memory growth.
        // Correctness note: every waiter must drop its semaphore permit on *all* paths.
        let mut pending = FuturesUnordered::new();
        let mut closed = false;
        loop {
            tokio::select! {
                changed = cancel_rx_waiter.changed() => {
                    if changed.is_err() || *cancel_rx_waiter.borrow() {
                        break;
                    }
                }
                message = ack_waiter_rx.recv() => {
                    match message {
                        Some(message) => {
                            let mut cancel_rx = cancel_rx_waiter.clone();
                            let fut = async move {
                                match message {
                                    AckWaiterMessage::Publish {
                                        request_id,
                                        payload_len,
                                        start,
                                        response_rx,
                                        permit,
                                    } => {
                                        let response = tokio::select! {
                                            _ = cancel_rx.changed() => return None,
                                            response = tokio::time::timeout(ack_wait_timeout, response_rx) => response,
                                        };
                                        drop(permit);
                                        match response {
                                            Ok(response) => Some(AckWaiterResult::Publish {
                                                request_id,
                                                payload_len,
                                                start,
                                                response,
                                            }),
                                            Err(_) => Some(AckWaiterResult::PublishTimeout {
                                                request_id,
                                                start,
                                            }),
                                        }
                                    }
                                    AckWaiterMessage::PublishBatch {
                                        request_id,
                                        payload_bytes,
                                        response_rx,
                                        permit,
                                    } => {
                                        let response = tokio::select! {
                                            _ = cancel_rx.changed() => return None,
                                            response = tokio::time::timeout(ack_wait_timeout, response_rx) => response,
                                        };
                                        drop(permit);
                                        match response {
                                            Ok(response) => Some(AckWaiterResult::PublishBatch {
                                                request_id,
                                                payload_bytes,
                                                response,
                                            }),
                                            Err(_) => Some(AckWaiterResult::PublishBatchTimeout {
                                                request_id,
                                                payload_bytes,
                                            }),
                                        }
                                    }
                                }
                            };
                            pending.push(fut);
                        }
                        None => {
                            closed = true;
                        }
                    }
                }
                result = pending.next(), if !pending.is_empty() => {
                    if let Some(result) = result.flatten() {
                        match result {
                            AckWaiterResult::Publish {
                                request_id,
                                payload_len,
                                start,
                                response,
                            } => {
                                super::telemetry::t_consume_instant(start);
                                match response {
                                    Ok(Ok(_)) => {
                                        t_counter!("felix_publish_requests_total", "result" => "ok")
                                            .increment(1);
                                        t_counter!("felix_publish_bytes_total")
                                            .increment(payload_len);
                                        #[cfg(feature = "telemetry")]
                                        {
                                            t_histogram!(
                                                "felix_publish_latency_ms",
                                                "mode" => "commit"
                                            )
                                            .record(start.elapsed().as_secs_f64() * 1000.0);
                                        }
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishOk { request_id }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                    Ok(Err(err)) => {
                                        t_counter!("felix_publish_requests_total", "result" => "error")
                                            .increment(1);
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishError {
                                                    request_id,
                                                    message: err.to_string(),
                                                }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                    Err(_) => {
                                        t_counter!("felix_publish_requests_total", "result" => "error")
                                            .increment(1);
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishError {
                                                    request_id,
                                                    message: "publish worker dropped response".to_string(),
                                                }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                }
                            }
                            AckWaiterResult::PublishTimeout { request_id, start } => {
                                super::telemetry::t_consume_instant(start);
                                t_counter!(
                                    "felix_publish_requests_total",
                                    "result" => "error"
                                )
                                .increment(1);
                                t_counter!("felix_broker_ack_waiter_timeout_total")
                                    .increment(1);
                                #[cfg(feature = "telemetry")]
                                {
                                    t_histogram!(
                                        "felix_publish_latency_ms",
                                        "mode" => "commit"
                                    )
                                    .record(start.elapsed().as_secs_f64() * 1000.0);
                                }
                                if let Err(err) = handle_ack_enqueue_result(
                                    send_outgoing_critical(
                                        &out_ack_tx_waiter,
                                        &out_ack_depth_waiter,
                                        "felix_broker_out_ack_depth",
                                        &ack_throttle_tx_waiter,
                                        Outgoing::Message(Message::PublishError {
                                            request_id,
                                            message: "publish commit timeout".to_string(),
                                        }),
                                    )
                                    .await,
                                    &ack_timeout_state_waiter,
                                    &ack_throttle_tx_waiter,
                                    &cancel_tx_waiter,
                                )
                                .await
                                {
                                    tracing::info!(error = %err, "ack enqueue failed");
                                }
                            }
                            AckWaiterResult::PublishBatch {
                                request_id,
                                payload_bytes,
                                response,
                            } => {
                                match response {
                                    Ok(Ok(_)) => {
                                        t_counter!(
                                            "felix_publish_requests_total",
                                            "result" => "ok"
                                        )
                                        .increment(1);
                                        for bytes in &payload_bytes {
                                            t_counter!("felix_publish_bytes_total")
                                                .increment(*bytes as u64);
                                        }
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishOk { request_id }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                    Ok(Err(err)) => {
                                        t_counter!("felix_publish_requests_total", "result" => "error")
                                            .increment(1);
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishError {
                                                    request_id,
                                                    message: err.to_string(),
                                                }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                    Err(_) => {
                                        t_counter!("felix_publish_requests_total", "result" => "error")
                                            .increment(1);
                                        if let Err(err) = handle_ack_enqueue_result(
                                            send_outgoing_critical(
                                                &out_ack_tx_waiter,
                                                &out_ack_depth_waiter,
                                                "felix_broker_out_ack_depth",
                                                &ack_throttle_tx_waiter,
                                                Outgoing::Message(Message::PublishError {
                                                    request_id,
                                                    message: "publish batch worker dropped response".to_string(),
                                                }),
                                            )
                                            .await,
                                            &ack_timeout_state_waiter,
                                            &ack_throttle_tx_waiter,
                                            &cancel_tx_waiter,
                                        )
                                        .await
                                        {
                                            tracing::info!(error = %err, "ack enqueue failed");
                                        }
                                    }
                                }
                            }
                            AckWaiterResult::PublishBatchTimeout {
                                request_id,
                                payload_bytes,
                            } => {
                                t_counter!(
                                    "felix_publish_requests_total",
                                    "result" => "error"
                                )
                                .increment(1);
                                t_counter!("felix_broker_ack_waiter_timeout_total")
                                    .increment(1);
                                for bytes in &payload_bytes {
                                    t_counter!("felix_publish_bytes_total")
                                        .increment(*bytes as u64);
                                }
                                if let Err(err) = handle_ack_enqueue_result(
                                    send_outgoing_critical(
                                        &out_ack_tx_waiter,
                                        &out_ack_depth_waiter,
                                        "felix_broker_out_ack_depth",
                                        &ack_throttle_tx_waiter,
                                        Outgoing::Message(Message::PublishError {
                                            request_id,
                                            message: "publish commit timeout".to_string(),
                                        }),
                                    )
                                    .await,
                                    &ack_timeout_state_waiter,
                                    &ack_throttle_tx_waiter,
                                    &cancel_tx_waiter,
                                )
                                .await
                                {
                                    tracing::info!(error = %err, "ack enqueue failed");
                                }
                            }
                        }
                    }
                }
            }
            if closed && pending.is_empty() {
                break;
            }
        }
    });
    // Read loop: decode control-plane frames and enqueue work; acks flow back on this stream.
    // Shutdown/drain note:
    // This function spawns background tasks (writer + ack-waiter). On exit we must ensure:
    // - both tasks are either joined or aborted
    // - local depth counters are reconciled
    // Otherwise we can leak detached tasks or leave gauges permanently high.
    let result: Result<bool> = (async {
        let mut graceful_close = false;
        let mut cancel_rx_read = cancel_rx.clone();
        loop {
            if *cancel_rx_read.borrow() {
                break;
            }
            // Throttling is driven by outbound ack queue depth. When outbound acks back up, we shed
            // load by rejecting/dropping new publish requests early (before doing expensive work).
            // This protects the single writer from unbounded backlog.
            let throttled = *ack_throttle_rx.borrow();
            let sample = t_should_sample();
            let read_start = t_now_if(sample);
            let frame = tokio::select! {
                changed = cancel_rx_read.changed() => {
                    if changed.is_err() || *cancel_rx_read.borrow() {
                        break;
                    }
                    continue;
                }
                frame = read_frame_limited_into(&mut recv, config.max_frame_bytes, &mut frame_scratch) => {
                    match frame? {
                        Some(frame) => frame,
                        None => {
                            graceful_close = true;
                            break;
                        }
                    }
                }
            };
            let read_ns = read_start.map(|start| start.elapsed().as_nanos() as u64);
            // Binary batches bypass JSON decoding for high-throughput publish.
            if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
                handle_binary_publish_batch_control(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &publish_ctx,
                    &frame,
                    sample,
                )
                .await?;
                continue;
            }
            let decode_start = t_now_if(sample);
            // Control-plane protocol: decode one frame into a typed Message, then route below.
            // Publish vs PublishBatch determines whether we ingest one payload or a batch and
            // which ack mode we honor (none, per-message, per-batch). If ack_on_commit is set,
            // we delay the ack until the publish worker completes (server-side policy, not a
            // distinct wire AckMode).
            let message = match Message::decode(frame.clone()).context("decode message") {
                Ok(message) => message,
                Err(err) => {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = super::telemetry::frame_counters();
                        counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                        counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                        counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                    }
                    super::telemetry::log_decode_error("control_message", &err, &frame);
                    return Err(err);
                }
            };
            let decode_ns = decode_start.map(|start| start.elapsed().as_nanos() as u64);
            if let Some(decode_ns) = decode_ns {
                timings::record_decode_ns(decode_ns);
                t_histogram!("felix_broker_decode_ns").record(decode_ns as f64);
            }
            match message {
                Message::Publish {
                    tenant_id,
                    namespace,
                    stream,
                    payload,
                    request_id,
                    ack,
                } => {
                    handle_publish_message(
                        &broker,
                        &publish_ctx,
                        &mut stream_cache,
                        &mut stream_cache_key,
                        throttled,
                        ack_on_commit,
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        &ack_waiters,
                        &ack_waiter_tx,
                        ack_wait_timeout,
                        tenant_id,
                        namespace,
                        stream,
                        payload,
                        request_id,
                        ack,
                        sample,
                    )
                    .await?;
                }
                Message::PublishBatch {
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    request_id,
                    ack,
                } => {
                    handle_publish_batch_message(
                        &broker,
                        &publish_ctx,
                        &mut stream_cache,
                        &mut stream_cache_key,
                        throttled,
                        ack_on_commit,
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        &ack_waiters,
                        &ack_waiter_tx,
                        tenant_id,
                        namespace,
                        stream,
                        payloads,
                        request_id,
                        ack,
                        sample,
                    )
                    .await?;
                }
                Message::Subscribe {
                    tenant_id,
                    namespace,
                    stream,
                    subscription_id,
                } => {
                    let done = handle_subscribe_message(
                        Arc::clone(&broker),
                        connection.clone(),
                        config.clone(),
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        tenant_id,
                        namespace,
                        stream,
                        subscription_id,
                    )
                    .await?;
                    if done {
                        return Ok(true);
                    }
                }
                Message::CachePut {
                    tenant_id,
                    namespace,
                    cache,
                    key,
                    value,
                    request_id,
                    ttl_ms,
                } => {
                    // Cache ops can be single-response (request_id set) or stream-terminating.
                    if let Some(read_ns) = read_ns {
                        timings::record_cache_read_ns(read_ns);
                    }
                    if let Some(decode_ns) = decode_ns {
                        timings::record_cache_decode_ns(decode_ns);
                    }
                    if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::CacheMessage(Message::Error {
                                    message: format!(
                                        "cache scope not found: {tenant_id}/{namespace}/{cache}"
                                    ),
                                }),
                            )
                            .await,
                            &ack_timeout_state,
                            &ack_throttle_tx,
                            &cancel_tx,
                        )
                        .await?;
                        if request_id.is_none() {
                            return Ok(true);
                        }
                        continue;
                    }
                    let ttl = ttl_ms.map(Duration::from_millis);
                    let lookup_start = t_now_if(sample);
                    broker
                        .cache()
                        .put(
                            tenant_id.as_str(),
                            namespace.as_str(),
                            cache.as_str(),
                            key.as_str(),
                            value,
                            ttl,
                        )
                        .await;
                    if let Some(start) = lookup_start {
                        let lookup_ns = start.elapsed().as_nanos() as u64;
                        timings::record_cache_insert_ns(lookup_ns);
                    }
                    if let Some(request_id) = request_id {
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::CacheMessage(Message::CacheOk { request_id }),
                            )
                            .await,
                            &ack_timeout_state,
                            &ack_throttle_tx,
                            &cancel_tx,
                        )
                        .await?;
                        continue;
                    }
                    match send_outgoing_best_effort(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::CacheMessage(Message::Ok),
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(AckEnqueueError::Full) => {}
                        Err(err) => return Err(record_ack_enqueue_failure(err)),
                    }
                    return Ok(true);
                }
                Message::CacheGet {
                    tenant_id,
                    namespace,
                    cache,
                    key,
                    request_id,
                } => {
                    // Cache gets return CacheValue and optionally finish the stream.
                    if let Some(read_ns) = read_ns {
                        timings::record_cache_read_ns(read_ns);
                    }
                    if let Some(decode_ns) = decode_ns {
                        timings::record_cache_decode_ns(decode_ns);
                    }
                    if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
                        handle_ack_enqueue_result(
                            send_outgoing_critical(
                                &out_ack_tx,
                                &out_ack_depth,
                                "felix_broker_out_ack_depth",
                                &ack_throttle_tx,
                                Outgoing::CacheMessage(Message::Error {
                                    message: format!(
                                        "cache scope not found: {tenant_id}/{namespace}/{cache}"
                                    ),
                                }),
                            )
                            .await,
                            &ack_timeout_state,
                            &ack_throttle_tx,
                            &cancel_tx,
                        )
                        .await?;
                        if request_id.is_none() {
                            return Ok(true);
                        }
                        continue;
                    }
                    let lookup_start = t_now_if(sample);
                    let value = broker.cache().get(&tenant_id, &namespace, &cache, &key).await;
                    if let Some(start) = lookup_start {
                        let lookup_ns = start.elapsed().as_nanos() as u64;
                        timings::record_cache_lookup_ns(lookup_ns);
                    }
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::CacheMessage(Message::CacheValue {
                                tenant_id,
                                namespace,
                                cache,
                                key,
                                value,
                                request_id,
                            }),
                        )
                        .await,
                        &ack_timeout_state,
                        &ack_throttle_tx,
                        &cancel_tx,
                    )
                    .await?;
                    if request_id.is_none() {
                        return Ok(true);
                    }
                }
                Message::CacheValue { .. }
                | Message::CacheOk { .. }
                | Message::Event { .. }
                | Message::EventBatch { .. }
                | Message::Subscribed { .. }
                | Message::EventStreamHello { .. }
                | Message::PublishOk { .. }
                | Message::PublishError { .. }
                | Message::Ok => {
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            &out_ack_tx,
                            &out_ack_depth,
                            "felix_broker_out_ack_depth",
                            &ack_throttle_tx,
                            Outgoing::Message(Message::Error {
                                message: "unexpected message type".to_string(),
                            }),
                        )
                        .await,
                        &ack_timeout_state,
                        &ack_throttle_tx,
                        &cancel_tx,
                    )
                    .await?;
                    return Ok(false);
                }
                Message::Error { .. } => {
                    return Ok(false);
                }
            }
        }
        Ok(graceful_close)
    })
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
    // Uni-directional streams are payload-only: fire-and-forget publishes with no acks/responses.
    let mut stream_cache = HashMap::new();
    let mut stream_cache_key = String::new();
    let mut frame_scratch = BytesMut::with_capacity(config.max_frame_bytes.min(64 * 1024));
    // Read loop: accept publish payloads only; control/ack traffic never appears here.
    loop {
        let frame =
            match read_frame_limited_into(&mut recv, config.max_frame_bytes, &mut frame_scratch)
                .await?
            {
                Some(frame) => frame,
                None => break,
            };
        // Uni streams are publish-only; responses are not sent.
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            let handled = handle_binary_publish_batch_uni(
                &broker,
                &mut stream_cache,
                &mut stream_cache_key,
                &publish_ctx,
                &frame,
            )
            .await?;
            if !handled {
                break;
            }
            continue;
        }
        // Publish-only protocol: decode one frame into a typed Message, then route below.
        // Only Publish/PublishBatch are honored on uni streams; no acks are sent.
        let message = match Message::decode(frame.clone()).context("decode message") {
            Ok(message) => message,
            Err(err) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = super::telemetry::frame_counters();
                    counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                }
                super::telemetry::log_decode_error("uni_message", &err, &frame);
                return Err(err);
            }
        };
        match message {
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                ..
            } => {
                let handled = handle_publish_message_uni(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &publish_ctx,
                    tenant_id,
                    namespace,
                    stream,
                    payload,
                )
                .await?;
                if !handled {
                    break;
                }
            }
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads,
                ..
            } => {
                let handled = handle_publish_batch_message_uni(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &publish_ctx,
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                )
                .await?;
                if !handled {
                    break;
                }
            }
            _ => {
                t_counter!(
                    "felix_quic_protocol_violation_total",
                    "stream" => "uni"
                )
                .increment(1);
                tracing::debug!("closing uni stream after non-publish message");
                break;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use felix_storage::EphemeralCache;
    use felix_transport::{QuicClient, QuicServer, TransportConfig};
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn cache_put_get_round_trip() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_cache("t1", "default", "primary", felix_broker::CacheMetadata)
            .await?;
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;

        let config = BrokerConfig::from_env()?;
        let max_frame_bytes = config.max_frame_bytes;
        let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
        let server_task = tokio::spawn(crate::transport::quic::serve(
            Arc::clone(&server),
            Arc::clone(&broker),
            config,
        ));

        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            build_quinn_client_config(cert)?,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;

        let (mut send, mut recv) = connection.open_bi().await?;
        crate::transport::quic::write_message(
            &mut send,
            Message::CachePut {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
                key: "demo-key".to_string(),
                value: Bytes::from_static(b"cached"),
                request_id: None,
                ttl_ms: None,
            },
        )
        .await?;
        send.finish()?;
        let response = crate::transport::quic::read_message_limited(
            &mut recv,
            max_frame_bytes,
            &mut frame_scratch,
        )
        .await?;
        assert_eq!(response, Some(Message::Ok));

        let (mut send, mut recv) = connection.open_bi().await?;
        crate::transport::quic::write_message(
            &mut send,
            Message::CacheGet {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
                key: "demo-key".to_string(),
                request_id: None,
            },
        )
        .await?;
        send.finish()?;
        let response = crate::transport::quic::read_message_limited(
            &mut recv,
            max_frame_bytes,
            &mut frame_scratch,
        )
        .await?;
        assert_eq!(
            response,
            Some(Message::CacheValue {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
                key: "demo-key".to_string(),
                value: Some(Bytes::from_static(b"cached")),
                request_id: None,
            })
        );

        drop(connection);
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn publish_rejects_unknown_stream() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;

        let config = BrokerConfig::from_env()?;
        let max_frame_bytes = config.max_frame_bytes;
        let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
        let server_task = tokio::spawn(crate::transport::quic::serve(
            Arc::clone(&server),
            Arc::clone(&broker),
            config,
        ));

        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            build_quinn_client_config(cert)?,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;

        let (mut send, mut recv) = connection.open_bi().await?;
        crate::transport::quic::write_message(
            &mut send,
            Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
                payload: b"payload".to_vec(),
                request_id: Some(1),
                ack: Some(felix_wire::AckMode::PerMessage),
            },
        )
        .await?;
        send.finish()?;
        let response = crate::transport::quic::read_message_limited(
            &mut recv,
            max_frame_bytes,
            &mut frame_scratch,
        )
        .await?;
        assert!(matches!(
            response,
            Some(Message::PublishError { request_id: 1, .. })
        ));

        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "missing", Default::default())
            .await
            .expect("register");
        let (mut send, mut recv) = connection.open_bi().await?;
        crate::transport::quic::write_message(
            &mut send,
            Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
                payload: b"payload".to_vec(),
                request_id: Some(2),
                ack: Some(felix_wire::AckMode::PerMessage),
            },
        )
        .await?;
        send.finish()?;
        let response = crate::transport::quic::read_message_limited(
            &mut recv,
            max_frame_bytes,
            &mut frame_scratch,
        )
        .await?;
        assert_eq!(response, Some(Message::PublishOk { request_id: 2 }));

        drop(connection);
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn publish_ack_on_commit_smoke() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream("t1", "default", "updates", Default::default())
            .await?;
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;

        let mut config = BrokerConfig::from_env()?;
        config.ack_on_commit = true;
        let max_frame_bytes = config.max_frame_bytes;
        let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
        let server_task = tokio::spawn(crate::transport::quic::serve(
            Arc::clone(&server),
            Arc::clone(&broker),
            config,
        ));

        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            build_quinn_client_config(cert)?,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;

        let (mut send, mut recv) = connection.open_bi().await?;
        crate::transport::quic::write_message(
            &mut send,
            Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "updates".to_string(),
                payload: b"payload".to_vec(),
                request_id: Some(1),
                ack: Some(felix_wire::AckMode::PerMessage),
            },
        )
        .await?;
        send.finish()?;
        let response = crate::transport::quic::read_message_limited(
            &mut recv,
            max_frame_bytes,
            &mut frame_scratch,
        )
        .await?;
        assert_eq!(response, Some(Message::PublishOk { request_id: 1 }));

        drop(connection);
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn publish_sharding_preserves_stream_order() -> Result<()> {
        unsafe {
            std::env::set_var("FELIX_BROKER_PUB_WORKERS_PER_CONN", "4");
            std::env::set_var("FELIX_BROKER_PUB_QUEUE_DEPTH", "1024");
            std::env::set_var("FELIX_PUB_CONN_POOL", "1");
            std::env::set_var("FELIX_PUB_STREAMS_PER_CONN", "2");
            std::env::set_var("FELIX_PUB_SHARDING", "hash_stream");
        }

        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream("t1", "default", "alpha", Default::default())
            .await?;
        broker
            .register_stream("t1", "default", "beta", Default::default())
            .await?;
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;

        let config = BrokerConfig::from_env()?;
        let server_task = tokio::spawn(crate::transport::quic::serve(
            Arc::clone(&server),
            Arc::clone(&broker),
            config,
        ));

        let client = felix_client::Client::connect_with_transport(
            addr,
            "localhost",
            build_client_config(cert)?,
            TransportConfig::default(),
        )
        .await?;

        let mut sub_alpha = client.subscribe("t1", "default", "alpha").await?;
        let mut sub_beta = client.subscribe("t1", "default", "beta").await?;
        let publisher = client.publisher().await?;

        let total = 200u64;
        for i in 0..total {
            let payload = i.to_be_bytes().to_vec();
            publisher
                .publish(
                    "t1",
                    "default",
                    "alpha",
                    payload.clone(),
                    felix_wire::AckMode::None,
                )
                .await?;
            publisher
                .publish("t1", "default", "beta", payload, felix_wire::AckMode::None)
                .await?;
        }

        for i in 0..total {
            let next = timeout(Duration::from_secs(2), sub_alpha.next_event()).await??;
            let event = next.expect("alpha event");
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&event.payload[..8]);
            assert_eq!(u64::from_be_bytes(raw), i);
        }

        for i in 0..total {
            let next = timeout(Duration::from_secs(2), sub_beta.next_event()).await??;
            let event = next.expect("beta event");
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&event.payload[..8]);
            assert_eq!(u64::from_be_bytes(raw), i);
        }

        publisher.finish().await?;
        server_task.abort();
        Ok(())
    }

    fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
        let cert = generate_simple_self_signed(vec!["localhost".into()])?;
        let cert_der = CertificateDer::from(cert.serialize_der()?);
        let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
        let server_config =
            quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
                .context("build server config")?;
        Ok((server_config, cert_der))
    }

    fn build_quinn_client_config(cert: CertificateDer<'static>) -> Result<QuinnClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(cert)?;
        let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
        Ok(quinn)
    }

    fn build_client_config(cert: CertificateDer<'static>) -> Result<felix_client::ClientConfig> {
        let quinn = build_quinn_client_config(cert)?;
        felix_client::ClientConfig::from_env_or_yaml(quinn, None)
    }
}
