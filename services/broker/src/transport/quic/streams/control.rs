use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_broker::Broker;
use felix_wire::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore, mpsc, watch};

use crate::config::BrokerConfig;
use crate::timings;
use crate::transport::quic::errors::{AckEnqueueError, record_ack_enqueue_failure};
use crate::transport::quic::handlers::publish::{
    AckTimeoutState, AckWaiterMessage, Outgoing, PublishContext, handle_ack_enqueue_result,
    handle_binary_publish_batch_control, handle_publish_batch_message, handle_publish_message,
    send_outgoing_best_effort, send_outgoing_critical,
};
use crate::transport::quic::handlers::subscribe::handle_subscribe_message;
use crate::transport::quic::telemetry::{t_histogram, t_now_if, t_should_sample};

use super::frame_source::FrameSource;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_control_loop<S: FrameSource + ?Sized>(
    source: &mut S,
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: BrokerConfig,
    publish_ctx: PublishContext,
    mut stream_cache: HashMap<String, (bool, Instant)>,
    mut stream_cache_key: String,
    out_ack_tx: mpsc::Sender<Outgoing>,
    out_ack_depth: Arc<AtomicUsize>,
    ack_throttle_rx: watch::Receiver<bool>,
    ack_throttle_tx: watch::Sender<bool>,
    ack_timeout_state: Arc<Mutex<AckTimeoutState>>,
    cancel_tx: watch::Sender<bool>,
    mut cancel_rx_read: watch::Receiver<bool>,
    ack_waiters: Arc<Semaphore>,
    ack_waiter_tx: mpsc::Sender<AckWaiterMessage>,
    ack_wait_timeout: Duration,
    frame_scratch: &mut BytesMut,
) -> Result<bool> {
    let mut graceful_close = false;
    loop {
        if *cancel_rx_read.borrow() {
            break;
        }
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
            frame = source.next_frame(config.max_frame_bytes, frame_scratch) => {
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
        let message = match Message::decode(frame.clone()).context("decode message") {
            Ok(message) => message,
            Err(err) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = crate::transport::quic::telemetry::frame_counters();
                    counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                }
                crate::transport::quic::telemetry::log_decode_error(
                    "control_message",
                    &err,
                    &frame,
                );
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
                    config.ack_on_commit,
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
                    config.ack_on_commit,
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
                let value = broker
                    .cache()
                    .get(&tenant_id, &namespace, &cache, &key)
                    .await;
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
}
