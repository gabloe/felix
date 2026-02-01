// Control stream (bi-directional QUIC stream)
//
// This module implements the *read side* of the broker's bidirectional control stream.
// The control stream is the request/response path used by clients for:
//   - Publish / PublishBatch (optionally requesting an ack)
//   - Subscribe (establishing a subscription and spawning a uni-directional event stream)
//   - CachePut / CacheGet (request/response cache API)
//
// Key design points:
//   1) Single-writer response path (implemented elsewhere): the read loop never writes to the
//      SendStream directly; it enqueues `Outgoing` responses into an outbound channel drained by a
//      dedicated writer task.
//
//   2) Fast-path binary batching: when a frame is marked with FLAG_BINARY_PUBLISH_BATCH we bypass
//      JSON decoding and dispatch to a specialized handler. This keeps the hot path cheap.
//
//   3) Cooperative cancellation: `cancel_rx_read` (watch) allows the writer or other tasks to
//      request the control loop stop (e.g., writer detects the peer closed, or backpressure logic
//      decides to tear down).
//
//   4) Backpressure / throttling coordination: `ack_throttle_rx/tx` is a watch channel used to
//      communicate whether the outbound response queue is in a throttled state (watermarks are
//      enforced in the writer/enqueue helpers). The control loop passes the current throttled state
//      into publish handlers so they can adjust behavior.
//
//   5) Ack-on-commit mode: when `config.ack_on_commit` is enabled, publish handlers may defer the
//      ack until the publish worker commits. `ack_waiters` bounds in-flight waiters and
//      `ack_waiter_tx` delivers waiter work to the background ack-waiter task.
//
// Return value convention:
//   Ok(true)  => graceful close / stream should be considered "done" (no error)
//   Ok(false) => protocol error or peer sent Error/unexpected message
//   Err(_)    => hard failure (decode/IO/etc.)
use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_authz::{Action, CacheScope, Namespace, StreamName, cache_resource, stream_resource};
use felix_broker::Broker;
use felix_wire::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore, mpsc, watch};

use crate::auth::{AuthContext, BrokerAuth};
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

// The loop is intentionally structured as:
//   read frame -> (optional fast-path) -> decode -> dispatch.
//
// Important parameters:
//   - `source`: abstract frame source (RecvStream in prod, test doubles in unit tests).
//   - `stream_cache` / `stream_cache_key`: per-connection cache of stream scope lookups used by
//     publish handlers to avoid repeatedly touching shared metadata for hot streams.
//   - `out_ack_tx` / `out_ack_depth`: outbound response queue + depth gauge used for backpressure.
//   - `ack_throttle_rx/tx`: shared throttling state; this loop reads current state, handlers/writer
//     update it.
//   - `ack_timeout_state`: shared state used to detect/report ack enqueue timeouts.
//   - `ack_waiters` / `ack_waiter_tx`: bounds and routes "ack when commit finishes" work.
//   - `frame_scratch`: reusable buffer to avoid per-frame allocations.
// Main control-loop: read frames, decode messages, and dispatch to handlers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_control_loop<S: FrameSource + ?Sized>(
    source: &mut S,
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: BrokerConfig,
    auth: Arc<BrokerAuth>,
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
    // If we observe EOF from the peer (source returns None), we treat it as a graceful close.
    // Otherwise, we will cancel downstream tasks and tear down the connection cooperatively.
    let mut graceful_close = false;
    let mut auth_ctx: Option<AuthContext> = None;
    let authz_ctx = AuthzResponseContext {
        out_ack_tx: &out_ack_tx,
        out_ack_depth: &out_ack_depth,
        ack_throttle_tx: &ack_throttle_tx,
        ack_timeout_state: &ack_timeout_state,
        cancel_tx: &cancel_tx,
    };
    loop {
        if *cancel_rx_read.borrow() {
            break;
        }
        // Snapshot current throttling state (set by writer/enqueue helpers when outbound queue
        // crosses watermarks). Handlers may use this to shed work or alter ack behavior.
        let throttled = *ack_throttle_rx.borrow();
        let sample = t_should_sample();
        let read_start = t_now_if(sample);
        // We need to be responsive to cancellation even while blocked on network reads.
        // `watch::Receiver::changed()` wakes when the cancel flag flips.
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
                    // EOF: the peer cleanly finished the control stream.
                    None => {
                        graceful_close = true;
                        break;
                    }
                }
            }
        };
        let read_ns = read_start.map(|start| start.elapsed().as_nanos() as u64);
        // Fast-path: binary publish batch frames avoid JSON decode/allocations.
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            if auth_ctx.is_none() {
                send_control_error(
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    "auth required",
                )
                .await?;
                return Ok(false);
            }
            handle_binary_publish_batch_control(
                &broker,
                &mut stream_cache,
                &mut stream_cache_key,
                &publish_ctx,
                &frame,
                auth_ctx.as_ref(),
                sample,
            )
            .await?;
            continue;
        }
        // Slow-path: decode JSON control message. Decode errors are considered fatal protocol
        // violations and terminate the stream.
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
        // Dispatch by message type. Most handlers are responsible for enqueuing responses into
        // `out_ack_tx` rather than writing directly to the network.
        match message {
            Message::Auth { tenant_id, token } => {
                if auth_ctx.is_some() {
                    send_control_error(
                        &out_ack_tx,
                        &out_ack_depth,
                        &ack_throttle_tx,
                        &ack_timeout_state,
                        &cancel_tx,
                        "auth already established",
                    )
                    .await?;
                    return Ok(false);
                }
                match auth.authenticate(&tenant_id, &token).await {
                    Ok(ctx) => {
                        auth_ctx = Some(ctx);
                        send_control_ok(
                            &out_ack_tx,
                            &out_ack_depth,
                            &ack_throttle_tx,
                            &ack_timeout_state,
                            &cancel_tx,
                        )
                        .await?;
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "auth failed");
                        send_control_error(
                            &out_ack_tx,
                            &out_ack_depth,
                            &ack_throttle_tx,
                            &ack_timeout_state,
                            &cancel_tx,
                            "auth failed",
                        )
                        .await?;
                        return Ok(false);
                    }
                }
            }
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                request_id,
                ack,
            } => {
                if !authorize_stream(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamPublish,
                    &namespace,
                    &stream,
                    request_id,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
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
                if !authorize_stream(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamPublish,
                    &namespace,
                    &stream,
                    request_id,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
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
                if !authorize_stream_simple(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::StreamSubscribe,
                    &namespace,
                    &stream,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                // Subscribe establishes server-side subscription state and typically spawns a
                // uni-directional event stream back to the client for delivery.
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
                if !authorize_cache(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::CacheWrite,
                    &namespace,
                    &cache,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                if let Some(read_ns) = read_ns {
                    timings::record_cache_read_ns(read_ns);
                }
                if let Some(decode_ns) = decode_ns {
                    timings::record_cache_decode_ns(decode_ns);
                }
                // Cache scope validation: cache operations are rejected if the cache isn't
                // registered for the (tenant, namespace, cache) triple.
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
                        // When request_id is None, the client is using a "best effort" cache API and the
                        // stream is closed after the single request/response completes.
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
                if !authorize_cache(
                    auth_ctx.as_ref(),
                    &tenant_id,
                    Action::CacheRead,
                    &namespace,
                    &cache,
                    &authz_ctx,
                )
                .await?
                {
                    return Ok(false);
                }
                if let Some(read_ns) = read_ns {
                    timings::record_cache_read_ns(read_ns);
                }
                if let Some(decode_ns) = decode_ns {
                    timings::record_cache_decode_ns(decode_ns);
                }
                // Cache scope validation: cache operations are rejected if the cache isn't
                // registered for the (tenant, namespace, cache) triple.
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
                        // When request_id is None, the client is using a "best effort" cache API and the
                        // stream is closed after the single request/response completes.
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
                    // When request_id is None, the client is using a "best effort" cache API and the
                    // stream is closed after the single request/response completes.
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
                // Protocol hygiene: these message types should never arrive on the control stream
                // from the client. Treat as a protocol violation and close.
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
    // `graceful_close` only tracks EOF from the peer. Any other early-exit path returns false
    // (protocol error) or Err (hard failure).
    Ok(graceful_close)
}

async fn send_control_error(
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    message: &str,
) -> Result<()> {
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::Error {
                message: message.to_string(),
            }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await
}

async fn send_control_ok(
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
) -> Result<()> {
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::Ok),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await
}

struct AuthzResponseContext<'a> {
    out_ack_tx: &'a mpsc::Sender<Outgoing>,
    out_ack_depth: &'a Arc<AtomicUsize>,
    ack_throttle_tx: &'a watch::Sender<bool>,
    ack_timeout_state: &'a Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &'a watch::Sender<bool>,
}

async fn authorize_stream(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    stream: &str,
    request_id: Option<u64>,
    ctx: &AuthzResponseContext<'_>,
) -> Result<bool> {
    let Some(auth_ctx) = auth_ctx else {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "auth required",
        )
        .await?;
        return Ok(false);
    };
    if auth_ctx.tenant_id != tenant_id {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "tenant mismatch",
        )
        .await?;
        return Ok(false);
    }
    let resource = stream_resource(&Namespace::new(namespace), &StreamName::new(stream));
    if auth_ctx.matcher.allows(action, &resource) {
        return Ok(true);
    }
    let outgoing = match request_id {
        Some(request_id) => Outgoing::Message(Message::PublishError {
            request_id,
            message: "forbidden".to_string(),
        }),
        None => Outgoing::Message(Message::Error {
            message: "forbidden".to_string(),
        }),
    };
    handle_ack_enqueue_result(
        send_outgoing_critical(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            "felix_broker_out_ack_depth",
            ctx.ack_throttle_tx,
            outgoing,
        )
        .await,
        ctx.ack_timeout_state,
        ctx.ack_throttle_tx,
        ctx.cancel_tx,
    )
    .await?;
    Ok(false)
}

async fn authorize_stream_simple(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    stream: &str,
    ctx: &AuthzResponseContext<'_>,
) -> Result<bool> {
    let Some(auth_ctx) = auth_ctx else {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "auth required",
        )
        .await?;
        return Ok(false);
    };
    if auth_ctx.tenant_id != tenant_id {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "tenant mismatch",
        )
        .await?;
        return Ok(false);
    }
    let resource = stream_resource(&Namespace::new(namespace), &StreamName::new(stream));
    if auth_ctx.matcher.allows(action, &resource) {
        return Ok(true);
    }
    send_control_error(
        ctx.out_ack_tx,
        ctx.out_ack_depth,
        ctx.ack_throttle_tx,
        ctx.ack_timeout_state,
        ctx.cancel_tx,
        "forbidden",
    )
    .await?;
    Ok(false)
}

async fn authorize_cache(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    cache: &str,
    ctx: &AuthzResponseContext<'_>,
) -> Result<bool> {
    let Some(auth_ctx) = auth_ctx else {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "auth required",
        )
        .await?;
        return Ok(false);
    };
    if auth_ctx.tenant_id != tenant_id {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            "tenant mismatch",
        )
        .await?;
        return Ok(false);
    }
    let resource = cache_resource(&Namespace::new(namespace), &CacheScope::new(cache));
    if auth_ctx.matcher.allows(action, &resource) {
        return Ok(true);
    }
    send_control_error(
        ctx.out_ack_tx,
        ctx.out_ack_depth,
        ctx.ack_throttle_tx,
        ctx.ack_timeout_state,
        ctx.cancel_tx,
        "forbidden",
    )
    .await?;
    Ok(false)
}
