// Publish handlers for the bi-directional control stream (acked paths).

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use felix_authz::{Action, Namespace, StreamName, TenantId, stream_resource};
use felix_broker::Broker;
use felix_wire::{Frame, Message};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};

use crate::auth::AuthContext;
use crate::timings;
use crate::transport::quic::errors::AckEnqueueError;
use crate::transport::quic::handlers::publish::ack::{
    AckEncoding, AckTimeoutState, AckWaiterMessage, EnqueuePolicy, Outgoing,
    handle_ack_enqueue_result, send_outgoing_best_effort, send_outgoing_critical,
};
use crate::transport::quic::handlers::publish::ingress::{PublishTarget, enqueue_publish};
use crate::transport::quic::handlers::publish::{
    PublishContext, PublishJob, StreamHandleCache, resolve_stream_cached,
};
use crate::transport::quic::telemetry::{log_decode_error, t_consume_instant, t_now_if};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_binary_publish_batch_control(
    broker: &Broker,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    frame: &Frame,
    auth_ctx: Option<&AuthContext>,
    sample: bool,
    // Bounds the unacked backpressure wait: without a timer, teardown is what
    // ends it.
    cancel_tx: &watch::Sender<bool>,
) -> Result<()> {
    let decode_start = t_now_if(sample);
    let batch = match felix_wire::binary::decode_publish_batch(frame)
        .context("decode binary publish batch")
    {
        Ok(batch) => batch,
        Err(err) => {
            #[cfg(feature = "telemetry")]
            {
                let counters = crate::transport::quic::telemetry::frame_counters();
                counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
            }
            log_decode_error("binary_publish_batch", &err, frame);
            return Err(err);
        }
    };
    let auth_ctx = auth_ctx.ok_or_else(|| anyhow!("auth required"))?;
    if auth_ctx.tenant_id != batch.tenant_id {
        return Err(anyhow!("tenant mismatch"));
    }
    let resource = stream_resource(
        &TenantId::new(batch.tenant_id.as_str()),
        &Namespace::new(batch.namespace.as_str()),
        &StreamName::new(batch.stream.as_str()),
    );
    if !auth_ctx.matcher.allows(Action::StreamPublish, &resource) {
        return Err(anyhow!("forbidden"));
    }
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters
            .pub_items_in_ok
            .fetch_add(batch.payloads.len() as u64, Ordering::Relaxed);
    }
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_decode_ns(decode_ns);
        t_histogram!("felix_broker_decode_ns").record(decode_ns as f64);
    }
    let Some(stream_handle) = resolve_stream_cached(
        broker,
        stream_cache,
        stream_cache_key,
        &batch.tenant_id,
        &batch.namespace,
        &batch.stream,
    )
    .await
    else {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        return Ok(());
    };
    let span = tracing::info_span!(
        "publish_batch_binary",
        tenant_id = %batch.tenant_id,
        namespace = %batch.namespace,
        stream = %batch.stream,
        count = batch.payloads.len()
    );
    let _enter = span.enter();
    let payloads = batch
        .payloads
        .into_iter()
        .map(Bytes::from)
        .collect::<Vec<_>>();
    let fanout_start = t_now_if(sample);
    let r = enqueue_publish(
        publish_ctx,
        PublishJob {
            target: PublishTarget::Resolved(stream_handle),
            payloads,
            response: None,
            admission_permit: None,
        },
        publish_ctx.overflow_policy(),
        Some(cancel_tx.subscribe()),
    )
    .await;
    match r {
        Ok(true) => {
            t_counter!("felix_publish_requests_total", "result" => "accepted").increment(1);
        }
        Ok(false) => {
            t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
        }
        Err(err) => {
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            tracing::warn!(error = %err, "publish enqueue failed");
        }
    }

    if let Some(start) = fanout_start {
        let fanout_ns = start.elapsed().as_nanos() as u64;
        timings::record_fanout_ns(fanout_ns);
        t_histogram!("felix_broker_ingress_enqueue_ns").record(fanout_ns as f64);
    }
    Ok(())
}

/// Handle a binary publish batch that asked to be acknowledged.
///
/// This is the binary counterpart of the JSON acked publish path. It decodes the
/// frame and then delegates to [`handle_publish_batch_message`] with
/// [`AckEncoding::Binary`], so both encodings share one set of admission,
/// authorization, overload and commit-ack semantics — the encoding only decides
/// how the reply is framed.
///
/// Failures here reply with an error ack rather than tearing down the stream.
/// Frames are length-prefixed, so a malformed *body* does not desynchronise the
/// framing, and the client is synchronously blocked waiting for this ack: killing
/// the stream would turn a bad request into a stalled connection.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_acked_binary_publish_batch_control(
    broker: &Broker,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    frame: &Frame,
    auth_ctx: Option<&AuthContext>,
    throttled: bool,
    ack_on_commit: bool,
    sample: bool,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    ack_waiters: &Arc<Semaphore>,
    ack_waiter_tx: &mpsc::Sender<AckWaiterMessage>,
) -> Result<()> {
    // Read the correlation prefix before the body, so even an undecodable batch
    // can be answered with an ack the client is able to match to its request.
    let (request_id, ack) = match felix_wire::binary::peek_acked_publish_prefix(frame) {
        Ok(prefix) => prefix,
        Err(err) => {
            // Without a request_id there is nothing to correlate, so this is a
            // protocol violation rather than a per-request failure.
            log_decode_error("acked_binary_publish_prefix", &anyhow!(err), frame);
            return Err(anyhow!("malformed acked publish prefix"));
        }
    };
    let reply_error = |message: String| async move {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                AckEncoding::Binary.error(request_id, message),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await
    };

    let decode_start = t_now_if(sample);
    let batch = match felix_wire::binary::decode_acked_publish_batch(frame) {
        Ok(batch) => batch,
        Err(err) => {
            #[cfg(feature = "telemetry")]
            {
                let counters = crate::transport::quic::telemetry::frame_counters();
                counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
            }
            log_decode_error("acked_binary_publish_batch", &anyhow!(err), frame);
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            reply_error("malformed publish batch".to_string()).await?;
            return Ok(());
        }
    };
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_decode_ns(decode_ns);
        t_histogram!("felix_broker_decode_ns").record(decode_ns as f64);
    }

    let batch = batch.batch;
    let Some(auth_ctx) = auth_ctx else {
        reply_error("auth required".to_string()).await?;
        return Ok(());
    };
    if auth_ctx.tenant_id != batch.tenant_id {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        reply_error("tenant mismatch".to_string()).await?;
        return Ok(());
    }
    let resource = stream_resource(
        &TenantId::new(batch.tenant_id.as_str()),
        &Namespace::new(batch.namespace.as_str()),
        &StreamName::new(batch.stream.as_str()),
    );
    if !auth_ctx.matcher.allows(Action::StreamPublish, &resource) {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        reply_error("forbidden".to_string()).await?;
        return Ok(());
    }

    handle_publish_batch_message(
        broker,
        publish_ctx,
        stream_cache,
        stream_cache_key,
        throttled,
        ack_on_commit,
        AckEncoding::Binary,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ack_waiters,
        ack_waiter_tx,
        batch.tenant_id,
        batch.namespace,
        batch.stream,
        batch.payloads,
        Some(request_id),
        Some(ack),
        sample,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_message(
    broker: &Broker,
    publish_ctx: &PublishContext,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    throttled: bool,
    ack_on_commit: bool,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    ack_waiters: &Arc<Semaphore>,
    ack_waiter_tx: &mpsc::Sender<AckWaiterMessage>,
    ack_wait_timeout: Duration,
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Vec<u8>,
    request_id: Option<u64>,
    ack: Option<felix_wire::AckMode>,
    sample: bool,
) -> Result<()> {
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_items_in_ok.fetch_add(1, Ordering::Relaxed);
    }
    if throttled {
        // Overload shed path:
        // - We intentionally skip broker work.
        // - We attempt to return a PublishError / Error if an ack was requested.
        // - If the outbound queue is full, we currently may drop this error ack.
        //   This can strand clients waiting for an ack. Consider switching these
        //   sends to critical enqueue or closing the stream when full.
        let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerMessage);
        if ack_mode != felix_wire::AckMode::None {
            if let Some(request_id) = request_id {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::PublishError {
                        request_id,
                        message: "server overloaded".to_string(),
                    }),
                )
                .await;
                if !matches!(result, Err(AckEnqueueError::Full)) {
                    handle_ack_enqueue_result(
                        result,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
            } else {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::Error {
                        message: "server overloaded".to_string(),
                    }),
                )
                .await;
                if !matches!(result, Err(AckEnqueueError::Full)) {
                    handle_ack_enqueue_result(
                        result,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
            }
        }
        t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
        return Ok(());
    }
    // Publish protocol (control stream):
    // - Client sends Publish { payload, request_id?, ack }.
    // - Broker enqueues payload (fanout path) and responds:
    //   - AckMode::None -> no response.
    //   - AckMode::PerMessage -> PublishOk/PublishError with request_id (acks may be out of order).
    // - request_id is required for any acked publish.
    let start = crate::transport::quic::telemetry::t_instant_now();
    t_consume_instant(start);
    let payload_len = payload.len();
    let enqueue_start = t_now_if(sample);
    // Ack mode determines if we wait for broker commit or reply immediately.
    let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerMessage);
    // Protocol invariant: any acked publish must include request_id, because acks
    // may be out-of-order and request_id is the only correlator.
    if ack_mode != felix_wire::AckMode::None && request_id.is_none() {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::Error {
                    message: "missing request_id for acked publish".to_string(),
                }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(());
    }
    let (response_tx, response_rx) = if ack_mode != felix_wire::AckMode::None && ack_on_commit {
        let (response_tx, response_rx) = oneshot::channel();
        (Some(response_tx), Some(response_rx))
    } else {
        (None, None)
    };
    let Some(stream_handle) = resolve_stream_cached(
        broker,
        stream_cache,
        stream_cache_key,
        &tenant_id,
        &namespace,
        &stream,
    )
    .await
    else {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        if ack_mode != felix_wire::AckMode::None {
            let request_id = request_id.expect("request id checked");
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::PublishError {
                        request_id,
                        message: format!(
                            "stream not found: tenant={tenant_id} namespace={namespace} stream={stream}"
                        ),
                    }),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
        }
        return Ok(());
    };
    let enqueue_result = enqueue_publish(
        publish_ctx,
        PublishJob {
            target: PublishTarget::Resolved(stream_handle),
            payloads: vec![Bytes::from(payload)],
            response: response_tx,
            admission_permit: None,
        },
        if ack_mode == felix_wire::AckMode::None {
            publish_ctx.overflow_policy()
        } else if ack_on_commit {
            EnqueuePolicy::Wait
        } else {
            EnqueuePolicy::Fail
        },
        Some(cancel_tx.subscribe()),
    )
    .await;
    if let Some(start) = enqueue_start {
        let enqueue_ns = start.elapsed().as_nanos() as u64;
        timings::record_fanout_ns(enqueue_ns);
        t_histogram!("felix_broker_ingress_enqueue_ns").record(enqueue_ns as f64);
    }
    let span = tracing::trace_span!(
        "publish",
        tenant_id = %tenant_id,
        namespace = %namespace,
        stream = %stream
    );
    let _enter = span.enter();
    match enqueue_result {
        Ok(true) => {
            if ack_mode == felix_wire::AckMode::None {
                t_counter!("felix_publish_requests_total", "result" => "accepted").increment(1);
            }
        }
        Ok(false) => {
            t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
            if ack_mode != felix_wire::AckMode::None {
                let request_id = request_id.expect("request id checked");
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        out_ack_tx,
                        out_ack_depth,
                        "felix_broker_out_ack_depth",
                        ack_throttle_tx,
                        Outgoing::Message(Message::PublishError {
                            request_id,
                            message: "ingress overloaded".to_string(),
                        }),
                    )
                    .await,
                    ack_timeout_state,
                    ack_throttle_tx,
                    cancel_tx,
                )
                .await?;
            }
            return Ok(());
        }
        Err(err) => {
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            if ack_mode != felix_wire::AckMode::None {
                let request_id = request_id.expect("request id checked");
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        out_ack_tx,
                        out_ack_depth,
                        "felix_broker_out_ack_depth",
                        ack_throttle_tx,
                        Outgoing::Message(Message::PublishError {
                            request_id,
                            message: err.to_string(),
                        }),
                    )
                    .await,
                    ack_timeout_state,
                    ack_throttle_tx,
                    cancel_tx,
                )
                .await?;
            }
            return Ok(());
        }
    }
    if ack_mode == felix_wire::AckMode::None {
        return Ok(());
    }
    if !ack_on_commit {
        // Enqueue-ack mode:
        // Ack means "accepted into the ingress queue", not "committed". This keeps
        // latency low but can report success even if a later broker error occurs.
        // Fire-and-forget ack after enqueue when configured.
        let request_id = request_id.expect("request id checked");
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishOk { request_id }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        t_counter!("felix_publish_requests_total", "result" => "ok").increment(1);
        t_counter!("felix_publish_bytes_total").increment(payload_len as u64);
        #[cfg(feature = "telemetry")]
        {
            t_histogram!("felix_publish_latency_ms", "mode" => "enqueue")
                .record(start.elapsed().as_secs_f64() * 1000.0);
        }
        return Ok(());
    }
    let request_id = request_id.expect("request id checked");
    let response_rx = response_rx.expect("response rx available");
    let payload_len_for_metrics = payload_len as u64;
    // Commit-ack mode:
    // We bound the number of in-flight commit acks. If exhausted, we fail fast.
    // Correctness note: failing after enqueue means the publish may still commit;
    // the client will see an error/overload even though the publish succeeded.
    // If that is unacceptable, we must enforce admission *before* enqueue.
    let permit = match Arc::clone(ack_waiters).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishError {
                    request_id,
                    message: "server overloaded".to_string(),
                }),
            )
            .await;
            t_counter!("felix_broker_ack_waiters_exhausted_total").increment(1);
            return Ok(());
        }
    };
    let msg = AckWaiterMessage::Publish {
        request_id,
        // There is no binary encoding for single publishes; the binary fast path
        // is batch-only, so this waiter always replies in JSON.
        encoding: AckEncoding::Json,
        payload_len: payload_len_for_metrics,
        start,
        response_rx,
        permit,
    };
    match ack_waiter_tx.try_send(msg) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => {
            drop(match msg {
                AckWaiterMessage::Publish { permit, .. }
                | AckWaiterMessage::PublishBatch { permit, .. } => permit,
            });
            t_counter!("felix_broker_ack_waiter_queue_full_total").increment(1);
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishError {
                    request_id,
                    message: "server overloaded".to_string(),
                }),
            )
            .await;
            return Ok(());
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(msg)) => {
            drop(match msg {
                AckWaiterMessage::Publish { permit, .. }
                | AckWaiterMessage::PublishBatch { permit, .. } => permit,
            });
            t_counter!("felix_broker_ack_waiter_queue_full_total").increment(1);
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::PublishError {
                    request_id,
                    message: "server overloaded".to_string(),
                }),
            )
            .await;
            return Ok(());
        }
    }
    let _ = ack_wait_timeout;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_batch_message(
    broker: &Broker,
    publish_ctx: &PublishContext,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    throttled: bool,
    ack_on_commit: bool,
    encoding: AckEncoding,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    ack_waiters: &Arc<Semaphore>,
    ack_waiter_tx: &mpsc::Sender<AckWaiterMessage>,
    tenant_id: String,
    namespace: String,
    stream: String,
    payloads: Vec<Vec<u8>>,
    request_id: Option<u64>,
    ack: Option<felix_wire::AckMode>,
    sample: bool,
) -> Result<()> {
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters
            .pub_items_in_ok
            .fetch_add(payloads.len() as u64, Ordering::Relaxed);
    }
    if throttled {
        // Overload shed path:
        // - We intentionally skip broker work.
        // - We attempt to return a PublishError / Error if an ack was requested.
        // - If the outbound queue is full, we currently may drop this error ack.
        //   This can strand clients waiting for an ack. Consider switching these
        //   sends to critical enqueue or closing the stream when full.
        let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerBatch);
        if ack_mode != felix_wire::AckMode::None {
            if let Some(request_id) = request_id {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    encoding.error(request_id, "server overloaded".to_string()),
                )
                .await;
                if !matches!(result, Err(AckEnqueueError::Full)) {
                    handle_ack_enqueue_result(
                        result,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
            } else {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::Error {
                        message: "server overloaded".to_string(),
                    }),
                )
                .await;
                if !matches!(result, Err(AckEnqueueError::Full)) {
                    handle_ack_enqueue_result(
                        result,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
            }
        }
        t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
        return Ok(());
    }
    // PublishBatch protocol (control stream):
    // - Client sends PublishBatch { payloads, request_id?, ack }.
    // - Broker enqueues all payloads as one unit and responds:
    //   - AckMode::None -> no response.
    //   - AckMode::PerBatch -> PublishOk/PublishError with request_id (acks may be out of order).
    // - request_id is required for any acked publish.
    let span = tracing::trace_span!(
        "publish_batch",
        tenant_id = %tenant_id,
        namespace = %namespace,
        stream = %stream,
        count = payloads.len()
    );
    let _enter = span.enter();
    let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerBatch);
    // Protocol invariant: any acked publish must include request_id, because acks
    // may be out-of-order and request_id is the only correlator.
    if ack_mode != felix_wire::AckMode::None && request_id.is_none() {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(Message::Error {
                    message: "missing request_id for acked publish batch".to_string(),
                }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(());
    }
    let Some(stream_handle) = resolve_stream_cached(
        broker,
        stream_cache,
        stream_cache_key,
        &tenant_id,
        &namespace,
        &stream,
    )
    .await
    else {
        t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
        if ack_mode != felix_wire::AckMode::None {
            let request_id = request_id.expect("request id checked");
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    encoding.error(request_id, format!(
                            "stream not found: tenant={tenant_id} namespace={namespace} stream={stream}"
                        )),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
        }
        return Ok(());
    };
    let payload_bytes = payloads
        .iter()
        .map(|payload| payload.len())
        .collect::<Vec<_>>();
    let payloads = payloads.into_iter().map(Bytes::from).collect::<Vec<_>>();
    let (response_tx, response_rx) = if ack_mode != felix_wire::AckMode::None && ack_on_commit {
        let (response_tx, response_rx) = oneshot::channel();
        (Some(response_tx), Some(response_rx))
    } else {
        (None, None)
    };
    let fanout_start = t_now_if(sample);
    let enqueue_result = enqueue_publish(
        publish_ctx,
        PublishJob {
            target: PublishTarget::Resolved(stream_handle),
            payloads,
            response: response_tx,
            admission_permit: None,
        },
        if ack_mode == felix_wire::AckMode::None {
            publish_ctx.overflow_policy()
        } else if ack_on_commit {
            EnqueuePolicy::Wait
        } else {
            EnqueuePolicy::Fail
        },
        Some(cancel_tx.subscribe()),
    )
    .await;
    if let Some(start) = fanout_start {
        let fanout_ns = start.elapsed().as_nanos() as u64;
        timings::record_fanout_ns(fanout_ns);
        t_histogram!("felix_broker_ingress_enqueue_ns").record(fanout_ns as f64);
    }
    match enqueue_result {
        Ok(true) => {
            if ack_mode == felix_wire::AckMode::None {
                t_counter!("felix_publish_requests_total", "result" => "accepted").increment(1);
            }
        }
        Ok(false) => {
            t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
            if ack_mode != felix_wire::AckMode::None {
                let request_id = request_id.expect("request id checked");
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        out_ack_tx,
                        out_ack_depth,
                        "felix_broker_out_ack_depth",
                        ack_throttle_tx,
                        encoding.error(request_id, "ingress overloaded".to_string()),
                    )
                    .await,
                    ack_timeout_state,
                    ack_throttle_tx,
                    cancel_tx,
                )
                .await?;
            }
            return Ok(());
        }
        Err(err) => {
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            if ack_mode != felix_wire::AckMode::None {
                let request_id = request_id.expect("request id checked");
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        out_ack_tx,
                        out_ack_depth,
                        "felix_broker_out_ack_depth",
                        ack_throttle_tx,
                        encoding.error(request_id, err.to_string()),
                    )
                    .await,
                    ack_timeout_state,
                    ack_throttle_tx,
                    cancel_tx,
                )
                .await?;
            }
            return Ok(());
        }
    }
    if ack_mode == felix_wire::AckMode::None {
        return Ok(());
    }
    if !ack_on_commit {
        // Enqueue-ack mode:
        // Ack means "accepted into the ingress queue", not "committed". This keeps
        // latency low but can report success even if a later broker error occurs.
        // Batch ack can be sent once enqueued if commit acks are disabled.
        let request_id = request_id.expect("request id checked");
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                encoding.ok(request_id),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        t_counter!("felix_publish_requests_total", "result" => "ok").increment(1);
        for bytes in &payload_bytes {
            t_counter!("felix_publish_bytes_total").increment(*bytes as u64);
        }
        return Ok(());
    }
    let request_id = request_id.expect("request id checked");
    let response_rx = response_rx.expect("response rx available");
    let payload_bytes_for_metrics = payload_bytes;
    // Commit-ack mode:
    // We bound the number of in-flight commit acks. If exhausted, we fail fast.
    // Correctness note: failing after enqueue means the publish may still commit;
    // the client will see an error/overload even though the publish succeeded.
    // If that is unacceptable, we must enforce admission *before* enqueue.
    let permit = match Arc::clone(ack_waiters).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                encoding.error(request_id, "server overloaded".to_string()),
            )
            .await;
            t_counter!("felix_broker_ack_waiters_exhausted_total").increment(1);
            return Ok(());
        }
    };
    let msg = AckWaiterMessage::PublishBatch {
        request_id,
        encoding,
        payload_bytes: payload_bytes_for_metrics,
        response_rx,
        permit,
    };
    match ack_waiter_tx.try_send(msg) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => {
            drop(match msg {
                AckWaiterMessage::Publish { permit, .. }
                | AckWaiterMessage::PublishBatch { permit, .. } => permit,
            });
            t_counter!("felix_broker_ack_waiter_queue_full_total").increment(1);
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                encoding.error(request_id, "server overloaded".to_string()),
            )
            .await;
            return Ok(());
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(msg)) => {
            drop(match msg {
                AckWaiterMessage::Publish { permit, .. }
                | AckWaiterMessage::PublishBatch { permit, .. } => permit,
            });
            t_counter!("felix_broker_ack_waiter_queue_full_total").increment(1);
            let _ = send_outgoing_best_effort(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                encoding.error(request_id, "server overloaded".to_string()),
            )
            .await;
            return Ok(());
        }
    }
    Ok(())
}
