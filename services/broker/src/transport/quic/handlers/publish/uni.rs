// Publish handlers for uni-directional streams (fire-and-forget, no acks).

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_authz::{Action, Namespace, StreamName, TenantId, stream_resource};
use felix_broker::Broker;
use felix_wire::Frame;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;

use crate::auth::AuthContext;
use crate::transport::quic::handlers::publish::ingress::{PublishTarget, enqueue_publish};
use crate::transport::quic::handlers::publish::{
    PublishContext, PublishJob, StreamHandleCache, resolve_stream_cached,
};
use crate::transport::quic::telemetry::log_decode_error;

pub(crate) async fn handle_binary_publish_batch_uni(
    broker: &Broker,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    frame: &Frame,
    auth_ctx: Option<&AuthContext>,
) -> Result<bool> {
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
            log_decode_error("uni_binary_publish_batch", &err, frame);
            return Err(err);
        }
    };
    let auth_ctx = match auth_ctx {
        Some(ctx) => ctx,
        None => return Ok(false),
    };
    if auth_ctx.tenant_id != batch.tenant_id {
        return Ok(false);
    }
    let resource = stream_resource(
        &TenantId::new(batch.tenant_id.as_str()),
        &Namespace::new(batch.namespace.as_str()),
        &StreamName::new(batch.stream.as_str()),
    );
    if !auth_ctx.matcher.allows(Action::StreamPublish, &resource) {
        return Ok(false);
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
        return Ok(true);
    };
    let payloads = batch
        .payloads
        .into_iter()
        .map(Bytes::from)
        .collect::<Vec<_>>();
    match enqueue_publish(
        publish_ctx,
        PublishJob {
            target: PublishTarget::Resolved(stream_handle),
            payloads,
            response: None,
            admission_permit: None,
        },
        publish_ctx.overflow_policy(),
    )
    .await
    {
        Ok(true) => {
            t_counter!("felix_publish_requests_total", "result" => "accepted").increment(1);
        }
        Ok(false) => {
            t_counter!("felix_publish_requests_total", "result" => "dropped").increment(1);
        }
        Err(_) => {
            t_counter!("felix_publish_requests_total", "result" => "error").increment(1);
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_message_uni(
    broker: &Broker,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Vec<u8>,
) -> Result<bool> {
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_items_in_ok.fetch_add(1, Ordering::Relaxed);
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
        return Ok(true);
    };

    let r = enqueue_publish(
        publish_ctx,
        PublishJob {
            target: PublishTarget::Resolved(stream_handle),
            payloads: vec![Bytes::from(payload)],
            response: None,
            admission_permit: None,
        },
        publish_ctx.overflow_policy(),
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
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_batch_message_uni(
    broker: &Broker,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    tenant_id: String,
    namespace: String,
    stream: String,
    payloads: Vec<Vec<u8>>,
) -> Result<bool> {
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::transport::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters
            .pub_items_in_ok
            .fetch_add(payloads.len() as u64, Ordering::Relaxed);
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
        return Ok(true);
    };
    let payloads = payloads.into_iter().map(Bytes::from).collect::<Vec<_>>();

    let r = enqueue_publish(
        publish_ctx,
        PublishJob {
            target: PublishTarget::Resolved(stream_handle),
            payloads,
            response: None,
            admission_permit: None,
        },
        publish_ctx.overflow_policy(),
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
            return Ok(false);
        }
    }
    Ok(true)
}
