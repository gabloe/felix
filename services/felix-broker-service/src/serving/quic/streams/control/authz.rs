//! Checking a request's token against the resource it names, and answering
//! the refusal.

use anyhow::Result;
use felix_authz::{
    Action, CacheScope, Namespace, StreamName, TenantId, cache_resource, stream_resource,
};
use felix_wire::Message;

use crate::serving::auth::AuthContext;
use crate::serving::quic::handlers::publish::{
    Outgoing, handle_ack_enqueue_result, send_outgoing_critical,
};

use super::responder::{Responder, send_control_error};

pub(super) async fn authorize_stream(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    stream: &str,
    request_id: Option<u64>,
    ctx: &Responder<'_>,
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
    let resource = stream_resource(
        &TenantId::new(tenant_id),
        &Namespace::new(namespace),
        &StreamName::new(stream),
    );
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

pub(super) async fn authorize_stream_simple(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    stream: &str,
    ctx: &Responder<'_>,
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
    let resource = stream_resource(
        &TenantId::new(tenant_id),
        &Namespace::new(namespace),
        &StreamName::new(stream),
    );
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

pub(super) async fn authorize_cache(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    cache: &str,
    ctx: &Responder<'_>,
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
    let resource = cache_resource(
        &TenantId::new(tenant_id),
        &Namespace::new(namespace),
        &CacheScope::new(cache),
    );
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
