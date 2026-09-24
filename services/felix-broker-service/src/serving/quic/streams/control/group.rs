//! Consumer-group requests on the control stream.

use felix_wire::Message;

use crate::serving::quic::handlers::publish::PublishContext;

/// A group operation for a shard another broker leads, answered with where to
/// go instead of refused.
///
/// Only a client that offered `FEATURE_REDIRECT` gets it; the rest keep the
/// plain refusal they always had. Every group request travels on its own
/// stream, so a `NotLeader` there answers exactly that request.
pub(super) fn group_redirect(
    publish_ctx: &PublishContext,
    peer_features: u32,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) -> Option<Message> {
    if !felix_wire::supports_feature(peer_features, felix_wire::FEATURE_REDIRECT) {
        return None;
    }
    match crate::serving::quic::handlers::redirect::redirect_for(
        publish_ctx.ingress.as_deref(),
        publish_ctx.client_endpoints.as_deref(),
        tenant_id,
        namespace,
        stream,
        shard,
        crate::shards::ShardKind::Stream,
        peer_features,
    ) {
        answer @ Some(Message::NotLeader { .. }) => answer,
        _ => None,
    }
}
