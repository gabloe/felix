//! Where a publish goes: served here, forwarded to the shard's owner, or
//! refused.

use std::time::Instant;

use felix_broker::{Broker, StreamHandle};

use super::PublishContext;
use super::ingress::PublishTarget;
use super::stream_cache::{StreamHandleCache, push_stream_cache_key};
use crate::serving::forward::ForwardTarget;
use crate::serving::quic::STREAM_CACHE_TTL;
use crate::serving::quic::telemetry::t_counter;
use crate::shards::routing::{Dispatch, IngressRouter, dispatch};
use crate::shards::{ShardKey, ShardKind};

/// The shard a publish that carries no routing key belongs to.
///
/// The same answer `shard_for` gives for `None`. Named so the keyless paths say
/// which shard they mean instead of each recomputing it, because route and batch
/// disagreeing about the shard is precisely the bug this replaced.
pub(crate) const UNKEYED_SHARD: u32 = 0;

/// Resolve a stream, or say where else the publish belongs.
///
/// The single chokepoint every publish path funnels through, which is why the
/// ownership gate is here: nothing reaches storage without passing it.
///
/// Ownership is checked *outside* the handle cache, deliberately. The cache
/// exists to avoid a registry lookup and holds for `STREAM_CACHE_TTL`; ownership
/// changes the instant the control plane says so, and caching it would keep a
/// broker serving a reassigned shard for up to a TTL. The check is two atomic
/// loads, so paying it per publish costs less than reasoning about staleness.
// Six of these are the shard's identity plus the two things needed to resolve
// it. Bundling them into a struct would move the argument list rather than
// shorten it, and the same allow is already on the publish handlers.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_route(
    broker: &Broker,
    authority: Authority<'_>,
    cache: &mut StreamHandleCache,
    key_scratch: &mut String,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) -> PublishRoute {
    // Single-node brokers short-circuit on a null check; a cluster member pays
    // two loads. Either way there is no lock and no await.
    let Authority { ingress, lease } = authority;

    // The admission fence. Cheap and possibly stale, so it only sheds early --
    // the authoritative check happens again before the record is committed. See
    // `crate::cluster::lease`.
    if let Some(lease) = lease
        && !lease.looks_valid()
    {
        crate::cluster::lease::metrics::record_refusal(
            crate::cluster::lease::metrics::BOUNDARY_ADMISSION,
        );
        tracing::debug!(
            tenant_id,
            namespace,
            stream,
            "publish refused: this broker no longer holds a lease on the shards it led",
        );
        return PublishRoute::Refused;
    }

    // Carried to the claim, where the fence refuses the write if this broker
    // stopped serving the shard in the meantime.
    let mut generation = 0;
    if ingress.is_some() {
        let key = ShardKey {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            // The shard the caller resolved. Dispatch and the log this publish
            // lands in must agree on it, or a record is written to one shard's
            // log and replicated from another's.
            shard,
            kind: ShardKind::Stream,
        };
        match dispatch(ingress, &key) {
            Dispatch::Local { generation: at } => generation = at,
            Dispatch::Forward {
                node_id,
                advertise_addr,
                generation,
            } => {
                return PublishRoute::Forward(ForwardTarget {
                    node_id,
                    advertise_addr,
                    generation,
                });
            }
            Dispatch::Unavailable(reason) => {
                t_counter!("felix_publish_requests_total", "result" => "unroutable").increment(1);
                tracing::debug!(
                    tenant_id, namespace, stream,
                    reason = %reason,
                    "publish refused: shard is not servable here",
                );
                return PublishRoute::Refused;
            }
        }
    }

    // Short-lived cache to avoid repeated stream lookups on hot paths.
    //
    // Keyed by shard as well as stream: a broker can own several shards of one
    // stream, they are separate logs, and a cache that ignored the shard would
    // hand a publish for one of them the handle of another.
    push_stream_cache_key(key_scratch, tenant_id, namespace, stream, shard);
    if let Some((handle, expires)) = cache.get(key_scratch.as_str())
        && *expires > Instant::now()
        && handle.as_ref().is_none_or(StreamHandle::is_active)
    {
        return PublishRoute::local(handle.clone(), generation);
    }
    let handle = broker
        .resolve_stream_handle(tenant_id, namespace, stream, shard)
        .await
        .ok();
    cache.insert(
        key_scratch.clone(),
        (handle.clone(), Instant::now() + STREAM_CACHE_TTL),
    );
    PublishRoute::local(handle, generation)
}

/// Where a publish should be applied.
#[derive(Debug)]
pub(crate) enum PublishRoute {
    /// This broker owns the shard, at `generation`, and the stream resolved
    /// here.
    Local {
        handle: StreamHandle,
        generation: u64,
    },
    /// Another broker owns it.
    Forward(ForwardTarget),
    /// Nobody can take it right now, or the stream does not resolve.
    Refused,
}

impl PublishRoute {
    fn local(handle: Option<StreamHandle>, generation: u64) -> Self {
        match handle {
            Some(handle) => Self::Local { handle, generation },
            None => Self::Refused,
        }
    }
}

/// What decides whether this broker may serve a publish at all.
///
/// The two travel together because they answer halves of one question — the
/// router says whether this broker *should* hold the shard, the lease says
/// whether it still *may* act on that. Separating them at a call site is how one
/// gets forgotten.
#[derive(Clone, Copy)]
pub(crate) struct Authority<'a> {
    pub(crate) ingress: Option<&'a IngressRouter>,
    pub(crate) lease: Option<&'a crate::cluster::lease::LeaseState>,
}

/// Which shard a record with this routing key belongs to.
///
/// The stream's width comes from the router's own snapshot rather than the
/// stream catalog: it is an `ArcSwap` read with no lock, on a path that resolves
/// a shard for every publish, and the router is already the thing that decides
/// how the cluster is divided.
///
/// A broker with no cluster behind it has one shard, so every key lands on 0 —
/// which is also what a stream placed with one shard does, and is why adding a
/// key to a single-shard stream changes nothing.
pub(crate) fn resolve_shard(
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    key: Option<&[u8]>,
) -> u32 {
    let shards = publish_ctx
        .ingress
        .as_ref()
        .map(|ingress| {
            ingress.shards_for(
                crate::shards::ShardKind::Stream,
                tenant_id,
                namespace,
                stream,
            )
        })
        .unwrap_or(1);
    crate::shards::routing::shard_for(shards, key)
}

/// Turn a resolved route into a publish target.
///
/// `None` means this broker cannot serve the publish: the stream did not
/// resolve, or the shard belongs to a peer this broker has no transport to.
/// Callers answer that the same way they always answered an unresolvable
/// stream.
#[allow(clippy::too_many_arguments)]
pub(crate) fn publish_target(
    route: PublishRoute,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    // The shard `resolve_route` resolved and dispatched on. It has to travel
    // with the batch: the owner writes it to the shard this names, so a
    // hardcoded 0 here sends every keyed publish to shard 0's owner no matter
    // which shard the key belongs to.
    shard: u32,
    ack: felix_wire::internal::AckMode,
    // The publisher's own token. It travels with a forward so the owner can
    // check it rather than trust that this broker did.
    credential: &str,
) -> Option<PublishTarget> {
    match route {
        PublishRoute::Local { handle, generation } => Some(PublishTarget::Resolved {
            handle,
            shard: local_shard_key(publish_ctx, tenant_id, namespace, stream, shard),
            generation,
        }),
        PublishRoute::Forward(target) => {
            if publish_ctx.peers.is_none() {
                t_counter!("felix_publish_requests_total", "result" => "not_owner").increment(1);
                tracing::debug!(
                    tenant_id, namespace, stream,
                    owner = %target.node_id,
                    "publish refused: shard is owned by another broker and this one cannot forward",
                );
                return None;
            }
            t_counter!("felix_publish_requests_total", "result" => "forwarded").increment(1);
            Some(PublishTarget::Forward {
                target,
                key: crate::serving::forward::ForwardKey {
                    tenant_id: tenant_id.to_string(),
                    namespace: namespace.to_string(),
                    stream: stream.to_string(),
                    // The shard the route was resolved for. The owner appends to
                    // exactly this shard, so it must match what `resolve_route`
                    // dispatched on or the record lands in another shard's log.
                    shard,
                },
                ack,
                credential: credential.to_string(),
            })
        }
        PublishRoute::Refused => None,
    }
}

/// How the owner should treat a forwarded batch, from what the client asked of
/// this broker.
///
/// Anything other than "no ack" becomes `OnCommit`, because the owner's write
/// path is durable-then-answer either way: a weaker mode would describe the
/// answer inaccurately rather than make it cheaper.
pub(crate) fn internal_ack(ack: Option<felix_wire::AckMode>) -> felix_wire::internal::AckMode {
    match ack {
        Some(felix_wire::AckMode::None) => felix_wire::internal::AckMode::None,
        _ => felix_wire::internal::AckMode::OnCommit,
    }
}

/// True when this publish must not be acknowledged until a majority holds it.
///
/// `Quorum` is the stream that said "accepted by this broker is not good
/// enough", so the local ack-on-commit policy cannot answer for it.
pub(crate) fn needs_quorum(target: &Option<PublishTarget>) -> bool {
    matches!(
        target,
        Some(PublishTarget::Resolved { handle, .. } | PublishTarget::Idempotent { handle, .. })
            if handle.consistency() == felix_broker::ConsistencyLevel::Quorum
    )
}

/// The shard a local publish waits on for its quorum, when this broker is in
/// a cluster. Built the same way `resolve_route` built the key it dispatched
/// on, so the shard a publish waits for is the shard it landed on. `None` on a
/// single-node broker, which has no replica set and so nothing to wait for.
pub(crate) fn local_shard_key(
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) -> Option<ShardKey> {
    publish_ctx.ingress.as_ref().map(|_| ShardKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    })
}
