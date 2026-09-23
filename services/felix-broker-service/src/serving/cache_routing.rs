//! Where a cache operation should be applied.
//!
//! A cache key hashes to a shard the same way a routing key does for a publish,
//! and that shard has exactly one owner. Resolving here, before any storage is
//! touched, is what stops two brokers accepting writes for the same key — the
//! divergence the log-backed cache had no defence against.
use bytes::Bytes;

use crate::peer::{CacheRequest, ForwardKey, ForwardTarget};
use crate::shards::routing::{Dispatch, IngressRouter, dispatch, shard_for};
use crate::shards::{ShardKey, ShardKind};

/// Where one cache operation belongs.
#[derive(Debug)]
pub(crate) enum CacheRoute {
    /// This broker owns the shard the key falls in.
    Local { shard: u32 },
    /// Another broker owns it.
    Forward {
        key: ForwardKey,
        target: ForwardTarget,
    },
    /// Nobody can serve it right now, and this says why.
    Refused(String),
}

/// Resolve a cache key to the broker that owns it.
///
/// A single-node broker has no ingress router and takes everything locally,
/// which is what an unsharded cache always did.
pub(crate) fn resolve_cache_route(
    ingress: Option<&IngressRouter>,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    key: &str,
) -> CacheRoute {
    let Some(router) = ingress else {
        return CacheRoute::Local { shard: 0 };
    };

    // The cache's own width, not the stream of the same name: `shards_for`
    // takes the kind precisely so those two cannot be confused.
    let shards = router.shards_for(ShardKind::Cache, tenant_id, namespace, cache);
    let shard = shard_for(shards, Some(key.as_bytes()));
    let shard_key = ShardKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: cache.to_string(),
        shard,
        kind: ShardKind::Cache,
    };

    match dispatch(Some(router), &shard_key) {
        Dispatch::Local => CacheRoute::Local { shard },
        Dispatch::Forward {
            node_id,
            advertise_addr,
            generation,
        } => CacheRoute::Forward {
            key: ForwardKey {
                tenant_id: shard_key.tenant_id,
                namespace: shard_key.namespace,
                stream: shard_key.stream,
                shard,
            },
            target: ForwardTarget {
                node_id,
                advertise_addr,
                generation,
            },
        },
        Dispatch::Unavailable(reason) => CacheRoute::Refused(reason.to_string()),
    }
}

/// Apply a cache operation wherever the key belongs.
///
/// The single chokepoint every cache request goes through, which is why the
/// ownership decision lives here: exactly one broker writes any given key, and
/// the others hand the operation to it rather than serving a second copy.
///
/// `Err` carries a message for the client. It never means "applied somewhere
/// else" — a forward that could not be resolved is refused, not silently
/// downgraded to a local write.
// A cache entry's identity is five fields, and routing needs two more sources
// to resolve it. Bundling them would move the argument list rather than shorten
// it, and the publish handlers carry the same allow for the same reason.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_cache_op(
    broker: &felix_broker::Broker,
    // Read by a write to a `Quorum` cache, which is not acknowledged until a
    // majority of the shard's replicas hold it.
    quorum: (
        Option<&crate::replication::quorum::QuorumMarks>,
        std::time::Duration,
    ),
    ingress: Option<&IngressRouter>,
    peers: Option<&crate::peer::PeerPool>,
    // The caller's token, carried on a forward for the owner to verify.
    credential: &str,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    key: &str,
    request: CacheRequest,
) -> Result<Option<Bytes>, String> {
    match resolve_cache_route(ingress, tenant_id, namespace, cache, key) {
        CacheRoute::Local { shard } => {
            let cache_store = broker.cache();
            let written = ShardKey {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: cache.to_string(),
                shard,
                kind: ShardKind::Cache,
            };
            let (marks, quorum_timeout) = quorum;
            Ok(match request {
                CacheRequest::Put { value, ttl_ms } => {
                    let ttl = (ttl_ms > 0).then(|| std::time::Duration::from_millis(ttl_ms));
                    cache_store
                        .put(tenant_id, namespace, cache, shard, key, value, ttl)
                        .await;
                    crate::replication::quorum::await_cache_quorum(
                        broker,
                        &written,
                        marks,
                        ingress,
                        quorum_timeout,
                    )
                    .await
                    .map_err(|err| err.to_string())?;
                    None
                }
                CacheRequest::Get => {
                    cache_store
                        .get(tenant_id, namespace, cache, shard, key)
                        .await
                }
                CacheRequest::Delete => {
                    let removed = cache_store
                        .delete(tenant_id, namespace, cache, shard, key)
                        .await;
                    crate::replication::quorum::await_cache_quorum(
                        broker,
                        &written,
                        marks,
                        ingress,
                        quorum_timeout,
                    )
                    .await
                    .map_err(|err| err.to_string())?;
                    removed
                }
                // Counter operations go through `apply_counter_op`, which owns the
                // counter store; routing them here would answer from the wrong
                // seam.
                CacheRequest::CounterAdd { .. } | CacheRequest::CounterGet => {
                    return Err("not a cache operation: counters route separately".to_string());
                }
            })
        }
        CacheRoute::Forward {
            key: forward_key,
            target,
        } => {
            // The route said forward and there is nothing to forward with.
            // Refusing beats serving another broker's key locally, which is
            // exactly the divergence this path exists to prevent.
            let Some(pool) = peers else {
                return Err(format!(
                    "no peer transport: this broker cannot forward to {}",
                    target.node_id
                ));
            };
            crate::peer::forward_cache_op(pool, &target, &forward_key, key, credential, &request)
                .await
                .map_err(|err| err.to_string())
        }
        CacheRoute::Refused(reason) => Err(reason),
    }
}

/// Apply a counter operation wherever the key belongs.
///
/// A counter key routes exactly as a cache key of the same cache does — same
/// hash, same shard, same owner — which is what "scoped like cache keys"
/// means in practice: one resolution, two stores. The same refusal rules
/// apply, for the same reasons: an unroutable operation is refused rather
/// than served locally, and a failed read is an error rather than a miss.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_counter_op(
    broker: &felix_broker::Broker,
    ingress: Option<&IngressRouter>,
    peers: Option<&crate::peer::PeerPool>,
    credential: &str,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    key: &str,
    request: CacheRequest,
) -> Result<Option<i64>, String> {
    match resolve_cache_route(ingress, tenant_id, namespace, cache, key) {
        CacheRoute::Local { shard } => {
            let Some(counters) = broker.counters() else {
                return Err("this broker has no durable storage for counters".to_string());
            };
            match request {
                CacheRequest::CounterAdd { delta } => counters
                    .add(tenant_id, namespace, cache, shard, key, delta)
                    .await
                    .map(|(sum, _)| Some(sum))
                    .map_err(|err| err.to_string()),
                CacheRequest::CounterGet => counters
                    .get(tenant_id, namespace, cache, shard, key)
                    .await
                    .map_err(|err| err.to_string()),
                // The counter path never builds these; reaching here is a bug
                // in this file, not in the caller.
                other => Err(format!("not a counter operation: {other:?}")),
            }
        }
        CacheRoute::Forward {
            key: forward_key,
            target,
        } => {
            let Some(pool) = peers else {
                return Err(format!(
                    "no peer transport: this broker cannot forward to {}",
                    target.node_id
                ));
            };
            let answer = crate::peer::forward_cache_op(
                pool,
                &target,
                &forward_key,
                key,
                credential,
                &request,
            )
            .await
            .map_err(|err| err.to_string())?;
            answer
                .map(|bytes| felix_storage::counter_log::decode_sum(&bytes))
                .transpose()
                .map_err(|err| err.to_string())
        }
        CacheRoute::Refused(reason) => Err(reason),
    }
}

/// A cache write, ready to forward.
pub(crate) fn put_request(value: Bytes, ttl: Option<std::time::Duration>) -> CacheRequest {
    CacheRequest::Put {
        value,
        // Zero means "no expiry" on the wire, and a TTL that rounds to zero
        // milliseconds is indistinguishable from none anyway.
        ttl_ms: ttl.map_or(0, |ttl| ttl.as_millis() as u64),
    }
}

#[cfg(test)]
mod tests;
