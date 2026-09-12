//! Where a cache operation should be applied.
//!
//! A cache key hashes to a shard the same way a routing key does for a publish,
//! and that shard has exactly one owner. Resolving here, before any storage is
//! touched, is what stops two brokers accepting writes for the same key — the
//! divergence the log-backed cache had no defence against.
use bytes::Bytes;

use crate::peer::{CacheRequest, ForwardKey, ForwardTarget};
use crate::shard_routing::{Dispatch, IngressRouter, dispatch, shard_for};
use crate::shard_watch::{ShardKey, ShardKind};

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
    cache_store: &dyn felix_storage::StorageApi,
    ingress: Option<&IngressRouter>,
    peers: Option<&crate::peer::PeerPool>,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    key: &str,
    request: CacheRequest,
) -> Result<Option<Bytes>, String> {
    match resolve_cache_route(ingress, tenant_id, namespace, cache, key) {
        CacheRoute::Local { shard } => Ok(match request {
            CacheRequest::Put { value, ttl_ms } => {
                let ttl = (ttl_ms > 0).then(|| std::time::Duration::from_millis(ttl_ms));
                cache_store
                    .put(tenant_id, namespace, cache, shard, key, value, ttl)
                    .await;
                None
            }
            CacheRequest::Get => {
                cache_store
                    .get(tenant_id, namespace, cache, shard, key)
                    .await
            }
            CacheRequest::Delete => {
                cache_store
                    .delete(tenant_id, namespace, cache, shard, key)
                    .await
            }
        }),
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
            crate::peer::forward_cache_op(pool, &target, &forward_key, key, &request)
                .await
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
#[path = "cache_routing_tests.rs"]
mod tests;
