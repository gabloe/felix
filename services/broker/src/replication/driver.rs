//! The task that keeps every shard this broker leads shipping.
//!
//! One pass visits each shard this node leads that has followers, ships what it
//! can to each, and reports the lag. Cursors live for as long as the assignment
//! does: a generation change discards them, because a cursor is a belief about
//! a follower's position under a particular leadership, and a new generation
//! invalidates the belief rather than the follower.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_router::{Route, ShardKey, ShardRouter};
use felix_wire::internal::ShardRef;
use tokio_util::sync::CancellationToken;

use super::{FollowerCursor, Progress, lag_records, metrics, ship_once};
use crate::peer::PeerRequester;

/// Cursors for one shard, valid only at `generation`.
pub struct ShardCursors {
    generation: u64,
    followers: Vec<FollowerCursor>,
}

/// How much of the log one exchange may carry.
///
/// Bounds the leader's memory per follower. A follower far behind costs one
/// batch, not the distance it is behind.
const MAX_BATCH_BYTES: usize = 1024 * 1024;

/// Ship for every shard this broker leads, once.
///
/// Returns the largest lag seen, so a caller can report it without recomputing.
pub async fn replicate_once<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
) -> Option<u64> {
    let Some(storage) = broker.durable_storage() else {
        // Nothing to replicate from. A broker without durable storage leads
        // only ephemeral streams, which have no log to ship.
        return None;
    };

    let table = router.snapshot();
    let mut worst_lag: Option<u64> = None;
    let mut halted = 0usize;
    let mut live_shards = Vec::new();

    for (key, route) in table.iter() {
        if route.leader.node_id != router.local_node_id() || route.replicas.is_empty() {
            continue;
        }
        live_shards.push(key.clone());

        let entry = cursors.entry(key.clone()).or_insert_with(|| ShardCursors {
            generation: route.generation,
            followers: Vec::new(),
        });
        if entry.generation != route.generation {
            // A new leadership, so every belief about where a follower stood
            // under the old one is discarded rather than carried forward.
            *entry = ShardCursors {
                generation: route.generation,
                followers: Vec::new(),
            };
        }
        reconcile_followers(entry, route);

        let log = match storage.open_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
        {
            Ok(log) => log,
            Err(err) => {
                tracing::warn!(stream = %key.stream, error = %err, "could not open a shard to replicate");
                continue;
            }
        };
        let tail = match log.tail_offset().await {
            Ok(tail) => tail,
            Err(err) => {
                tracing::warn!(stream = %key.stream, error = %err, "could not read a shard's tail");
                continue;
            }
        };

        let shard = ShardRef {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            shard: key.shard,
            generation: route.generation,
        };
        for cursor in &mut entry.followers {
            // Keep going while there is more to send, so a follower catching up
            // is not limited to one batch per tick. It ends on the first answer
            // that is not progress, which bounds the work per pass.
            while let Progress::Stored { .. } =
                ship_once(requester, &log, &shard, cursor, MAX_BATCH_BYTES).await
            {}
        }

        halted += entry
            .followers
            .iter()
            .filter(|follower| follower.halted.is_some())
            .count();
        if let Some(lag) = lag_records(tail, &entry.followers) {
            worst_lag = Some(worst_lag.map_or(lag, |worst: u64| worst.max(lag)));
        }
    }

    // A shard this broker no longer leads keeps no cursors: they would be a
    // belief about a follower under a leadership that has ended.
    cursors.retain(|key, _| live_shards.contains(key));

    metrics::record_halted(halted);
    if let Some(lag) = worst_lag {
        metrics::record_lag(lag);
    }
    worst_lag
}

/// Add cursors for new replicas and drop those no longer in the set.
///
/// A follower added mid-generation starts at zero rather than at the tail: the
/// leader does not know what it holds, and starting at the tail would silently
/// declare it caught up. Starting at zero lets the follower's first `LogGap`
/// say where it actually is.
fn reconcile_followers(entry: &mut ShardCursors, route: &Route) {
    entry
        .followers
        .retain(|cursor| route.replicas.iter().any(|r| r.node_id == cursor.node_id));
    for replica in &route.replicas {
        if !entry
            .followers
            .iter()
            .any(|cursor| cursor.node_id == replica.node_id)
        {
            entry.followers.push(FollowerCursor::new(
                replica.node_id.clone(),
                replica.advertise_addr,
                0,
            ));
        }
    }
}

/// Run replication until cancelled.
pub fn spawn<R: PeerRequester + Send + Sync + 'static>(
    requester: Arc<R>,
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
    interval: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut cursors = HashMap::new();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }
            replicate_once(requester.as_ref(), &broker, &router, &mut cursors).await;
        }
    })
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;
