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

use super::quorum::QuorumMarks;
use super::{FollowerCursor, Progress, caught_up, lag_records, metrics, quorum_offset, ship_once};
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
    marks: &QuorumMarks,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
) -> Pass {
    let Some(storage) = broker.durable_storage() else {
        // Nothing to replicate from. A broker without durable storage leads
        // only ephemeral streams, which have no log to ship.
        return Pass::default();
    };

    let table = router.snapshot();
    let mut worst_lag: Option<u64> = None;
    let mut halted = 0usize;
    let mut live_shards = Vec::new();
    let mut reports = Vec::new();

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

        // Who could take this shard over, as of this pass. Collected for the
        // control plane, which gates promotion on it: without this a lost
        // leader cannot be replaced at all.
        reports.push(ShardReport {
            key: key.clone(),
            generation: route.generation,
            caught_up: caught_up(tail, &entry.followers),
        });

        // Published after shipping, so a publish waiting on this shard sees the
        // majority move as soon as this pass establishes it.
        marks.publish(
            &crate::shard_watch::ShardKey {
                tenant_id: key.tenant_id.clone(),
                namespace: key.namespace.clone(),
                stream: key.stream.clone(),
                shard: key.shard,
            },
            route.generation,
            quorum_offset(tail, &entry.followers),
        );

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
    // A shard this broker no longer leads stops promising a quorum. Dropping
    // the mark ends any publish still waiting on it, rather than leaving it to
    // run out its timeout for an answer that can no longer come.
    marks.retain(
        &live_shards
            .iter()
            .map(|key| crate::shard_watch::ShardKey {
                tenant_id: key.tenant_id.clone(),
                namespace: key.namespace.clone(),
                stream: key.stream.clone(),
                shard: key.shard,
            })
            .collect::<Vec<_>>(),
    );

    metrics::record_halted(halted);
    if let Some(lag) = worst_lag {
        metrics::record_lag(lag);
    }
    Pass { worst_lag, reports }
}

/// What one replication pass established.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Pass {
    /// How far the slowest follower is behind, across every shard led here.
    pub worst_lag: Option<u64>,
    /// Per shard, who could take it over.
    pub reports: Vec<ShardReport>,
}

/// One shard's replicas, as this leader currently sees them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardReport {
    pub key: ShardKey,
    pub generation: u64,
    pub caught_up: Vec<String>,
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

/// Where a leader sends its replica reports.
///
/// Optional: a broker with no control plane has nobody to tell, and the reports
/// are only ever read by one.
pub struct ReportTo {
    pub client: reqwest::Client,
    pub base_url: String,
    pub node_id: String,
    pub token: Option<String>,
    pub incarnation: u64,
}

/// Tell the control plane which replicas could take each shard over.
///
/// A failure here is logged and dropped rather than retried. The next pass
/// sends a fresher report anyway, and a queue of stale ones is worse than none:
/// promotion is gated on *recent* positions, so a late report is at best
/// ignored and at worst believed after it stopped being true.
async fn send_reports(to: &ReportTo, reports: &[ShardReport]) {
    if reports.is_empty() {
        return;
    }
    let body = serde_json::json!({
        "incarnation": to.incarnation,
        "shards": reports
            .iter()
            .map(|report| serde_json::json!({
                "tenant_id": report.key.tenant_id,
                "namespace": report.key.namespace,
                "stream": report.key.stream,
                "shard": report.key.shard,
                "generation": report.generation,
                "caught_up": report.caught_up,
            }))
            .collect::<Vec<_>>(),
    });
    let url = format!("{}/v1/nodes/{}/replica-status", to.base_url, to.node_id);
    let mut request = to.client.post(&url).json(&body);
    if let Some(token) = &to.token {
        request = request.bearer_auth(token);
    }
    match request.send().await {
        Ok(response) if response.status().is_success() => {}
        Ok(response) => {
            tracing::warn!(
                status = %response.status(),
                "the control plane refused a replica report",
            );
        }
        Err(err) => {
            tracing::warn!(error = %err, "could not send a replica report");
        }
    }
}

/// Run replication until cancelled.
pub fn spawn<R: PeerRequester + Send + Sync + 'static>(
    requester: Arc<R>,
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
    marks: Arc<QuorumMarks>,
    report_to: Option<ReportTo>,
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
            let pass =
                replicate_once(requester.as_ref(), &broker, &router, &marks, &mut cursors).await;
            if let Some(report_to) = &report_to {
                send_reports(report_to, &pass.reports).await;
            }
        }
    })
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;
