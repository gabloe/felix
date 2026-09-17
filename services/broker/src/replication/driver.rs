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
// One cursor map per log kind the pass ships, plus the shared context: folding
// the maps into a struct would move the argument list rather than shorten it.
#[allow(clippy::too_many_arguments)]
pub async fn replicate_once<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    marks: &QuorumMarks,
    report_to: Option<&ReportTo>,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
    group_cursors: &mut HashMap<ShardKey, ShardCursors>,
    dead_letter_cursors: &mut HashMap<ShardKey, ShardCursors>,
    counter_cursors: &mut HashMap<ShardKey, ShardCursors>,
) -> Pass {
    if broker.durable_storage().is_none() {
        // Nothing to replicate from. Without durable storage a broker's streams
        // are ephemeral and its cache is in memory, so no shard it leads has a
        // log to ship.
        return Pass::default();
    }

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

        // Resolved per pass rather than cached: a cache shard's log is replaced
        // by compaction, and a handle held across one reads the retired copy.
        let log_kind = match key.kind {
            felix_router::ShardKind::Cache => felix_broker::LogKind::Cache,
            felix_router::ShardKind::Stream => felix_broker::LogKind::Stream,
        };
        let Some(log) = broker
            .shard_log(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
            )
            .await
        else {
            tracing::warn!(
                kind = ?key.kind,
                name = %key.stream,
                shard = key.shard,
                "could not open a shard to replicate",
            );
            continue;
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
        // Followers are shipped to concurrently.
        //
        // Sequentially, one follower that is gone holds up every follower
        // behind it *and* the mark published below, so the cost of unreachable
        // replicas adds up instead of overlapping. A minority failure is the
        // case `Quorum` exists to tolerate, so it must not be the case that
        // stalls it.
        //
        // Nothing here is given a deadline. A pass that cancelled a follower
        // mid-exchange would be cancelling the slow ones as readily as the dead
        // ones, and a dial cut short caches no connection -- so the next pass
        // dials again and is cut again. What a peer that is gone costs is
        // bounded by the pool's handshake timeout and then by its reconnect
        // backoff.
        let shipping = entry.followers.iter_mut().map(|cursor| async {
            // Keep going while there is more to send, so a follower catching up
            // is not limited to one batch per tick. It ends on the first answer
            // that is not progress, which bounds the work per pass.
            while let Progress::Stored { .. } =
                ship_once(requester, &log, &shard, log_kind, cursor, MAX_BATCH_BYTES).await
            {
            }
        });
        futures::future::join_all(shipping).await;

        // Re-read after shipping, not before.
        //
        // A publish landing between the earlier read and here leaves `tail`
        // describing a log that is already shorter than the one on disk. Both
        // the report and the mark below are relative to it, so a follower level
        // with the *old* tail would be reported caught up and counted toward the
        // quorum for a record it does not have — which is exactly how a
        // quorum-acknowledged record ends up on a promoted broker that never
        // stored it.
        let tail = log.tail_offset().await.unwrap_or(tail);

        // Who could take this shard over, as of this pass.
        let report = ShardReport {
            key: key.clone(),
            generation: route.generation,
            caught_up: caught_up(tail, &entry.followers),
            offsets: entry
                .followers
                .iter()
                .filter(|follower| follower.halted.is_none())
                .map(|follower| (follower.node_id.clone(), follower.next_offset))
                .collect(),
        };
        reports.push(report.clone());

        // **Reported before the mark is published, and awaited.**
        //
        // The mark is what releases a `Quorum` publish, and the report is what
        // promotion later reads. Releasing the publish first leaves a window in
        // which a leader has told a client its record is on a majority and has
        // told the control plane nothing about which replica holds it — and a
        // leader that dies in that window is replaced by whichever replica
        // scores highest, which may be the one that does not have it. The
        // acknowledged record is then gone, which is the one thing `Quorum` is
        // supposed to rule out.
        //
        // Reporting first costs a round trip to the control plane on the path
        // of a quorum publish. That is the price of the acknowledgement meaning
        // what it says.
        //
        // A report that did not land leaves the mark where it was. The whole
        // argument above rests on the control plane knowing which replica holds
        // the record, so releasing the publish on a report that failed to send
        // is the same window the ordering exists to close — just reached by a
        // different route. The publish waits, the next pass retries, and a
        // client is told a timeout rather than an acknowledgement this broker
        // cannot stand behind.
        let reported = match report_to {
            Some(report_to) => send_reports(report_to, std::slice::from_ref(&report)).await,
            // Nothing to report to, so nothing to be behind: a broker with no
            // cluster membership has no promotion to inform.
            None => true,
        };

        if reported {
            // Published after shipping, so a publish waiting on this shard sees
            // the majority move as soon as this pass establishes it.
            marks.publish(
                &watch_key(key),
                route.generation,
                quorum_offset(tail, &entry.followers),
            );
        } else {
            metrics::record_mark_withheld();
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                "holding the quorum mark: the replica report did not reach the \
                 control plane, so an acknowledgement now could not be made good \
                 on at failover",
            );
        }

        // A stream shard has two logs beside it: the positions its consumer
        // groups have reached, and the offsets those groups gave up on. Both
        // ride the same replica set and the same generation, so they are
        // shipped here rather than placed separately — group state has to be
        // wherever the shard's leader is, and move when the shard moves.
        //
        // Deliberately after the report and the quorum mark, and never gating
        // either: no publish waits on group state, and group state lagging
        // must not hold up the records it describes.
        if key.kind == felix_router::ShardKind::Stream {
            ship_aux_log(
                requester,
                broker,
                key,
                route,
                felix_broker::LogKind::GroupCursors,
                group_cursors,
            )
            .await;
            ship_aux_log(
                requester,
                broker,
                key,
                route,
                felix_broker::LogKind::GroupDeadLetters,
                dead_letter_cursors,
            )
            .await;
        }
        // A cache shard's counterpart: the counter log rides the cache's
        // replica set the way group state rides the stream's, and gates
        // nothing for the same reason.
        if key.kind == felix_router::ShardKind::Cache {
            ship_aux_log(
                requester,
                broker,
                key,
                route,
                felix_broker::LogKind::Counters,
                counter_cursors,
            )
            .await;
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
    group_cursors.retain(|key, _| live_shards.contains(key));
    dead_letter_cursors.retain(|key, _| live_shards.contains(key));
    counter_cursors.retain(|key, _| live_shards.contains(key));
    // A shard this broker no longer leads stops promising a quorum. Dropping
    // the mark ends any publish still waiting on it, rather than leaving it to
    // run out its timeout for an answer that can no longer come.
    marks.retain(&live_shards.iter().map(watch_key).collect::<Vec<_>>());

    metrics::record_halted(halted);
    if let Some(lag) = worst_lag {
        metrics::record_lag(lag);
    }
    Pass { worst_lag, reports }
}

/// Ship one of a stream shard's group-state logs — cursors or dead letters —
/// to the same replicas.
///
/// Separate from the shard's own shipping because it must not affect it: no
/// report is sent for it, no quorum mark is published, and a failure here is
/// logged rather than allowed to stall the records. Both logs are small and
/// written only when group state actually changes, so this is usually a no-op
/// pass.
async fn ship_aux_log<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    key: &ShardKey,
    route: &felix_router::Route,
    log_kind: felix_broker::LogKind,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
) {
    let Some(log) = broker
        .shard_log(
            log_kind,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
        )
        .await
    else {
        // No such state on this broker, so there is nothing to ship.
        return;
    };

    let entry = cursors.entry(key.clone()).or_insert_with(|| ShardCursors {
        generation: route.generation,
        followers: Vec::new(),
    });
    if entry.generation != route.generation {
        *entry = ShardCursors {
            generation: route.generation,
            followers: Vec::new(),
        };
    }
    reconcile_followers(entry, route);

    let shard = ShardRef {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        generation: route.generation,
    };
    let shipping = entry.followers.iter_mut().map(|cursor| async {
        while let Progress::Stored { .. } = crate::replication::ship_once(
            requester,
            &log,
            &shard,
            log_kind,
            cursor,
            MAX_BATCH_BYTES,
        )
        .await
        {}
    });
    futures::future::join_all(shipping).await;
}

/// The watch's key for a route, carrying the kind across rather than assuming
/// it. A cache shard filed under a stream key would take the mark belonging to
/// the stream of the same name.
fn watch_key(key: &ShardKey) -> crate::shard_watch::ShardKey {
    crate::shard_watch::ShardKey {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        kind: match key.kind {
            felix_router::ShardKind::Cache => crate::shard_watch::ShardKind::Cache,
            felix_router::ShardKind::Stream => crate::shard_watch::ShardKind::Stream,
        },
    }
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
    /// How far each follower had got. Sent as well as `caught_up` because
    /// "caught up" is only true of the tail it was measured against, and a
    /// leader that reports and then writes more before dying leaves a report
    /// that says every replica was level without saying level with what.
    pub offsets: Vec<(String, u64)>,
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
    /// Held, not copied: this reports for the life of the process, across
    /// however many access tokens that spans.
    pub token: Option<crate::credential::NodeCredential>,
    pub incarnation: u64,
}

/// Tell the control plane which replicas could take each shard over, and say
/// whether it took the report.
///
/// Not retried here: the next pass sends a fresher one, and a queue of stale
/// reports is worse than none, since promotion is gated on *recent* positions.
/// The answer instead gates the quorum mark, so "could not tell" reads as
/// false — see the caller.
async fn send_reports(to: &ReportTo, reports: &[ShardReport]) -> bool {
    if reports.is_empty() {
        return true;
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
                // Without the kind the control plane files a cache's report
                // under the stream of the same name, so placement finds no
                // caught-up replica for the cache and its shard is never
                // promoted -- the contents are unreachable after a failover.
                "kind": match report.key.kind {
                    felix_router::ShardKind::Cache => "cache",
                    felix_router::ShardKind::Stream => "stream",
                },
                "generation": report.generation,
                "caught_up": report.caught_up,
                "replica_offsets": report
                    .offsets
                    .iter()
                    .map(|(node_id, durable_offset)| serde_json::json!({
                        "node_id": node_id,
                        "durable_offset": durable_offset,
                    }))
                    .collect::<Vec<_>>(),
            }))
            .collect::<Vec<_>>(),
    });
    let url = format!("{}/v1/nodes/{}/replica-status", to.base_url, to.node_id);
    let mut request = to.client.post(&url).json(&body);
    if let Some(token) = &to.token {
        request = request.bearer_auth(token.bearer());
    }
    match request.send().await {
        Ok(response) if response.status().is_success() => true,
        Ok(response) => {
            tracing::warn!(
                status = %response.status(),
                "the control plane refused a replica report",
            );
            false
        }
        Err(err) => {
            tracing::warn!(error = %err, "could not send a replica report");
            false
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
        let mut group_cursors = HashMap::new();
        let mut dead_letter_cursors = HashMap::new();
        let mut counter_cursors = HashMap::new();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }
            replicate_once(
                requester.as_ref(),
                &broker,
                &router,
                &marks,
                report_to.as_ref(),
                &mut cursors,
                &mut group_cursors,
                &mut dead_letter_cursors,
                &mut counter_cursors,
            )
            .await;
        }
    })
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;
