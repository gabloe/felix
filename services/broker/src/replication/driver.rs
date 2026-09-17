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
use felix_common::membership::{
    ReplicaOffset, ReplicaStatusRequest, ShardKind as WireShardKind, ShardReplicaStatus,
};
use felix_router::{Route, ShardKey, ShardRouter};
use felix_wire::internal::ShardRef;
use tokio_util::sync::CancellationToken;

use futures::StreamExt;

use super::quorum::QuorumMarks;
use super::{FollowerCursor, Progress, caught_up, lag_records, metrics, quorum_offset, ship_once};
use crate::peer::PeerRequester;

/// Cursors for one shard, valid only at `generation`.
pub struct ShardCursors {
    generation: u64,
    /// Where a follower with no cursor yet starts. See [`compare_from`].
    base: u64,
    followers: Vec<FollowerCursor>,
}

impl ShardCursors {
    /// Empty, at `generation`. A leadership change discards every belief about
    /// where a follower stood under the old one rather than carrying it
    /// forward.
    fn at(generation: u64) -> Self {
        Self {
            generation,
            base: 0,
            followers: Vec::new(),
        }
    }
}

/// Tell the control plane who holds what, then move the mark if it listened.
///
/// Returns whether the mark moved. A caller with nothing waiting on the mark
/// can ignore it; a caller on the quorum path cannot.
async fn publish_mark(
    report_to: Option<&ReportTo>,
    marks: &QuorumMarks,
    key: &ShardKey,
    generation: u64,
    report: &ShardReport,
    offset: u64,
) -> bool {
    let reported = match report_to {
        Some(report_to) => send_reports(report_to, std::slice::from_ref(report)).await,
        // Nothing to report to, so nothing to be behind: a broker with no
        // cluster membership has no promotion to inform.
        None => true,
    };
    if reported {
        marks.publish(&watch_key(key), generation, offset);
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
    reported
}

/// Who could take this shard over, as of `tail`.
fn shard_report(
    key: &ShardKey,
    generation: u64,
    tail: u64,
    followers: &[FollowerCursor],
) -> ShardReport {
    ShardReport {
        key: key.clone(),
        generation,
        caught_up: caught_up(tail, followers),
        offsets: followers
            .iter()
            .filter(|follower| follower.halted.is_none())
            .map(|follower| (follower.node_id.clone(), follower.next_offset))
            .collect(),
    }
}

/// Where to start comparing with a follower whose position is unknown.
///
/// Offset zero re-reads the whole log — a batch per pass, per shard, at the
/// sweep's interval — which after a failover is minutes to hours of shipping
/// records the follower already has. The generation history bounds it.
///
/// `generations` says where each leadership began *here*. Below the current
/// one, this broker's records were taken from earlier leaders as a follower,
/// and so were the follower's; two prefixes of the same log agree. At or above
/// it is where they can differ: records this leadership wrote, and records a
/// predecessor left on the follower alone.
///
/// One record earlier than that, so the first batch overlaps something the
/// follower already holds and the boundary itself gets compared rather than
/// assumed — the same check Raft makes at `prevLogIndex`.
///
/// Zero when the history has no entry for this generation: a shard written
/// before the history existed, or a log that could not record it. Slow, and
/// the behaviour that was there before.
fn compare_from(generations: &[felix_storage::disk_log::epochs::Epoch], generation: u64) -> u64 {
    generations
        .iter()
        .find(|epoch| epoch.generation == generation)
        .map(|epoch| epoch.start_offset.saturating_sub(1))
        .unwrap_or(0)
}

/// How many shards a pass ships at the same time.
///
/// Each one in flight holds a peer request and may hold an HTTP report, so a
/// broker leading thousands of shards must not open thousands of those at
/// once. High enough that one slow follower does not gate the rest, low enough
/// to stay a bounded amount of concurrent work.
const SHARD_CONCURRENCY: usize = 16;

/// How much of the log one exchange may carry.
///
/// Bounds the leader's memory per follower. A follower far behind costs one
/// batch, not the distance it is behind.
const MAX_BATCH_BYTES: usize = 1024 * 1024;

/// One shard's pass: what it shipped, and the cursors it owned while doing it.
struct ShardPass {
    key: ShardKey,
    cursors: ShardCursors,
    aux: AuxCursors,
    report: Option<ShardReport>,
    halted: usize,
    lag: Option<u64>,
}

impl ShardPass {
    /// Nothing shipped, nothing to report — the shard could not be opened or
    /// read. The cursors travel back untouched so the next pass resumes from
    /// where this one found them.
    fn quiet(key: ShardKey, cursors: ShardCursors, aux: AuxCursors) -> Self {
        Self {
            key,
            cursors,
            aux,
            report: None,
            halted: 0,
            lag: None,
        }
    }
}

/// The auxiliary logs that ride a shard's replica set.
struct AuxCursors {
    group: ShardCursors,
    dead_letters: ShardCursors,
    counters: ShardCursors,
}

/// Ship one shard, and everything that rides with it.
#[allow(clippy::too_many_arguments)]
async fn replicate_shard<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    marks: &QuorumMarks,
    report_to: Option<&ReportTo>,
    key: ShardKey,
    route: felix_router::Route,
    mut entry: ShardCursors,
    mut aux: AuxCursors,
) -> ShardPass {
    let route = &route;
    let shard_key = key.clone();
    let key = &key;
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
        return ShardPass::quiet(shard_key, entry, aux);
    };
    let tail = match log.tail_offset().await {
        Ok(tail) => tail,
        Err(err) => {
            tracing::warn!(stream = %key.stream, error = %err, "could not read a shard's tail");
            return ShardPass::quiet(shard_key, entry, aux);
        }
    };

    // Read only when a cursor has to be created — a generation change, or a
    // replica added to the set. The usual pass creates none, and the history
    // is a few hundred entries to clone.
    let needs_base = route.replicas.iter().any(|replica| {
        !entry
            .followers
            .iter()
            .any(|cursor| cursor.node_id == replica.node_id)
    });
    if needs_base {
        entry.base = compare_from(&log.generations(), route.generation);
    }
    reconcile_followers(&mut entry, route);

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
    // **The mark advances at the majority, not at the last follower.**
    //
    // Waiting for every follower before publishing meant a `Quorum` publish
    // was held by the *slowest* replica, which is not what quorum means: with
    // three replicas a record is on a majority the moment one follower has it,
    // and the second is a durability margin, not a precondition. One dead
    // replica put its whole handshake timeout in front of every acknowledgement
    // on the shard, every pass, which is the failure `Quorum` is meant to
    // tolerate rather than be stalled by.
    //
    // Each follower's cursor goes into its own future and comes back from it,
    // so a position can be read while others are still in flight. A follower
    // that has not answered yet keeps the position it came in with, which is
    // behind where it may already be — so the mark it contributes to is a
    // floor, never a claim beyond what has been established.
    let mut positions: Vec<FollowerCursor> = entry.followers.clone();
    let mut in_flight: futures::stream::FuturesUnordered<_> = entry
        .followers
        .drain(..)
        .map(|mut cursor| async {
            // Keep going while there is more to send, so a follower catching up
            // is not limited to one batch per tick. It ends on the first answer
            // that is not progress, which bounds the work per pass.
            while let Progress::Stored { .. } = ship_once(
                requester,
                &log,
                &shard,
                log_kind,
                &mut cursor,
                MAX_BATCH_BYTES,
            )
            .await
            {}
            cursor
        })
        .collect();

    /// Put a finished cursor back where its stale copy was.
    fn settle(positions: &mut [FollowerCursor], cursor: FollowerCursor) {
        if let Some(slot) = positions
            .iter_mut()
            .find(|held| held.node_id == cursor.node_id)
        {
            *slot = cursor;
        }
    }

    // Drain until a majority is established, or until everyone is in.
    //
    // The tail is re-read after each answer rather than once before shipping,
    // for the reason it was already re-read once: a publish landing in between
    // leaves it describing a log that is already shorter than the one on disk,
    // and a follower level with the *old* tail would be counted toward the
    // quorum for a record it does not have.
    let mut majority = None;
    while let Some(cursor) = in_flight.next().await {
        settle(&mut positions, cursor);
        let tail = log.tail_offset().await.unwrap_or(tail);
        let offset = quorum_offset(tail, &positions);
        if offset > 0 {
            majority = Some((
                shard_report(key, route.generation, tail, &positions),
                offset,
            ));
            break;
        }
    }

    // The report goes out *beside* the followers still shipping, not in front
    // of them. Awaiting it inside the drain loop suspends every future still in
    // `in_flight`, so a slow control plane would stall replication to the rest
    // of the replica set — the same head-of-line block this change exists to
    // remove, just moved onto the reporting hop.
    let (rest, reported) = futures::future::join(
        async {
            let mut rest = Vec::new();
            while let Some(cursor) = in_flight.next().await {
                rest.push(cursor);
            }
            rest
        },
        async {
            let (report, offset) = majority?;

            // **Reported before the mark is published, and awaited.**
            //
            // The mark is what releases a `Quorum` publish, and the report is
            // what promotion later reads. Releasing the publish first leaves a
            // window in which a leader has told a client its record is on a
            // majority and has told the control plane nothing about which
            // replica holds it — and a leader that dies in that window is
            // replaced by whichever replica scores highest, which may be the
            // one that does not have it. The acknowledged record is then gone,
            // which is the one thing `Quorum` is supposed to rule out.
            //
            // A report that did not land leaves the mark where it was, because
            // the argument above rests on the control plane knowing who holds
            // the record: releasing on a failed report reaches the same window
            // by another route. The publish waits, the next pass retries, and a
            // client is told a timeout rather than an acknowledgement this
            // broker cannot stand behind.
            publish_mark(report_to, marks, key, route.generation, &report, offset).await;
            Some(report)
        },
    )
    .await;
    for cursor in rest {
        settle(&mut positions, cursor);
    }
    let mut report_out = reported;

    entry.followers = positions;

    // What the pass ended up seeing, when that is not what was already sent.
    //
    // Two cases reach here. A pass where no majority ever advanced sent
    // nothing, and the report is the promotion signal as much as the mark's
    // precondition — a shard whose followers are all stuck is the one the
    // control plane most needs a current view of. And a follower that answered
    // after the majority report went out has moved since; leaving it until the
    // next pass would keep a replica that is level looking behind, and so out
    // of promotion, for no reason.
    //
    // Equal reports send nothing, which is the healthy case: followers finish
    // together, so the majority report already described all of them.
    let tail = log.tail_offset().await.unwrap_or(tail);
    let settled = shard_report(key, route.generation, tail, &entry.followers);
    if report_out.as_ref() != Some(&settled) {
        // And the mark with it. Usually a no-op — the mark is monotonic and the
        // majority already moved it — but with five replicas a second follower
        // answering raises the offset a majority holds, and that is this pass's
        // to publish rather than the next one's.
        publish_mark(
            report_to,
            marks,
            key,
            route.generation,
            &settled,
            quorum_offset(tail, &entry.followers),
        )
        .await;
        report_out = Some(settled);
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
            &mut aux.group,
        )
        .await;
        ship_aux_log(
            requester,
            broker,
            key,
            route,
            felix_broker::LogKind::GroupDeadLetters,
            &mut aux.dead_letters,
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
            &mut aux.counters,
        )
        .await;
    }

    let halted = entry
        .followers
        .iter()
        .filter(|follower| follower.halted.is_some())
        .count();
    let lag = lag_records(tail, &entry.followers);

    ShardPass {
        key: shard_key,
        cursors: entry,
        aux,
        report: report_out,
        halted,
        lag,
    }
}

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

    // Every shard this broker leads, each with the cursors it owns for the
    // duration. Taken out of the maps rather than borrowed from them, which is
    // what lets the shards run at the same time below.
    let mut work = Vec::new();
    for (key, route) in table.iter() {
        if route.leader.node_id != router.local_node_id() || route.replicas.is_empty() {
            continue;
        }
        live_shards.push(key.clone());

        let mut entry = cursors
            .remove(key)
            .unwrap_or_else(|| ShardCursors::at(route.generation));
        if entry.generation != route.generation {
            entry = ShardCursors::at(route.generation);
        }
        let aux = AuxCursors {
            group: group_cursors
                .remove(key)
                .unwrap_or_else(|| ShardCursors::at(route.generation)),
            dead_letters: dead_letter_cursors
                .remove(key)
                .unwrap_or_else(|| ShardCursors::at(route.generation)),
            counters: counter_cursors
                .remove(key)
                .unwrap_or_else(|| ShardCursors::at(route.generation)),
        };
        work.push((key.clone(), route.clone(), entry, aux));
    }

    // Shards at the same time, not one after another.
    //
    // Sequentially, a shard whose follower is slow held up every shard behind
    // it — including their quorum marks, and so every `Quorum` publish waiting
    // on them. One tenant's unlucky replica became every tenant's latency.
    //
    // Bounded, because each shard in flight holds a peer request and may hold
    // an HTTP report: a broker leading thousands of shards must not open
    // thousands of those at once.
    let passes: Vec<ShardPass> =
        futures::stream::iter(work.into_iter().map(|(key, route, entry, aux)| {
            replicate_shard(requester, broker, marks, report_to, key, route, entry, aux)
        }))
        .buffer_unordered(SHARD_CONCURRENCY)
        .collect()
        .await;

    for pass in passes {
        cursors.insert(pass.key.clone(), pass.cursors);
        group_cursors.insert(pass.key.clone(), pass.aux.group);
        dead_letter_cursors.insert(pass.key.clone(), pass.aux.dead_letters);
        counter_cursors.insert(pass.key, pass.aux.counters);
        halted += pass.halted;
        if let Some(lag) = pass.lag {
            worst_lag = Some(worst_lag.map_or(lag, |worst: u64| worst.max(lag)));
        }
        if let Some(report) = pass.report {
            reports.push(report);
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
    entry: &mut ShardCursors,
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

    if entry.generation != route.generation {
        *entry = ShardCursors::at(route.generation);
    }
    // Base stays zero here. These logs are written only when group state
    // changes, so comparing from the start costs almost nothing, and the
    // leader does not open them at takeover to record a generation against.
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
/// A new follower starts at `entry.base`, never at the tail: the leader does
/// not know what it holds, and starting at the tail would declare it caught up
/// without comparing anything. One behind that point it either agrees — and
/// the first batch appends — or conflicts, which is the answer worth having.
/// A follower further behind still says so with a `LogGap`, and the leader
/// rewinds to it in that one exchange.
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
                entry.base,
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
    // The shared type, not a `json!` literal: the control plane parses this
    // same definition, so a field renamed on one side stops compiling instead
    // of quietly arriving as a missing one.
    let body = ReplicaStatusRequest {
        incarnation: to.incarnation,
        shards: reports
            .iter()
            .map(|report| ShardReplicaStatus {
                tenant_id: report.key.tenant_id.clone(),
                namespace: report.key.namespace.clone(),
                stream: report.key.stream.clone(),
                shard: report.key.shard,
                // Without the kind the control plane files a cache's report
                // under the stream of the same name, so placement finds no
                // caught-up replica for the cache and its shard is never
                // promoted -- the contents are unreachable after a failover.
                kind: match report.key.kind {
                    felix_router::ShardKind::Cache => WireShardKind::Cache,
                    felix_router::ShardKind::Stream => WireShardKind::Stream,
                },
                generation: report.generation,
                caught_up: report.caught_up.to_vec(),
                replica_offsets: report
                    .offsets
                    .iter()
                    .map(|(node_id, durable_offset)| ReplicaOffset {
                        node_id: node_id.clone(),
                        durable_offset: *durable_offset,
                    })
                    .collect(),
            })
            .collect(),
    };
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
        // Woken by a durable append as well as by the tick. Under `Quorum` the
        // publish that just landed is about to wait on a majority, and waiting
        // out a tick first put seconds in front of milliseconds of shipping.
        //
        // The tick stays: it covers shards with no recent appends, the
        // auxiliary logs, and the replica report, none of which an append
        // signals.
        let appended = broker.appended();
        let mut cursors = HashMap::new();
        let mut group_cursors = HashMap::new();
        let mut dead_letter_cursors = HashMap::new();
        let mut counter_cursors = HashMap::new();
        loop {
            // An append during the previous pass left a permit, so this
            // returns at once rather than waiting for the tick — see
            // `Broker::appended`.
            let woken = appended.notified();
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
                _ = woken => {}
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
