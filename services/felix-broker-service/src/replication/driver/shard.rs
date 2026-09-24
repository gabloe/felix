//! One shard's pass: ship to each follower, advance the quorum mark, and
//! build the report.

use std::sync::Arc;

use felix_broker::Broker;
use felix_router::{Route, ShardKey};
use felix_wire::internal::ShardRef;
use futures::StreamExt;

use crate::peer::PeerRequester;
use crate::replication::halted::HaltedReplica;
use crate::replication::quorum::QuorumMarks;
use crate::replication::reporter::Reporter;
use crate::replication::reporter::{ShardReport, shard_report};
use crate::replication::{
    FollowerCursor, Progress, Rebuilds, lag_records, metrics, quorum_offset, ship_once,
};

/// Passes a draining shard's tail must hold still before it reports drained.
pub(super) const DRAIN_SETTLE_PASSES: u32 = 2;

/// How much of the log one exchange may carry.
///
/// Bounds the leader's memory per follower. A follower far behind costs one
/// batch, not the distance it is behind.
pub(super) const MAX_BATCH_BYTES: usize = 1024 * 1024;

/// Cursors for one shard, valid only at `generation`.
pub struct ShardCursors {
    pub(super) generation: u64,
    /// Where a follower with no cursor yet starts. See [`compare_from`].
    pub(super) base: u64,
    pub(super) followers: Vec<FollowerCursor>,
    /// The tail this draining shard had at the end of the last pass, and how
    /// many passes in a row it has stayed there with nothing in flight.
    pub(super) settled: Option<(u64, u32)>,
}

impl ShardCursors {
    /// Empty, at `generation`. A leadership change discards every belief about
    /// where a follower stood under the old one rather than carrying it
    /// forward.
    pub(super) fn at(generation: u64) -> Self {
        Self {
            generation,
            base: 0,
            followers: Vec::new(),
            settled: None,
        }
    }

    /// Whether a draining shard's log has stopped growing.
    ///
    /// Admission closed when the route went draining, but a publish admitted
    /// just before may still be committing. `in_flight` covers what has been
    /// claimed; the tail holding still across two passes covers the gap
    /// between admission and the claim, since an append wakes another pass.
    fn settle(&mut self, tail: u64, in_flight: usize) -> bool {
        let passes = match self.settled {
            Some((seen, passes)) if seen == tail && in_flight == 0 => passes + 1,
            _ => 0,
        };
        self.settled = Some((tail, passes));
        passes >= DRAIN_SETTLE_PASSES
    }
}

/// One shard's pass: what it shipped, and the cursors it owned while doing it.
pub(super) struct ShardPass {
    pub(super) key: ShardKey,
    pub(super) cursors: ShardCursors,
    pub(super) aux: AuxCursors,
    pub(super) report: Option<ShardReport>,
    pub(super) halted: Vec<HaltedReplica>,
    pub(super) lag: Option<u64>,
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
            halted: Vec::new(),
            lag: None,
        }
    }
}

/// The auxiliary logs that ride a shard's replica set.
pub(super) struct AuxCursors {
    pub(super) group: ShardCursors,
    pub(super) dead_letters: ShardCursors,
    pub(super) counters: ShardCursors,
}

/// Ship one shard, and everything that rides with it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn replicate_shard<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    marks: &QuorumMarks,
    reporter: Option<&Reporter>,
    rebuilds: &Rebuilds,
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
                rebuilds,
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
                shard_report(key, route.generation, tail, &positions, false),
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
            publish_mark(reporter, marks, key, route.generation, &report, offset).await;
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
    // A draining shard says so once its log has stopped growing, and only
    // then: the control plane hands it on against exactly this tail.
    let drained = route.draining && {
        let in_flight = match key.kind {
            felix_router::ShardKind::Stream => {
                broker
                    .in_flight_publishes(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                    .await
            }
            // Cache writes are not claimed the way publishes are; the tail
            // holding still is the whole check.
            felix_router::ShardKind::Cache => 0,
        };
        entry.settle(tail, in_flight)
    };
    let settled = shard_report(key, route.generation, tail, &entry.followers, drained);
    if report_out.as_ref() != Some(&settled) {
        // And the mark with it. Usually a no-op — the mark is monotonic and the
        // majority already moved it — but with five replicas a second follower
        // answering raises the offset a majority holds, and that is this pass's
        // to publish rather than the next one's.
        publish_mark(
            reporter,
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
            rebuilds,
        )
        .await;
        ship_aux_log(
            requester,
            broker,
            key,
            route,
            felix_broker::LogKind::GroupDeadLetters,
            &mut aux.dead_letters,
            rebuilds,
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
            rebuilds,
        )
        .await;
    }

    // Named, not counted. The metric cannot carry the shard without a label
    // per tenant; the listing can, and a halt is useless to act on without it.
    let halted: Vec<HaltedReplica> = entry
        .followers
        .iter()
        .filter_map(|follower| {
            let (reason, remedy) = crate::replication::halted::describe(follower.halted?);
            Some(HaltedReplica {
                tenant_id: key.tenant_id.clone(),
                namespace: key.namespace.clone(),
                stream: key.stream.clone(),
                shard: key.shard,
                kind: match key.kind {
                    felix_router::ShardKind::Cache => "cache",
                    felix_router::ShardKind::Stream => "stream",
                },
                node_id: follower.node_id.clone(),
                generation: route.generation,
                next_offset: follower.next_offset,
                reason,
                remedy,
            })
        })
        .collect();
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

/// Ship one of a stream shard's group-state logs — cursors or dead letters —
/// to the same replicas.
///
/// Separate from the shard's own shipping because it must not affect it: no
/// report is sent for it, no quorum mark is published, and a failure here is
/// logged rather than allowed to stall the records. Both logs are small and
/// written only when group state actually changes, so this is usually a no-op
/// pass.
#[allow(clippy::too_many_arguments)]
pub(super) async fn ship_aux_log<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    key: &ShardKey,
    route: &felix_router::Route,
    log_kind: felix_broker::LogKind,
    entry: &mut ShardCursors,
    rebuilds: &Rebuilds,
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
            rebuilds,
        )
        .await
        {}
    });
    futures::future::join_all(shipping).await;
}

/// Add cursors for new replicas and drop those no longer in the set.
///
/// A new follower starts at `entry.base`, never at the tail: the leader does
/// not know what it holds, and starting at the tail would declare it caught up
/// without comparing anything. One behind that point it either agrees — and
/// the first batch appends — or conflicts, which is the answer worth having.
/// A follower further behind still says so with a `LogGap`, and the leader
/// rewinds to it in that one exchange.
pub(super) fn reconcile_followers(entry: &mut ShardCursors, route: &Route) {
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
pub(super) fn compare_from(generations: &[felix_storage::log::Epoch], generation: u64) -> u64 {
    generations
        .iter()
        .find(|epoch| epoch.generation == generation)
        .map(|epoch| epoch.start_offset.saturating_sub(1))
        .unwrap_or(0)
}

/// The watch's key for a route, carrying the kind across rather than assuming
/// it. A cache shard filed under a stream key would take the mark belonging to
/// the stream of the same name.
pub(super) fn watch_key(key: &ShardKey) -> crate::shards::ShardKey {
    crate::shards::ShardKey {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        kind: match key.kind {
            felix_router::ShardKind::Cache => crate::shards::ShardKind::Cache,
            felix_router::ShardKind::Stream => crate::shards::ShardKind::Stream,
        },
    }
}

/// Tell the control plane who holds what, then move the mark if it listened.
///
/// Returns whether the mark moved. A caller with nothing waiting on the mark
/// can ignore it; a caller on the quorum path cannot.
pub(super) async fn publish_mark(
    reporter: Option<&Reporter>,
    marks: &QuorumMarks,
    key: &ShardKey,
    generation: u64,
    report: &ShardReport,
    offset: u64,
) -> bool {
    let reported = match reporter {
        Some(reporter) => reporter.send(report.clone()).await,
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
