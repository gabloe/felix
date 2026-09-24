//! The task that keeps every shard this broker leads shipping.
//!
//! One pass visits each shard this node leads that has followers, ships what it
//! can to each, and reports the lag. Cursors live for as long as the assignment
//! does: a generation change discards them, because a cursor is a belief about
//! a follower's position under a particular leadership, and a new generation
//! invalidates the belief rather than the follower.

mod shard;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_router::{ShardKey, ShardRouter};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use super::halted::{HaltedReplica, HaltedReplicas};
use super::quorum::QuorumMarks;
use super::reporter::Reporter;
use super::reporter::ShardReport;
use super::{RebuildPolicy, Rebuilds, metrics};
use crate::peer::PeerRequester;
use crate::shards::lifecycle::fence::ShardFence;
use shard::{AuxCursors, ShardCursors, ShardPass, replicate_shard, watch_key};

/// How many shards a pass ships at the same time.
///
/// Each one in flight holds a peer request and may hold an HTTP report, so a
/// broker leading thousands of shards must not open thousands of those at
/// once. High enough that one slow follower does not gate the rest, low enough
/// to stay a bounded amount of concurrent work.
const SHARD_CONCURRENCY: usize = 16;

/// Run replication until cancelled.
/// What a pass publishes for the rest of the broker to read.
///
/// Together because they are the same thing from two sides: the mark is what a
/// `Quorum` publish waits on, and the listing is what an operator reads when a
/// replica stops contributing to one.
pub struct Published {
    pub marks: Arc<QuorumMarks>,
    pub halted: Arc<HaltedReplicas>,
}

#[allow(clippy::too_many_arguments)]
pub fn spawn<R: PeerRequester + Send + Sync + 'static>(
    requester: Arc<R>,
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
    fence: Arc<ShardFence>,
    published: Published,
    reporter: Option<Reporter>,
    interval: Duration,
    rebuild_policy: RebuildPolicy,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let rebuilds = Rebuilds::new(rebuild_policy);
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
            let pass = replicate_once_with(
                requester.as_ref(),
                &broker,
                &router,
                &fence,
                &published.marks,
                reporter.as_ref(),
                &mut cursors,
                &mut group_cursors,
                &mut dead_letter_cursors,
                &mut counter_cursors,
                &rebuilds,
            )
            .await;
            // Replaced wholesale, so a halt that has since resolved stops being
            // listed rather than sending an operator after a replica that is
            // already shipping again.
            published.halted.publish(pass.halted);
        }
    })
}

/// Ship for every shard this broker leads, once.
///
/// Returns the largest lag seen, so a caller can report it without recomputing.
/// Nothing is fenced here, so a draining shard counts as quiet at once; use
/// [`replicate_once_with`] to drain against a real fence.
// One cursor map per log kind the pass ships, plus the shared context: folding
// the maps into a struct would move the argument list rather than shorten it.
#[allow(clippy::too_many_arguments)]
pub async fn replicate_once<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    marks: &QuorumMarks,
    reporter: Option<&Reporter>,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
    group_cursors: &mut HashMap<ShardKey, ShardCursors>,
    dead_letter_cursors: &mut HashMap<ShardKey, ShardCursors>,
    counter_cursors: &mut HashMap<ShardKey, ShardCursors>,
) -> Pass {
    replicate_once_with(
        requester,
        broker,
        router,
        &ShardFence::default(),
        marks,
        reporter,
        cursors,
        group_cursors,
        dead_letter_cursors,
        counter_cursors,
        &Rebuilds::disabled(),
    )
    .await
}

/// [`replicate_once`] with halted followers rebuilt under `rebuilds`, and a
/// draining shard held back until `fence` says its writes have stopped.
#[allow(clippy::too_many_arguments)]
pub async fn replicate_once_with<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    fence: &ShardFence,
    marks: &QuorumMarks,
    reporter: Option<&Reporter>,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
    group_cursors: &mut HashMap<ShardKey, ShardCursors>,
    dead_letter_cursors: &mut HashMap<ShardKey, ShardCursors>,
    counter_cursors: &mut HashMap<ShardKey, ShardCursors>,
    rebuilds: &Rebuilds,
) -> Pass {
    if broker.durable_storage().is_none() {
        // Nothing to replicate from. Without durable storage a broker's streams
        // are ephemeral and its cache is in memory, so no shard it leads has a
        // log to ship.
        return Pass::default();
    }
    // Slots in use are whatever the cursors still say is rebuilding. A cursor
    // discarded on a generation change or a lost shard took its slot with it,
    // and nothing else would give it back.
    rebuilds.set_in_flight(rebuilding_count(&[
        cursors,
        group_cursors,
        dead_letter_cursors,
        counter_cursors,
    ]));

    let table = router.snapshot();
    let mut worst_lag: Option<u64> = None;
    let mut halted: Vec<HaltedReplica> = Vec::new();
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
            replicate_shard(
                requester, broker, fence, marks, reporter, rebuilds, key, route, entry, aux,
            )
        }))
        .buffer_unordered(SHARD_CONCURRENCY)
        .collect()
        .await;

    for pass in passes {
        cursors.insert(pass.key.clone(), pass.cursors);
        group_cursors.insert(pass.key.clone(), pass.aux.group);
        dead_letter_cursors.insert(pass.key.clone(), pass.aux.dead_letters);
        counter_cursors.insert(pass.key, pass.aux.counters);
        halted.extend(pass.halted);
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

    metrics::record_halted(halted.len());
    if let Some(lag) = worst_lag {
        metrics::record_lag(lag);
    }
    Pass {
        worst_lag,
        reports,
        halted,
    }
}

/// What one replication pass established.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Pass {
    /// How far the slowest follower is behind, across every shard led here.
    pub worst_lag: Option<u64>,
    /// Per shard, who could take it over.
    pub reports: Vec<ShardReport>,
    /// Replicas this broker has stopped shipping to, named rather than counted.
    /// The metric cannot carry a shard without a label per tenant; this is what
    /// an operator reads instead.
    pub halted: Vec<HaltedReplica>,
}

fn rebuilding_count(maps: &[&HashMap<ShardKey, ShardCursors>]) -> usize {
    maps.iter()
        .flat_map(|map| map.values())
        .flat_map(|entry| entry.followers.iter())
        .filter(|cursor| cursor.rebuilding)
        .count()
}

#[cfg(test)]
mod tests;
