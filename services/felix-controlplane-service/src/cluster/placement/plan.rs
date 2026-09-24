//! One pass of placement over a metadata snapshot, as a pure function.
use std::collections::HashMap;

use super::moves::{MovePolicy, Moves, move_step};
use super::rendezvous::{choose, choose_replicas, promote};
use super::{Blocked, CaughtUp, Decision, MoveStep, Unplaceable};
use crate::model::{
    Cache, Node, NodeLifecycle, ShardAssignment, ShardKey, ShardKind, ShardState, Stream,
};

/// The full result of one reconciliation pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    pub shards: Vec<ShardPlan>,
}

impl Plan {
    pub fn to_place(&self) -> impl Iterator<Item = (&ShardKey, &str, &[String])> {
        self.shards.iter().filter_map(|plan| match &plan.decision {
            Decision::Place(leader, replicas) => {
                Some((&plan.key, leader.as_str(), replicas.as_slice()))
            }
            _ => None,
        })
    }

    pub fn unplaceable(&self) -> impl Iterator<Item = (&ShardKey, &Unplaceable)> {
        self.shards.iter().filter_map(|plan| match &plan.decision {
            Decision::Unplaceable(reason) => Some((&plan.key, reason)),
            _ => None,
        })
    }

    pub fn kept(&self) -> usize {
        self.shards
            .iter()
            .filter(|plan| plan.decision == Decision::Kept)
            .count()
    }

    pub fn moves(&self) -> impl Iterator<Item = (&ShardKey, &MoveStep, &ShardAssignment)> {
        self.shards.iter().filter_map(|plan| match &plan.decision {
            Decision::Move(step, assignment) => Some((&plan.key, step, assignment)),
            _ => None,
        })
    }

    pub fn waiting(&self) -> impl Iterator<Item = (&ShardKey, &Blocked)> {
        self.shards.iter().filter_map(|plan| match &plan.decision {
            Decision::Waiting(reason) => Some((&plan.key, reason)),
            _ => None,
        })
    }
}

/// One shard's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardPlan {
    pub key: ShardKey,
    pub decision: Decision,
}

/// Decide where every shard of every stream belongs.
///
/// Independent of the order `streams`, `nodes`, and `existing` arrive in: shards
/// are planned in sorted order and candidates are chosen by score, so two
/// control-plane instances reading the same rows agree without coordinating.
pub fn plan(
    streams: &[Stream],
    caches: &[Cache],
    nodes: &[Node],
    existing: &[ShardAssignment],
    caught_up: &dyn CaughtUp,
) -> Plan {
    plan_with(
        streams,
        caches,
        nodes,
        existing,
        caught_up,
        MovePolicy::default(),
    )
}

/// [`plan`] under an explicit move policy.
pub fn plan_with(
    streams: &[Stream],
    caches: &[Cache],
    nodes: &[Node],
    existing: &[ShardAssignment],
    caught_up: &dyn CaughtUp,
    policy: MovePolicy,
) -> Plan {
    let placeables: Vec<Placeable<'_>> = streams
        .iter()
        .map(Placeable::of_stream)
        .chain(caches.iter().map(Placeable::of_cache))
        .collect();

    // A live node may be given shards. A draining node keeps serving what it
    // has until each shard is moved off it, so its assignments go through the
    // move path, not the failover path.
    let eligible: Vec<&Node> = {
        let mut live: Vec<&Node> = nodes
            .iter()
            .filter(|node| node.status.lifecycle == NodeLifecycle::Live)
            .collect();
        // Sorted so the tie-break below is stable regardless of input order.
        live.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        live
    };
    let is_live = |id: &str| eligible.iter().any(|node| node.node_id == id);
    let is_draining = |id: &str| {
        nodes
            .iter()
            .any(|node| node.node_id == id && node.status.lifecycle == NodeLifecycle::Draining)
    };
    let is_serving = |id: &str| is_live(id) || is_draining(id);

    let current: HashMap<&ShardKey, &ShardAssignment> =
        existing.iter().map(|a| (&a.key, a)).collect();

    // Load counts every assignment we intend to exist after this pass, so a
    // cap is respected across kept and newly placed shards alike.
    let mut load: HashMap<&str, u32> = HashMap::new();
    // Leaders per live node as they will stand once every move in flight
    // completes: a staged successor already counts. Otherwise an over-share
    // node stages more moves than it needs and the destination ends up over.
    let mut leaders: HashMap<&str, u32> = HashMap::new();
    for assignment in existing {
        // Every role, followers too: `max_shards` caps roles.
        for node in assignment.nodes().filter(|node| is_live(node)) {
            *load.entry(node.as_str()).or_default() += 1;
        }
        let will_lead = assignment
            .successor
            .as_deref()
            .filter(|successor| is_live(successor))
            .unwrap_or(assignment.leader.as_str());
        if is_live(will_lead) {
            *leaders.entry(will_lead).or_default() += 1;
        }
    }

    // Keyed by the same string `owner_of` builds, so the lookup below cannot
    // disagree with the key it is derived from.
    let placeable_of: HashMap<String, &Placeable<'_>> = placeables
        .iter()
        .map(|placeable| {
            (
                format!(
                    "{}/{}/{}/{}",
                    placeable.kind, placeable.tenant_id, placeable.namespace, placeable.name
                ),
                placeable,
            )
        })
        .collect();

    let mut keys: Vec<ShardKey> = placeables
        .iter()
        .flat_map(|placeable| (0..placeable.shards).map(move |shard| placeable.key(shard)))
        .collect();
    keys.sort_by(|a, b| order(a).cmp(&order(b)));

    // The balanced share for bounded-load rendezvous: total roles (leaders and
    // replicas) spread evenly across the eligible nodes. Counting replicas keeps
    // a replicated stream's replica load from starving a node of leadership.
    let total_roles: u32 = placeables
        .iter()
        .map(|p| p.shards.saturating_mul(p.replication_factor.max(1)))
        .sum();
    let cap = if eligible.is_empty() {
        u32::MAX
    } else {
        total_roles.div_ceil(eligible.len() as u32).max(1)
    };
    // Fair share of leadership, for the rebalance trigger. Leaders only: that
    // is the load a client feels.
    let leader_share = if eligible.is_empty() {
        u32::MAX
    } else {
        (keys.len() as u32).div_ceil(eligible.len() as u32).max(1)
    };

    let mut moves = Moves::counting(existing, policy);

    // Slots go to drains first, then to rebalancing, and last to shards whose
    // previous move timed out; see `start_order`. Planned in that order, and
    // listed in key order.
    let class = |key: &ShardKey| {
        current
            .get(key)
            .map_or(0, |existing| start_order(existing, &is_live, &is_draining))
    };
    keys.sort_by(|a, b| {
        class(a)
            .cmp(&class(b))
            .then_with(|| order(a).cmp(&order(b)))
    });

    let mut shards = Vec::with_capacity(keys.len());
    for key in keys {
        let placeable = placeable_of.get(owner_of(&key).as_str()).copied();
        let replication_factor = placeable.map_or(1, |p| p.replication_factor);
        let durable = placeable.is_none_or(|p| p.durable);

        // The leader is still serving: a move, not a reassignment, unless
        // there is no log to move.
        if let Some(existing) = current.get(&key)
            && is_serving(&existing.leader)
            && (durable || is_live(&existing.leader))
        {
            let decision = move_step(
                &key,
                existing,
                replication_factor,
                &eligible,
                &is_live,
                &is_draining,
                caught_up,
                &mut load,
                &mut leaders,
                leader_share,
                &mut moves,
            );
            shards.push(ShardPlan { key, decision });
            continue;
        }

        // Replaced below, so its roles are not the ones that will exist.
        if let Some(previous) = current.get(&key) {
            for node in previous.nodes() {
                if let Some(count) = load.get_mut(node.as_str()) {
                    *count = count.saturating_sub(1);
                }
            }
        }

        // The leader is gone. Prefer one of its followers -- but only one that
        // actually holds the log, or the failover is the data loss.
        if let Some(previous) = current.get(&key)
            && let Some(promoted) = promote(&key, previous, &eligible, caught_up)
        {
            *load.entry(promoted).or_default() += 1;
            let replicas = choose_replicas(
                &key,
                &eligible,
                &mut load,
                promoted,
                replication_factor.saturating_sub(1),
            );
            shards.push(ShardPlan {
                key,
                decision: Decision::Place(promoted.to_string(), replicas),
            });
            continue;
        }

        // The shard was replicated and nothing that holds it can lead. Placing
        // it on a node that has never seen it is not a failover, it is a
        // silently empty shard: the records stay on the replicas, unreachable,
        // while a new leader serves nothing at a newer generation.
        //
        // A stream that never asked for replication is untouched by this — it
        // has no replicas, so there was never a copy to prefer, and a fresh
        // placement remains the only thing available.
        if let Some(previous) = current.get(&key)
            && !previous.replicas.is_empty()
            && durable
        {
            shards.push(ShardPlan {
                key,
                decision: Decision::Unplaceable(Unplaceable::NoCaughtUpReplica),
            });
            continue;
        }

        let decision = match choose(&key, &eligible, &load, cap, &leaders, leader_share) {
            Some(leader) => {
                *load.entry(leader).or_default() += 1;
                *leaders.entry(leader).or_default() += 1;
                // Followers are the next best-scoring nodes for this shard,
                // excluding the leader. Chosen by the same score so the whole
                // replica set is a deterministic function of the shard key and
                // the cluster -- two control-plane instances planning the same
                // cluster produce the same set.
                let replicas = choose_replicas(
                    &key,
                    &eligible,
                    &mut load,
                    leader,
                    replication_factor.saturating_sub(1),
                );
                Decision::Place(leader.to_string(), replicas)
            }
            None if eligible.is_empty() => Decision::Unplaceable(Unplaceable::NoEligibleNode),
            None => Decision::Unplaceable(Unplaceable::AllNodesAtCapacity),
        };
        shards.push(ShardPlan { key, decision });
    }

    shards.sort_by(|a, b| order(&a.key).cmp(&order(&b.key)));
    Plan { shards }
}

/// Build the assignment a `Place` decision calls for.
///
/// New assignments start `Assigning`: placement has decided, and the broker has
/// not yet confirmed it is serving. Generation is the store's.
pub fn assignment_for(key: &ShardKey, leader: &str, replicas: Vec<String>) -> ShardAssignment {
    ShardAssignment {
        key: key.clone(),
        leader: leader.to_string(),
        replicas,
        generation: 0,
        state: ShardState::Assigning,
        successor: None,
        joining: None,
        move_started_at_millis: None,
    }
}

/// Which shards get a move slot first. Only matters for shards that may
/// start a move; one already moving holds its slot whatever its class.
///
/// A draining node is waiting to leave, where an imbalance only costs
/// evenness, so drains go before rebalancing. A shard whose last move timed
/// out goes behind both, or the one move that keeps failing takes the slot
/// every time.
fn start_order(
    existing: &ShardAssignment,
    is_live: &dyn Fn(&str) -> bool,
    is_draining: &dyn Fn(&str) -> bool,
) -> u8 {
    let moving = existing.successor.is_some()
        || existing.joining.is_some()
        || existing.state == ShardState::Draining;
    if moving {
        return 0;
    }
    let draining =
        !is_live(&existing.leader) || existing.replicas.iter().any(|replica| is_draining(replica));
    let class = if draining { 0 } else { 1 };
    if existing.move_started_at_millis.is_some() {
        class + 2
    } else {
        class
    }
}

/// One thing with shards to place, viewed the only way placement cares about.
///
/// Streams and caches are placed by the same algorithm because they are the
/// same log underneath. Collapsing them here rather than running two passes is
/// what makes the `max_shards` cap and the load counting apply across both — two
/// passes would each believe it had the whole cluster to itself.
#[derive(Debug, Clone, Copy)]
struct Placeable<'a> {
    tenant_id: &'a str,
    namespace: &'a str,
    name: &'a str,
    kind: ShardKind,
    shards: u32,
    replication_factor: u32,
    /// Whether there is a log to hand off. An ephemeral stream is reassigned
    /// outright.
    durable: bool,
}

impl<'a> Placeable<'a> {
    fn of_stream(stream: &'a Stream) -> Self {
        Self {
            tenant_id: &stream.tenant_id,
            namespace: &stream.namespace,
            name: &stream.stream,
            kind: ShardKind::Stream,
            shards: stream.shards,
            replication_factor: stream.replication_factor.max(1),
            durable: stream.durable,
        }
    }

    fn of_cache(cache: &'a Cache) -> Self {
        Self {
            tenant_id: &cache.tenant_id,
            namespace: &cache.namespace,
            name: &cache.cache,
            kind: ShardKind::Cache,
            shards: cache.shards,
            replication_factor: cache.replication_factor.max(1),
            // A cache is durable wherever the broker is; assume it is.
            durable: true,
        }
    }

    fn key(&self, shard: u32) -> ShardKey {
        ShardKey {
            tenant_id: self.tenant_id.to_string(),
            namespace: self.namespace.to_string(),
            stream: self.name.to_string(),
            shard,
            kind: self.kind,
        }
    }
}

/// The stream or cache a shard belongs to, as `kind/tenant/namespace/name`.
///
/// The kind leads because a cache and a stream may share every other field.
fn owner_of(key: &ShardKey) -> String {
    format!(
        "{}/{}/{}/{}",
        key.kind, key.tenant_id, key.namespace, key.stream
    )
}

/// Kind sorts last so the relative order of stream shards is what it always
/// was, and a cache sharing a stream's name is never an unstable tie.
fn order(key: &ShardKey) -> (&str, &str, &str, u32, ShardKind) {
    (
        &key.tenant_id,
        &key.namespace,
        &key.stream,
        key.shard,
        key.kind,
    )
}
