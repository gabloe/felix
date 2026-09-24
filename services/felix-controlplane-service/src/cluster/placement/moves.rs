//! Moving a shard off a leader that is still serving: stage, fence, cut over.
//!
//! Each step is one assignment write, decided from the store and fresh
//! reports alone, so any instance resumes a half-done move where the last
//! pass left it.
use std::collections::HashMap;

use super::rendezvous::{choose_replicas, promote, score};
use super::{Blocked, CaughtUp, Decision, MoveStep};
use crate::model::{Node, ShardAssignment, ShardKey, ShardState};

/// One at a time, like the broker's rebuild limit: a move is a full copy of
/// a shard's log.
pub const DEFAULT_MAX_CONCURRENT_MOVES: usize = 1;

/// How many moves may be in progress at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MovePolicy {
    /// Cluster-wide. A move holds a slot from staging to cut-over. `0` starts
    /// nothing: a drain waits and an imbalance stays, both visibly.
    pub max_concurrent: usize,
}

impl Default for MovePolicy {
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_MAX_CONCURRENT_MOVES,
        }
    }
}

/// The move slots one pass hands out.
pub(super) struct Moves {
    pub(super) in_flight: usize,
    pub(super) policy: MovePolicy,
}

impl Moves {
    /// Take a slot for a new move, if one is free.
    fn begin(&mut self) -> bool {
        if self.in_flight < self.policy.max_concurrent {
            self.in_flight += 1;
            true
        } else {
            false
        }
    }
}

/// What to do about a shard whose leader is still serving.
///
/// Stage the destination as a replica; fence once it is caught up, which
/// stops the leader; cut over once the leader reports drained. A leader that
/// dies mid-move is handled by the failover path, where the successor is a
/// candidate like any other replica. Every input is in the store or a fresh
/// report, so any pass resumes where the last left off.
#[allow(clippy::too_many_arguments)]
pub(super) fn move_step<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    replication_factor: u32,
    eligible: &[&'a Node],
    is_live: &dyn Fn(&str) -> bool,
    is_draining: &dyn Fn(&str) -> bool,
    caught_up: &dyn CaughtUp,
    load: &mut HashMap<&'a str, u32>,
    leaders: &mut HashMap<&'a str, u32>,
    leader_share: u32,
    moves: &mut Moves,
) -> Decision {
    let leader = existing.leader.as_str();
    let leader_live = is_live(leader);
    let caught_up = &AtGeneration {
        inner: caught_up,
        generation: existing.generation,
    };
    // The maps are keyed by catalog-lifetime strings.
    let catalog_id = |id: &str| -> Option<&'a str> {
        eligible
            .iter()
            .find(|node| node.node_id == id)
            .map(|node| node.node_id.as_str())
    };

    if existing.state == ShardState::Draining {
        if !caught_up.is_drained(key, existing.generation) {
            return Decision::Waiting(Blocked::LeaderStopping);
        }
        // Whoever leads next must hold everything the leader held.
        let target = existing
            .successor
            .as_deref()
            .filter(|successor| is_live(successor) && caught_up.is_caught_up(key, successor))
            .or_else(|| promote(key, existing, eligible, caught_up))
            // Nothing else holds the log; the leader takes it back at a new
            // generation and the move is chosen again.
            .unwrap_or(leader);
        let replicas = replicas_after_cut_over(
            key,
            existing,
            target,
            replication_factor,
            eligible,
            is_live,
            load,
        );
        // The tally already credits this shard to its live successor, so
        // only a cut-over somewhere else moves the count.
        let counted = existing
            .successor
            .as_deref()
            .filter(|successor| is_live(successor))
            .unwrap_or(leader);
        if counted != target {
            if let Some(from) = catalog_id(counted) {
                leaders
                    .entry(from)
                    .and_modify(|count| *count = count.saturating_sub(1));
            }
            if let Some(to) = catalog_id(target) {
                *leaders.entry(to).or_default() += 1;
            }
        }
        return Decision::Move(
            MoveStep::CutOver {
                from: leader.to_string(),
                to: target.to_string(),
            },
            ShardAssignment {
                key: key.clone(),
                leader: target.to_string(),
                replicas,
                generation: 0,
                state: ShardState::Assigning,
                successor: None,
                joining: None,
                move_started_at_millis: None,
            },
        );
    }

    if let Some(successor) = existing.successor.as_deref() {
        if !is_live(successor) {
            // Gone before it led. Undone rather than waited out, or the move
            // holds its slot for as long as the node is away.
            let mut replicas = existing.replicas.clone();
            replicas.retain(|replica| replica != successor);
            return Decision::Move(
                MoveStep::Abandon {
                    successor: successor.to_string(),
                },
                ShardAssignment {
                    key: key.clone(),
                    leader: leader.to_string(),
                    replicas,
                    generation: 0,
                    state: existing.state,
                    successor: None,
                    joining: None,
                    move_started_at_millis: None,
                },
            );
        }
        if caught_up.is_caught_up(key, successor) {
            return Decision::Move(
                MoveStep::Fence,
                ShardAssignment {
                    key: key.clone(),
                    leader: leader.to_string(),
                    replicas: existing.replicas.clone(),
                    generation: 0,
                    state: ShardState::Draining,
                    successor: Some(successor.to_string()),
                    joining: None,
                    move_started_at_millis: None,
                },
            );
        }
        return Decision::Waiting(Blocked::DestinationCatchingUp {
            successor: successor.to_string(),
        });
    }

    // Nothing in progress. Should a move start?
    let over_share = leaders.get(leader).copied().unwrap_or(0) > leader_share;
    let wanted = if !leader_live {
        // Draining: a caught-up live replica under its share is cheapest, the
        // copy already exists; otherwise the bounded choice, or any live node
        // with room.
        promote_under_share(key, existing, eligible, caught_up, leaders, leader_share)
            .or_else(|| choose_destination(key, eligible, leaders, leader_share, leader, false))
    } else if leader_live && over_share {
        // Only from over share to under share, so moves converge instead of
        // trading shards back and forth.
        choose_destination(key, eligible, leaders, leader_share, leader, true)
    } else {
        None
    };

    match wanted {
        Some(destination) => {
            if !moves.begin() {
                return Decision::Waiting(Blocked::MoveLimit);
            }
            if let Some(from) = catalog_id(leader) {
                leaders
                    .entry(from)
                    .and_modify(|count| *count = count.saturating_sub(1));
            }
            *leaders.entry(destination).or_default() += 1;
            let already_a_replica = existing.replicas.iter().any(|r| r == destination);
            if already_a_replica && caught_up.is_caught_up(key, destination) {
                // The copy is already there and level: straight to the fence.
                return Decision::Move(
                    MoveStep::Fence,
                    ShardAssignment {
                        key: key.clone(),
                        leader: leader.to_string(),
                        replicas: existing.replicas.clone(),
                        generation: 0,
                        state: ShardState::Draining,
                        successor: Some(destination.to_string()),
                        joining: None,
                        move_started_at_millis: None,
                    },
                );
            }
            let mut replicas = existing.replicas.clone();
            if !already_a_replica {
                replicas.push(destination.to_string());
                *load.entry(destination).or_default() += 1;
            }
            Decision::Move(
                MoveStep::Stage {
                    successor: destination.to_string(),
                },
                ShardAssignment {
                    key: key.clone(),
                    leader: leader.to_string(),
                    replicas,
                    generation: 0,
                    state: existing.state,
                    successor: Some(destination.to_string()),
                    joining: None,
                    move_started_at_millis: None,
                },
            )
        }
        None if !leader_live => Decision::Waiting(Blocked::NoDestination),
        None => reseat(key, existing, eligible, is_draining, load, moves),
    }
}

/// Replace a follower on a draining node with one that is staying.
///
/// Draining only, never merely down: a rolling restart takes every node down
/// in turn, and reseating each would copy every shard once per restart.
fn reseat<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    eligible: &[&'a Node],
    is_draining: &dyn Fn(&str) -> bool,
    load: &mut HashMap<&'a str, u32>,
    moves: &mut Moves,
) -> Decision {
    let Some(departing) = existing
        .replicas
        .iter()
        .find(|replica| is_draining(replica))
    else {
        return Decision::Kept;
    };
    // Swapped only when someone can take its place; dropping it costs a copy.
    let mut taken: Vec<&str> = existing.nodes().map(String::as_str).collect();
    taken.retain(|node| node != departing);
    let Some(replacement) = eligible
        .iter()
        .filter(|node| !taken.contains(&node.node_id.as_str()))
        .filter(|node| match node.spec.capacity.max_shards {
            Some(max) => load.get(node.node_id.as_str()).copied().unwrap_or(0) < max,
            None => true,
        })
        .max_by(|a, b| {
            score(key, &a.node_id)
                .cmp(&score(key, &b.node_id))
                .then_with(|| a.node_id.cmp(&b.node_id))
        })
    else {
        return Decision::Kept;
    };
    if !moves.begin() {
        return Decision::Waiting(Blocked::MoveLimit);
    }
    *load.entry(replacement.node_id.as_str()).or_default() += 1;
    let replicas = existing
        .replicas
        .iter()
        .map(|replica| {
            if replica == departing {
                replacement.node_id.clone()
            } else {
                replica.clone()
            }
        })
        .collect();
    Decision::Move(
        MoveStep::Reseat {
            from: departing.clone(),
            to: replacement.node_id.clone(),
        },
        ShardAssignment {
            key: key.clone(),
            leader: existing.leader.clone(),
            replicas,
            generation: 0,
            state: existing.state,
            successor: None,
            joining: None,
            move_started_at_millis: None,
        },
    )
}

/// The replica set once `target` leads: nodes that already hold a copy first
/// (the old leader if it is staying, then live followers), topped up by
/// score, and cut to the replication factor.
fn replicas_after_cut_over<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    target: &str,
    replication_factor: u32,
    eligible: &[&'a Node],
    is_live: &dyn Fn(&str) -> bool,
    load: &mut HashMap<&'a str, u32>,
) -> Vec<String> {
    let wanted = replication_factor.saturating_sub(1) as usize;
    let mut replicas: Vec<String> = existing
        .nodes()
        .filter(|node| node.as_str() != target && is_live(node))
        .cloned()
        .collect();
    replicas.truncate(wanted);
    if replicas.len() < wanted {
        let taken: Vec<String> = replicas.clone();
        let mut chosen = choose_replicas(
            key,
            eligible,
            load,
            target,
            (wanted - replicas.len()) as u32,
        );
        // `choose_replicas` does not know about the ones kept above.
        chosen.retain(|node| !taken.contains(node));
        replicas.extend(chosen);
        replicas.truncate(wanted);
    }
    replicas
}

/// A caught-up live follower under its leadership share, furthest ahead first.
fn promote_under_share<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    eligible: &[&'a Node],
    caught_up: &dyn CaughtUp,
    leaders: &HashMap<&str, u32>,
    leader_share: u32,
) -> Option<&'a str> {
    let under_share: Vec<&'a Node> = eligible
        .iter()
        .copied()
        .filter(|node| leaders.get(node.node_id.as_str()).copied().unwrap_or(0) < leader_share)
        .collect();
    promote(key, existing, &under_share, caught_up)
}

/// The live node to move a shard to, by score, preferring nodes under their
/// leadership share. `balanced_only` answers `None` rather than spill over.
fn choose_destination<'a>(
    key: &ShardKey,
    eligible: &[&'a Node],
    leaders: &HashMap<&str, u32>,
    leader_share: u32,
    exclude: &str,
    balanced_only: bool,
) -> Option<&'a str> {
    let candidates: Vec<&'a Node> = eligible
        .iter()
        .copied()
        .filter(|node| node.node_id != exclude)
        .collect();
    let leader_load: HashMap<&str, u32> = candidates
        .iter()
        .map(|node| {
            (
                node.node_id.as_str(),
                leaders.get(node.node_id.as_str()).copied().unwrap_or(0),
            )
        })
        .collect();
    let under_share = candidates
        .iter()
        .copied()
        .filter(|node| leader_load.get(node.node_id.as_str()).copied().unwrap_or(0) < leader_share)
        .collect::<Vec<_>>();
    let pick = |from: &[&'a Node]| {
        from.iter()
            .copied()
            .filter(|node| match node.spec.capacity.max_shards {
                Some(max) => leader_load.get(node.node_id.as_str()).copied().unwrap_or(0) < max,
                None => true,
            })
            .max_by(|a, b| {
                score(key, &a.node_id)
                    .cmp(&score(key, &b.node_id))
                    .then_with(|| a.node_id.cmp(&b.node_id))
            })
            .map(|node| node.node_id.as_str())
    };
    if balanced_only {
        pick(&under_share)
    } else {
        pick(&under_share).or_else(|| pick(&candidates))
    }
}

/// `inner`, believed only where its report is from `generation`.
///
/// A move is decided on the assignment's own generation. The report the store
/// holds may be the previous leader's, still fresh and still listing the
/// replicas it had level -- and a fence made on that would stop a leader for
/// a destination that may hold nothing of what it has written since.
struct AtGeneration<'a> {
    inner: &'a dyn CaughtUp,
    generation: u64,
}

impl AtGeneration<'_> {
    fn current(&self, key: &ShardKey) -> bool {
        self.inner.reported_generation(key) == Some(self.generation)
    }
}

impl CaughtUp for AtGeneration<'_> {
    fn is_caught_up(&self, key: &ShardKey, node_id: &str) -> bool {
        self.current(key) && self.inner.is_caught_up(key, node_id)
    }

    fn reported_offset(&self, key: &ShardKey, node_id: &str) -> Option<u64> {
        self.current(key)
            .then(|| self.inner.reported_offset(key, node_id))
            .flatten()
    }

    fn is_drained(&self, key: &ShardKey, generation: u64) -> bool {
        self.inner.is_drained(key, generation)
    }

    fn reported_generation(&self, key: &ShardKey) -> Option<u64> {
        self.inner.reported_generation(key)
    }
}
