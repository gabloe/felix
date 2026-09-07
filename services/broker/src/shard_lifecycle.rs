//! Local shard ownership: what this broker does about what the control plane
//! decided.
//!
//! The watch says who *should* own each shard. This says what this node has
//! actually done about it — because becoming an owner is not instant. The
//! durable log has to be opened and recovered first, and a broker that served
//! writes in that window would acknowledge records it could not yet persist.
//!
//! Ownership is therefore two-phase in both directions: `Opening` before
//! `Active`, and `Draining` before `Closed`. A shard serves only in `Active`,
//! and only at the generation the control plane currently names.
//!
//! The decision half is a pure state machine and the I/O half is a driver, for
//! the same reason placement is split that way: every interesting rule is then
//! testable without a disk.
use std::collections::HashMap;

use crate::shard_lifecycle_metrics as mm;
use crate::shard_watch::{ShardAssignment, ShardKey};

/// Where this broker is with one shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
    /// Not ours, and nothing open.
    Unassigned,
    /// Ours, but the local log is not ready. Not serving.
    Opening,
    /// Ours and ready.
    Active,
    /// No longer ours. In-flight work is finishing; no new writes.
    Draining,
    /// Drained and closed.
    Closed,
    /// The local log could not be opened. Not serving, and not retried until
    /// the assignment changes -- a failure that repeats every poll is noise,
    /// and the operator needs the state to stay visible.
    Failed,
}

impl Phase {
    pub fn label(self) -> &'static str {
        match self {
            Phase::Unassigned => "unassigned",
            Phase::Opening => "opening",
            Phase::Active => "active",
            Phase::Draining => "draining",
            Phase::Closed => "closed",
            Phase::Failed => "failed",
        }
    }

    /// Whether a shard in this phase may accept writes.
    ///
    /// Only `Active`. `Opening` is the whole reason this type exists: the
    /// assignment has arrived but the log has not been recovered, and
    /// acknowledging a write there would promise durability that is not set up.
    pub fn may_serve(self) -> bool {
        matches!(self, Phase::Active)
    }
}

/// What this broker holds for one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalShard {
    pub phase: Phase,
    /// The assignment generation this state was reached for.
    pub generation: u64,
}

/// Work the driver must do to catch up with a decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Open and recover the local log, then report back.
    Open { key: ShardKey, generation: u64 },
    /// Stop writes, drain, flush, and close.
    Release { key: ShardKey, generation: u64 },
    /// Nothing to do.
    None,
}

/// This broker's view of the shards it holds.
#[derive(Debug)]
pub struct ShardLifecycle {
    node_id: String,
    shards: HashMap<ShardKey, LocalShard>,
}

impl ShardLifecycle {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            shards: HashMap::new(),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Whether this broker may serve writes for `key` right now.
    pub fn may_serve(&self, key: &ShardKey) -> bool {
        self.shards
            .get(key)
            .is_some_and(|shard| shard.phase.may_serve())
    }

    /// Whether this broker may serve `key` at exactly `generation`.
    ///
    /// The stricter gate, for a caller that read an assignment and wants to act
    /// on it: serving at a generation the control plane has moved past is how
    /// two brokers end up believing they lead the same shard.
    pub fn may_serve_at(&self, key: &ShardKey, generation: u64) -> bool {
        self.shards
            .get(key)
            .is_some_and(|shard| shard.phase.may_serve() && shard.generation == generation)
    }

    pub fn phase(&self, key: &ShardKey) -> Phase {
        self.shards
            .get(key)
            .map_or(Phase::Unassigned, |shard| shard.phase)
    }

    pub fn generation(&self, key: &ShardKey) -> Option<u64> {
        self.shards.get(key).map(|shard| shard.generation)
    }

    /// Shards this broker is currently serving.
    pub fn active(&self) -> impl Iterator<Item = &ShardKey> {
        self.shards
            .iter()
            .filter(|(_, shard)| shard.phase.may_serve())
            .map(|(key, _)| key)
    }

    /// React to what the control plane now says about one shard.
    ///
    /// `assignment` is `None` when the shard has no assignment at all. Returns
    /// the work the driver must do; repeated calls with the same or an older
    /// generation return [`Action::None`], so a duplicate delivery or a replayed
    /// snapshot costs nothing.
    pub fn observe(&mut self, key: &ShardKey, assignment: Option<&ShardAssignment>) -> Action {
        let ours = assignment.is_some_and(|a| a.leader == self.node_id);
        let generation = assignment.map_or(0, |a| a.generation);
        let current = self.shards.get(key).cloned();

        match (ours, current) {
            // Newly ours: open before serving.
            (true, None) => self.begin_open(key, generation),

            (true, Some(existing)) => {
                if generation < existing.generation {
                    // Older than what we already acted on: a duplicate, a
                    // reordered poll, or a snapshot replay.
                    mm::record_stale_event();
                    return Action::None;
                }
                match existing.phase {
                    // Already serving this generation: nothing to do. This is
                    // the common case on every poll.
                    Phase::Active | Phase::Opening if generation == existing.generation => {
                        Action::None
                    }
                    // A new generation for a shard we already hold. Reopening is
                    // what makes the generation meaningful: the control plane
                    // moved the shard away and back, and the local state has to
                    // be re-established rather than assumed.
                    Phase::Active | Phase::Opening => self.begin_open(key, generation),
                    // A failed open is not retried by the same assignment
                    // arriving again. Every poll re-delivers it, and retrying
                    // each time buries the failure in noise while hammering a
                    // log that is not opening. A new generation is real news.
                    Phase::Failed if generation == existing.generation => Action::None,
                    // Ours again after we let it go, or after a failure at an
                    // older generation.
                    Phase::Draining | Phase::Closed | Phase::Unassigned | Phase::Failed => {
                        self.begin_open(key, generation)
                    }
                }
            }

            // Not ours, and we hold nothing.
            (false, None) => Action::None,

            (false, Some(existing)) => match existing.phase {
                // Already given up.
                Phase::Closed | Phase::Unassigned | Phase::Draining => Action::None,
                // A failed open never served, so there is nothing to drain.
                Phase::Failed => {
                    self.set(key, Phase::Closed, existing.generation);
                    Action::None
                }
                Phase::Active | Phase::Opening => {
                    self.set(key, Phase::Draining, existing.generation);
                    Action::Release {
                        key: key.clone(),
                        generation: existing.generation,
                    }
                }
            },
        }
    }

    /// Every shard this broker holds that the given assignment set no longer
    /// mentions.
    ///
    /// A snapshot replaces the whole picture, so a shard that vanished from it
    /// has to be released as surely as one reassigned away — otherwise a broker
    /// keeps serving a shard whose assignment was deleted.
    pub fn missing_from<'a>(
        &self,
        present: impl IntoIterator<Item = &'a ShardKey>,
    ) -> Vec<ShardKey> {
        let present: std::collections::HashSet<&ShardKey> = present.into_iter().collect();
        self.shards
            .iter()
            .filter(|(key, shard)| {
                !present.contains(*key) && !matches!(shard.phase, Phase::Closed | Phase::Unassigned)
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Record that the driver opened the log successfully.
    ///
    /// Ignored if the generation has moved on since the open began: the shard
    /// was reassigned while we were recovering it, and activating now would
    /// serve a generation we no longer hold.
    pub fn opened(&mut self, key: &ShardKey, generation: u64) -> bool {
        match self.shards.get(key) {
            Some(shard) if shard.phase == Phase::Opening && shard.generation == generation => {
                self.set(key, Phase::Active, generation);
                true
            }
            _ => {
                mm::record_stale_event();
                false
            }
        }
    }

    /// Record that the driver could not open the log.
    pub fn open_failed(&mut self, key: &ShardKey, generation: u64) {
        if let Some(shard) = self.shards.get(key)
            && shard.phase == Phase::Opening
            && shard.generation == generation
        {
            self.set(key, Phase::Failed, generation);
        }
    }

    /// Record that the driver finished draining and closed the log.
    pub fn released(&mut self, key: &ShardKey, generation: u64) {
        if let Some(shard) = self.shards.get(key)
            && shard.phase == Phase::Draining
            && shard.generation == generation
        {
            self.set(key, Phase::Closed, generation);
        }
    }

    fn begin_open(&mut self, key: &ShardKey, generation: u64) -> Action {
        self.set(key, Phase::Opening, generation);
        Action::Open {
            key: key.clone(),
            generation,
        }
    }

    fn set(&mut self, key: &ShardKey, phase: Phase, generation: u64) {
        let previous = self.shards.get(key).map(|shard| shard.phase);
        self.shards
            .insert(key.clone(), LocalShard { phase, generation });
        if previous != Some(phase) {
            mm::record_transition(previous.unwrap_or(Phase::Unassigned), phase);
            mm::record_phase_counts(self.counts());
        }
    }

    /// How many shards sit in each phase, for the gauge.
    pub fn counts(&self) -> HashMap<Phase, usize> {
        let mut counts = HashMap::new();
        for shard in self.shards.values() {
            *counts.entry(shard.phase).or_default() += 1;
        }
        counts
    }
}

/// What a driver needs to open and close local shard state.
///
/// A trait so the lifecycle can be exercised without a disk, and so the durable
/// and non-durable cases are one code path: an in-memory stream has no log to
/// open, and saying so is cheaper than branching everywhere.
#[async_trait::async_trait]
pub trait ShardStore: Send + Sync {
    /// Open and recover local state for a shard.
    async fn open(&self, key: &ShardKey) -> anyhow::Result<()>;
    /// Flush everything accepted for a shard, before ownership is given up.
    ///
    /// This is the point at which "no accepted write is unaccounted for" is
    /// either true or not.
    async fn release(&self, key: &ShardKey) -> anyhow::Result<()>;
}

/// A [`ShardStore`] over the broker's durable storage.
///
/// Opening recovers the shard's log from disk, which is the work `Opening`
/// exists to wait for. Releasing flushes it, so nothing acknowledged here is
/// still only in the page cache when another node takes the shard.
pub struct DurableShardStore {
    storage: std::sync::Arc<felix_broker::DurableStorage>,
}

impl DurableShardStore {
    pub fn new(storage: std::sync::Arc<felix_broker::DurableStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl ShardStore for DurableShardStore {
    async fn open(&self, key: &ShardKey) -> anyhow::Result<()> {
        // Recovery happens here: validating the tail and rebuilding indexes is
        // exactly the cost the `Opening` phase is holding writes back for.
        self.storage
            .open_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .map(|_| ())
            .map_err(|err| anyhow::anyhow!("open shard log: {err}"))
    }

    async fn release(&self, key: &ShardKey) -> anyhow::Result<()> {
        let log = self
            .storage
            .open_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .map_err(|err| anyhow::anyhow!("open shard log to flush it: {err}"))?;
        log.sync()
            .await
            .map_err(|err| anyhow::anyhow!("flush shard log: {err}"))
    }
}

/// Drive one decision to completion.
pub async fn apply(
    lifecycle: &tokio::sync::Mutex<ShardLifecycle>,
    store: &dyn ShardStore,
    action: Action,
) {
    match action {
        Action::None => {}
        Action::Open { key, generation } => match store.open(&key).await {
            Ok(()) => {
                let activated = lifecycle.lock().await.opened(&key, generation);
                if activated {
                    tracing::info!(
                        stream = %key.stream,
                        shard = key.shard,
                        generation,
                        "shard opened and now serving",
                    );
                } else {
                    // Reassigned while we were recovering it. Not an error, and
                    // deliberately not activated -- see `ShardLifecycle::opened`.
                    tracing::info!(
                        stream = %key.stream,
                        shard = key.shard,
                        generation,
                        "shard was reassigned while opening; not activating",
                    );
                }
            }
            Err(err) => {
                mm::record_open_failure();
                lifecycle.lock().await.open_failed(&key, generation);
                tracing::error!(
                    stream = %key.stream,
                    shard = key.shard,
                    generation,
                    error = %err,
                    "could not open local shard state; this broker is not serving it",
                );
            }
        },
        Action::Release { key, generation } => {
            if let Err(err) = store.release(&key).await {
                // Still closed: ownership has moved regardless, and continuing
                // to serve would be worse than an unflushed tail. The error is
                // the operator's to see.
                tracing::error!(
                    stream = %key.stream,
                    shard = key.shard,
                    generation,
                    error = %err,
                    "could not flush local shard state while releasing it",
                );
            }
            lifecycle.lock().await.released(&key, generation);
            tracing::info!(
                stream = %key.stream,
                shard = key.shard,
                generation,
                "shard released",
            );
        }
    }
}

/// Bring local state in line with a full assignment set.
///
/// Takes the whole set rather than a delta because a snapshot replaces the
/// picture: a shard that vanished from it must be released as surely as one
/// reassigned away, and only the full set can say which vanished.
pub async fn reconcile(
    lifecycle: &tokio::sync::Mutex<ShardLifecycle>,
    store: &dyn ShardStore,
    assignments: &HashMap<ShardKey, ShardAssignment>,
) {
    let mut actions = Vec::new();
    {
        let mut owned = lifecycle.lock().await;
        for (key, assignment) in assignments {
            actions.push(owned.observe(key, Some(assignment)));
        }
        for key in owned.missing_from(assignments.keys()) {
            actions.push(owned.observe(&key, None));
        }
    }
    for action in actions {
        apply(lifecycle, store, action).await;
    }
}

#[cfg(test)]
#[path = "shard_lifecycle_tests.rs"]
mod tests;
