//! The per-shard write fence: the line between the writes a broker commits
//! for a shard and the ones it refuses once it has stopped serving it.
//!
//! Admission checks ownership, but an admitted write can then wait in a queue
//! for as long as the queue is deep. So every write enters the fence again at
//! the moment it claims its place in the log, and stays counted until it is
//! durable and fanned out. Once the fence is closed, a closed fence with
//! nothing in flight means nothing more can land: that is what makes the
//! drained report exact rather than a guess about how long a queue can hold a
//! write. See "Planned handoff" in `docs/replication-design.md`.
//!
//! [`ShardLifecycle`](super::ShardLifecycle) is the only thing that opens and
//! closes it: open while the shard is `Active` at a generation, closed in
//! every other phase.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};

use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::shards::ShardKey;
use crate::shards::routing::IngressRouter;

/// `Gate::open_at` for a closed fence. No assignment reaches this generation.
const CLOSED: u64 = u64::MAX;

/// Write fences for every shard this broker has served, keyed by kind as well
/// as name: a cache and a stream of the same name are different shards.
#[derive(Debug, Default)]
pub struct ShardFence {
    gates: RwLock<HashMap<ShardKey, Arc<Gate>>>,
}

impl ShardFence {
    /// Let writes to `key` in, if they were admitted at `generation`.
    pub fn open(&self, key: &ShardKey, generation: u64) {
        let gate = Arc::clone(self.gates.write().entry(key.clone()).or_default());
        gate.open_at.store(generation, SeqCst);
    }

    /// Refuse every write to `key` that has not entered yet. Writes already in
    /// finish; [`Self::quiesced`] says when they have.
    pub fn close(&self, key: &ShardKey) {
        if let Some(gate) = self.gates.read().get(key) {
            gate.open_at.store(CLOSED, SeqCst);
        }
    }

    /// Enter the fence for one write admitted at `generation`, right before it
    /// claims its place in the log. `None` means the shard stopped serving
    /// since admission and the write must be refused. Hold the guard until the
    /// write is durable and fanned out.
    pub fn enter(&self, key: &ShardKey, generation: u64) -> Option<FenceGuard> {
        let gate = Arc::clone(self.gates.read().get(key)?);
        // Counted before the check. A close that lands between the two then
        // still sees this write in flight, so `quiesced` cannot miss a write
        // that got in; one that gets refused only delays it a moment.
        gate.in_flight.fetch_add(1, SeqCst);
        let guard = FenceGuard { gate };
        (guard.gate.open_at.load(SeqCst) == generation).then_some(guard)
    }

    /// [`Self::enter`], with the refusal as an error.
    pub fn admit(&self, key: &ShardKey, generation: u64) -> Result<FenceGuard, Fenced> {
        self.enter(key, generation).ok_or(Fenced)
    }

    /// Whether `key` is closed with no write in flight, so its log cannot grow
    /// until it is opened again. True for a shard that was never opened here.
    pub fn quiesced(&self, key: &ShardKey) -> bool {
        self.gates
            .read()
            .get(key)
            .is_none_or(|gate| gate.quiesced())
    }

    /// Wait until [`Self::quiesced`] holds for `key`.
    pub async fn quiesce(&self, key: &ShardKey) {
        let Some(gate) = self.gates.read().get(key).cloned() else {
            return;
        };
        loop {
            let idle = gate.idle.notified();
            tokio::pin!(idle);
            // Registered before the check, so a last write leaving in between
            // still wakes this.
            idle.as_mut().enable();
            if gate.quiesced() {
                return;
            }
            idle.await;
        }
    }
}

/// Enter the fence for a write about to claim its place in `key`'s log, having
/// been admitted at `generation`.
///
/// `Ok(None)` on a single-node broker: no router, so no fence and nothing to
/// refuse. A cluster member with no shard to name refuses rather than skip the
/// fence.
pub fn enter(
    ingress: Option<&IngressRouter>,
    key: Option<&ShardKey>,
    generation: u64,
) -> Result<Option<FenceGuard>, Fenced> {
    match (ingress, key) {
        (None, _) => Ok(None),
        (Some(ingress), Some(key)) => ingress.fence().admit(key, generation).map(Some),
        (Some(_), None) => Err(Fenced),
    }
}

/// [`enter`], for a write that may already hold a guard from admission.
pub fn enter_or_keep(
    held: &mut Option<FenceGuard>,
    ingress: Option<&IngressRouter>,
    key: Option<&ShardKey>,
    generation: u64,
) -> Result<Option<FenceGuard>, Fenced> {
    match held.take() {
        Some(guard) => Ok(Some(guard)),
        None => enter(ingress, key, generation),
    }
}

/// A write refused at its claim: the shard stopped serving here after the
/// write was admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fenced;

impl std::fmt::Display for Fenced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shard is not servable here: this broker stopped serving it after the write was admitted"
        )
    }
}

impl std::error::Error for Fenced {}

/// One write inside the fence. Dropping it lets the fence quiesce.
#[derive(Debug)]
pub struct FenceGuard {
    gate: Arc<Gate>,
}

impl Drop for FenceGuard {
    fn drop(&mut self) {
        if self.gate.in_flight.fetch_sub(1, SeqCst) == 1 {
            self.gate.idle.notify_waiters();
        }
    }
}

#[derive(Debug)]
struct Gate {
    /// The generation writes must have been admitted at, or [`CLOSED`].
    open_at: AtomicU64,
    in_flight: AtomicUsize,
    idle: Notify,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            open_at: AtomicU64::new(CLOSED),
            in_flight: AtomicUsize::new(0),
            idle: Notify::new(),
        }
    }
}

impl Gate {
    fn quiesced(&self) -> bool {
        self.open_at.load(SeqCst) == CLOSED && self.in_flight.load(SeqCst) == 0
    }
}

#[cfg(test)]
mod tests;
