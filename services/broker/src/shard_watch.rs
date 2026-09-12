//! Broker-side view of which shards this node owns.
//!
//! Snapshot once, then poll changes from where the snapshot left off. The
//! control plane guarantees those two together describe every committed change
//! exactly once, so the interesting work here is what to do when that guarantee
//! stops holding: the broker fell behind the retention window, or the sequence
//! it was following reset under it.
//!
//! Both are recoverable, and both recover the same way — take a fresh snapshot.
//! What must never happen is carrying on from a checkpoint the control plane can
//! no longer honour, because that silently drops ownership changes and leaves
//! this broker serving shards it no longer owns.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::shard_watch_metrics as mm;

/// Ceiling on poll backoff after a failure.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// What an assignment is an assignment *of*.
///
/// The control plane places cache shards alongside stream shards, and the two
/// share every other field of the key. A broker that ignored this would file a
/// cache's shard under the stream of the same name and let one overwrite the
/// other's ownership.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum ShardKind {
    /// Absent on the wire means this, which is what a control plane that
    /// predates cache placement sends.
    #[default]
    Stream,
    Cache,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardKey {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    #[serde(default)]
    pub kind: ShardKind,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ShardAssignment {
    #[serde(flatten)]
    pub key: ShardKey,
    pub leader: String,
    #[serde(default)]
    pub replicas: Vec<String>,
    pub generation: u64,
    pub state: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ShardAssignmentChange {
    seq: u64,
    key: ShardKey,
    assignment: Option<ShardAssignment>,
}

#[derive(Debug, Deserialize)]
struct SnapshotResponse {
    items: Vec<ShardAssignment>,
    next_seq: u64,
}

#[derive(Debug, Deserialize)]
struct ChangesResponse {
    items: Vec<ShardAssignmentChange>,
    next_seq: u64,
}

/// What this broker currently believes about shard ownership.
///
/// Shared, so the ingress path can ask "do I own this shard?" without touching
/// the control plane.
#[derive(Debug, Default)]
pub struct ShardOwnership {
    assignments: HashMap<ShardKey, ShardAssignment>,
}

impl ShardOwnership {
    /// Apply a change, ignoring one that would move a shard backwards.
    ///
    /// Returns whether anything changed. A duplicate delivery is therefore
    /// harmless, and a stale generation — from a retry, a reordered poll, or a
    /// control plane that was behind — cannot roll ownership back.
    pub fn apply(&mut self, key: &ShardKey, assignment: Option<ShardAssignment>) -> bool {
        match assignment {
            Some(next) => {
                if let Some(current) = self.assignments.get(key)
                    && next.generation <= current.generation
                {
                    mm::record_stale_change();
                    return false;
                }
                self.assignments.insert(key.clone(), next);
                true
            }
            None => self.assignments.remove(key).is_some(),
        }
    }

    /// Replace everything with a snapshot.
    pub fn reset(&mut self, items: Vec<ShardAssignment>) {
        self.assignments = items
            .into_iter()
            .map(|assignment| (assignment.key.clone(), assignment))
            .collect();
    }

    /// Everything this broker currently believes about ownership.
    pub fn assignments(&self) -> &HashMap<ShardKey, ShardAssignment> {
        &self.assignments
    }

    pub fn get(&self, key: &ShardKey) -> Option<&ShardAssignment> {
        self.assignments.get(key)
    }

    /// Whether `node_id` leads this shard.
    pub fn is_leader(&self, key: &ShardKey, node_id: &str) -> bool {
        self.assignments
            .get(key)
            .is_some_and(|assignment| assignment.leader == node_id)
    }

    pub fn len(&self) -> usize {
        self.assignments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.assignments.is_empty()
    }
}

/// Why the broker must take a fresh snapshot rather than keep polling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resync {
    /// Changes before the first returned one were evicted from the window.
    GapInHistory,
    /// The control plane's sequence is behind our checkpoint, so it restarted
    /// onto a store that does not persist the sequence.
    SequenceReset,
}

/// Decide whether a changes response can be applied as-is.
///
/// Split out from the polling loop because this is the part that is easy to get
/// wrong and easy to test: every branch is a way to silently lose an ownership
/// change.
pub fn check_continuity(since: u64, first_seq: Option<u64>, next_seq: u64) -> Option<Resync> {
    if next_seq < since {
        return Some(Resync::SequenceReset);
    }
    match first_seq {
        // A first change above where we asked means the ones between were
        // evicted while we were away.
        Some(seq) if seq > since => Some(Resync::GapInHistory),
        // Nothing returned, but the log has moved past us: the whole span was
        // evicted rather than being empty.
        None if next_seq > since => Some(Resync::GapInHistory),
        _ => None,
    }
}

/// Poll shard ownership until `shutdown` fires.
///
/// Never returns an error: the control plane being unreachable is not a reason
/// for a broker to stop serving what it already owns. Failures back off, and are
/// visible through `felix_broker_shard_watch_failures_total`.
pub async fn run(
    client: reqwest::Client,
    base_url: String,
    bearer: Option<String>,
    ownership: Arc<RwLock<ShardOwnership>>,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let base_url = base_url.trim_end_matches('/').to_string();
    // `None` means "no valid checkpoint": take a snapshot before polling.
    let mut checkpoint: Option<u64> = None;
    let mut failures: u32 = 0;

    loop {
        let delay = if failures == 0 {
            interval
        } else {
            backoff(interval, failures)
        };
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(delay) => {}
        }

        let result = match checkpoint {
            None => snapshot(&client, &base_url, bearer.as_deref(), &ownership).await,
            Some(since) => poll(&client, &base_url, bearer.as_deref(), &ownership, since).await,
        };

        match result {
            Ok(Progress::At(seq)) => {
                failures = 0;
                checkpoint = Some(seq);
                mm::record_checkpoint(seq);
            }
            Ok(Progress::MustResync(reason)) => {
                failures = 0;
                // Deliberately not an error: falling behind the window is an
                // expected consequence of a broker being away long enough.
                tracing::warn!(?reason, "shard watch must resnapshot");
                mm::record_resync(reason);
                checkpoint = None;
            }
            Err(err) => {
                failures = failures.saturating_add(1);
                mm::record_failure();
                tracing::warn!(
                    consecutive_failures = failures,
                    error = %err,
                    "shard watch poll failed; retrying with backoff",
                );
            }
        }
    }
}

enum Progress {
    At(u64),
    MustResync(Resync),
}

async fn snapshot(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    ownership: &Arc<RwLock<ShardOwnership>>,
) -> Result<Progress> {
    let response: SnapshotResponse = get(
        client,
        &format!("{base_url}/v1/shard-assignments/snapshot"),
        bearer,
    )
    .await
    .context("shard assignment snapshot")?;

    let count = response.items.len();
    ownership.write().await.reset(response.items);
    mm::record_snapshot(count);
    tracing::info!(
        assignments = count,
        next_seq = response.next_seq,
        "shard ownership seeded from snapshot",
    );
    Ok(Progress::At(response.next_seq))
}

async fn poll(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    ownership: &Arc<RwLock<ShardOwnership>>,
    since: u64,
) -> Result<Progress> {
    let response: ChangesResponse = get(
        client,
        &format!("{base_url}/v1/shard-assignments/changes?since={since}"),
        bearer,
    )
    .await
    .context("shard assignment changes")?;

    let first_seq = response.items.first().map(|change| change.seq);
    if let Some(reason) = check_continuity(since, first_seq, response.next_seq) {
        return Ok(Progress::MustResync(reason));
    }

    if !response.items.is_empty() {
        let mut owned = ownership.write().await;
        for change in &response.items {
            owned.apply(&change.key, change.assignment.clone());
        }
        mm::record_applied(response.items.len());
    }
    Ok(Progress::At(response.next_seq))
}

async fn get<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    bearer: Option<&str>,
) -> Result<T> {
    let mut request = client.get(url);
    if let Some(bearer) = bearer {
        request = request.bearer_auth(bearer);
    }
    let response = request.send().await.context("send request")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("{status}: {body}"));
    }
    response.json().await.context("decode response")
}

/// Exponential backoff, capped, so a long control-plane outage does not turn
/// into an hours-long gap before the broker notices it came back.
fn backoff(interval: Duration, failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(16);
    interval
        .saturating_mul(2u32.saturating_pow(shift))
        .min(MAX_BACKOFF)
}

#[cfg(test)]
#[path = "shard_watch_tests.rs"]
mod tests;
