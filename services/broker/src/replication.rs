//! The leader's half of replication: shipping committed records to followers.
//!
//! # One cursor per follower
//!
//! A follower is a position in this shard's log, nothing more. The leader keeps
//! the next offset it believes each follower wants, reads that range from its
//! own log, and ships it. The follower's answer is what moves the cursor —
//! never the send itself, because a batch that was sent is not a batch that was
//! stored.
//!
//! That is why the cursor can go *backwards*. A follower that has lost records,
//! or that was rebuilt, answers `LogGap` naming the offset it actually wants,
//! and the leader resumes there. Resume needs no separate negotiation and no
//! state on disk: the follower is the authority on its own position, and it
//! says so in every refusal.
//!
//! # What stops, and what retries
//!
//! `LogConflict` and `FencedEpoch` stop this shard's replication to that
//! follower. Neither converges by trying again: the first means the two logs
//! disagree about bytes already stored, and the second means this broker is no
//! longer the leader, so it has no business shipping anything. Everything else
//! is transient and retried.
//!
//! # What bounds it
//!
//! One batch in flight per follower, and each batch is bounded by
//! `max_batch_bytes` read from the log. So the memory a lagging follower costs
//! the leader is one batch, not the distance it is behind — and a follower that
//! stops answering stops consuming anything at all, because the next read does
//! not start until the last answer arrives.
use std::net::SocketAddr;

use bytes::Bytes;
use felix_broker::StreamLog;
use felix_wire::internal::{
    ErrorCode, InternalMessage, ReplicaLog, ReplicateBootstrap, ReplicateRebuild, ReplicateRecords,
    ShardRef, batch_checksum,
};

use crate::peer::{PeerError, PeerRequester};

pub mod driver;
pub mod halted;
pub mod metrics;
pub mod quorum;
pub mod reporter;

/// How far a follower has got, as this leader understands it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowerCursor {
    pub node_id: String,
    pub addr: SocketAddr,
    /// The offset to send next. Moved only by the follower's own answers.
    pub next_offset: u64,
    /// Set once this follower has answered something that does not resolve by
    /// retrying. Nothing more is shipped to it at this generation.
    pub halted: Option<Halt>,
    /// This follower discarded its copy at the leader's request and is being
    /// shipped from the leader's base. Holds one of the policy's slots until
    /// it reaches the tail.
    pub rebuilding: bool,
    /// This follower refused, or did not understand, a rebuild. Asked once per
    /// generation: nothing about a refusal changes with the next pass.
    pub rebuild_refused: bool,
}

/// What the leader may do about a halted follower on its own.
///
/// A halt does not resolve itself: the follower is out of every quorum until
/// its copy is discarded and rebuilt. Doing that automatically is a full
/// transfer per shard, and doing it for every halted follower at once, across
/// every shard a failed broker led, is how a recovery becomes an outage. So
/// it happens under a cap and, optionally, a rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildPolicy {
    /// Rebuilds in flight at once, across every shard this broker leads.
    /// Zero leaves every halt to an operator.
    pub max_concurrent: usize,
    /// Bytes per second a rebuilding follower is shipped at; zero is
    /// unlimited. Applied per rebuilding follower, on this leader.
    pub bytes_per_sec: u64,
}

impl Default for RebuildPolicy {
    fn default() -> Self {
        Self {
            max_concurrent: 1,
            bytes_per_sec: 0,
        }
    }
}

/// The policy plus how much of it is in use, shared by every shard this
/// broker ships in one pass so the cap holds across them.
#[derive(Debug)]
pub struct Rebuilds {
    policy: RebuildPolicy,
    in_flight: std::sync::Mutex<usize>,
}

impl Rebuilds {
    pub fn new(policy: RebuildPolicy) -> Self {
        Self {
            policy,
            in_flight: std::sync::Mutex::new(0),
        }
    }

    /// Never rebuild; every halt waits for an operator.
    pub fn disabled() -> Self {
        Self::new(RebuildPolicy {
            max_concurrent: 0,
            bytes_per_sec: 0,
        })
    }

    pub fn policy(&self) -> RebuildPolicy {
        self.policy
    }

    /// Reconcile the count with the cursors that actually carry a rebuild,
    /// once per pass: a cursor discarded on a generation change takes its
    /// slot with it, and nothing else would give it back.
    pub fn set_in_flight(&self, count: usize) {
        *self.in_flight.lock().expect("rebuild slots") = count;
        metrics::record_rebuilding(count);
    }

    fn try_begin(&self) -> bool {
        let mut in_flight = self.in_flight.lock().expect("rebuild slots");
        if *in_flight >= self.policy.max_concurrent {
            return false;
        }
        *in_flight += 1;
        metrics::record_rebuilding(*in_flight);
        true
    }

    fn finish(&self) {
        let mut in_flight = self.in_flight.lock().expect("rebuild slots");
        *in_flight = in_flight.saturating_sub(1);
        metrics::record_rebuilding(*in_flight);
    }
}

impl Halt {
    /// Whether discarding the follower's copy would resolve this.
    ///
    /// A fenced halt is this broker's problem, not the follower's: it is no
    /// longer the leader, and nothing it ships is authoritative.
    fn rebuildable(self) -> bool {
        matches!(self, Halt::Diverged | Halt::NeedsBootstrap)
    }
}

fn replica_log(log_kind: felix_broker::LogKind) -> ReplicaLog {
    match log_kind {
        felix_broker::LogKind::Stream => ReplicaLog::Stream,
        felix_broker::LogKind::Cache => ReplicaLog::Cache,
        felix_broker::LogKind::GroupCursors => ReplicaLog::GroupCursors,
        felix_broker::LogKind::GroupDeadLetters => ReplicaLog::GroupDeadLetters,
        felix_broker::LogKind::Counters => ReplicaLog::Counters,
    }
}

/// Why replication to a follower stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Halt {
    /// The follower holds different bytes at an offset both sides have. Records
    /// are never rewritten, so there is no repair.
    #[error("the follower's log has diverged from this one")]
    Diverged,
    /// The follower knows a newer generation than this broker is shipping at.
    /// This broker is not the leader any more.
    #[error("this broker has been superseded as leader")]
    Fenced,
    /// The follower wants records retention has already removed from the
    /// leader, so shipping cannot reach it. It needs its history transferred
    /// before live replication can resume.
    #[error("the follower needs history this leader no longer holds")]
    NeedsBootstrap,
}

impl FollowerCursor {
    pub fn new(node_id: impl Into<String>, addr: SocketAddr, next_offset: u64) -> Self {
        Self {
            node_id: node_id.into(),
            addr,
            next_offset,
            halted: None,
            rebuilding: false,
            rebuild_refused: false,
        }
    }
}

/// What one exchange with a follower concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// The follower stored the batch and is now at this offset.
    Stored { durable_offset: u64 },
    /// The follower wants a different offset. Not an error: it is how a
    /// follower that has fallen behind, or been rebuilt, is caught up.
    Resume { offset: u64 },
    /// Nothing to ship; the follower is level with this leader.
    UpToDate,
    /// Transient. The same batch may go again.
    Retry,
    /// Replication to this follower is over until the assignment changes.
    Halted(Halt),
}

/// Read the follower's answer.
///
/// Separate from the exchange so the table in this module's documentation is
/// testable without a follower on the other end.
pub fn read_answer(answer: &InternalMessage) -> Progress {
    match answer {
        InternalMessage::ReplicateOk(ok) => Progress::Stored {
            durable_offset: ok.durable_offset,
        },
        InternalMessage::ReplicateError(err) => match err.code {
            ErrorCode::LogGap => Progress::Resume {
                offset: err.expected_offset,
            },
            ErrorCode::LogConflict => Progress::Halted(Halt::Diverged),
            ErrorCode::FencedEpoch => Progress::Halted(Halt::Fenced),
            // Everything else is this moment rather than this pairing: the
            // follower is behind, busy, or could not read the frame.
            _ => Progress::Retry,
        },
        // Anything else is a peer that did not understand the request. Retrying
        // is harmless and the alternative -- treating it as divergence -- would
        // stop replication over a protocol confusion.
        _ => Progress::Retry,
    }
}

/// Ship one batch to one follower and apply its answer to the cursor.
///
/// Returns what the exchange concluded. The cursor is advanced, rewound, or
/// halted here, so a caller only has to decide whether to go round again.
pub async fn ship_once<R: PeerRequester>(
    requester: &R,
    log: &StreamLog,
    shard: &ShardRef,
    log_kind: felix_broker::LogKind,
    cursor: &mut FollowerCursor,
    max_batch_bytes: usize,
    rebuilds: &Rebuilds,
) -> Progress {
    if let Some(halt) = cursor.halted {
        if !halt.rebuildable() || cursor.rebuild_refused || !rebuilds.try_begin() {
            return Progress::Halted(halt);
        }
        match request_rebuild(requester, log, shard, log_kind, cursor).await {
            Rebuild::Accepted { base_offset } => {
                cursor.halted = None;
                cursor.next_offset = base_offset;
                cursor.rebuilding = true;
                tracing::warn!(
                    node_id = %cursor.node_id,
                    stream = %shard.stream,
                    shard = shard.shard,
                    generation = shard.generation,
                    reason = %halt,
                    base_offset,
                    "the follower discarded its copy of this shard; rebuilding it from here",
                );
                metrics::record_rebuild(metrics::OUTCOME_REBUILD_STARTED);
            }
            Rebuild::Unreachable => {
                rebuilds.finish();
                return Progress::Halted(halt);
            }
            Rebuild::Refused => {
                cursor.rebuild_refused = true;
                rebuilds.finish();
                metrics::record_rebuild(metrics::OUTCOME_REBUILD_REFUSED);
                return Progress::Halted(halt);
            }
        }
    }

    let records = match log.read_from(cursor.next_offset, max_batch_bytes).await {
        Ok(records) => records,
        // The follower is asking for records this leader has already trimmed.
        // Shipping cannot bridge that: the records are not here to send, and
        // starting the follower at the surviving base would leave its log with
        // a hole that nothing downstream could detect.
        //
        // Halting is the honest answer. It stops a retry that could never
        // succeed, keeps the follower out of every quorum, and says plainly
        // that the shard needs its history transferred -- which is #114's
        // subject and is not implemented.
        Err(felix_broker::BrokerError::CursorTooOld { oldest, requested }) => {
            // The records between the follower's position and this leader's
            // oldest are gone from both. Shipping cannot bridge that, so the
            // follower is offered the one fact it cannot work out for itself:
            // where the surviving log begins.
            tracing::info!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                requested,
                oldest,
                "the follower is below this leader's oldest record; offering a bootstrap",
            );
            return offer_bootstrap(requester, shard, log_kind, cursor, oldest).await;
        }
        Err(err) => {
            // The leader could not read its own log. Nothing is wrong with the
            // follower, so this must not look like divergence.
            tracing::warn!(
                node_id = %cursor.node_id,
                offset = cursor.next_offset,
                error = %err,
                "could not read the local log to replicate",
            );
            metrics::record_shipped(metrics::OUTCOME_READ_FAILED);
            return Progress::Retry;
        }
    };

    if records.is_empty() {
        if cursor.rebuilding {
            cursor.rebuilding = false;
            rebuilds.finish();
            tracing::info!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                "the rebuilt follower has reached this leader's tail",
            );
            metrics::record_rebuild(metrics::OUTCOME_REBUILD_COMPLETED);
        }
        return Progress::UpToDate;
    }

    let first_offset = records[0].offset;
    let payloads: Vec<Bytes> = records.into_iter().map(|record| record.payload).collect();
    let batch_end = first_offset + payloads.len() as u64;
    let batch_bytes: usize = payloads.iter().map(Bytes::len).sum();
    let batch = ReplicateRecords {
        // The pool assigns the real id; it owns the connection this lands on.
        correlation_id: 0,
        shard: shard.clone(),
        first_offset,
        checksum: batch_checksum(&payloads),
        payloads,
    };
    // Which log this is belongs in the message kind, not in the shard
    // reference: the bodies are identical, and a follower that guessed wrong
    // would append one of a shard's logs into another.
    let request = match log_kind {
        felix_broker::LogKind::Cache => InternalMessage::ReplicateCacheRecords(batch),
        felix_broker::LogKind::GroupCursors => InternalMessage::ReplicateGroupRecords(batch),
        felix_broker::LogKind::GroupDeadLetters => {
            InternalMessage::ReplicateDeadLetterRecords(batch)
        }
        felix_broker::LogKind::Counters => InternalMessage::ReplicateCounterRecords(batch),
        felix_broker::LogKind::Stream => InternalMessage::ReplicateRecords(batch),
    };

    let answer = match requester
        .request(&cursor.node_id, cursor.addr, request)
        .await
    {
        Ok(answer) => answer,
        Err(err) => {
            metrics::record_shipped(unreachable_outcome(&err));
            return Progress::Retry;
        }
    };

    let progress = read_answer(&answer);
    match progress {
        Progress::Stored { durable_offset } => {
            // The follower's own account of where it is, since it did the
            // writing and a partially applied batch would leave the two
            // disagreeing — but never past the end of what was sent. The leader
            // has compared nothing beyond that, so accepting a higher answer
            // means resuming past records neither side has checked.
            cursor.next_offset = durable_offset.min(batch_end);
            metrics::record_shipped(metrics::OUTCOME_OK);
            // A rebuild is a full transfer, and the policy may say how fast.
            // Paced after the batch landed, so the follower is never waiting
            // on records that were already read.
            let rate = rebuilds.policy().bytes_per_sec;
            if cursor.rebuilding && rate > 0 {
                let nanos = (batch_bytes as u128 * 1_000_000_000) / rate as u128;
                let nanos = nanos.min(u64::MAX as u128) as u64;
                tokio::time::sleep(std::time::Duration::from_nanos(nanos)).await;
            }
        }
        Progress::Resume { offset } => {
            cursor.next_offset = offset;
            metrics::record_shipped(metrics::OUTCOME_RESUMED);
        }
        Progress::Halted(halt) => {
            cursor.halted = Some(halt);
            tracing::error!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                generation = shard.generation,
                reason = %halt,
                "replication to a follower stopped",
            );
            metrics::record_shipped(match halt {
                Halt::Diverged => metrics::OUTCOME_DIVERGED,
                Halt::Fenced => metrics::OUTCOME_FENCED,
                Halt::NeedsBootstrap => metrics::OUTCOME_NEEDS_BOOTSTRAP,
            });
        }
        Progress::Retry => metrics::record_shipped(metrics::OUTCOME_REFUSED),
        Progress::UpToDate => {}
    }
    progress
}

enum Rebuild {
    /// The follower discarded its copy; its new one begins here.
    Accepted { base_offset: u64 },
    /// Not answered. Asked again next pass.
    Unreachable,
    /// Answered no, or with something that was not an answer. Not asked again
    /// at this generation.
    Refused,
}

/// Ask a halted follower to discard its copy and start again at this
/// leader's oldest record.
async fn request_rebuild<R: PeerRequester>(
    requester: &R,
    log: &StreamLog,
    shard: &ShardRef,
    log_kind: felix_broker::LogKind,
    cursor: &FollowerCursor,
) -> Rebuild {
    let base_offset = log.base_offset();
    let request = InternalMessage::ReplicateRebuild(ReplicateRebuild {
        // The pool assigns the real id; it owns the connection this lands on.
        correlation_id: 0,
        shard: shard.clone(),
        log: replica_log(log_kind),
        base_offset,
    });
    let answer = match requester
        .request(&cursor.node_id, cursor.addr, request)
        .await
    {
        Ok(answer) => answer,
        Err(err) => {
            tracing::warn!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                error = %err,
                "could not reach the follower to rebuild it",
            );
            return Rebuild::Unreachable;
        }
    };
    match answer {
        InternalMessage::ReplicateOk(ok) => Rebuild::Accepted {
            base_offset: ok.durable_offset,
        },
        InternalMessage::ReplicateError(err) => {
            tracing::warn!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                code = ?err.code,
                detail = %err.detail,
                "the follower refused to rebuild",
            );
            Rebuild::Refused
        }
        // Anything else is a peer that did not understand the request,
        // most likely one that predates rebuilds. The halt stands until it
        // is upgraded or an operator acts.
        other => {
            tracing::warn!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                kind = ?other.kind(),
                "the follower did not understand a rebuild; it stays halted",
            );
            Rebuild::Refused
        }
    }
}

/// Offer a follower a log that begins where this leader's surviving log does.
///
/// Only the follower can accept: one holding records of its own has a gap it
/// cannot fill, and discarding them is an operator's decision rather than a
/// leader's. A refusal halts this follower, which is where it stood before the
/// offer was made.
async fn offer_bootstrap<R: PeerRequester>(
    requester: &R,
    shard: &ShardRef,
    log_kind: felix_broker::LogKind,
    cursor: &mut FollowerCursor,
    base_offset: u64,
) -> Progress {
    let offer = ReplicateBootstrap {
        // The pool assigns the real id; it owns the connection this lands on.
        correlation_id: 0,
        shard: shard.clone(),
        base_offset,
    };
    let request = match log_kind {
        felix_broker::LogKind::Cache => InternalMessage::ReplicateCacheBootstrap(offer),
        felix_broker::LogKind::GroupCursors => InternalMessage::ReplicateGroupBootstrap(offer),
        felix_broker::LogKind::GroupDeadLetters => {
            InternalMessage::ReplicateDeadLetterBootstrap(offer)
        }
        felix_broker::LogKind::Counters => InternalMessage::ReplicateCounterBootstrap(offer),
        felix_broker::LogKind::Stream => InternalMessage::ReplicateBootstrap(offer),
    };
    let answer = match requester
        .request(&cursor.node_id, cursor.addr, request)
        .await
    {
        Ok(answer) => answer,
        Err(err) => {
            // Unreachable, not unwilling. The offer stands and goes again.
            metrics::record_shipped(unreachable_outcome(&err));
            return Progress::Retry;
        }
    };

    match read_answer(&answer) {
        Progress::Stored { durable_offset } => {
            cursor.next_offset = durable_offset;
            tracing::info!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                base_offset,
                "the follower placed its log and replication resumed",
            );
            metrics::record_shipped(metrics::OUTCOME_BOOTSTRAPPED);
            Progress::Resume {
                offset: durable_offset,
            }
        }
        // The follower will not take it, and nothing about that resolves by
        // asking again: it holds records of its own, so a person has to decide
        // what becomes of them.
        _ => {
            tracing::error!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                base_offset,
                "replication stopped: the follower refused a bootstrap and cannot be caught up",
            );
            cursor.halted = Some(Halt::NeedsBootstrap);
            metrics::record_shipped(metrics::OUTCOME_NEEDS_BOOTSTRAP);
            Progress::Halted(Halt::NeedsBootstrap)
        }
    }
}

fn unreachable_outcome(err: &PeerError) -> &'static str {
    match err {
        PeerError::Timeout { .. } => metrics::OUTCOME_TIMEOUT,
        PeerError::Disconnected { .. } => metrics::OUTCOME_DISCONNECTED,
        _ => metrics::OUTCOME_UNREACHABLE,
    }
}

/// How far a follower may be behind and still be fit to lead.
///
/// Zero: a follower is caught up when it holds every record the leader does.
///
/// A bound above zero is a bound on how much a promotion may silently lose, and
/// there is no honest value for it that is not a policy decision. Zero needs no
/// such decision, and a follower reaches it constantly on a healthy shard — the
/// leader only has to be momentarily idle. Loosening it is a change to make
/// deliberately, with a measurement behind it, rather than a default nobody
/// chose.
pub const CATCH_UP_BOUND: u64 = 0;

/// Which followers hold enough of the log to lead it.
///
/// Reported to the control plane, which gates promotion on it. A halted
/// follower never qualifies however close its last position was: it has stopped
/// rather than fallen behind, and its position is no longer moving toward the
/// leader's.
// The bound is zero today, so "within it" is an equality and clippy says so.
// Written as a comparison because the bound is the thing meant to change: if it
// is ever raised, this reads correctly without being rediscovered.
#[allow(clippy::absurd_extreme_comparisons)]
pub fn caught_up(tail: u64, followers: &[FollowerCursor]) -> Vec<String> {
    followers
        .iter()
        .filter(|follower| follower.halted.is_none())
        .filter(|follower| tail.saturating_sub(follower.next_offset) <= CATCH_UP_BOUND)
        .map(|follower| follower.node_id.clone())
        .collect()
}

/// How far behind the slowest follower is, in records.
///
/// This is the `Leader` consistency level's loss window, and the design note is
/// explicit that it has to be observable: an operator choosing `Leader` is
/// choosing this window, and a bound nobody can see is not a bound.
///
/// A halted follower is excluded. It is not lagging, it has stopped, and
/// folding "stopped" into a lag figure hides it behind a number that merely
/// looks large.
pub fn lag_records(tail: u64, followers: &[FollowerCursor]) -> Option<u64> {
    followers
        .iter()
        .filter(|follower| follower.halted.is_none())
        .map(|follower| tail.saturating_sub(follower.next_offset))
        .max()
}

/// How many copies must hold a record for a majority, the leader included.
///
/// `replicas` is the follower count, so the replica set is one larger. A set of
/// three needs two, a set of five needs three — and a set of one needs one,
/// which is the leader alone and is why `replication_factor: 1` costs nothing.
pub fn majority_of(replicas: usize) -> usize {
    replicas.div_ceil(2) + 1
}

/// The highest offset a majority of the replica set holds durably.
///
/// The leader is counted as holding everything up to `leader_tail`: it wrote the
/// records, and a record it has not written is not a candidate for a quorum in
/// the first place.
///
/// **A halted follower counts for nothing.** It is not slow, it has stopped —
/// its log has diverged, or this broker has been superseded — and letting a
/// stale position count toward a majority is how an acknowledgement comes to
/// mean less than it says.
///
/// Cursors belong to one generation. The caller passes the set for the
/// generation it is publishing at, which is what stops an older generation's
/// acknowledgements satisfying a newer generation's quorum.
pub fn quorum_offset(leader_tail: u64, followers: &[FollowerCursor]) -> u64 {
    let needed = majority_of(followers.len());
    // The leader is one of them, and it holds the most.
    let mut held: Vec<u64> = std::iter::once(leader_tail)
        .chain(
            followers
                .iter()
                .filter(|follower| follower.halted.is_none())
                .map(|follower| follower.next_offset.min(leader_tail)),
        )
        .collect();
    // Descending, so the `needed`-th is the highest offset that many hold.
    held.sort_unstable_by(|a, b| b.cmp(a));
    held.get(needed - 1).copied().unwrap_or(0)
}

#[cfg(test)]
#[path = "replication_tests.rs"]
mod tests;
