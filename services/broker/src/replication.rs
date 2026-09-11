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
    ErrorCode, InternalMessage, ReplicateRecords, ShardRef, batch_checksum,
};

use crate::peer::{PeerError, PeerRequester};

pub mod driver;
pub mod metrics;

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
}

impl FollowerCursor {
    pub fn new(node_id: impl Into<String>, addr: SocketAddr, next_offset: u64) -> Self {
        Self {
            node_id: node_id.into(),
            addr,
            next_offset,
            halted: None,
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
    cursor: &mut FollowerCursor,
    max_batch_bytes: usize,
) -> Progress {
    if let Some(halt) = cursor.halted {
        return Progress::Halted(halt);
    }

    let records = match log.read_from(cursor.next_offset, max_batch_bytes).await {
        Ok(records) => records,
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
        return Progress::UpToDate;
    }

    let first_offset = records[0].offset;
    let payloads: Vec<Bytes> = records.into_iter().map(|record| record.payload).collect();
    let request = InternalMessage::ReplicateRecords(ReplicateRecords {
        // The pool assigns the real id; it owns the connection this lands on.
        correlation_id: 0,
        shard: shard.clone(),
        first_offset,
        checksum: batch_checksum(&payloads),
        payloads,
    });

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
            // The follower's own account of where it is. Trusted over the
            // leader's arithmetic: it is the side that did the writing, and a
            // partially applied batch would leave the two disagreeing.
            cursor.next_offset = durable_offset;
            metrics::record_shipped(metrics::OUTCOME_OK);
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
            });
        }
        Progress::Retry => metrics::record_shipped(metrics::OUTCOME_REFUSED),
        Progress::UpToDate => {}
    }
    progress
}

fn unreachable_outcome(err: &PeerError) -> &'static str {
    match err {
        PeerError::Timeout { .. } => metrics::OUTCOME_TIMEOUT,
        PeerError::Disconnected { .. } => metrics::OUTCOME_DISCONNECTED,
        _ => metrics::OUTCOME_UNREACHABLE,
    }
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

#[cfg(test)]
#[path = "replication_tests.rs"]
mod tests;
