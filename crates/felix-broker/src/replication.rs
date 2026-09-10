//! The follower's half of replication: storing what a leader shipped.
//!
//! # The rule
//!
//! A follower stores a record at the **leader's** offset or not at all. That is
//! what makes the two logs comparable by offset, and every other part of
//! replication rests on it — the acknowledged mark, the catch-up range, the
//! caught-up test that gates promotion.
//!
//! The log underneath appends at its own tail and cannot be told where to put a
//! record. So the follower does not ask it to: it checks that the batch begins
//! exactly at its tail, and refuses otherwise. Position is verified rather than
//! commanded, which is stricter and needs nothing from the storage layer.
//!
//! # Why each refusal is its own answer
//!
//! | The batch | Meaning | What the leader does |
//! | --- | --- | --- |
//! | starts past the tail | records are missing in between | resume from `expected_offset` |
//! | is entirely below the tail | a retry of something already stored | nothing; already acknowledged |
//! | straddles the tail | a retry that overlaps | the new suffix is stored |
//! | disagrees on stored bytes | the logs have diverged | stop |
//!
//! The middle two are why a retry is safe. Replication has to be able to resend
//! a batch whose acknowledgement was lost, and resending must not duplicate a
//! record: an overlap is resolved by position, and the bytes are checked rather
//! than assumed.
//!
//! # Durable, not buffered
//!
//! [`apply`] returns only after the batch satisfies the log's fsync policy. A
//! follower that acknowledged sooner would let the leader believe a record had
//! survived a failure it would not have survived — and under
//! `ConsistencyLevel::Quorum` that belief is the guarantee.
use bytes::Bytes;

use crate::durable::StreamLog;
use crate::error::{BrokerError, Result};

/// What applying a replication batch did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    /// One past the last record now durably stored. Both the acknowledgement
    /// and the offset the leader should send next.
    pub durable_offset: u64,
    /// Records actually written. Zero for a batch that was entirely a retry.
    pub appended: usize,
}

/// Why a batch was not applied.
///
/// Separate from [`BrokerError`] because these are answers about *position*,
/// which the leader acts on, rather than failures of this broker.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Divergence {
    /// The batch starts past the follower's tail. Applying it would leave a
    /// hole, and a log with a hole cannot be read back.
    #[error("batch starts at {first_offset}, expected {expected}")]
    Gap { expected: u64, first_offset: u64 },
    /// The batch disagrees with bytes already stored. Records are never
    /// rewritten, so there is no repair: progress stops here.
    #[error("batch disagrees with the stored record at offset {offset}")]
    Conflict { offset: u64, expected: u64 },
    /// The batch did not survive the trip.
    #[error("batch checksum mismatch (leader {leader:#x}, computed {computed:#x})")]
    Corrupt { leader: u64, computed: u64 },
}

impl Divergence {
    /// The offset the follower wants next, for the leader to resume from.
    pub fn expected_offset(&self) -> u64 {
        match self {
            Self::Gap { expected, .. } => *expected,
            Self::Conflict { expected, .. } => *expected,
            Self::Corrupt { .. } => 0,
        }
    }
}

/// Store a batch the leader shipped, at the leader's offsets.
///
/// `first_offset` is where `payloads[0]` belongs. Returns once the batch is
/// durable.
pub async fn apply(
    log: &StreamLog,
    first_offset: u64,
    checksum: u64,
    payloads: &[Bytes],
) -> Result<std::result::Result<Applied, Divergence>> {
    let tail = log.tail_offset().await?;

    // Checked before the tail is consulted for anything else: a batch that did
    // not survive the trip says nothing reliable about position either.
    let computed = felix_wire::internal::batch_checksum(payloads);
    if computed != checksum {
        return Ok(Err(Divergence::Corrupt {
            leader: checksum,
            computed,
        }));
    }

    if payloads.is_empty() {
        // Nothing to store, and nothing wrong: an empty batch is a position
        // probe, and the tail is the answer.
        return Ok(Ok(Applied {
            durable_offset: tail,
            appended: 0,
        }));
    }

    if first_offset > tail {
        return Ok(Err(Divergence::Gap {
            expected: tail,
            first_offset,
        }));
    }

    // The batch reaches back into what is already stored. That is a retry, so
    // the overlap is verified rather than trusted, and only the suffix past the
    // tail is new.
    let overlap = (tail - first_offset).min(payloads.len() as u64) as usize;
    if overlap > 0
        && let Some(divergence) = conflict_in(log, first_offset, &payloads[..overlap], tail).await?
    {
        return Ok(Err(divergence));
    }

    let fresh = &payloads[overlap..];
    if fresh.is_empty() {
        // Wholly a retry of records already held. Acknowledging the tail is
        // what makes a lost acknowledgement cost nothing.
        return Ok(Ok(Applied {
            durable_offset: tail,
            appended: 0,
        }));
    }

    let pending = log.begin_append(fresh).await?;
    log.commit(&pending).await?;

    Ok(Ok(Applied {
        durable_offset: tail + fresh.len() as u64,
        appended: fresh.len(),
    }))
}

/// Compare a batch's overlapping prefix against what is already stored.
///
/// Reads only the overlap, which is bounded by the batch, so a retry costs a
/// read proportional to the resend rather than to the log.
async fn conflict_in(
    log: &StreamLog,
    first_offset: u64,
    overlapping: &[Bytes],
    tail: u64,
) -> Result<Option<Divergence>> {
    let wanted: usize = overlapping.iter().map(|payload| payload.len() + 32).sum();
    let stored = match log.read_from(first_offset, wanted.max(1)).await {
        Ok(stored) => stored,
        // Retention discarded the records this batch overlaps. There is nothing
        // left to compare against, and refusing on that basis would stall a
        // follower for a reason that is not divergence. The suffix past the
        // tail is still appended, which is the part that matters.
        Err(BrokerError::CursorTooOld { .. }) => return Ok(None),
        Err(err) => return Err(err),
    };

    for (index, payload) in overlapping.iter().enumerate() {
        let offset = first_offset + index as u64;
        let Some(record) = stored.iter().find(|record| record.offset == offset) else {
            // A short read, not a disagreement: the comparison simply cannot be
            // made for the rest of this batch.
            break;
        };
        if record.payload != *payload {
            return Ok(Some(Divergence::Conflict {
                offset,
                expected: tail,
            }));
        }
    }
    Ok(None)
}

#[cfg(test)]
#[path = "replication_tests.rs"]
mod tests;
