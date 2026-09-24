//! Leader-to-follower replication: records, bootstrap, rebuild, and the
//! follower's answers.

use bytes::Bytes;

use super::{ErrorCode, ShardRef};
use crate::error::{Error, Result};

/// Records the leader has committed, shipped to a follower.
///
/// Append-only, and identified by the offsets the *leader* assigned: a follower
/// stores a record at the leader's offset or not at all. That is what makes the
/// two logs comparable by offset, which every other part of replication relies
/// on.
///
/// `shard.generation` is the leader's epoch. A follower that knows of a newer
/// one refuses, because a leader at an older epoch may already have been
/// replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateRecords {
    pub correlation_id: u64,
    pub shard: ShardRef,
    /// Offset of `payloads[0]`. Payload `i` belongs at `first_offset + i`.
    pub first_offset: u64,
    /// Over the payload bytes, in order. Checked before anything is written, so
    /// a batch corrupted in transit is refused rather than stored.
    pub checksum: u64,
    pub payloads: Vec<Bytes>,
}

/// The follower stored the batch.
///
/// `durable_offset` is one past the last record the follower has on disk, so it
/// is both an acknowledgement and the offset the leader should send next. It
/// reports **durable** data, never buffered: a follower that acknowledged
/// before its own fsync would let the leader believe a record survived a
/// failure it would not have survived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicateOk {
    pub correlation_id: u64,
    pub durable_offset: u64,
}

/// The follower refused, and where it stands.
///
/// `expected_offset` is what the follower wants next. For `LogGap` it is how
/// the leader repairs without a separate negotiation; for the rest it is
/// diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateError {
    pub correlation_id: u64,
    pub code: ErrorCode,
    pub expected_offset: u64,
    pub detail: String,
}

/// The leader has nothing older than `base_offset` left.
///
/// Sent when a follower's position is below everything the leader still holds,
/// so shipping cannot reach it: the records in between are gone from the leader
/// too. It says "the surviving log starts here" — which is the one fact a
/// follower cannot work out for itself, and the fact it needs before it may
/// place a log that begins anywhere other than zero.
///
/// A follower with records of its own refuses. Discarding them is an operator's
/// decision, not a leader's.
///
/// A separate kind rather than a field on [`ReplicateRecords`]: this protocol
/// freezes existing body layouts, and an older peer already rejects an unknown
/// kind rather than misreading it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateBootstrap {
    pub correlation_id: u64,
    pub shard: ShardRef,
    /// Offset of the oldest record the leader still holds.
    pub base_offset: u64,
}

/// Discard a follower's copy of one log and start again at `base_offset`.
///
/// Sent by the leader to a follower whose replication has halted -- a
/// diverged copy, or one that refused a bootstrap -- and only under the
/// leader's rebuild policy. The follower checks it is a replica of the shard
/// at this generation, discards every record it holds for that log, places
/// an empty log at `base_offset`, and answers `ReplicateOk` with that offset;
/// the leader then ships from there as it would to any follower that far
/// behind. A peer that predates this kind refuses it, and the halt stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateRebuild {
    pub correlation_id: u64,
    pub shard: ShardRef,
    pub log: ReplicaLog,
    /// Offset of the oldest record the leader still holds, where the new copy
    /// begins.
    pub base_offset: u64,
}

/// Which of a shard's logs a rebuild is about.
///
/// A field rather than five kinds, unlike the records and bootstrap messages:
/// those share a body with something else and the kind is what tells them
/// apart, whereas nothing shares this body. The values are this protocol's,
/// not the broker's own enum, so a renumbering there cannot change the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplicaLog {
    Stream = 1,
    Cache = 2,
    GroupCursors = 3,
    GroupDeadLetters = 4,
    Counters = 5,
}

impl ReplicaLog {
    /// Parse the wire value; an unknown one is an error.
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(ReplicaLog::Stream),
            2 => Ok(ReplicaLog::Cache),
            3 => Ok(ReplicaLog::GroupCursors),
            4 => Ok(ReplicaLog::GroupDeadLetters),
            5 => Ok(ReplicaLog::Counters),
            other => Err(Error::UnknownInternalReplicaLog(other)),
        }
    }
}

/// The checksum a [`ReplicateRecords`] batch carries.
///
/// Defined once, here, so the leader and the follower cannot compute it
/// differently — a checksum the two sides disagree about reports corruption on
/// every healthy batch, which is worse than not having one.
///
/// It covers each payload's length and its bytes, in order. Including the
/// length is what stops `["ab", "c"]` and `["a", "bc"]` hashing alike: they are
/// different records, and a follower storing one where the leader has the other
/// is exactly the divergence this is here to catch.
pub fn batch_checksum(payloads: &[Bytes]) -> u64 {
    let mut hasher = crc32fast::Hasher::new();
    for payload in payloads {
        hasher.update(&(payload.len() as u32).to_be_bytes());
        hasher.update(payload);
    }
    u64::from(hasher.finalize())
}
