//! The plain objects the handles resolve with.
//!
//! `#[napi(object)]` types cross into JavaScript as ordinary objects, copied
//! out of the Rust values, so a caller can keep them after the handle that
//! produced them is gone.

use napi::bindgen_prelude::*;
use napi_derive::napi;

/// One delivered record.
#[napi(object)]
pub struct Event {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub payload: Buffer,
    /// The record's log offset on a durable stream, absent on an ephemeral one.
    ///
    /// A jump in these is exactly a drop: subscriber queues shed under the
    /// default policy rather than blocking the publisher, so a gap here is the
    /// signal that it happened.
    pub offset: Option<BigInt>,
}

/// One record handed out by a consumer group.
#[napi(object)]
pub struct GroupRecord {
    pub offset: BigInt,
    pub payload: Buffer,
    /// How many times this record has been handed out, this delivery included.
    /// `1` is a first attempt; anything higher is a redelivery, so a consumer
    /// can treat a retry differently. `0` means the broker did not report it.
    pub attempts: u32,
}

/// One change observed by a cache watch.
#[napi(object)]
pub struct CacheChange {
    pub key: String,
    /// Absent when the key was deleted or expired.
    pub value: Option<Buffer>,
    pub offset: BigInt,
    pub expires_at_millis: BigInt,
}

/// An item from a cache watch: a change, or notice that the watch fell behind.
#[napi(object)]
pub struct CacheWatchItem {
    pub change: Option<CacheChange>,
    /// Set when the watch lagged and the broker ended it. Re-watching with
    /// `start = resumeFrom` is gapless.
    pub lagged_resume_from: Option<BigInt>,
}

/// An item from a sharded subscription.
///
/// Exactly one of these is set. A lost shard does not affect the others: they
/// keep delivering while that one is re-established, and it resumes from its
/// own last offset so nothing is skipped.
#[napi(object)]
pub struct ShardEvent {
    pub shard: u32,
    pub event: Option<Event>,
    pub lost_error: Option<String>,
    pub recovered: Option<bool>,
}
