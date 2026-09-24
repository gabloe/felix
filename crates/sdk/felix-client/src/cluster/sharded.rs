//! Every shard of a stream or cache, read as one thing.
//!
//! A subscription, a consumer group and a cache watch each read **one**
//! shard, and a stream's shards can have different owners. These open one per
//! shard, each following its own redirect, and merge what comes back, without
//! pretending the result has an order across shards that it does not.

mod group;
mod subscription;
mod watch;

pub use group::{ShardedGroup, ShardedGroupRecord};
pub use subscription::{ShardEvent, ShardedSubscription};
pub use watch::{ShardedCacheWatch, ShardedCacheWatchItem};

pub(crate) use subscription::subscribe_sharded;
pub(crate) use watch::watch_sharded;

use std::collections::BTreeMap;

/// Where a sharded consumer had reached, one offset per shard.
///
/// `Event.offset` is per shard, so a single number cannot describe the position
/// of a consumer reading several. This is the shape a resume actually needs.
///
/// A shard is absent when nothing has been delivered from it yet, which resumes
/// that shard at the position the original call asked for rather than at zero.
pub type ShardOffsets = BTreeMap<u32, u64>;
