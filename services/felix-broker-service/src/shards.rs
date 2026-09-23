//! Owning shards.
//!
//! [`watch`] follows what the control plane assigned, by snapshot and then by
//! change feed. [`lifecycle`] is what this broker has actually *done* about
//! that, which is deliberately separate state: a shard serves only once its log
//! is open, and only at the generation the control plane currently names.
//! [`routing`] answers the question every publish asks first: is this shard
//! mine, should it be forwarded, or can nobody serve it right now?
//!
//! [`ShardKey`] and [`ShardKind`] live here because all three use them.

pub mod lifecycle;
pub mod routing;
pub mod watch;

use serde::Deserialize;

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

/// A shard's identity, as the control plane names it in assignments.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardKey {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    #[serde(default)]
    pub kind: ShardKind,
}
