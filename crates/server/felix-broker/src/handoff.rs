//! What a reader is told when its shard moves to another broker.
//!
//! A broker that stops serving a shard ends that shard's subscriptions and
//! cache watches. Each reader's feed records why before it closes, so the
//! delivery path can send a last frame saying where to resume instead of just
//! finishing the stream.

/// Where a shard went when this broker stopped serving it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardHandoff {
    /// The broker taking the shard, when this one knows.
    pub node_id: Option<String>,
    /// That broker's client address, when the cluster publishes one.
    pub addr: Option<String>,
    /// The assignment generation that moved the shard.
    pub generation: u64,
}

/// A reader's feed ended because its shard moved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMoved {
    /// The first offset this reader was not offered; see
    /// `felix_wire::Message::ShardMoved`. `None` when there is no offset that
    /// means anything on the next owner.
    pub resume_from: Option<u64>,
    /// Where the shard went.
    pub to: ShardHandoff,
}
