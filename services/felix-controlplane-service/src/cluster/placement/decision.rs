//! What placement decided for a shard, and why it could not do more.
use crate::model::ShardAssignment;

/// What placement decided for one shard.
// One per shard per pass, so the size of the move variant costs nothing worth
// a box at every match.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The existing assignment is still valid. Nothing to write.
    Kept,
    /// This shard needs an assignment written: a leader, and the followers that
    /// will hold a copy of it.
    Place(String, Vec<String>),
    /// One step of a planned move, as the assignment to write for it.
    Move(MoveStep, ShardAssignment),
    /// A move is in progress and this pass can do nothing for it yet.
    Waiting(Blocked),
    Unplaceable(Unplaceable),
}

/// The steps of a planned move. Each is one assignment write at a new
/// generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoveStep {
    /// The destination joins the replica set as `successor`.
    Stage { successor: String },
    /// The destination is caught up: the assignment goes `Draining` and the
    /// leader stops serving.
    Fence,
    /// The leader has stopped. `to` leads from the next generation.
    CutOver { from: String, to: String },
    /// The destination stopped being live before it led; its staging is undone.
    Abandon { successor: String },
    /// A follower on a draining node is replaced by one that is staying.
    Reseat { from: String, to: String },
}

impl MoveStep {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Stage { .. } => "stage",
            Self::Fence => "fence",
            Self::CutOver { .. } => "cut_over",
            Self::Abandon { .. } => "abandon",
            Self::Reseat { .. } => "reseat",
        }
    }
}

/// Why a move could not advance this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Blocked {
    /// The successor is a replica but has not reported caught up.
    DestinationCatchingUp { successor: String },
    /// The assignment is `Draining` and the leader has not reported drained:
    /// writes are still in flight there, or its group state or counters are
    /// not yet on every replica that could take over.
    LeaderStopping,
    /// A move is wanted, and `MovePolicy::max_concurrent` is reached.
    MoveLimit,
    /// The leader is draining and no live node can take the shard.
    NoDestination,
}

impl std::fmt::Display for Blocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DestinationCatchingUp { successor } => {
                write!(f, "waiting for {successor} to catch up")
            }
            Self::LeaderStopping => write!(
                f,
                "waiting for the leader to stop serving and hand over its logs"
            ),
            Self::MoveLimit => write!(f, "waiting for a move slot"),
            Self::NoDestination => write!(f, "no live node can take this shard"),
        }
    }
}

/// Why a shard could not be placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unplaceable {
    /// No node is live.
    NoEligibleNode,
    /// Every live node is at its `max_shards` cap.
    AllNodesAtCapacity,
    /// The shard was replicated, its leader is gone, and no replica that holds
    /// the log can take over.
    ///
    /// Deliberately unavailable rather than placed elsewhere. A node that has
    /// never seen the shard would serve an empty log at a new generation while
    /// the records sat on replicas that were not chosen — the failover would
    /// *be* the data loss, and nothing downstream would report it as one. This
    /// is visible, and it resolves on its own when a replica catches up or the
    /// old leader returns.
    NoCaughtUpReplica,
}

impl std::fmt::Display for Unplaceable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoEligibleNode => write!(f, "no live node is available to lead this shard"),
            Self::AllNodesAtCapacity => {
                write!(f, "every live node is at its max_shards capacity")
            }
            Self::NoCaughtUpReplica => write!(
                f,
                "the leader is gone and no replica holding this shard's log can take over"
            ),
        }
    }
}
