//! The openraft type configuration for the metadata group.
//!
//! Commands (`D`) and responses (`R`) are opaque byte vectors here on
//! purpose: this layer moves and persists them, and only the application
//! state machine knows what they mean. That keeps the consensus core and the
//! command set (#338) evolvable independently — a new command variant is not
//! a change to anything in `raft/`.
use std::io::Cursor;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Vec<u8>,
        R = Vec<u8>,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub(crate) type Raft = openraft::Raft<TypeConfig>;
pub(crate) type LogId = openraft::LogId<u64>;
pub(crate) type Entry = openraft::Entry<TypeConfig>;
pub(crate) type StorageError = openraft::StorageError<u64>;
pub(crate) type StoredMembership = openraft::StoredMembership<u64, openraft::BasicNode>;
pub(crate) type SnapshotMeta = openraft::SnapshotMeta<u64, openraft::BasicNode>;
pub(crate) type Snapshot = openraft::Snapshot<TypeConfig>;
