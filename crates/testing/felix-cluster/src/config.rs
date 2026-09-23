//! What a test asks for when it starts a cluster.

/// How to build a cluster.
#[derive(Clone)]
pub struct ClusterConfig {
    pub nodes: usize,
    pub tenant_id: String,
    pub namespace: String,
    /// Streams to create.
    pub streams: Vec<StreamSpec>,
    /// Caches to create. Empty by default, so a test that says nothing about
    /// caches builds the cluster it always did.
    pub caches: Vec<CacheSpec>,
    /// Inherit the parent's stdout/stderr rather than discarding it. Useful when
    /// running the harness by hand; noisy inside a test.
    pub inherit_output: bool,
    /// How many client-facing QUIC listeners each broker binds, on consecutive
    /// ports. One by default, which is what every test that says nothing about
    /// listeners gets.
    pub quic_listeners: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            streams: vec![StreamSpec::new("orders", 1)],
            caches: Vec::new(),
            inherit_output: false,
            quic_listeners: 1,
        }
    }
}

/// A stream to create when the cluster starts.
///
/// Replication and consistency are separate from the shard count because a
/// failure test needs them apart: a shard with no replicas has nothing to fail
/// over to, and a `Leader` stream acknowledges before a follower has the record,
/// so neither proves anything about the other.
#[derive(Clone, Debug)]
pub struct StreamSpec {
    pub name: String,
    pub shards: u32,
    /// Copies of each shard, leader included. `1` is leader-only.
    pub replication_factor: u32,
    /// `"Leader"` or `"Quorum"`, as the control plane spells them.
    pub consistency: String,
}

impl StreamSpec {
    /// An unreplicated, leader-acknowledged stream — the default a cluster gets
    /// when a test says nothing about replication.
    pub fn new(name: impl Into<String>, shards: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor: 1,
            consistency: "Leader".to_string(),
        }
    }

    /// Replicated across `replication_factor` brokers, acknowledged by a
    /// majority. What a failover test needs: records that are on more than one
    /// broker by the time the publish returns.
    pub fn quorum(name: impl Into<String>, shards: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor,
            consistency: "Quorum".to_string(),
        }
    }

    /// Replicated, but acknowledged by the leader alone.
    pub fn replicated(name: impl Into<String>, shards: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor,
            consistency: "Leader".to_string(),
        }
    }
}

/// A cache the cluster should serve.
#[derive(Clone, Debug)]
pub struct CacheSpec {
    pub name: String,
    /// How many shards the keyspace is split across. More than one is what
    /// makes a test actually exercise routing rather than a single owner.
    pub shards: u32,
    pub replication_factor: u32,
    /// `"Leader"` or `"Quorum"`, as the control plane spells them.
    pub consistency: String,
}

impl CacheSpec {
    /// An unreplicated, leader-acknowledged cache.
    pub fn new(name: impl Into<String>, shards: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor: 1,
            consistency: "Leader".to_string(),
        }
    }

    /// Replicated, with each write acknowledged only once a majority holds it.
    pub fn quorum(name: impl Into<String>, shards: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor,
            consistency: "Quorum".to_string(),
        }
    }

    /// Replicated across `replication_factor` brokers, leader included. What a
    /// failover test needs: a cache whose contents are on more than one broker
    /// before its leader is killed.
    pub fn replicated(name: impl Into<String>, shards: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor,
            consistency: "Leader".to_string(),
        }
    }
}
