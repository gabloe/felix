//! Which backend holds the metadata, and how to reach it.
use std::str::FromStr;

use anyhow::{Context, Result, anyhow};

use super::{
    DEFAULT_PG_ACQUIRE_TIMEOUT_MS, DEFAULT_PG_CONNECT_TIMEOUT_MS, DEFAULT_PG_MAX_CONNECTIONS,
    parse_positive_env,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    Memory,
    Postgres,
    /// Metadata replicated by the control-plane instances themselves —
    /// no external database. See `docs/metadata-raft-design.md`.
    Raft,
}

impl FromStr for StorageBackend {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_lowercase().as_str() {
            "memory" => Ok(StorageBackend::Memory),
            "postgres" => Ok(StorageBackend::Postgres),
            "raft" => Ok(StorageBackend::Raft),
            other => Err(anyhow!("invalid storage backend: {other}")),
        }
    }
}

/// The Raft backend's identity and group shape.
#[derive(Debug, Clone)]
pub struct RaftBackendConfig {
    /// This instance's id within the group; must appear in `peers`.
    pub node_id: u64,
    /// Where the Raft log, vote, and snapshots live. Must survive restarts —
    /// this is what makes a restart a rejoin instead of a fresh member.
    pub data_dir: std::path::PathBuf,
    /// The initial group, `id -> api-address` (host:port of each instance's
    /// main listener, which also serves the Raft RPC routes). Every member
    /// must be configured with the same map: initializing two disjoint
    /// member sets is how split brain is manufactured.
    pub peers: std::collections::BTreeMap<u64, String>,
    /// Timing overrides; `None` keeps the seam's defaults, which are sized
    /// for a three-instance group on one network.
    pub heartbeat_ms: Option<u64>,
    pub election_timeout_min_ms: Option<u64>,
    pub election_timeout_max_ms: Option<u64>,
    pub snapshot_logs_since_last: Option<u64>,
    pub logs_kept_behind_snapshot: Option<u64>,
    pub write_timeout_ms: Option<u64>,
}

impl RaftBackendConfig {
    pub(super) fn validate(&self) -> Result<()> {
        let heartbeat = self.heartbeat_ms.unwrap_or(150);
        let min = self.election_timeout_min_ms.unwrap_or(600);
        let max = self.election_timeout_max_ms.unwrap_or(1200);
        // An election timeout at or below the heartbeat elects against
        // healthy leaders — the same class of self-harm as a node expiry
        // timeout below the heartbeat interval, and refused the same way.
        if min <= heartbeat {
            return Err(anyhow!(
                "FELIX_RAFT_ELECTION_TIMEOUT_MIN_MS ({min}) must exceed FELIX_RAFT_HEARTBEAT_MS ({heartbeat})"
            ));
        }
        if max <= min {
            return Err(anyhow!(
                "FELIX_RAFT_ELECTION_TIMEOUT_MAX_MS ({max}) must exceed the minimum ({min})"
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub url: String,
    pub max_connections: u32,
    pub connect_timeout_ms: u64,
    pub acquire_timeout_ms: u64,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: DEFAULT_PG_MAX_CONNECTIONS,
            connect_timeout_ms: DEFAULT_PG_CONNECT_TIMEOUT_MS,
            acquire_timeout_ms: DEFAULT_PG_ACQUIRE_TIMEOUT_MS,
        }
    }
}

/// The Raft backend from the environment: all three variables or none.
///
/// Partial configuration is an error for the same reason as bootstrap TLS:
/// an operator who set two of the three believed they configured a group.
pub(super) fn raft_from_env() -> Result<Option<RaftBackendConfig>> {
    let node_id = std::env::var("FELIX_RAFT_NODE_ID").ok();
    let data_dir = std::env::var("FELIX_RAFT_DATA_DIR").ok();
    let peers = std::env::var("FELIX_RAFT_PEERS").ok();
    match (node_id, data_dir, peers) {
        (None, None, None) => Ok(None),
        (Some(node_id), Some(data_dir), Some(peers)) => {
            let node_id: u64 = node_id
                .parse()
                .with_context(|| "parse FELIX_RAFT_NODE_ID")?;
            Ok(Some(RaftBackendConfig {
                node_id,
                data_dir: data_dir.into(),
                peers: parse_raft_peers(&peers)?,
                heartbeat_ms: parse_positive_env("FELIX_RAFT_HEARTBEAT_MS"),
                election_timeout_min_ms: parse_positive_env("FELIX_RAFT_ELECTION_TIMEOUT_MIN_MS"),
                election_timeout_max_ms: parse_positive_env("FELIX_RAFT_ELECTION_TIMEOUT_MAX_MS"),
                snapshot_logs_since_last: parse_positive_env("FELIX_RAFT_SNAPSHOT_LOGS_SINCE_LAST"),
                logs_kept_behind_snapshot: std::env::var("FELIX_RAFT_LOGS_KEPT_BEHIND_SNAPSHOT")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok()),
                write_timeout_ms: parse_positive_env("FELIX_RAFT_WRITE_TIMEOUT_MS"),
            }))
        }
        _ => Err(anyhow!(
            "raft backend needs all of FELIX_RAFT_NODE_ID, FELIX_RAFT_DATA_DIR, and FELIX_RAFT_PEERS"
        )),
    }
}

/// `"1=host:port,2=host:port"` — the id the instance answers to, and the
/// address its main listener (which serves the Raft routes) is reached on.
fn parse_raft_peers(value: &str) -> Result<std::collections::BTreeMap<u64, String>> {
    let mut peers = std::collections::BTreeMap::new();
    for entry in value.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addr) = entry
            .split_once('=')
            .ok_or_else(|| anyhow!("FELIX_RAFT_PEERS entry '{entry}' is not id=host:port"))?;
        let id: u64 = id
            .trim()
            .parse()
            .with_context(|| format!("parse peer id in '{entry}'"))?;
        let addr = addr.trim();
        if addr.is_empty() {
            return Err(anyhow!(
                "FELIX_RAFT_PEERS entry '{entry}' has an empty address"
            ));
        }
        if peers.insert(id, addr.to_string()).is_some() {
            return Err(anyhow!("FELIX_RAFT_PEERS lists id {id} twice"));
        }
    }
    if peers.is_empty() {
        return Err(anyhow!("FELIX_RAFT_PEERS is empty"));
    }
    Ok(peers)
}
