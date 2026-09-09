//! How a second terminal finds a running cluster.
//!
//! `felix-cluster up` holds a cluster in one process. Everything it needs to be
//! talked to — the broker addresses and a credential — lives in that process and
//! nowhere else, so a `publish` or `subscribe` in another window has nothing to
//! connect to. This writes it down.
//!
//! **The file contains a bearer token**, so it is written with owner-only
//! permissions and lives in the temp directory. It is a local development
//! harness whose brokers listen on loopback with dev certificates; the token is
//! no more sensitive than the cluster it opens, and that cluster disappears when
//! the process does.
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// A running cluster, as another process needs to see it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub control_plane: String,
    pub tenant_id: String,
    pub namespace: String,
    /// Publishes and subscribes. Stream permissions only.
    pub client_token: String,
    /// Reads membership and shard ownership, so a client can say which broker
    /// owns what.
    pub admin_token: String,
    pub nodes: Vec<SessionNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionNode {
    pub node_id: String,
    pub client_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
}

/// Where `up` leaves the file and the other commands look for it.
///
/// A fixed path, so a second window needs nothing copied into it.
pub fn default_path() -> PathBuf {
    std::env::temp_dir().join("felix-cluster.json")
}

impl Session {
    pub fn write(&self, path: &Path) -> Result<()> {
        let body = serde_json::to_vec_pretty(self).context("encode session")?;
        std::fs::write(path, body).with_context(|| format!("write {}", path.display()))?;
        restrict(path)?;
        Ok(())
    }

    pub fn read(path: &Path) -> Result<Self> {
        let body = std::fs::read(path).with_context(|| {
            format!(
                "no cluster session at {} -- start one with `task cluster:up`",
                path.display()
            )
        })?;
        serde_json::from_slice(&body).context("decode session")
    }

    pub fn node(&self, node_id: &str) -> Option<&SessionNode> {
        self.nodes.iter().find(|node| node.node_id == node_id)
    }
}

/// Owner-only, because the file holds a bearer token.
#[cfg(unix)]
fn restrict(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restrict {}", path.display()))
}

#[cfg(not(unix))]
fn restrict(_path: &Path) -> Result<()> {
    Ok(())
}
