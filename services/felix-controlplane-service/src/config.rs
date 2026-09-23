//! Control-plane configuration and defaults.
//!
//! Defines config structs, defaults, and env/YAML parsing for the control-plane
//! service, including storage backend selection and bootstrap settings.
//!
//! Defaults are chosen to keep dev setups simple while still bounding resource use.
use anyhow::{Context, Result, anyhow};
use jsonwebtoken::Algorithm;
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use std::str::FromStr;

pub const DEFAULT_CHANGES_LIMIT: u64 = 1000;
// Total budget for draining in-flight HTTP requests after a termination signal.
// Kubernetes defaults `terminationGracePeriodSeconds` to 30 and sends SIGKILL once
// it expires, so this leaves headroom to finish the drain and exit before then.
pub const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS: u64 = 25_000;

/// How long the instance keeps serving after it starts reporting unready.
///
/// Readiness-first shutdown only helps if something has time to act on it. A
/// load balancer learns this instance is draining by polling, so closing the
/// listener the moment readiness flips means requests are still being routed
/// here when the socket goes away. This is that gap, and it should exceed the
/// prober's interval times its failure threshold.
pub const DEFAULT_SHUTDOWN_PREDRAIN_MS: u64 = 5_000;

/// Two seconds, matching `readiness::DEFAULT_CHECK_TIMEOUT`.
pub const DEFAULT_READINESS_TIMEOUT_MS: u64 = 2_000;
/// One second, matching `readiness::DEFAULT_CACHE_TTL`.
pub const DEFAULT_READINESS_CACHE_TTL_MS: u64 = 1_000;
pub const DEFAULT_CHANGE_RETENTION_MAX_ROWS: i64 = 10_000;
/// How often a healthy broker is expected to report health.
pub const DEFAULT_NODE_HEARTBEAT_INTERVAL_MS: u64 = 5_000;
/// How long a node may go unheard before the cluster calls it down.
///
/// Three intervals: one lost heartbeat is a hiccup, three is a pattern.
pub const DEFAULT_NODE_EXPIRY_TIMEOUT_MS: u64 = 15_000;
/// How often shards are placed onto live brokers.
///
/// Placement is idempotent, so a pass over a settled cluster writes nothing;
/// this only bounds how long a new stream waits for an owner, or a failed
/// broker's shards wait to move.
pub const DEFAULT_SHARD_RECONCILE_INTERVAL_MS: u64 = 5_000;
/// How often the expiry sweep runs. Finer than the timeout so a node is marked
/// down close to when it actually expires rather than a whole timeout later.
pub const DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS: u64 = 2_000;
const DEFAULT_PG_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_PG_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PG_ACQUIRE_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_BOOTSTRAP_BIND_ADDR: &str = "127.0.0.1:9095";
const DEFAULT_OIDC_ALLOWED_ALGORITHMS: [Algorithm; 1] = [Algorithm::ES256];

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
    fn validate(&self) -> Result<()> {
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

/// Timings for broker liveness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLivenessConfig {
    /// Advertised to brokers in every heartbeat response.
    pub heartbeat_interval_ms: u64,
    /// Silence beyond this marks a node down.
    pub expiry_timeout_ms: u64,
    /// How often the sweep looks for expired nodes.
    pub sweep_interval_ms: u64,
    /// How often unplaced shards are assigned to live brokers.
    pub shard_reconcile_interval_ms: u64,
}

impl Default for NodeLivenessConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: DEFAULT_NODE_HEARTBEAT_INTERVAL_MS,
            expiry_timeout_ms: DEFAULT_NODE_EXPIRY_TIMEOUT_MS,
            sweep_interval_ms: DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS,
            shard_reconcile_interval_ms: DEFAULT_SHARD_RECONCILE_INTERVAL_MS,
        }
    }
}

impl NodeLivenessConfig {
    fn validate(&self) -> Result<()> {
        if self.heartbeat_interval_ms == 0 {
            return Err(anyhow!(
                "node heartbeat_interval_ms must be greater than zero"
            ));
        }
        if self.sweep_interval_ms == 0 {
            return Err(anyhow!("node sweep_interval_ms must be greater than zero"));
        }
        if self.shard_reconcile_interval_ms == 0 {
            return Err(anyhow!(
                "shard_reconcile_interval_ms must be greater than zero"
            ));
        }
        // A timeout at or below the interval expires brokers that are heartbeating
        // exactly as told to, which takes down a healthy cluster.
        if self.expiry_timeout_ms <= self.heartbeat_interval_ms {
            return Err(anyhow!(
                "node expiry_timeout_ms ({}) must exceed heartbeat_interval_ms ({})",
                self.expiry_timeout_ms,
                self.heartbeat_interval_ms
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    pub bind_addr: SocketAddr,
    pub metrics_bind: SocketAddr,
    pub region_id: String,
    pub storage: StorageBackend,
    pub postgres: Option<PostgresConfig>,
    pub raft: Option<RaftBackendConfig>,
    pub changes_limit: u64,
    pub change_retention_max_rows: Option<i64>,
    pub oidc_allowed_algorithms: Vec<Algorithm>,
    pub bootstrap: BootstrapConfig,
    pub node_liveness: NodeLivenessConfig,
    /// Shard moves in progress at once, across the cluster. See
    /// `placement::MovePolicy`.
    pub max_concurrent_shard_moves: usize,
    // Total budget for draining in-flight requests after SIGTERM/SIGINT before
    // remaining tasks are force-cancelled.
    pub shutdown_drain_timeout_ms: u64,
    /// How long to keep serving after readiness flips to draining, giving load
    /// balancers time to remove this instance before the listener closes.
    ///
    /// Zero skips the wait, which is right for a single instance nothing routes
    /// to and wrong behind a load balancer. A second termination signal cuts it
    /// short.
    pub shutdown_predrain_ms: u64,
    /// Longest a readiness check may take before it is treated as a failure.
    ///
    /// Set below the prober's own timeout, so the answer is this service's
    /// rather than the network giving up first.
    pub readiness_timeout_ms: u64,
    /// How long a readiness answer is reused before the store is asked again.
    ///
    /// Bounds probe cost at one query per window however many probers there
    /// are, and bounds how long recovery takes to become visible.
    pub readiness_cache_ttl_ms: u64,
}

/// The settings a config file may override.
///
/// `deny_unknown_fields`, here and on every nested override, because a key
/// nobody reads is a lie: an operator who writes `bind_adrr` gets the default,
/// no error, and an instance listening somewhere they did not ask for. A typo
/// inside `postgres:` is just as silent, which is why the nested ones carry it
/// too.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlPlaneConfigOverride {
    bind_addr: Option<String>,
    metrics_bind: Option<String>,
    region_id: Option<String>,
    storage: Option<StorageOverride>,
    postgres: Option<PostgresOverride>,
    changes_limit: Option<u64>,
    change_retention_max_rows: Option<i64>,
    oidc_allowed_algorithms: Option<Vec<String>>,
    bootstrap: Option<BootstrapOverride>,
    node_liveness: Option<NodeLivenessOverride>,
    max_concurrent_shard_moves: Option<usize>,
    shutdown_drain_timeout_ms: Option<u64>,
    shutdown_predrain_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeLivenessOverride {
    heartbeat_interval_ms: Option<u64>,
    expiry_timeout_ms: Option<u64>,
    sweep_interval_ms: Option<u64>,
    shard_reconcile_interval_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageOverride {
    backend: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresOverride {
    url: Option<String>,
    max_connections: Option<u32>,
    connect_timeout_ms: Option<u64>,
    acquire_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    pub enabled: bool,
    pub bind_addr: SocketAddr,
    pub token: Option<String>,
    /// The token being rotated out. Still accepted, so a rotation is two
    /// rolling deploys (add the new token, then drop this) with no window in
    /// which some instances refuse a token others require.
    pub previous_token: Option<String>,
    /// When set, the bootstrap listener terminates TLS and refuses any client
    /// that does not present a certificate signed by `client_ca_path`.
    pub tls: Option<BootstrapTlsConfig>,
}

impl BootstrapConfig {
    /// Tokens the bootstrap endpoint accepts, current first.
    pub fn accepted_tokens(&self) -> Vec<String> {
        self.token
            .iter()
            .chain(self.previous_token.iter())
            .cloned()
            .collect()
    }
}

/// mTLS for the bootstrap listener: all three or nothing, because a TLS
/// bootstrap endpoint that skips client verification would look secured while
/// still letting anyone on the network present the token.
#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapTlsConfig {
    /// PEM certificate chain the listener presents.
    pub cert_path: String,
    /// PEM private key for `cert_path`.
    pub key_path: String,
    /// PEM CA bundle; only clients holding a certificate signed by it may
    /// reach the bootstrap API at all.
    pub client_ca_path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapOverride {
    enabled: Option<bool>,
    bind_addr: Option<String>,
    token: Option<String>,
    previous_token: Option<String>,
    tls: Option<BootstrapTlsConfig>,
}

impl ControlPlaneConfig {
    pub fn from_env() -> Result<Self> {
        let metrics_bind = std::env::var("FELIX_CONTROLPLANE_METRICS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .with_context(|| "parse FELIX_CONTROLPLANE_METRICS_BIND")?;
        let bind_addr = std::env::var("FELIX_CONTROLPLANE_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8443".to_string())
            .parse()
            .with_context(|| "parse FELIX_CONTROLPLANE_BIND")?;
        let region_id = std::env::var("FELIX_REGION_ID").unwrap_or_else(|_| "local".to_string());

        let mut storage = std::env::var("FELIX_CONTROLPLANE_STORAGE_BACKEND")
            .ok()
            .and_then(|v| StorageBackend::from_str(&v).ok())
            .unwrap_or(StorageBackend::Memory);

        let changes_limit = std::env::var("FELIX_CONTROLPLANE_CHANGES_LIMIT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CHANGES_LIMIT);
        let node_liveness = NodeLivenessConfig {
            heartbeat_interval_ms: parse_positive_env("FELIX_NODE_HEARTBEAT_INTERVAL_MS")
                .unwrap_or(DEFAULT_NODE_HEARTBEAT_INTERVAL_MS),
            expiry_timeout_ms: parse_positive_env("FELIX_NODE_EXPIRY_TIMEOUT_MS")
                .unwrap_or(DEFAULT_NODE_EXPIRY_TIMEOUT_MS),
            sweep_interval_ms: parse_positive_env("FELIX_NODE_EXPIRY_SWEEP_INTERVAL_MS")
                .unwrap_or(DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS),
            shard_reconcile_interval_ms: parse_positive_env("FELIX_SHARD_RECONCILE_INTERVAL_MS")
                .unwrap_or(DEFAULT_SHARD_RECONCILE_INTERVAL_MS),
        };
        // Zero is meaningful: it holds every move.
        let max_concurrent_shard_moves = std::env::var("FELIX_SHARD_MOVES_MAX_CONCURRENT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(crate::placement::DEFAULT_MAX_CONCURRENT_MOVES);
        let readiness_timeout_ms = parse_positive_env("FELIX_READINESS_TIMEOUT_MS")
            .unwrap_or(DEFAULT_READINESS_TIMEOUT_MS);
        let readiness_cache_ttl_ms = parse_positive_env("FELIX_READINESS_CACHE_TTL_MS")
            .unwrap_or(DEFAULT_READINESS_CACHE_TTL_MS);
        let shutdown_drain_timeout_ms = std::env::var("FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS);
        // Unlike the drain timeout, zero is a meaningful setting here rather
        // than an unset one, so it is not filtered out.
        let shutdown_predrain_ms = std::env::var("FELIX_SHUTDOWN_PREDRAIN_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SHUTDOWN_PREDRAIN_MS);
        let change_retention_max_rows =
            std::env::var("FELIX_CONTROLPLANE_CHANGE_RETENTION_MAX_ROWS")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .or(Some(DEFAULT_CHANGE_RETENTION_MAX_ROWS));

        let pg_url = std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .ok();
        let mut postgres = None;
        if let Some(url) = pg_url {
            postgres = Some(PostgresConfig {
                url,
                max_connections: std::env::var("FELIX_CONTROLPLANE_POSTGRES_MAX_CONNECTIONS")
                    .ok()
                    .and_then(|v| v.parse::<u32>().ok())
                    .unwrap_or(DEFAULT_PG_MAX_CONNECTIONS),
                connect_timeout_ms: std::env::var("FELIX_CONTROLPLANE_POSTGRES_CONNECT_TIMEOUT_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_PG_CONNECT_TIMEOUT_MS),
                acquire_timeout_ms: std::env::var("FELIX_CONTROLPLANE_POSTGRES_ACQUIRE_TIMEOUT_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_PG_ACQUIRE_TIMEOUT_MS),
            });
            if matches!(storage, StorageBackend::Memory) {
                storage = StorageBackend::Postgres;
            }
        }

        let raft = raft_from_env()?;
        if raft.is_some() && matches!(storage, StorageBackend::Memory) {
            storage = StorageBackend::Raft;
        }

        let config = Self {
            bind_addr,
            metrics_bind,
            region_id,
            storage,
            postgres,
            raft,
            changes_limit,
            change_retention_max_rows,
            oidc_allowed_algorithms: std::env::var("FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS")
                .ok()
                .map(|value| parse_oidc_allowed_algorithms_csv(&value))
                .transpose()?
                .unwrap_or_else(|| DEFAULT_OIDC_ALLOWED_ALGORITHMS.to_vec()),
            bootstrap: BootstrapConfig {
                enabled: std::env::var("FELIX_BOOTSTRAP_ENABLED")
                    .ok()
                    .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
                    .unwrap_or(false),
                bind_addr: std::env::var("FELIX_BOOTSTRAP_BIND_ADDR")
                    .unwrap_or_else(|_| DEFAULT_BOOTSTRAP_BIND_ADDR.to_string())
                    .parse()
                    .with_context(|| "parse FELIX_BOOTSTRAP_BIND_ADDR")?,
                token: std::env::var("FELIX_BOOTSTRAP_TOKEN").ok(),
                previous_token: std::env::var("FELIX_BOOTSTRAP_TOKEN_PREVIOUS").ok(),
                tls: bootstrap_tls_from_env()?,
            },
            node_liveness,
            max_concurrent_shard_moves,
            shutdown_drain_timeout_ms,
            shutdown_predrain_ms,
            readiness_timeout_ms,
            readiness_cache_ttl_ms,
        };
        config.validate()?;
        Ok(config)
    }

    /// Fold a parsed config file over the values already taken from the
    /// environment.
    ///
    /// Separate from the read so the precedence rules — including a postgres
    /// block selecting the backend — are testable without a file on disk.
    fn apply(&mut self, override_cfg: ControlPlaneConfigOverride) -> Result<()> {
        let config = self;

        if let Some(value) = override_cfg.bind_addr {
            config.bind_addr = value.parse().with_context(|| "parse bind_addr")?;
        }
        if let Some(value) = override_cfg.metrics_bind {
            config.metrics_bind = value.parse().with_context(|| "parse metrics_bind")?;
        }
        if let Some(value) = override_cfg.region_id {
            config.region_id = value;
        }
        if let Some(value) = override_cfg.changes_limit {
            config.changes_limit = value;
        }
        if let Some(value) = override_cfg.change_retention_max_rows {
            config.change_retention_max_rows = Some(value);
        }
        if let Some(liveness) = override_cfg.node_liveness {
            if let Some(value) = liveness.heartbeat_interval_ms {
                config.node_liveness.heartbeat_interval_ms = value;
            }
            if let Some(value) = liveness.expiry_timeout_ms {
                config.node_liveness.expiry_timeout_ms = value;
            }
            if let Some(value) = liveness.sweep_interval_ms {
                config.node_liveness.sweep_interval_ms = value;
            }
            if let Some(value) = liveness.shard_reconcile_interval_ms {
                config.node_liveness.shard_reconcile_interval_ms = value;
            }
        }
        if let Some(value) = override_cfg.max_concurrent_shard_moves {
            config.max_concurrent_shard_moves = value;
        }
        if let Some(value) = override_cfg.shutdown_drain_timeout_ms
            && value > 0
        {
            config.shutdown_drain_timeout_ms = value;
        }
        if let Some(value) = override_cfg.shutdown_predrain_ms {
            config.shutdown_predrain_ms = value;
        }
        if let Some(values) = override_cfg.oidc_allowed_algorithms {
            config.oidc_allowed_algorithms = parse_oidc_allowed_algorithms(values)?;
        }
        if let Some(storage_override) = override_cfg.storage
            && let Some(backend) = storage_override.backend
        {
            config.storage = StorageBackend::from_str(&backend)?;
        }
        if let Some(pg_override) = override_cfg.postgres {
            let mut pg_cfg = config.postgres.take().unwrap_or_default();
            if let Some(url) = pg_override.url {
                pg_cfg.url = url;
            }
            if let Some(max) = pg_override.max_connections {
                pg_cfg.max_connections = max;
            }
            if let Some(timeout) = pg_override.connect_timeout_ms {
                pg_cfg.connect_timeout_ms = timeout;
            }
            if let Some(timeout) = pg_override.acquire_timeout_ms {
                pg_cfg.acquire_timeout_ms = timeout;
            }
            config.postgres = Some(pg_cfg);
            if matches!(config.storage, StorageBackend::Memory) {
                config.storage = StorageBackend::Postgres;
            }
        }
        if let Some(bootstrap_override) = override_cfg.bootstrap {
            if let Some(enabled) = bootstrap_override.enabled {
                config.bootstrap.enabled = enabled;
            }
            if let Some(value) = bootstrap_override.bind_addr {
                config.bootstrap.bind_addr = value.parse().with_context(|| "parse bind_addr")?;
            }
            if let Some(token) = bootstrap_override.token {
                config.bootstrap.token = Some(token);
            }
            if let Some(token) = bootstrap_override.previous_token {
                config.bootstrap.previous_token = Some(token);
            }
            if let Some(tls) = bootstrap_override.tls {
                config.bootstrap.tls = Some(tls);
            }
        }
        Ok(())
    }

    pub fn from_env_or_yaml() -> Result<Self> {
        let mut config = Self::from_env()?;
        if let Ok(path) = std::env::var("FELIX_CONTROLPLANE_CONFIG") {
            let contents = fs::read_to_string(&path)
                .with_context(|| format!("read FELIX_CONTROLPLANE_CONFIG: {path}"))?;
            let override_cfg: ControlPlaneConfigOverride = serde_yaml_ng::from_str(&contents)
                .with_context(|| "parse control plane config yaml")?;
            config.apply(override_cfg)?;
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if matches!(self.storage, StorageBackend::Postgres) && self.postgres.is_none() {
            return Err(anyhow!(
                "postgres backend requested but FELIX_CONTROLPLANE_POSTGRES_URL / postgres.url is not set"
            ));
        }
        if matches!(self.storage, StorageBackend::Raft) {
            let raft = self.raft.as_ref().ok_or_else(|| {
                anyhow!(
                    "raft backend requested but FELIX_RAFT_NODE_ID / FELIX_RAFT_DATA_DIR / FELIX_RAFT_PEERS are not set"
                )
            })?;
            if !raft.peers.contains_key(&raft.node_id) {
                return Err(anyhow!(
                    "FELIX_RAFT_PEERS must include this instance's own FELIX_RAFT_NODE_ID ({})",
                    raft.node_id
                ));
            }
            raft.validate()?;
        }
        if self.bootstrap.enabled && self.bootstrap.token.is_none() {
            return Err(anyhow!(
                "bootstrap enabled but FELIX_BOOTSTRAP_TOKEN / bootstrap.token is not set"
            ));
        }
        // A previous token with no current one means the rotation removed the
        // wrong half; refusing beats quietly running on the token being retired.
        if self.bootstrap.previous_token.is_some() && self.bootstrap.token.is_none() {
            return Err(anyhow!(
                "bootstrap.previous_token is set without bootstrap.token; \
                 the rotation should replace token and demote the old one"
            ));
        }
        if self.oidc_allowed_algorithms.is_empty() {
            return Err(anyhow!(
                "oidc_allowed_algorithms cannot be empty; include at least ES256"
            ));
        }
        self.node_liveness.validate()?;
        Ok(())
    }
}

/// Bootstrap mTLS from the environment: all three variables or none.
///
/// A partial set is an error rather than "TLS off", because an operator who set
/// two of the three believed the listener was secured.
fn bootstrap_tls_from_env() -> Result<Option<BootstrapTlsConfig>> {
    let cert = std::env::var("FELIX_BOOTSTRAP_TLS_CERT").ok();
    let key = std::env::var("FELIX_BOOTSTRAP_TLS_KEY").ok();
    let client_ca = std::env::var("FELIX_BOOTSTRAP_TLS_CLIENT_CA").ok();
    match (cert, key, client_ca) {
        (None, None, None) => Ok(None),
        (Some(cert_path), Some(key_path), Some(client_ca_path)) => Ok(Some(BootstrapTlsConfig {
            cert_path,
            key_path,
            client_ca_path,
        })),
        _ => Err(anyhow!(
            "bootstrap TLS needs all of FELIX_BOOTSTRAP_TLS_CERT, \
             FELIX_BOOTSTRAP_TLS_KEY, and FELIX_BOOTSTRAP_TLS_CLIENT_CA"
        )),
    }
}

/// The Raft backend from the environment: all three variables or none.
///
/// Partial configuration is an error for the same reason as bootstrap TLS:
/// an operator who set two of the three believed they configured a group.
fn raft_from_env() -> Result<Option<RaftBackendConfig>> {
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

/// Read a positive integer from the environment, ignoring absent, unparsable,
/// and zero values so a typo falls back to the default rather than disabling a
/// timer.
fn parse_positive_env(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

fn parse_oidc_allowed_algorithms_csv(value: &str) -> Result<Vec<Algorithm>> {
    parse_oidc_allowed_algorithms(
        value
            .split(',')
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(ToString::to_string)
            .collect(),
    )
}

fn parse_oidc_allowed_algorithms(values: Vec<String>) -> Result<Vec<Algorithm>> {
    let mut algorithms = Vec::new();
    for raw in values {
        let alg = parse_oidc_algorithm(&raw).ok_or_else(|| {
            anyhow!(
                "invalid OIDC algorithm '{}'; supported: ES256, RS256, RS384, RS512, PS256, PS384, PS512",
                raw
            )
        })?;
        if !algorithms.contains(&alg) {
            algorithms.push(alg);
        }
    }
    Ok(algorithms)
}

fn parse_oidc_algorithm(value: &str) -> Option<Algorithm> {
    match value.trim().to_ascii_uppercase().as_str() {
        "ES256" => Some(Algorithm::ES256),
        "RS256" => Some(Algorithm::RS256),
        "RS384" => Some(Algorithm::RS384),
        "RS512" => Some(Algorithm::RS512),
        "PS256" => Some(Algorithm::PS256),
        "PS384" => Some(Algorithm::PS384),
        "PS512" => Some(Algorithm::PS512),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
