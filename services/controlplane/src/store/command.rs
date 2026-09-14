//! The metadata command set: every mutation of the store, as data.
//!
//! One variant per mutating store-trait method, shaped like the API call
//! rather than the rows it touches — that is what makes multi-step
//! operations (a tenant bootstrap, a cascading delete) atomic under Raft for
//! free: the log totally orders whole operations, so there is no interleaved
//! half to protect against.
//!
//! Everything nondeterministic is **in** the command, decided before it is
//! proposed: heartbeat and expiry carry their timestamps, bootstrap carries
//! its candidate signing keys. Applying a command reads nothing but the
//! command and prior state, which is the property the determinism harness in
//! `state_machine_tests.rs` enforces.
//!
//! The wire form is a versioned JSON envelope. A follower that does not
//! understand a command must fail loudly rather than misparse it: skipping a
//! committed command it cannot read would silently fork its state from the
//! leader's.
use serde::{Deserialize, Serialize};

use crate::auth::felix_token::TenantSigningKeys;
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::{
    Cache, CacheKey, CachePatchRequest, Namespace, NamespaceKey, Node, NodeLifecycle,
    NodePatchRequest, ShardAssignment, ShardKey, Stream, StreamKey, StreamPatchRequest, Tenant,
};
use crate::store::{StoreError, TenantAuthSeed};

/// The newest envelope version this build can apply.
///
/// Bump only when an existing variant's meaning changes; *adding* a variant
/// is not a version bump, because an old follower rejects the unknown `op`
/// on deserialization, which is the failure we want.
pub const COMMAND_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MetaCommand {
    CreateTenant {
        tenant: Tenant,
    },
    DeleteTenant {
        tenant_id: String,
    },
    CreateNamespace {
        namespace: Namespace,
    },
    DeleteNamespace {
        key: NamespaceKey,
    },
    CreateStream {
        stream: Stream,
    },
    PatchStream {
        key: StreamKey,
        patch: StreamPatchRequest,
    },
    DeleteStream {
        key: StreamKey,
    },
    CreateCache {
        cache: Cache,
    },
    PatchCache {
        key: CacheKey,
        patch: CachePatchRequest,
    },
    DeleteCache {
        key: CacheKey,
    },
    RegisterNode {
        node: Node,
    },
    PatchNode {
        node_id: String,
        patch: NodePatchRequest,
    },
    DeleteNode {
        node_id: String,
    },
    /// `at_millis` is the proposer's clock — the leader's, under Raft —
    /// preserving the rule that a broker cannot supply its own liveness time.
    RecordNodeHeartbeat {
        node_id: String,
        incarnation: u64,
        at_millis: u64,
    },
    /// The cutoff is computed by the proposer; every replica expires exactly
    /// the same set, in sorted order, whatever its own clock says.
    ExpireStaleNodes {
        expiry_before_millis: u64,
    },
    SetNodeLifecycle {
        node_id: String,
        lifecycle: NodeLifecycle,
    },
    PutShardAssignment {
        assignment: ShardAssignment,
    },
    DeleteShardAssignment {
        key: ShardKey,
    },
    UpsertIdpIssuer {
        tenant_id: String,
        issuer: IdpIssuerConfig,
    },
    DeleteIdpIssuer {
        tenant_id: String,
        issuer: String,
    },
    AddRbacPolicy {
        tenant_id: String,
        policy: PolicyRule,
    },
    AddRbacGrouping {
        tenant_id: String,
        grouping: GroupingRule,
    },
    SetTenantSigningKeys {
        tenant_id: String,
        keys: TenantSigningKeys,
    },
    /// Install `candidate` only if the tenant has no keys; existing keys win.
    ///
    /// Distinct from `SetTenantSigningKeys` (an unconditional rotation)
    /// because the proposer decides from a possibly-stale local read: an
    /// overwrite proposed off stale emptiness would clobber a rotation that
    /// committed in between. Install-if-absent makes the race harmless.
    EnsureSigningKeys {
        tenant_id: String,
        candidate: TenantSigningKeys,
    },
    SetTenantAuthBootstrapped {
        tenant_id: String,
        bootstrapped: bool,
    },
    SeedRbac {
        tenant_id: String,
        policies: Vec<PolicyRule>,
        groupings: Vec<GroupingRule>,
    },
    /// The whole day-0 seed as one command; the seed carries the candidate
    /// signing keys, so exactly-once needs nothing beyond log order.
    BootstrapTenantAuth {
        tenant_id: String,
        seed: TenantAuthSeed,
    },
    /// Replace the entire store with an exported state — the migration
    /// cutover from Postgres, and the beyond-quorum-loss restore.
    ///
    /// One command, so the whole import is one log entry applied atomically
    /// on every member; sequence high-water marks inside the state are what
    /// let broker watches resume with at most one resnapshot. Refused unless
    /// the store has never held anything, or `overwrite` says the operator
    /// really means to discard what is there.
    ImportState {
        // Boxed: this one variant is as big as the whole store, and every
        // command would otherwise pay its size.
        state: Box<crate::store::memory::ExportedState>,
        overwrite: bool,
    },
}

/// What a command returns, mirroring the store method it stands for.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetaResponse {
    Unit,
    Tenant {
        tenant: Tenant,
    },
    Namespace {
        namespace: Namespace,
    },
    Stream {
        stream: Stream,
    },
    Cache {
        cache: Cache,
    },
    Node {
        node: Node,
    },
    Nodes {
        nodes: Vec<Node>,
    },
    /// `set_node_lifecycle`'s "already there" answer must survive the trip.
    MaybeNode {
        node: Option<Node>,
    },
    Assignment {
        assignment: ShardAssignment,
    },
    SigningKeys {
        keys: TenantSigningKeys,
    },
}

/// `StoreError`, flattened into something that serializes and compares.
///
/// `Unexpected` loses its structure deliberately: an anyhow chain is for
/// humans, and by the time an error crosses the log it is a string either way.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "error", content = "detail", rename_all = "snake_case")]
pub enum MetaError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("{0}")]
    Internal(String),
    /// The command could not be decoded — a version or variant this build
    /// does not know. Fatal to serve from, never silently skipped.
    #[error("unsupported command: {0}")]
    Unsupported(String),
}

impl From<StoreError> for MetaError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::NotFound(what) => MetaError::NotFound(what),
            StoreError::Conflict(what) => MetaError::Conflict(what),
            StoreError::Unexpected(err) => MetaError::Internal(format!("{err:#}")),
        }
    }
}

impl From<MetaError> for StoreError {
    fn from(err: MetaError) -> Self {
        match err {
            MetaError::NotFound(what) => StoreError::NotFound(what),
            MetaError::Conflict(what) => StoreError::Conflict(what),
            MetaError::Internal(msg) | MetaError::Unsupported(msg) => {
                StoreError::Unexpected(anyhow::anyhow!(msg))
            }
        }
    }
}

pub type MetaResult = Result<MetaResponse, MetaError>;

#[derive(Serialize, Deserialize)]
struct Envelope {
    v: u16,
    #[serde(flatten)]
    command: MetaCommand,
}

/// Serialize a command for proposal.
pub fn encode_command(command: &MetaCommand) -> Vec<u8> {
    serde_json::to_vec(&Envelope {
        v: COMMAND_VERSION,
        command: command.clone(),
    })
    .expect("commands serialize by construction")
}

/// Decode a committed command. An unreadable command is an error *response*,
/// not a skip: every replica answers it identically, and the caller sees
/// exactly what the log holds that this build cannot honour.
pub fn decode_command(bytes: &[u8]) -> Result<MetaCommand, MetaError> {
    let envelope: Envelope = serde_json::from_slice(bytes)
        .map_err(|err| MetaError::Unsupported(format!("undecodable command: {err}")))?;
    if envelope.v > COMMAND_VERSION {
        return Err(MetaError::Unsupported(format!(
            "command version {} is newer than this build's {}",
            envelope.v, COMMAND_VERSION
        )));
    }
    Ok(envelope.command)
}

/// Serialize a command's outcome for the Raft response channel.
pub fn encode_result(result: &MetaResult) -> Vec<u8> {
    serde_json::to_vec(result).expect("results serialize by construction")
}

pub fn decode_result(bytes: &[u8]) -> Result<MetaResult, MetaError> {
    serde_json::from_slice(bytes)
        .map_err(|err| MetaError::Internal(format!("undecodable result: {err}")))
}
