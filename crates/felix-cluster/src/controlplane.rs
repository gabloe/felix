//! The control plane the harness runs.
//!
//! In process, on a real TCP port, so the brokers it starts reach it over HTTP
//! exactly as they would in a deployment. It is in process for one reason: a
//! broker needs a credential, and the only way to obtain one today is an OIDC
//! token exchange against a real identity provider. Holding the store here lets
//! the harness mint node and client tokens directly with the tenant's signing
//! keys — the same thing `services/broker/tests/membership_lifecycle.rs` does,
//! and the same reason.
//!
//! The consequence is worth stating plainly: **the control plane is not under
//! test as a process.** Its router, store, placement, and HTTP contract all are.
//! What is not exercised is its `main`, its own configuration, and its shutdown.
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_router};
use controlplane::auth::felix_token::TenantSigningKeys;
use controlplane::config::NodeLivenessConfig;
use controlplane::store::memory::InMemoryStore;
use controlplane::store::{AuthStore, StoreConfig};
use tokio_util::sync::CancellationToken;

use crate::ports;

/// Liveness tuned for a harness: brokers are local, so a lapsed heartbeat means
/// a broker that actually stopped rather than a slow network. Short windows make
/// a stopped broker observable in seconds instead of tens of seconds, which is
/// what failure tests wait on.
const LIVENESS: NodeLivenessConfig = NodeLivenessConfig {
    heartbeat_interval_ms: 200,
    expiry_timeout_ms: 1_000,
    sweep_interval_ms: 100,
    // Placement is driven explicitly by the harness, so this only matters as a
    // backstop for anything the harness does not step itself.
    shard_reconcile_interval_ms: 500,
};

/// A running control plane, and the keys to mint credentials against it.
pub struct ControlPlane {
    pub base_url: String,
    pub store: Arc<InMemoryStore>,
    keys: TenantSigningKeys,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl ControlPlane {
    /// Start the control plane, holding the signing keys it will issue against.
    pub async fn start(tenant_id: &str) -> Result<Self> {
        let store = Arc::new(InMemoryStore::new(StoreConfig {
            changes_limit: controlplane::config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: Some(10_000),
        }));
        let keys = controlplane::auth::keys::generate_signing_keys()
            .context("generate tenant signing keys")?;
        store
            .set_tenant_signing_keys(tenant_id, keys.clone())
            .await
            .context("seed tenant signing keys")?;

        let state = AppState {
            region: Region {
                region_id: "local".to_string(),
                display_name: "Local".to_string(),
            },
            api_version: "v1".to_string(),
            features: FeatureFlags {
                durable_storage: false,
                tiered_storage: false,
                bridges: false,
            },
            store: Arc::clone(&store)
                as Arc<dyn controlplane::store::ControlPlaneAuthStore + Send + Sync>,
            oidc_validator: controlplane::auth::oidc::UpstreamOidcValidator::default(),
            bootstrap_enabled: false,
            bootstrap_token: None,
            node_liveness: LIVENESS,
        };

        let addr = ports::free_tcp()?;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("bind control plane on {addr}"))?;
        let addr = listener
            .local_addr()
            .context("read control plane address")?;
        let shutdown = CancellationToken::new();
        let serve_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state).into_make_service())
                .with_graceful_shutdown(async move { serve_shutdown.cancelled().await })
                .await;
        });

        // Expiry has to run, or a stopped broker stays `live` forever and no
        // failure test can observe it leaving.
        let expiry = controlplane::membership::spawn_expiry_sweep(
            Arc::clone(&store) as Arc<dyn controlplane::store::ControlPlaneStore + Send + Sync>,
            LIVENESS,
            shutdown.clone(),
        );
        // Owned by the same token; nothing waits on it separately.
        drop(expiry);

        Ok(Self {
            base_url: format!("http://{addr}"),
            store,
            keys,
            shutdown,
            task,
        })
    }

    /// Associate this control plane's signing keys with `tenant_id`.
    ///
    /// Called again after the tenant is created through the API: keys set for a
    /// tenant that does not exist yet do not survive its creation, and every
    /// token minted here verifies against whatever the store holds at the time
    /// the request arrives.
    pub async fn seed_tenant_keys(&self, tenant_id: &str) -> Result<()> {
        self.store
            .set_tenant_signing_keys(tenant_id, self.keys.clone())
            .await
            .context("seed tenant signing keys")
    }

    /// A credential for one broker: manage its own membership, and read the
    /// cluster's assignments and node catalog.
    ///
    /// Scoped to `node:{node_id}` on purpose, which is what a real deployment
    /// would hand it — a broker presenting this for another node is refused.
    pub fn node_token(&self, tenant_id: &str, node_id: &str) -> Result<String> {
        controlplane::auth::felix_token::mint_token(
            &self.keys,
            tenant_id,
            &format!("p:{node_id}"),
            vec![
                format!("node.manage:node:{node_id}"),
                // Assignments and `/v1/nodes` both require this; without it a
                // broker cannot learn who owns what, or where they are.
                "node.view:cluster:*".to_string(),
            ],
            Duration::from_secs(3600),
        )
        .context("mint node token")
    }

    /// A credential for a client publishing and subscribing in `tenant_id`.
    ///
    /// Stream permissions only. A broker validates every action in a token it is
    /// presented, and rejects the whole token if one is not a client-facing
    /// action — so a cluster-scoped permission here would make this credential
    /// unusable for its actual purpose.
    pub fn client_token(&self, tenant_id: &str) -> Result<String> {
        controlplane::auth::felix_token::mint_token(
            &self.keys,
            tenant_id,
            "p:harness-client",
            vec![
                format!("stream.publish:stream:{tenant_id}/*/*"),
                format!("stream.subscribe:stream:{tenant_id}/*/*"),
            ],
            Duration::from_secs(3600),
        )
        .context("mint client token")
    }

    /// A credential for the harness itself, against the control plane's HTTP
    /// API: create metadata, and read membership and ownership.
    ///
    /// Separate from [`Self::client_token`] because the two are presented to
    /// different services, which accept different actions.
    pub fn admin_token(&self, tenant_id: &str) -> Result<String> {
        controlplane::auth::felix_token::mint_token(
            &self.keys,
            tenant_id,
            "p:harness-admin",
            vec![
                format!("tenant.manage:tenant:{tenant_id}"),
                format!("ns.manage:namespace:{tenant_id}/*"),
                format!("stream.manage:stream:{tenant_id}/*/*"),
                "node.view:cluster:*".to_string(),
            ],
            Duration::from_secs(3600),
        )
        .context("mint admin token")
    }

    /// Place any unassigned shard onto a live broker.
    ///
    /// Driven explicitly rather than waited for: the reconciler runs on a timer,
    /// and a harness that slept for one would be timing-dependent in exactly the
    /// way the acceptance criteria rule out.
    pub async fn place_shards(&self) -> controlplane::placement::ReconcileOutcome {
        controlplane::placement::reconcile_once(self.store.as_ref()).await
    }

    pub async fn shutdown(self) {
        self.shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), self.task).await;
    }
}
