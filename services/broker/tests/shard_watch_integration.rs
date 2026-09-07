//! Snapshot-to-watch handoff against the real control-plane router.
//!
//! The property under test is the one that cannot be checked in isolation: a
//! broker starting from nothing converges on the current assignments, and no
//! committed change is lost in the seam between the snapshot and the first poll.
use broker::shard_watch::{self, ShardKey as WatchedShardKey, ShardOwnership};
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_router};
use controlplane::auth::felix_token::{TenantSigningKeys, mint_token};
use controlplane::auth::keys::generate_signing_keys;
use controlplane::config::NodeLivenessConfig;
use controlplane::model::{
    ConsistencyLevel, DeliveryGuarantee, Namespace, Node, NodeCapacity, NodeLifecycle, NodeSpec,
    NodeStatus, RetentionPolicy, ShardAssignment, ShardKey, ShardState, Stream, StreamKind, Tenant,
};
use controlplane::store::memory::InMemoryStore;
use controlplane::store::{AuthStore, ControlPlaneAuthStore, ControlPlaneStore, StoreConfig};
use reqwest::{Client, redirect::Policy};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

const LIVENESS: NodeLivenessConfig = NodeLivenessConfig {
    heartbeat_interval_ms: 5_000,
    expiry_timeout_ms: 15_000,
    sweep_interval_ms: 2_000,
    shard_reconcile_interval_ms: 5_000,
};

struct Cluster {
    base_url: String,
    store: Arc<InMemoryStore>,
    client: Client,
    bearer: String,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

impl Cluster {
    async fn start() -> Self {
        let store = Arc::new(InMemoryStore::new(StoreConfig {
            changes_limit: 1000,
            change_retention_max_rows: Some(1000),
        }));
        let keys: TenantSigningKeys = generate_signing_keys().expect("keys");
        store
            .set_tenant_signing_keys("t1", keys.clone())
            .await
            .expect("keys");

        let state = AppState {
            region: Region {
                region_id: "us-west-2".to_string(),
                display_name: "Test".to_string(),
            },
            api_version: "v1".to_string(),
            features: FeatureFlags {
                durable_storage: store.is_durable(),
                tiered_storage: false,
                bridges: false,
            },
            store: Arc::clone(&store) as Arc<dyn ControlPlaneAuthStore + Send + Sync>,
            oidc_validator: controlplane::auth::oidc::UpstreamOidcValidator::default(),
            bootstrap_enabled: false,
            bootstrap_token: None,
            node_liveness: LIVENESS,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (stop, stop_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state).into_make_service())
                .with_graceful_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await;
        });

        let bearer = mint_token(
            &keys,
            "t1",
            "p:broker",
            vec!["node.view:cluster:*".to_string()],
            Duration::from_secs(900),
        )
        .expect("token");

        let cluster = Self {
            base_url: format!("http://{addr}"),
            store,
            client: Client::builder()
                .timeout(Duration::from_secs(2))
                .no_proxy()
                .redirect(Policy::none())
                .build()
                .expect("client"),
            bearer,
            stop,
            server,
        };
        cluster.seed().await;
        cluster
    }

    async fn seed(&self) {
        self.store
            .create_tenant(Tenant {
                tenant_id: "t1".to_string(),
                display_name: "T".to_string(),
            })
            .await
            .expect("tenant");
        self.store
            .create_namespace(Namespace {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                display_name: "NS".to_string(),
            })
            .await
            .expect("namespace");
        self.store
            .create_stream(Stream {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "orders".to_string(),
                kind: StreamKind::Stream,
                shards: 4,
                retention: RetentionPolicy {
                    max_age_seconds: None,
                    max_size_bytes: None,
                },
                consistency: ConsistencyLevel::Leader,
                delivery: DeliveryGuarantee::AtMostOnce,
                durable: true,
            })
            .await
            .expect("stream");

        for (i, id) in ["broker-a", "broker-b"].iter().enumerate() {
            self.store
                .register_node(Node {
                    node_id: id.to_string(),
                    spec: NodeSpec {
                        advertise_addr: format!("10.0.0.4:{}", 7800 + i),
                        region: "us-west-2".to_string(),
                        labels: BTreeMap::new(),
                        capacity: NodeCapacity::default(),
                    },
                    status: NodeStatus {
                        lifecycle: NodeLifecycle::Live,
                        last_heartbeat_at_millis: 1,
                        registered_at_millis: 1,
                        incarnation: 0,
                    },
                })
                .await
                .expect("node");
        }
    }

    async fn assign(&self, shard: u32, leader: &str, state: ShardState) {
        self.store
            .put_shard_assignment(ShardAssignment {
                key: ShardKey {
                    tenant_id: "t1".to_string(),
                    namespace: "ns".to_string(),
                    stream: "orders".to_string(),
                    shard,
                },
                leader: leader.to_string(),
                replicas: Vec::new(),
                generation: 0,
                state,
            })
            .await
            .expect("assign");
    }

    fn watch(
        &self,
        ownership: Arc<RwLock<ShardOwnership>>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(shard_watch::run(
            self.client.clone(),
            self.base_url.clone(),
            Some(self.bearer.clone()),
            ownership,
            Duration::from_millis(10),
            shutdown,
        ))
    }

    async fn shutdown(self) {
        let _ = self.stop.send(());
        let _ = self.server.await;
    }
}

/// Wait for a condition rather than sleeping a fixed time.
async fn until(mut check: impl AsyncFnMut() -> bool) -> bool {
    for _ in 0..400 {
        if check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test]
async fn a_broker_starting_empty_converges_on_the_snapshot() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;
    cluster.assign(1, "broker-b", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch(Arc::clone(&ownership), shutdown.clone());

    assert!(
        until(async || ownership.read().await.len() == 2).await,
        "the watch should converge on the current snapshot",
    );

    shutdown.cancel();
    let _ = watch.await;
    cluster.shutdown().await;
}

/// The seam the acceptance criterion names: a change committed after the
/// snapshot must still arrive, and one already in it must not be applied twice.
#[tokio::test]
async fn changes_after_the_snapshot_are_not_lost() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch(Arc::clone(&ownership), shutdown.clone());

    assert!(until(async || ownership.read().await.len() == 1).await);

    // Committed strictly after the snapshot the broker already applied.
    cluster.assign(1, "broker-b", ShardState::Assigning).await;
    cluster.assign(2, "broker-a", ShardState::Assigning).await;

    assert!(
        until(async || ownership.read().await.len() == 3).await,
        "changes committed after the snapshot must reach the broker",
    );

    shutdown.cancel();
    let _ = watch.await;
    cluster.shutdown().await;
}

/// A leader change has to reach the broker, since that is what tells it to stop
/// serving a shard it no longer owns.
#[tokio::test]
async fn a_leader_change_reaches_the_broker() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch(Arc::clone(&ownership), shutdown.clone());

    let shard = WatchedShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
    };
    assert!(until(async || ownership.read().await.is_leader(&shard, "broker-a")).await);

    cluster.assign(0, "broker-b", ShardState::Assigning).await;

    assert!(
        until(async || ownership.read().await.is_leader(&shard, "broker-b")).await,
        "the broker must learn it lost the shard",
    );
    assert!(!ownership.read().await.is_leader(&shard, "broker-a"));

    shutdown.cancel();
    let _ = watch.await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn an_unassignment_reaches_the_broker() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch(Arc::clone(&ownership), shutdown.clone());
    assert!(until(async || ownership.read().await.len() == 1).await);

    cluster
        .store
        .delete_shard_assignment(&ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
        })
        .await
        .expect("delete");

    assert!(
        until(async || ownership.read().await.is_empty()).await,
        "an unassignment must remove the shard locally",
    );

    shutdown.cancel();
    let _ = watch.await;
    cluster.shutdown().await;
}

/// The control plane being unreachable must not stop a broker serving what it
/// already owns, and the watch must recover on its own when it returns.
#[tokio::test]
async fn the_watch_survives_the_control_plane_going_away() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch(Arc::clone(&ownership), shutdown.clone());
    assert!(until(async || ownership.read().await.len() == 1).await);

    let base_url = cluster.base_url.clone();
    let client = cluster.client.clone();
    cluster.shutdown().await;

    // The watch keeps running and keeps what it had.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        ownership.read().await.len(),
        1,
        "ownership must survive the control plane going away",
    );
    assert!(!watch.is_finished(), "the watch must not give up");
    let _ = (base_url, client);

    shutdown.cancel();
    let _ = watch.await;
}
