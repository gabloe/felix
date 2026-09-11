//! Authorization on the membership write path.
//!
//! Registration, heartbeat, drain, and deregistration were reachable by any
//! caller. These are the tests that say they are not, one per endpoint and one
//! per way of getting it wrong.
use axum::body::Body;
use axum::http::{Request, StatusCode};
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_router};
use controlplane::auth::felix_token::{TenantSigningKeys, mint_token};
use controlplane::auth::keys::generate_signing_keys;
use controlplane::config::NodeLivenessConfig;
use controlplane::model::{Node, NodeCapacity, NodeLifecycle, NodeSpec, NodeStatus};
use controlplane::store::memory::InMemoryStore;
use controlplane::store::{AuthStore, ControlPlaneAuthStore, ControlPlaneStore, StoreConfig};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

const LIVENESS: NodeLivenessConfig = NodeLivenessConfig {
    heartbeat_interval_ms: 5_000,
    expiry_timeout_ms: 15_000,
    sweep_interval_ms: 2_000,
    shard_reconcile_interval_ms: 5_000,
};

type App = axum::routing::RouterIntoService<Body, ()>;

async fn setup() -> (App, Arc<InMemoryStore>, TenantSigningKeys) {
    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 1000,
        change_retention_max_rows: Some(1000),
    }));
    let keys = generate_signing_keys().expect("keys");
    store
        .set_tenant_signing_keys("t1", keys.clone())
        .await
        .expect("keys");

    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
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
        replica_positions: std::sync::Arc::new(
            controlplane::replica_positions::ReplicaPositions::new(&LIVENESS),
        ),
    };
    (build_router(state).into_service(), store, keys)
}

fn token(keys: &TenantSigningKeys, perms: Vec<&str>) -> String {
    mint_token(
        keys,
        "t1",
        "p:broker",
        perms.into_iter().map(str::to_string).collect(),
        Duration::from_secs(900),
    )
    .expect("token")
}

fn post(path: &str, bearer: Option<&str>, body: serde_json::Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(bearer) = bearer {
        builder = builder.header("authorization", format!("Bearer {bearer}"));
    }
    builder.body(Body::from(body.to_string())).expect("request")
}

fn registration(node_id: &str, port: u16) -> serde_json::Value {
    serde_json::json!({
        "node_id": node_id,
        "advertise_addr": format!("10.0.0.4:{port}"),
        "region": "us-west-2",
    })
}

async fn seed_node(store: &InMemoryStore, node_id: &str, port: u16) {
    store
        .register_node(Node {
            node_id: node_id.to_string(),
            spec: NodeSpec {
                advertise_addr: format!("10.0.0.4:{port}"),
                client_addr: None,
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
        .expect("seed");
}

/// Every write endpoint, with no credential at all. This is the hole being
/// closed, and there is one case per endpoint so a regression names itself.
#[tokio::test]
async fn every_membership_write_requires_a_token() {
    let (app, store, _keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;

    for (path, body) in [
        ("/v1/nodes", registration("broker-a", 7001)),
        (
            "/v1/nodes/broker-a/heartbeat",
            serde_json::json!({"incarnation": 0}),
        ),
        ("/v1/nodes/broker-a/drain", serde_json::json!({})),
        ("/v1/nodes/broker-a/deregister", serde_json::json!({})),
    ] {
        let response = app
            .clone()
            .oneshot(post(path, None, body))
            .await
            .expect("request");
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{path} must not be reachable without a token",
        );
    }
}

/// The property the node-scoped object exists for: broker-a's own credential
/// cannot be used to speak for broker-b.
#[tokio::test]
async fn a_broker_cannot_act_for_another_broker() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;
    seed_node(&store, "broker-b", 7002).await;
    let bearer = token(&keys, vec!["node.manage:node:broker-a"]);

    for (path, body) in [
        ("/v1/nodes", registration("broker-b", 7002)),
        (
            "/v1/nodes/broker-b/heartbeat",
            serde_json::json!({"incarnation": 0}),
        ),
        ("/v1/nodes/broker-b/drain", serde_json::json!({})),
        ("/v1/nodes/broker-b/deregister", serde_json::json!({})),
    ] {
        let response = app
            .clone()
            .oneshot(post(path, Some(&bearer), body))
            .await
            .expect("request");
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{path} must reject a credential scoped to a different node",
        );
    }

    // And broker-b is untouched by the attempt.
    assert_eq!(
        store
            .get_node("broker-b")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Live,
    );
}

#[tokio::test]
async fn a_broker_can_manage_itself() {
    let (app, store, keys) = setup().await;
    let bearer = token(&keys, vec!["node.manage:node:broker-a"]);

    let registered = app
        .clone()
        .oneshot(post(
            "/v1/nodes",
            Some(&bearer),
            registration("broker-a", 7001),
        ))
        .await
        .expect("request");
    assert_eq!(registered.status(), StatusCode::OK);

    for path in [
        "/v1/nodes/broker-a/heartbeat",
        "/v1/nodes/broker-a/drain",
        "/v1/nodes/broker-a/deregister",
    ] {
        let body = if path.ends_with("heartbeat") {
            serde_json::json!({"incarnation": 0})
        } else {
            serde_json::json!({})
        };
        let response = app
            .clone()
            .oneshot(post(path, Some(&bearer), body))
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }

    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Left,
    );
}

/// An operator managing the fleet holds cluster scope, which covers every node.
#[tokio::test]
async fn cluster_scope_can_manage_any_node() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-b", 7002).await;
    let bearer = token(&keys, vec!["node.manage:cluster:*"]);

    let response = app
        .clone()
        .oneshot(post(
            "/v1/nodes/broker-b/drain",
            Some(&bearer),
            serde_json::json!({}),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        store
            .get_node("broker-b")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Draining,
    );
}

/// Reading the fleet is not permission to change it.
#[tokio::test]
async fn node_view_does_not_grant_node_manage() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;
    let bearer = token(&keys, vec!["node.view:cluster:*"]);

    let response = app
        .oneshot(post(
            "/v1/nodes/broker-a/deregister",
            Some(&bearer),
            serde_json::json!({}),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

/// A tenant admin's own scope must not open the membership write path.
#[tokio::test]
async fn tenant_scoped_permissions_do_not_grant_node_manage() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;

    for perm in [
        "tenant.manage:tenant:t1",
        "ns.manage:namespace:t1/*",
        "rbac.policy.manage:tenant:t1",
    ] {
        let response = app
            .clone()
            .oneshot(post(
                "/v1/nodes/broker-a/drain",
                Some(&token(&keys, vec![perm])),
                serde_json::json!({}),
            ))
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{perm}");
    }
}

/// A correctly shaped token signed by the wrong key is still no token.
#[tokio::test]
async fn a_token_signed_by_another_key_is_rejected() {
    let (app, store, _keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;
    let foreign = generate_signing_keys().expect("keys");
    let bearer = mint_token(
        &foreign,
        "t1",
        "p:attacker",
        vec!["node.manage:cluster:*".to_string()],
        Duration::from_secs(900),
    )
    .expect("token");

    let response = app
        .oneshot(post(
            "/v1/nodes/broker-a/drain",
            Some(&bearer),
            serde_json::json!({}),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Registration takes its identity from the body, so that is what must be
/// authorised -- otherwise a broker could claim any name it liked.
#[tokio::test]
async fn registration_authorises_the_identity_in_the_body() {
    let (app, _store, keys) = setup().await;
    let bearer = token(&keys, vec!["node.manage:node:broker-a"]);

    let response = app
        .clone()
        .oneshot(post(
            "/v1/nodes",
            Some(&bearer),
            registration("broker-impostor", 7009),
        ))
        .await
        .expect("request");
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "a broker must not register under another identity",
    );
}
