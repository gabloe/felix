//! # Purpose
//! Demonstrate a full end-to-end RBAC mutation flow in Felix using real
//! networking, real auth, and the existing control-plane API surface.
//!
//! # What this demo proves
//! - RBAC policy/grouping changes applied via the control plane take effect
//!   immediately for broker authorization.
//! - Felix access tokens are issued through the standard token exchange flow.
//! - Broker operations (publish/subscribe/cache) are enforced by real authZ.
//!
//! # Why this exists
//! This demo is an integration-style sanity check that exercises:
//! - Control plane bootstrap + admin RBAC endpoints
//! - Upstream OIDC validation (ES256) and token exchange
//! - Broker control-plane sync and authorization gates
//! - Real client operations over QUIC
//!
//! # High-level flow
//! 1. Start a fake OIDC IdP (ES256) with discovery + JWKS endpoints.
//! 2. Start the control plane HTTP server (in-memory store, bootstrap enabled).
//! 3. Start the broker server and sync loop against the control plane.
//! 4. Bootstrap tenant `t1` with issuer config + initial RBAC (no publish/cache).
//! 5. Create namespace/stream/cache via control plane HTTP APIs.
//! 6. Exchange upstream ID tokens for Felix access tokens (admin + alice).
//! 7. Verify alice is denied publish/subscribe/cache.
//! 8. Add RBAC policies + grouping via admin endpoints.
//! 9. Re-exchange alice token (permissions are embedded in Felix tokens).
//! 10. Verify alice is now allowed publish/subscribe/cache.
//!
//! # Notes on determinism
//! - Fixed ES256 key material and deterministic token timestamps.
//! - Explicit timeouts and retry loops prevent flaky startup timing issues.
//! - Uses only existing public/admin/bootstrap endpoints; no demo-only routes.
//!
//! # Security / safety
//! - Bootstrap is protected by `X-Felix-Bootstrap-Token`.
//! - Admin operations require a Felix admin token obtained via exchange.
//! - No identity shortcuts (no custom headers, no direct store calls).
use anyhow::{Context, Result, bail};
use axum::http::StatusCode;
use axum::{Json, Router};
use base64::Engine as _;
use broker::{auth::BrokerAuth, controlplane as broker_controlplane, quic};
use controlplane::api::bootstrap::BootstrapInitializeRequest;
use controlplane::api::types::{CacheCreateRequest, NamespaceCreateRequest, StreamCreateRequest};
use controlplane::app::{AppState, build_bootstrap_router, build_router};
use controlplane::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use controlplane::auth::principal::principal_id;
use controlplane::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use controlplane::config::{DEFAULT_CHANGE_RETENTION_MAX_ROWS, DEFAULT_CHANGES_LIMIT};
use controlplane::model::{ConsistencyLevel, DeliveryGuarantee, RetentionPolicy, StreamKind};
use controlplane::store::{ControlPlaneStore, StoreConfig, memory::InMemoryStore};
use felix_broker::Broker;
use felix_client::{Client, ClientConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

const BOOTSTRAP_TOKEN: &str = "demo-bootstrap-token";
const TENANT_ID: &str = "t1";
const NAMESPACE: &str = "default";
const STREAM: &str = "orders";
const CACHE: &str = "primary";
const ALICE_SUB: &str = "p:alice";
const ADMIN_SUB: &str = "p:admin";
const IDP_KID: &str = "kid-1";

const EC_PRIVATE_KEY_DER_B64: &str = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgkcZLhh5bmc6yfv8ZrDxWybm+E+aoz2euIJD3fM73VSyhRANCAAQRkD6ZJEwqBms4JDddpbTjl4Ro49h8WRoNVnEcR/Tp6LhwGGZ8Ku1Gw9spY/BCsiW+5AqIqVlNVgGgJFMRbR1V";
const EC_JWK_X: &str = "EZA-mSRMKgZrOCQ3XaW045eEaOPYfFkaDVZxHEf06eg";
const EC_JWK_Y: &str = "uHAYZnwq7UbD2ylj8EKyJb7kCoipWU1WAaAkUxFtHVU";
const ID_TOKEN_IAT: i64 = 1_700_000_000;
const ID_TOKEN_EXP: i64 = 2_000_000_000;

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use std::time::Duration;

    #[tokio::test]
    async fn rbac_live_demo_end_to_end() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(25), run_demo())
            .await
            .context("rbac-live demo timeout")?
    }
}

async fn run_demo() -> Result<()> {
    println!("== Felix Demo: Live RBAC Policy Change (Control Plane Mutation) ==");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("build http client")?;

    let (idp_addr, idp_handle) = spawn_fake_idp().await?;
    let idp_base = format!("http://{idp_addr}");
    wait_for_idp(&http, &idp_base).await?;
    println!("STEP 0 fake IdP up: PASS (addr={idp_addr})");

    let (cp_addr, cp_handle) = spawn_controlplane().await?;
    let cp_base = format!("http://{cp_addr}");
    wait_for_controlplane(&http, &cp_base).await?;
    println!("STEP 1 control plane up: PASS (addr={cp_addr})");

    let (broker_addr, broker_cert, broker_handles) = spawn_broker(&cp_base).await?;
    println!("STEP 2 broker up: PASS (addr={broker_addr})");

    let admin_principal = principal_id(&idp_base, ADMIN_SUB);
    let alice_principal = principal_id(&idp_base, ALICE_SUB);

    let bootstrap_status = bootstrap_tenant(
        &http,
        &cp_base,
        &idp_base,
        &admin_principal,
        &alice_principal,
    )
    .await?;
    print_http("STEP 3 bootstrap tenant", bootstrap_status, StatusCode::OK)?;

    let ns_status = create_namespace(&http, &cp_base).await?;
    print_http("STEP 4 create namespace", ns_status, StatusCode::CREATED)?;

    let stream_status = create_stream(&http, &cp_base).await?;
    print_http("STEP 5 create stream", stream_status, StatusCode::CREATED)?;

    let cache_status = create_cache(&http, &cp_base).await?;
    print_http("STEP 6 create cache", cache_status, StatusCode::CREATED)?;

    // Allow broker to sync metadata before client ops.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let admin_id_token = mint_id_token(&idp_base, ADMIN_SUB, &["role:tenant-admin"])?;
    let alice_id_token = mint_id_token(&idp_base, ALICE_SUB, &["role:reader"])?;

    let admin_felix = exchange_token(&http, &cp_base, &admin_id_token).await?;
    println!("STEP 7 admin token exchange: PASS (status=200)");

    let alice_felix = exchange_token(&http, &cp_base, &alice_id_token).await?;
    println!("STEP 8 alice token exchange: PASS (status=200)");

    let client = build_client(&broker_addr, &broker_cert, &alice_felix).await?;

    let publish_denied =
        retry_op("publish", Duration::from_secs(5), || publish_once(&client)).await;
    print_op("STEP 9 publish denied", publish_denied, false)?;

    let subscribe_denied = retry_op("subscribe", Duration::from_secs(5), || {
        subscribe_once(&client)
    })
    .await;
    print_op("STEP 10 subscribe denied", subscribe_denied, false)?;

    let cache_denied = retry_op("cache", Duration::from_secs(5), || cache_roundtrip(&client)).await;
    print_op("STEP 11 cache denied", cache_denied, false)?;

    let policy_status = add_rbac_policies(&http, &cp_base, &admin_felix).await?;
    print_http(
        "STEP 12 RBAC policies added",
        policy_status,
        StatusCode::NO_CONTENT,
    )?;

    let grouping_status =
        add_rbac_grouping(&http, &cp_base, &admin_felix, &alice_principal).await?;
    print_http(
        "STEP 13 RBAC grouping added",
        grouping_status,
        StatusCode::NO_CONTENT,
    )?;

    println!(
        "STEP 14 re-exchange alice token: Felix tokens embed permissions; reissuing to reflect RBAC changes."
    );
    let alice_felix_updated = exchange_token(&http, &cp_base, &alice_id_token).await?;

    let client = build_client(&broker_addr, &broker_cert, &alice_felix_updated).await?;

    let publish_ok = retry_op("publish", Duration::from_secs(5), || publish_once(&client)).await;
    print_op("STEP 15 publish allowed", publish_ok, true)?;

    let subscribe_ok = retry_op("subscribe", Duration::from_secs(6), || {
        subscribe_and_receive(&client)
    })
    .await;
    print_op("STEP 16 subscribe allowed", subscribe_ok, true)?;

    let cache_ok = retry_op("cache", Duration::from_secs(5), || cache_roundtrip(&client)).await;
    print_op("STEP 17 cache allowed", cache_ok, true)?;

    idp_handle.abort();
    cp_handle.abort();
    for handle in broker_handles {
        handle.abort();
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run_demo().await
}

/// # What it does
/// Starts a minimal OIDC IdP server that serves discovery + JWKS.
///
/// # Why it exists
/// Exercises the real upstream validation path used by token exchange.
///
/// # Invariants
/// - Serves ES256 keys only.
/// - `issuer` matches the bound server URL.
async fn spawn_fake_idp() -> Result<(SocketAddr, JoinHandle<()>)> {
    let jwks = json!({
        "keys": [
            {
                "kty": "EC",
                "crv": "P-256",
                "x": EC_JWK_X,
                "y": EC_JWK_Y,
                "use": "sig",
                "alg": "ES256",
                "kid": IDP_KID
            }
        ]
    });

    let app = Router::new()
        .route(
            "/.well-known/openid-configuration",
            axum::routing::get({
                move |axum::extract::State(state): axum::extract::State<IdpState>| {
                    let jwks_uri = format!("{}/jwks.json", state.issuer);
                    let discovery = json!({
                        "issuer": state.issuer,
                        "jwks_uri": jwks_uri,
                    });
                    async move { Json(discovery) }
                }
            }),
        )
        .route(
            "/jwks.json",
            axum::routing::get({
                let jwks = jwks.clone();
                move || {
                    let jwks = jwks.clone();
                    async move { Json(jwks) }
                }
            }),
        )
        .with_state(IdpState {
            issuer: String::new(),
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let issuer = format!("http://{addr}");

    let app = app.with_state(IdpState { issuer });
    let handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app.into_make_service()).await {
            eprintln!("fake idp server error: {err}");
        }
    });
    Ok((addr, handle))
}

#[derive(Clone)]
struct IdpState {
    issuer: String,
}

/// # What it does
/// Starts the control plane HTTP server on a random local port.
///
/// # Why it exists
/// Provides the real API surface used by this demo (bootstrap + admin + core).
///
/// # Invariants
/// - In-memory store only.
/// - Bootstrap enabled with a fixed token.
async fn spawn_controlplane() -> Result<(SocketAddr, JoinHandle<()>)> {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    });
    let state = AppState {
        region: controlplane::api::types::Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
        },
        api_version: "v1".to_string(),
        features: controlplane::api::types::FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::new(store),
        oidc_validator: controlplane::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: true,
        bootstrap_token: Some(BOOTSTRAP_TOKEN.to_string()),
    };

    let app = build_router(state.clone()).merge(build_bootstrap_router(state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app.into_make_service()).await {
            eprintln!("controlplane server error: {err}");
        }
    });
    Ok((addr, handle))
}

/// # What it does
/// Starts a real broker QUIC server and a control-plane sync loop.
///
/// # Why it exists
/// Ensures authZ decisions are enforced by the broker using live metadata.
///
/// # Invariants
/// - Sync interval is short to keep the demo fast and deterministic.
/// - Uses an ephemeral cache backend.
async fn spawn_broker(
    controlplane_url: &str,
) -> Result<(SocketAddr, CertificateDer<'static>, Vec<JoinHandle<()>>)> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let mut config = broker::config::BrokerConfig::from_env()?;
    config.controlplane_url = Some(controlplane_url.to_string());
    config.controlplane_sync_interval_ms = 200;
    config.disable_timings = true;

    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        transport,
    )?);
    let addr = server.local_addr()?;

    let controlplane_url = controlplane_url.to_string();
    let auth = Arc::new(BrokerAuth::new(controlplane_url.clone()));
    let broker_config = config.clone();
    let sync_interval_ms = config.controlplane_sync_interval_ms;
    let broker_quic = Arc::clone(&broker);
    let broker_sync = Arc::clone(&broker);
    let server_quic = Arc::clone(&server);
    let broker_task = tokio::spawn(async move {
        if let Err(err) = quic::serve(server_quic, broker_quic, broker_config, auth).await {
            eprintln!("broker server error: {err}");
        }
    });

    let sync_task = tokio::spawn(async move {
        let interval = Duration::from_millis(sync_interval_ms);
        if let Err(err) =
            broker_controlplane::start_sync(broker_sync, controlplane_url, interval).await
        {
            eprintln!("controlplane sync error: {err}");
        }
    });

    Ok((addr, cert, vec![broker_task, sync_task]))
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let rcgen::CertifiedKey { cert, signing_key } = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

async fn build_client(
    broker_addr: &SocketAddr,
    broker_cert: &CertificateDer<'static>,
    token: &str,
) -> Result<Client> {
    let mut roots = RootCertStore::empty();
    roots.add(broker_cert.clone())?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::optimized_defaults(quinn);
    config.auth_tenant_id = Some(TENANT_ID.to_string());
    config.auth_token = Some(token.to_string());
    match tokio::time::timeout(
        Duration::from_secs(3),
        Client::connect(*broker_addr, "localhost", config),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => bail!("broker client connect timeout"),
    }
}

async fn bootstrap_tenant(
    http: &reqwest::Client,
    cp_base: &str,
    idp_base: &str,
    admin_principal: &str,
    alice_principal: &str,
) -> Result<StatusCode> {
    let idp = IdpIssuerConfig {
        issuer: idp_base.to_string(),
        audiences: vec!["felix-controlplane".to_string()],
        discovery_url: None,
        jwks_url: Some(format!("{idp_base}/jwks.json")),
        claim_mappings: ClaimMappings {
            subject_claim: "sub".to_string(),
            groups_claim: Some("groups".to_string()),
        },
    };

    let policies = vec![PolicyRule {
        subject: "role:reader".to_string(),
        // Canonical RBAC objects are tenant-qualified.
        object: format!("stream:{TENANT_ID}/{NAMESPACE}/other"),
        action: "stream.subscribe".to_string(),
    }];

    let groupings = vec![GroupingRule {
        user: alice_principal.to_string(),
        role: "role:reader".to_string(),
    }];

    let body = BootstrapInitializeRequest {
        display_name: "Tenant One".to_string(),
        idp_issuers: vec![idp],
        initial_admin_principals: vec![admin_principal.to_string()],
        policies,
        groupings,
    };

    let url = format!("{cp_base}/internal/bootstrap/tenants/{TENANT_ID}/initialize");
    let response = http
        .post(url)
        .header("X-Felix-Bootstrap-Token", BOOTSTRAP_TOKEN)
        .json(&body)
        .send()
        .await?;
    Ok(response.status())
}

async fn create_namespace(http: &reqwest::Client, cp_base: &str) -> Result<StatusCode> {
    let url = format!("{cp_base}/v1/tenants/{TENANT_ID}/namespaces");
    let body = NamespaceCreateRequest {
        namespace: NAMESPACE.to_string(),
        display_name: "Default".to_string(),
    };
    let response = http.post(url).json(&body).send().await?;
    Ok(response.status())
}

async fn create_stream(http: &reqwest::Client, cp_base: &str) -> Result<StatusCode> {
    let url = format!("{cp_base}/v1/tenants/{TENANT_ID}/namespaces/{NAMESPACE}/streams");
    let body = StreamCreateRequest {
        stream: STREAM.to_string(),
        kind: StreamKind::Stream,
        shards: 1,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtLeastOnce,
        durable: false,
    };
    let response = http.post(url).json(&body).send().await?;
    Ok(response.status())
}

async fn create_cache(http: &reqwest::Client, cp_base: &str) -> Result<StatusCode> {
    let url = format!("{cp_base}/v1/tenants/{TENANT_ID}/namespaces/{NAMESPACE}/caches");
    let body = CacheCreateRequest {
        cache: CACHE.to_string(),
        display_name: "Primary".to_string(),
    };
    let response = http.post(url).json(&body).send().await?;
    Ok(response.status())
}

#[derive(Serialize, Deserialize)]
struct TokenExchangeResponse {
    felix_token: String,
}

/// # What it does
/// Exchanges an upstream ID token for a Felix access token.
///
/// # Why it exists
/// Ensures the demo uses the real auth flow instead of shortcuts.
async fn exchange_token(http: &reqwest::Client, cp_base: &str, id_token: &str) -> Result<String> {
    let url = format!("{cp_base}/v1/tenants/{TENANT_ID}/token/exchange");
    let response = http
        .post(url)
        .bearer_auth(id_token)
        .json(&json!({}))
        .send()
        .await?;
    let status = response.status();
    if status != StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        bail!("token exchange failed: status={} body={}", status, body);
    }
    let body: TokenExchangeResponse = response.json().await?;
    Ok(body.felix_token)
}

/// # What it does
/// Mints a deterministic ES256 ID token for the fake IdP.
///
/// # Invariants
/// - Uses fixed iat/exp for reproducibility.
/// - `iss` matches the configured issuer.
fn mint_id_token(issuer: &str, subject: &str, groups: &[&str]) -> Result<String> {
    let claims = json!({
        "iss": issuer,
        "sub": subject,
        "aud": "felix-controlplane",
        "iat": ID_TOKEN_IAT,
        "exp": ID_TOKEN_EXP,
        "groups": groups,
    });
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(IDP_KID.to_string());

    let der = base64::engine::general_purpose::STANDARD.decode(EC_PRIVATE_KEY_DER_B64)?;
    let key = EncodingKey::from_ec_der(&der);
    let token = jsonwebtoken::encode(&header, &claims, &key)?;
    Ok(token)
}

/// # What it does
/// Adds the policies needed for publish/subscribe/cache access.
///
/// # Why it exists
/// Demonstrates live RBAC mutation through the official admin API.
async fn add_rbac_policies(
    http: &reqwest::Client,
    cp_base: &str,
    admin_token: &str,
) -> Result<StatusCode> {
    let url = format!("{cp_base}/v1/tenants/{TENANT_ID}/rbac/policies");
    let policies = [
        PolicyRule {
            subject: "role:reader".to_string(),
            // Canonical stream scope: stream:{tenant}/{namespace}/{stream_or_*}
            object: format!("stream:{TENANT_ID}/{NAMESPACE}/{STREAM}"),
            action: "stream.publish".to_string(),
        },
        PolicyRule {
            subject: "role:reader".to_string(),
            object: format!("stream:{TENANT_ID}/{NAMESPACE}/{STREAM}"),
            action: "stream.subscribe".to_string(),
        },
        PolicyRule {
            subject: "role:reader".to_string(),
            // Canonical cache scope: cache:{tenant}/{namespace}/{cache_or_*}
            object: format!("cache:{TENANT_ID}/{NAMESPACE}/{CACHE}"),
            action: "cache.read".to_string(),
        },
        PolicyRule {
            subject: "role:reader".to_string(),
            object: format!("cache:{TENANT_ID}/{NAMESPACE}/{CACHE}"),
            action: "cache.write".to_string(),
        },
    ];

    for policy in policies {
        let response = http
            .post(&url)
            .bearer_auth(admin_token)
            .json(&policy)
            .send()
            .await?;
        if response.status() != StatusCode::NO_CONTENT {
            return Ok(response.status());
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

/// # What it does
/// Binds alice's principal ID to the reader role.
///
/// # Why it exists
/// Demonstrates live role assignment via the admin API.
async fn add_rbac_grouping(
    http: &reqwest::Client,
    cp_base: &str,
    admin_token: &str,
    alice_principal: &str,
) -> Result<StatusCode> {
    let url = format!("{cp_base}/v1/tenants/{TENANT_ID}/rbac/groupings");
    let grouping = GroupingRule {
        user: alice_principal.to_string(),
        role: "role:reader".to_string(),
    };
    let response = http
        .post(url)
        .bearer_auth(admin_token)
        .json(&grouping)
        .send()
        .await?;
    Ok(response.status())
}

async fn publish_once(client: &Client) -> Result<()> {
    let publisher = client.publisher().await?;
    publisher
        .publish(
            TENANT_ID,
            NAMESPACE,
            STREAM,
            b"rbac-demo".to_vec(),
            AckMode::PerMessage,
        )
        .await
}

async fn subscribe_once(client: &Client) -> Result<()> {
    let _sub = client.subscribe(TENANT_ID, NAMESPACE, STREAM).await?;
    Ok(())
}

async fn subscribe_and_receive(client: &Client) -> Result<()> {
    let mut sub = client.subscribe(TENANT_ID, NAMESPACE, STREAM).await?;
    let publisher = client.publisher().await?;
    publisher
        .publish(
            TENANT_ID,
            NAMESPACE,
            STREAM,
            b"rbac-demo".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    let event = tokio::time::timeout(Duration::from_secs(2), sub.next_event()).await;
    match event {
        Ok(Ok(Some(_))) => Ok(()),
        Ok(Ok(None)) => bail!("subscription closed"),
        Ok(Err(err)) => bail!("subscribe error: {err}"),
        Err(_) => bail!("subscribe timeout"),
    }
}

async fn cache_roundtrip(client: &Client) -> Result<()> {
    client
        .cache_put(
            TENANT_ID,
            NAMESPACE,
            CACHE,
            "key",
            b"value".to_vec().into(),
            Some(10_000),
        )
        .await?;
    let value = client.cache_get(TENANT_ID, NAMESPACE, CACHE, "key").await?;
    if value.is_none() {
        bail!("cache miss");
    }
    Ok(())
}

fn print_http(label: &str, status: StatusCode, expected: StatusCode) -> Result<()> {
    if status == expected {
        println!("{}: PASS (status={})", label, status.as_u16());
        Ok(())
    } else {
        bail!("{}: FAIL (status={})", label, status.as_u16());
    }
}

fn print_op(label: &str, result: Result<()>, expect_ok: bool) -> Result<()> {
    match (result.is_ok(), expect_ok) {
        (true, true) => {
            println!("{}: PASS (status=OK)", label);
            Ok(())
        }
        (false, false) => {
            let err = result.err().map(|e| e.to_string()).unwrap_or_default();
            println!("{}: PASS (status=DENIED) error={}", label, err);
            Ok(())
        }
        (true, false) => bail!("{}: FAIL (expected denied)", label),
        (false, true) => {
            let err = result.err().map(|e| e.to_string()).unwrap_or_default();
            bail!("{}: FAIL error={}", label, err)
        }
    }
}

/// # What it does
/// Retries a broker operation until it succeeds or the timeout elapses.
///
/// # Why it exists
/// Broker metadata sync is eventually consistent; initial "not found" can
/// occur before the broker has applied control-plane changes.
async fn retry_op<F, Fut>(label: &str, timeout: Duration, mut op: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let start = std::time::Instant::now();
    loop {
        match with_timeout(label, Duration::from_secs(2), op()).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                let msg = err.to_string();
                if is_retryable_broker_error(&msg) {
                    if start.elapsed() > timeout {
                        bail!("{label} timeout: {msg}");
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
}

fn is_retryable_broker_error(message: &str) -> bool {
    message.contains("not found")
        || message.contains("None")
        || message.contains("closed")
        || message.contains("timeout")
}

async fn wait_for_idp(http: &reqwest::Client, idp_base: &str) -> Result<()> {
    let url = format!("{idp_base}/.well-known/openid-configuration");
    for _ in 0..10 {
        if let Ok(response) = http.get(&url).send().await {
            if response.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("fake IdP did not become ready");
}

async fn wait_for_controlplane(http: &reqwest::Client, cp_base: &str) -> Result<()> {
    let url = format!("{cp_base}/v1/system/health");
    for _ in 0..10 {
        if let Ok(response) = http.get(&url).send().await {
            if response.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("control plane did not become ready");
}

async fn with_timeout<T>(
    label: &str,
    timeout: Duration,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result,
        Err(_) => bail!("{label} timed out"),
    }
}
