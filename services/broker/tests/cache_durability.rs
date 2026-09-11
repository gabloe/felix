//! The cache outliving the process that wrote it.
//!
//! The storage-level tests prove the log-backed cache keeps its entries; this
//! proves the broker is actually wired to it. Both matter: a correct
//! implementation nothing is connected to would pass the first and fail here.
//!
//! "Restart" is the broker and its storage torn down and rebuilt over the same
//! directory, which is what a process restart is from the cache's point of
//! view: nothing in memory survives, and only the log does.
use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use broker::{auth::BrokerAuth, quic};
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{FelixTokenIssuer, Jwks, TenantId, TenantKeyCache, TenantKeyMaterial};
use felix_broker::{Broker, CacheMetadata};
use felix_client::{Client, ClientConfig};
use felix_storage::LogCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_transport::{QuicServer, TransportConfig};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const DEMO_PRIVATE_KEY: [u8; 32] = [42u8; 32];
const CACHE: &str = "sessions";

struct DemoAuthBundle {
    auth: Arc<BrokerAuth>,
    tokens: HashMap<String, String>,
}

/// A broker serving over QUIC with its cache on `root`.
struct Running {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    tokens: HashMap<String, String>,
    server: Arc<QuicServer>,
    task: tokio::task::JoinHandle<Result<()>>,
}

async fn start(root: &std::path::Path) -> Result<Running> {
    let cache = LogCache::open(
        root,
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .context("open the cache log")?;

    let broker = Arc::new(Broker::new(Box::new(cache)));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", CACHE, CacheMetadata)
        .await?;

    let config = broker::config::BrokerConfig::from_env()?;
    let demo_auth = demo_auth_for_tenants(&["t1"], Duration::from_secs(900))?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let task = tokio::spawn(quic::serve(
        Arc::clone(&server),
        broker,
        config,
        demo_auth.auth,
    ));

    Ok(Running {
        addr,
        cert,
        tokens: demo_auth.tokens,
        server,
        task,
    })
}

impl Running {
    async fn client(&self) -> Result<Client> {
        let mut config = build_client_config(self.cert.clone())?;
        config.auth_tenant_id = Some("t1".to_string());
        config.auth_token = self.tokens.get("t1").cloned();
        Client::connect(self.addr, "localhost", config).await
    }

    /// Stop serving, the way a process going away does.
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
        drop(self.server);
    }
}

/// **A cached value outlives the broker that stored it.** The claim the whole
/// design exists to make true: the cache is a log, so it is on disk, so a
/// restart does not lose it.
#[tokio::test]
async fn a_cached_value_survives_a_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;

    let running = start(dir.path()).await?;
    running
        .client()
        .await?
        .cache_put(
            "t1",
            "default",
            CACHE,
            "user:1",
            b"alice".to_vec().into(),
            None,
        )
        .await?;
    running.stop().await;

    let restarted = start(dir.path()).await?;
    let value = restarted
        .client()
        .await?
        .cache_get("t1", "default", CACHE, "user:1")
        .await?;
    assert_eq!(
        value.as_deref(),
        Some(&b"alice"[..]),
        "the cache lost its entry across a restart",
    );
    restarted.stop().await;
    Ok(())
}

/// The newest write is the one that survives, not the first.
#[tokio::test]
async fn the_latest_value_is_the_one_that_survives() -> Result<()> {
    let dir = tempfile::tempdir()?;

    let running = start(dir.path()).await?;
    let client = running.client().await?;
    for value in ["one", "two", "three"] {
        client
            .cache_put(
                "t1",
                "default",
                CACHE,
                "user:1",
                value.as_bytes().to_vec().into(),
                None,
            )
            .await?;
    }
    drop(client);
    running.stop().await;

    let restarted = start(dir.path()).await?;
    let value = restarted
        .client()
        .await?
        .cache_get("t1", "default", CACHE, "user:1")
        .await?;
    assert_eq!(value.as_deref(), Some(&b"three"[..]));
    restarted.stop().await;
    Ok(())
}

fn demo_auth_for_tenants(tenants: &[&str], ttl: Duration) -> Result<DemoAuthBundle> {
    let mut key_store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    let mut tokens = HashMap::new();
    let mut jwks_per_tenant = HashMap::new();

    for tenant in tenants {
        let signing_key = Ed25519SigningKey::from_bytes(&DEMO_PRIVATE_KEY);
        let public_key = signing_key.verifying_key().to_bytes();
        let jwks = build_demo_jwks("demo-k1", &public_key)?;
        key_store.insert(
            (*tenant).to_string(),
            TenantKeyMaterial {
                kid: "demo-k1".to_string(),
                alg: jsonwebtoken::Algorithm::EdDSA,
                private_key: DEMO_PRIVATE_KEY,
                public_key,
                jwks: jwks.clone(),
            },
        );
        jwks_per_tenant.insert((*tenant).to_string(), jwks);
    }

    let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", ttl, Arc::new(key_store));
    for tenant in tenants {
        let perms = vec![
            format!("tenant.manage:tenant:{tenant}"),
            format!("ns.manage:namespace:{tenant}/*"),
            format!("cache.read:cache:{tenant}/*/*"),
            format!("cache.write:cache:{tenant}/*/*"),
        ];
        tokens.insert(
            (*tenant).to_string(),
            issuer.mint(&TenantId::new(*tenant), "p:demo", perms)?,
        );
    }

    let key_store = Arc::new(broker::auth::ControlPlaneKeyStore::new(
        "http://127.0.0.1".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    for (tenant, jwks) in jwks_per_tenant {
        key_store.insert_jwks(&TenantId::new(&tenant), jwks);
    }

    Ok(DemoAuthBundle {
        auth: Arc::new(BrokerAuth::with_key_store(key_store)),
        tokens,
    })
}

fn build_demo_jwks(kid: &str, public_key: &[u8; 32]) -> Result<Jwks> {
    Ok(Jwks {
        keys: vec![felix_authz::Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: felix_authz::KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(URL_SAFE_NO_PAD.encode(public_key)),
        }],
    })
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    Ok((
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?,
        cert_der,
    ))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    ClientConfig::from_env_or_yaml(
        QuinnClientConfig::with_root_certificates(Arc::new(roots))?,
        None,
    )
}
