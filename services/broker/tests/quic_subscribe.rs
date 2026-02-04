//! QUIC subscribe integration tests for broker control/event streams.
//!
//! # Purpose
//! Validate subscription lifecycle and error handling over QUIC, including:
//! - auth enforcement and missing stream errors
//! - event delivery + cancel cleanup
//! - fanout behavior with multiple subscribers
//! - malformed control frames on the subscribe path
//!
//! These tests use ephemeral QUIC servers and in-memory broker state.
//!
//! # Key invariants
//! - Felix tokens are EdDSA and verified via JWKS.
//! - Subscription ordering is preserved per stream.
//!
//! # Security model / threat assumptions
//! - Test keys are fixtures only and must not be logged in production.
//! - No database or token secrets are written to logs.
//!
//! # Concurrency + ordering guarantees
//! - Tests are serialized to avoid port collisions and shared state races.
//!
//! # How to use
//! Run with `cargo test -p broker quic_subscribe`.
use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use broker::auth::{BrokerAuth, ControlPlaneKeyStore};
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, StreamMetadata};
use felix_client::{Client, ClientConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::{FrameHeader, Message};
use jsonwebtoken::Algorithm;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

const TEST_PRIVATE_KEY: [u8; 32] = [9u8; 32];

struct AuthFixture {
    tenant_id: String,
    token: String,
    auth: Arc<BrokerAuth>,
}

fn auth_fixture(tenant_id: &str, perms: Vec<String>) -> AuthFixture {
    // Build a deterministic Ed25519 keypair and JWKS for repeatable auth tests.
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let jwks = jwks_from_public_key(&public_key, "k1");
    let mut key_materials = std::collections::HashMap::new();
    key_materials.insert(
        tenant_id.to_string(),
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TEST_PRIVATE_KEY,
            public_key,
            jwks: jwks.clone(),
        },
    );
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(key_materials),
    );
    // Mint a Felix token to authenticate the QUIC client.
    let token = issuer
        .mint(&TenantId::new(tenant_id), "p:test", perms)
        .expect("mint token");

    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    // Inject JWKS directly to avoid network dependencies in tests.
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    let auth = Arc::new(BrokerAuth::with_key_store(key_store));
    AuthFixture {
        tenant_id: tenant_id.to_string(),
        token,
        auth,
    }
}

fn jwks_from_public_key(public_key: &[u8], kid: &str) -> Jwks {
    // Encode Ed25519 public key into JWK `x` using base64url.
    let x = URL_SAFE_NO_PAD.encode(public_key);
    Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    }
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    // Self-signed cert is sufficient for loopback QUIC tests.
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    Ok((server_config, cert_der))
}

fn build_quinn_client_config(cert: CertificateDer<'static>) -> Result<QuinnClientConfig> {
    // Trust the test server certificate to avoid TLS validation failures.
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    Ok(QuinnClientConfig::with_root_certificates(Arc::new(roots))?)
}

fn build_client_config(cert: CertificateDer<'static>, auth: &AuthFixture) -> Result<ClientConfig> {
    // Embed auth token in client config for automated auth handshake.
    let quinn = build_quinn_client_config(cert)?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(auth.tenant_id.clone());
    config.auth_token = Some(auth.token.clone());
    Ok(config)
}

#[tokio::test]
#[serial]
// This test prevents regressions in `quic_subscribe_unauthorized_and_stream_missing` behavior.
async fn quic_subscribe_unauthorized_and_stream_missing() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = broker::config::BrokerConfig::from_env()?;
    let auth = auth_fixture("t1", vec!["stream.publish:stream:t1/*/*".to_string()]);
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client =
        Client::connect(addr, "localhost", build_client_config(cert.clone(), &auth)?).await?;
    let err = match client.subscribe("t1", "default", "orders").await {
        Ok(_) => anyhow::bail!("expected unauthorized subscribe"),
        Err(err) => err,
    };
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("forbidden")
            || err_msg.contains("unauthorized")
            || err_msg.contains("auth failed")
            || err_msg.contains("subscribe failed: None"),
        "unexpected subscribe auth error: {err_msg}"
    );

    let auth = auth_fixture("t1", vec!["stream.subscribe:stream:t1/*/*".to_string()]);
    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let err = match client.subscribe("t1", "default", "missing").await {
        Ok(_) => anyhow::bail!("expected missing stream"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("stream not found"));

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
// This test prevents regressions in `quic_subscribe_batch_receive_and_cancel` behavior.
async fn quic_subscribe_batch_receive_and_cancel() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_EVENT_BATCH_MAX_EVENTS", "2");
        std::env::set_var("FELIX_EVENT_BATCH_MAX_BYTES", "1024");
        std::env::set_var("FELIX_EVENT_BATCH_MAX_DELAY_US", "200");
        std::env::set_var("FELIX_FANOUT_BATCH", "4");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = broker::config::BrokerConfig::from_env()?;
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let mut sub = client.subscribe("t1", "default", "orders").await?;
    let publisher = client.publisher().await?;

    // Use AckMode::None to avoid control-stream ack timing affecting subscription delivery.
    publisher
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            felix_wire::AckMode::None,
        )
        .await?;

    let mut received = Vec::new();
    while received.len() < 3 {
        let next = timeout(Duration::from_secs(2), sub.next_event()).await??;
        if let Some(event) = next {
            received.push(event.payload.to_vec());
        }
    }
    assert_eq!(received.len(), 3);
    drop(sub);

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
// This test prevents regressions in `quic_subscribe_fanout_and_drop_cleanup` behavior.
async fn quic_subscribe_fanout_and_drop_cleanup() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_EVENT_BATCH_MAX_EVENTS", "2");
        std::env::set_var("FELIX_EVENT_BATCH_MAX_BYTES", "1024");
        std::env::set_var("FELIX_EVENT_BATCH_MAX_DELAY_US", "200");
        std::env::set_var("FELIX_FANOUT_BATCH", "4");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = broker::config::BrokerConfig::from_env()?;
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let mut sub_a = client.subscribe("t1", "default", "orders").await?;
    let mut sub_b = client.subscribe("t1", "default", "orders").await?;
    let publisher = client.publisher().await?;

    // Use AckMode::None to reduce flakiness in coverage runs.
    publisher
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"a".to_vec(), b"b".to_vec()],
            felix_wire::AckMode::None,
        )
        .await?;

    let first_a = timeout(Duration::from_secs(2), sub_a.next_event()).await??;
    let first_b = timeout(Duration::from_secs(2), sub_b.next_event()).await??;
    assert!(first_a.is_some());
    assert!(first_b.is_some());

    // Drop one subscriber and ensure the other keeps receiving events.
    drop(sub_a);

    publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"c".to_vec(),
            felix_wire::AckMode::None,
        )
        .await?;
    let remaining = timeout(Duration::from_secs(2), sub_b.next_event()).await??;
    assert!(remaining.is_some());

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
// This test prevents regressions in `quic_subscribe_invalid_frame_closes_stream` behavior.
async fn quic_subscribe_invalid_frame_closes_stream() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = broker::config::BrokerConfig::from_env()?;
    let auth = auth_fixture("t1", vec!["stream.subscribe:stream:t1/*/*".to_string()]);
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config.clone(),
        Arc::clone(&auth.auth),
    ));

    let client = felix_transport::QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    broker::quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
        },
    )
    .await?;
    let mut frame_scratch = bytes::BytesMut::with_capacity(1024);
    let response =
        broker::quic::read_message_limited(&mut recv, config.max_frame_bytes, &mut frame_scratch)
            .await?;
    assert!(matches!(response, Some(Message::Ok)));

    // Send an invalid JSON frame to trigger decode error handling on the control stream.
    let header = FrameHeader::new(0, 2);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    header.encode_into(&mut header_bytes);
    send.write_all(&header_bytes).await?;
    send.write_all(&[0, 5]).await?;
    send.flush().await?;

    let close = timeout(
        Duration::from_millis(200),
        broker::quic::read_message_limited(&mut recv, config.max_frame_bytes, &mut frame_scratch),
    )
    .await;
    assert!(close.is_ok());

    server_task.abort();
    Ok(())
}
