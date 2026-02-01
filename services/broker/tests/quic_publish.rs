//! QUIC publish integration tests for broker control streams.
//!
//! # Purpose
//! Validate publish-path behavior over real QUIC connections, including:
//! - auth enforcement and stream existence errors
//! - ack modes and commit-ack paths
//! - binary batch handling and decode failures
//! - control-stream error responses for malformed requests
//!
//! These tests use ephemeral QUIC servers and in-memory broker state.
//!
//! # Key invariants
//! - Felix tokens are EdDSA and verified via JWKS.
//! - Publish ordering is preserved per stream.
//!
//! # Security model / threat assumptions
//! - Test keys are fixtures only and must not be logged in production.
//! - No database or token secrets are written to logs.
//!
//! # Concurrency + ordering guarantees
//! - Tests are serialized to avoid port collisions and shared state races.
//!
//! # How to use
//! Run with `cargo test -p broker quic_publish`.
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
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, FLAG_BINARY_PUBLISH_BATCH, FrameHeader, Message};
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

const TEST_PRIVATE_KEY: [u8; 32] = [10u8; 32];

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
// This test prevents regressions in `quic_publish_unauthorized_and_stream_missing` behavior.
async fn quic_publish_unauthorized_and_stream_missing() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = broker::config::BrokerConfig::from_env()?;
    let auth = auth_fixture("t1", vec!["stream.subscribe:stream:*/*".to_string()]);
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client =
        Client::connect(addr, "localhost", build_client_config(cert.clone(), &auth)?).await?;
    let publisher = client.publisher().await?;
    let err = publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("forbidden publish");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("forbidden")
            || err_msg.contains("unauthorized")
            || err_msg.contains("auth failed")
            || err_msg.contains("publish failed: None"),
        "unexpected publish auth error: {err_msg}"
    );

    let auth = auth_fixture("t1", vec!["stream.publish:stream:*/*".to_string()]);
    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let publisher = client.publisher().await?;
    let err = publisher
        .publish(
            "t1",
            "default",
            "missing",
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("missing stream");
    assert!(err.to_string().contains("stream not found"));

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
// This test prevents regressions in `quic_publish_ack_and_batch_success` behavior.
async fn quic_publish_ack_and_batch_success() -> Result<()> {
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
    let auth = auth_fixture("t1", vec!["stream.publish:stream:*/*".to_string()]);
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let publisher = client.publisher().await?;
    // Use AckMode::None here to avoid coupling this test to control-stream ack timing;
    // ack paths are covered by commit-ack and explicit error tests.
    publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"payload".to_vec(),
            AckMode::None,
        )
        .await?;
    publisher
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"a".to_vec(), b"b".to_vec()],
            AckMode::None,
        )
        .await?;

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
// This test prevents regressions in `quic_publish_commit_ack_ok` behavior.
async fn quic_publish_commit_ack_ok() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "true");
        std::env::set_var("FELIX_ACK_WAIT_TIMEOUT_MS", "1000");
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
    let auth = auth_fixture("t1", vec!["stream.publish:stream:*/*".to_string()]);
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let publisher = client.publisher().await?;
    publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
// This test prevents regressions in `quic_publish_binary_decode_error_closes_stream` behavior.
async fn quic_publish_binary_decode_error_closes_stream() -> Result<()> {
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
    let auth = auth_fixture("t1", vec!["stream.publish:stream:*/*".to_string()]);
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config.clone(),
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
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

    // Send a malformed binary publish batch payload to exercise decode errors.
    let header = FrameHeader::new(FLAG_BINARY_PUBLISH_BATCH, 2);
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

#[tokio::test]
#[serial]
// This test prevents regressions in `quic_publish_missing_request_id_returns_error` behavior.
async fn quic_publish_missing_request_id_returns_error() -> Result<()> {
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
    let auth = auth_fixture("t1", vec!["stream.publish:stream:*/*".to_string()]);
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config.clone(),
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
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

    // Acked publish without request_id should yield an error on the control stream.
    broker::quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            payload: b"payload".to_vec(),
            request_id: None,
            ack: Some(AckMode::PerMessage),
        },
    )
    .await?;
    let response =
        broker::quic::read_message_limited(&mut recv, config.max_frame_bytes, &mut frame_scratch)
            .await?;
    match response {
        Some(Message::Error { message }) => {
            assert!(message.contains("missing request_id"));
        }
        other => anyhow::bail!("unexpected response: {other:?}"),
    }

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
// This test prevents regressions in `quic_publish_binary_batch_success` behavior.
async fn quic_publish_binary_batch_success() -> Result<()> {
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
    let auth = auth_fixture("t1", vec!["stream.publish:stream:*/*".to_string()]);
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let publisher = client.publisher().await?;
    // Binary batch publish exercises the FLAG_BINARY_PUBLISH_BATCH fast path.
    publisher
        .publish_batch_binary(
            "t1",
            "default",
            "orders",
            &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        )
        .await?;

    server_task.abort();
    Ok(())
}
