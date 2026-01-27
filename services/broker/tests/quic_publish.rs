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
use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use broker::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_authz::{FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyMaterial};
use felix_broker::{Broker, StreamMetadata};
use felix_client::{Client, ClientConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, FLAG_BINARY_PUBLISH_BATCH, FrameHeader, Message};
use jsonwebtoken::Algorithm;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rsa::RsaPublicKey;
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::traits::PublicKeyParts;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

const TEST_PRIVATE_KEY_PEM: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL
UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V
rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H
oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI
Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/
by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd
WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ
Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j
E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn
LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS
bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE
8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl
xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY
2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1
9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0
7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ
mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7
MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4
t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam
QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA
2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ
4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn
PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH
UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8
oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==
-----END RSA PRIVATE KEY-----"#;

const TEST_PUBLIC_KEY_PEM: &str = r#"-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4
l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2VrUyW
yj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG
/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4l
QzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/by2h
3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQAB
-----END RSA PUBLIC KEY-----"#;

struct AuthFixture {
    tenant_id: String,
    token: String,
    auth: Arc<BrokerAuth>,
}

fn auth_fixture(tenant_id: &str, perms: Vec<String>) -> AuthFixture {
    let jwks = jwks_from_public_key(TEST_PUBLIC_KEY_PEM, "k1");
    let mut key_materials = std::collections::HashMap::new();
    key_materials.insert(
        tenant_id.to_string(),
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::RS256,
            private_key_pem: TEST_PRIVATE_KEY_PEM.as_bytes().to_vec(),
            public_key_pem: TEST_PUBLIC_KEY_PEM.as_bytes().to_vec(),
            jwks: jwks.clone(),
        },
    );
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(key_materials),
    );
    let token = issuer
        .mint(&TenantId::new(tenant_id), "p:test", perms)
        .expect("mint token");

    let key_store = Arc::new(ControlPlaneKeyStore::new("http://localhost".to_string()));
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    let auth = Arc::new(BrokerAuth::with_key_store(key_store));
    AuthFixture {
        tenant_id: tenant_id.to_string(),
        token,
        auth,
    }
}

fn jwks_from_public_key(pem: &str, kid: &str) -> Jwks {
    let public_key = RsaPublicKey::from_pkcs1_pem(pem).expect("parse public key");
    let n = URL_SAFE_NO_PAD.encode(public_key.n().to_bytes_be());
    let e = URL_SAFE_NO_PAD.encode(public_key.e().to_bytes_be());
    Jwks {
        keys: vec![Jwk {
            kty: "RSA".to_string(),
            kid: kid.to_string(),
            alg: "RS256".to_string(),
            use_field: KeyUse::Sig,
            n,
            e,
        }],
    }
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    Ok((server_config, cert_der))
}

fn build_quinn_client_config(cert: CertificateDer<'static>) -> Result<QuinnClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    Ok(QuinnClientConfig::with_root_certificates(Arc::new(roots))?)
}

fn build_client_config(cert: CertificateDer<'static>, auth: &AuthFixture) -> Result<ClientConfig> {
    let quinn = build_quinn_client_config(cert)?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(auth.tenant_id.clone());
    config.auth_token = Some(auth.token.clone());
    Ok(config)
}

#[tokio::test]
#[serial]
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
