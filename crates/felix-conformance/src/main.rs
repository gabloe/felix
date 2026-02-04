//! Felix conformance test runner.
//!
//! # Purpose
//! Exercises end-to-end broker and client behaviors (auth, QUIC, wire encoding)
//! to validate protocol and security invariants outside unit tests.
//!
//! # Notes
//! This binary is intended for CI and developer verification, not production use.
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use broker::auth::{BrokerAuth, ControlPlaneKeyStore};
use broker::quic;
use bytes::{Bytes, BytesMut};
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
    TenantKeyStore,
};
use felix_broker::{Broker, CacheMetadata};
use felix_client::{Client, ClientConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, FLAG_BINARY_EVENT_BATCH, Frame, FrameHeader, Message};
use jsonwebtoken::Algorithm;
use quinn::{ClientConfig as QuinnClientConfig, ReadExactError, RecvStream};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const MAX_TEST_FRAME_BYTES: usize = 64 * 1024;

// Static test keypair used to make JWT/JWKS tests deterministic and self-contained.
const TEST_PRIVATE_KEY: [u8; 32] = [21u8; 32];

struct AuthFixture {
    tenant_id: String,
    token: String,
    broker_auth: Arc<BrokerAuth>,
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("== Felix Conformance Runner ==");
    let auth = build_auth_fixture()?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", "primary", CacheMetadata)
        .await?;
    broker
        .register_stream("t1", "default", "conformance", Default::default())
        .await?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let config = broker::config::BrokerConfig::from_env()?;
    let server_task = tokio::spawn(quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.broker_auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    run_pubsub(&connection, &auth).await?;
    run_cache(&connection, &auth).await?;
    run_client_pubsub(addr, cert.clone(), &auth).await?;
    run_client_cache(addr, cert, &auth).await?;

    drop(connection);
    server_task.abort();
    println!("Conformance checks passed.");
    Ok(())
}

fn build_auth_fixture() -> Result<AuthFixture> {
    let tenant_id = "t1".to_string();
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let x = URL_SAFE_NO_PAD.encode(public_key);
    let jwks = Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: "k1".to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    };
    let key_material = TenantKeyMaterial {
        kid: "k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key: TEST_PRIVATE_KEY,
        public_key,
        jwks: jwks.clone(),
    };
    let mut keys = HashMap::new();
    keys.insert(tenant_id.clone(), key_material);
    let key_store: Arc<dyn TenantKeyStore> = Arc::new(keys);
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        key_store,
    );
    let perms = vec![
        format!("stream.publish:stream:{tenant_id}/default/*"),
        format!("stream.subscribe:stream:{tenant_id}/default/*"),
        format!("cache.read:cache:{tenant_id}/default/*"),
        format!("cache.write:cache:{tenant_id}/default/*"),
    ];
    let token = issuer.mint(&TenantId::new(&tenant_id), "conformance", perms)?;

    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(&tenant_id), jwks);
    let broker_auth = Arc::new(BrokerAuth::with_key_store(key_store));
    Ok(AuthFixture {
        tenant_id,
        token,
        broker_auth,
    })
}

async fn run_pubsub(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running pub/sub checks...");
    let (mut sub_send, mut sub_recv) = connection.open_bi().await?;
    let mut frame_scratch = BytesMut::with_capacity(MAX_TEST_FRAME_BYTES.min(64 * 1024));
    quic::write_message(
        &mut sub_send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
        },
    )
    .await?;
    let auth_response =
        quic::read_message_limited(&mut sub_recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_ok_response(auth_response, "auth")?;
    quic::write_message(
        &mut sub_send,
        Message::Subscribe {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "conformance".to_string(),
            subscription_id: None,
        },
    )
    .await?;
    sub_send.finish()?;
    let response =
        quic::read_message_limited(&mut sub_recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    let expected_id = parse_subscribe_response(response)?;
    let mut event_recv = connection.accept_uni().await?;

    publish(connection, auth, b"alpha").await?;
    publish(connection, auth, b"beta").await?;

    let mut pending = VecDeque::new();
    if let Some(frame) = read_frame(&mut event_recv).await? {
        handle_event_frame(expected_id, frame, &mut pending, true)?;
    }

    let mut received = Vec::new();
    for _ in 0..2 {
        let payload = if let Some(payload) = pending.pop_front() {
            payload
        } else {
            let frame = read_frame(&mut event_recv)
                .await?
                .ok_or_else(|| anyhow!("subscription ended early"))?;
            handle_event_frame(expected_id, frame, &mut pending, false)?;
            pending
                .pop_front()
                .ok_or_else(|| anyhow!("subscription ended early"))?
        };
        received.push(payload);
    }

    ensure_event_order(&received)?;
    Ok(())
}

async fn publish(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
    payload: &[u8],
) -> Result<()> {
    static REQUEST_ID: AtomicU64 = AtomicU64::new(1);
    let request_id = REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut frame_scratch = BytesMut::with_capacity(MAX_TEST_FRAME_BYTES.min(64 * 1024));
    quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
        },
    )
    .await?;
    let auth_response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_ok_response(auth_response, "auth")?;
    quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "conformance".to_string(),
            payload: payload.to_vec(),
            request_id: Some(request_id),
            ack: Some(AckMode::PerMessage),
        },
    )
    .await?;
    send.finish()?;
    let response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_publish_ok(response, request_id)?;
    Ok(())
}

async fn run_cache(connection: &felix_transport::QuicConnection, auth: &AuthFixture) -> Result<()> {
    println!("Running cache checks...");
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut frame_scratch = BytesMut::with_capacity(MAX_TEST_FRAME_BYTES.min(64 * 1024));
    quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
        },
    )
    .await?;
    let auth_response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_ok_response(auth_response, "auth")?;
    quic::write_message(
        &mut send,
        Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "conformance-key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: None,
            ttl_ms: Some(100),
        },
    )
    .await?;
    send.finish()?;
    let response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_ok_response(response, "cache put")?;

    let value = cache_get(connection, auth, "conformance-key").await?;
    ensure_cache_value(value.clone(), Bytes::from_static(b"value"), "cache get")?;

    tokio::time::sleep(Duration::from_millis(150)).await;
    let expired = cache_get(connection, auth, "conformance-key").await?;
    ensure_cache_expired(expired, "cache entry should be expired")?;
    Ok(())
}

async fn run_client_pubsub(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running client pub/sub checks...");
    let client = Client::connect(addr, "localhost", build_client_config(cert, auth)?).await?;
    let mut subscription = client.subscribe("t1", "default", "conformance").await?;
    let publisher = client.publisher().await?;
    publisher
        .publish(
            "t1",
            "default",
            "conformance",
            b"client-alpha".to_vec(),
            AckMode::None,
        )
        .await?;
    let event = subscription
        .next_event()
        .await?
        .ok_or_else(|| anyhow!("client subscription ended early"))?;
    ensure_client_event(&event.payload, Bytes::from_static(b"client-alpha"))?;
    publisher.finish().await?;
    Ok(())
}

async fn run_client_cache(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running client cache checks...");
    let client = Client::connect(addr, "localhost", build_client_config(cert, auth)?).await?;
    client
        .cache_put(
            "t1",
            "default",
            "primary",
            "client-key",
            Bytes::from_static(b"value"),
            Some(100),
        )
        .await?;
    let value = client
        .cache_get("t1", "default", "primary", "client-key")
        .await?;
    ensure_cache_value(
        value.clone(),
        Bytes::from_static(b"value"),
        "client cache get",
    )?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let expired = client
        .cache_get("t1", "default", "primary", "client-key")
        .await?;
    ensure_cache_expired(expired, "client cache entry should be expired")?;
    Ok(())
}

async fn cache_get(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
    key: &str,
) -> Result<Option<Bytes>> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut frame_scratch = BytesMut::with_capacity(MAX_TEST_FRAME_BYTES.min(64 * 1024));
    quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
        },
    )
    .await?;
    let auth_response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_ok_response(auth_response, "auth")?;
    quic::write_message(
        &mut send,
        Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: key.to_string(),
            request_id: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    parse_cache_get_response(response)
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_quinn_client_config(cert: CertificateDer<'static>) -> Result<QuinnClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    Ok(quinn)
}

fn build_client_config(cert: CertificateDer<'static>, auth: &AuthFixture) -> Result<ClientConfig> {
    let quinn = build_quinn_client_config(cert)?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(auth.tenant_id.clone());
    config.auth_token = Some(auth.token.clone());
    Ok(config)
}

trait FrameReader {
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ReadExactError>> + 'a>>;
}

impl FrameReader for RecvStream {
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ReadExactError>> + 'a>> {
        Box::pin(RecvStream::read_exact(self, buf))
    }
}

async fn read_frame<R: FrameReader + ?Sized>(recv: &mut R) -> Result<Option<Frame>> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;
    let mut payload = vec![0u8; length];
    recv.read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    Ok(Some(Frame {
        header,
        payload: Bytes::from(payload),
    }))
}

fn handle_event_frame(
    expected_id: Option<u64>,
    frame: Frame,
    pending: &mut VecDeque<Vec<u8>>,
    allow_hello: bool,
) -> Result<()> {
    if frame.header.flags & FLAG_BINARY_EVENT_BATCH != 0 {
        if expected_id.is_some() && allow_hello {
            return Err(anyhow!("missing event stream hello for subscription"));
        }
        let batch =
            felix_wire::binary::decode_event_batch(&frame).context("decode binary event batch")?;
        if let Some(expected) = expected_id
            && expected != batch.subscription_id
        {
            return Err(anyhow!(
                "subscription id mismatch: expected {expected} got {}",
                batch.subscription_id
            ));
        }
        for payload in batch.payloads {
            pending.push_back(payload.to_vec());
        }
        return Ok(());
    }

    let message = Message::decode(frame).context("decode event message")?;
    match message {
        Message::EventStreamHello { subscription_id } => {
            if !allow_hello {
                return Err(anyhow!("unexpected hello after subscription start"));
            }
            if let Some(expected) = expected_id {
                if expected != subscription_id {
                    return Err(anyhow!(
                        "subscription id mismatch: expected {expected} got {subscription_id}"
                    ));
                }
            } else {
                return Err(anyhow!("unexpected hello for legacy subscription"));
            }
        }
        Message::Event { payload, .. } => {
            if expected_id.is_some() && allow_hello {
                return Err(anyhow!("missing event stream hello for subscription"));
            }
            pending.push_back(payload);
        }
        Message::EventBatch { payloads, .. } => {
            if expected_id.is_some() && allow_hello {
                return Err(anyhow!("missing event stream hello for subscription"));
            }
            for payload in payloads {
                pending.push_back(payload);
            }
        }
        other => return Err(anyhow!("unexpected message: {other:?}")),
    }
    Ok(())
}

fn parse_subscribe_response(response: Option<Message>) -> Result<Option<u64>> {
    match response {
        Some(Message::Subscribed { subscription_id }) => Ok(Some(subscription_id)),
        Some(Message::Ok) => Ok(None),
        other => Err(anyhow!("subscribe failed: {other:?}")),
    }
}

fn ensure_publish_ok(response: Option<Message>, request_id: u64) -> Result<()> {
    if response != Some(Message::PublishOk { request_id }) {
        return Err(anyhow!("publish failed: {response:?}"));
    }
    Ok(())
}

fn ensure_ok_response(response: Option<Message>, context: &str) -> Result<()> {
    if response != Some(Message::Ok) {
        return Err(anyhow!("{context} failed: {response:?}"));
    }
    Ok(())
}

fn parse_cache_get_response(response: Option<Message>) -> Result<Option<Bytes>> {
    match response {
        Some(Message::CacheValue { value, .. }) => Ok(value),
        other => Err(anyhow!("unexpected cache response: {other:?}")),
    }
}

fn ensure_event_order(received: &[Vec<u8>]) -> Result<()> {
    if received != [b"alpha".to_vec(), b"beta".to_vec()] {
        return Err(anyhow!("unexpected event order: {received:?}"));
    }
    Ok(())
}

fn ensure_cache_value(value: Option<Bytes>, expected: Bytes, context: &str) -> Result<()> {
    if value != Some(expected) {
        return Err(anyhow!("{context} mismatch: {value:?}"));
    }
    Ok(())
}

fn ensure_cache_expired(value: Option<Bytes>, context: &str) -> Result<()> {
    if value.is_some() {
        return Err(anyhow!("{context}"));
    }
    Ok(())
}

fn ensure_client_event(payload: &Bytes, expected: Bytes) -> Result<()> {
    if payload != &expected {
        return Err(anyhow!("client event mismatch: {:?}", payload));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use felix_wire::binary;

    struct TestReader {
        data: Vec<u8>,
        pos: usize,
        read_calls: usize,
        error_on_call: Option<(usize, ReadExactError)>,
    }

    impl TestReader {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data,
                pos: 0,
                read_calls: 0,
                error_on_call: None,
            }
        }

        fn with_error_on_call(mut self, call: usize, error: ReadExactError) -> Self {
            self.error_on_call = Some((call, error));
            self
        }
    }

    impl FrameReader for TestReader {
        fn read_exact<'a>(
            &'a mut self,
            buf: &'a mut [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), ReadExactError>> + 'a>> {
            Box::pin(async move {
                self.read_calls += 1;
                if let Some((call, err)) = &self.error_on_call
                    && *call == self.read_calls
                {
                    return Err(err.clone());
                }
                let remaining = self.data.len().saturating_sub(self.pos);
                if remaining < buf.len() {
                    let read = remaining;
                    if read > 0 {
                        buf[..read].copy_from_slice(&self.data[self.pos..self.pos + read]);
                        self.pos += read;
                    }
                    return Err(ReadExactError::FinishedEarly(read));
                }
                buf.copy_from_slice(&self.data[self.pos..self.pos + buf.len()]);
                self.pos += buf.len();
                Ok(())
            })
        }
    }

    fn encode_frame_bytes(flags: u16, payload: &[u8]) -> Vec<u8> {
        let header = FrameHeader::new(flags, payload.len() as u32);
        let mut header_bytes = [0u8; FrameHeader::LEN];
        header.encode_into(&mut header_bytes);
        let mut bytes = Vec::with_capacity(FrameHeader::LEN + payload.len());
        bytes.extend_from_slice(&header_bytes);
        bytes.extend_from_slice(payload);
        bytes
    }

    fn message_frame(message: Message) -> Frame {
        message.encode().expect("encode message")
    }

    fn binary_event_frame(subscription_id: u64, payloads: &[Bytes]) -> Frame {
        let bytes = binary::encode_event_batch_bytes(subscription_id, payloads)
            .expect("encode binary batch");
        Frame::decode(bytes).expect("decode frame")
    }

    #[test]
    fn conformance_main_smoke() {
        super::main().expect("conformance run");
    }

    #[test]
    fn parse_subscribe_response_variants() {
        assert_eq!(
            parse_subscribe_response(Some(Message::Subscribed { subscription_id: 7 })).expect("ok"),
            Some(7)
        );
        assert_eq!(
            parse_subscribe_response(Some(Message::Ok)).expect("ok"),
            None
        );
        assert!(
            parse_subscribe_response(Some(Message::Error {
                message: "nope".into()
            }))
            .is_err()
        );
        assert!(parse_subscribe_response(None).is_err());
    }

    #[test]
    fn ensure_publish_ok_variants() {
        ensure_publish_ok(Some(Message::PublishOk { request_id: 9 }), 9).expect("ok");
        assert!(ensure_publish_ok(Some(Message::PublishOk { request_id: 8 }), 9).is_err());
        assert!(
            ensure_publish_ok(
                Some(Message::Error {
                    message: "no".into()
                }),
                9
            )
            .is_err()
        );
        assert!(ensure_publish_ok(None, 9).is_err());
    }

    #[test]
    fn ensure_ok_response_variants() {
        ensure_ok_response(Some(Message::Ok), "cache put").expect("ok");
        assert!(
            ensure_ok_response(
                Some(Message::Error {
                    message: "no".into()
                }),
                "cache put"
            )
            .is_err()
        );
        assert!(ensure_ok_response(None, "cache put").is_err());
    }

    #[test]
    fn parse_cache_get_response_variants() {
        let value = Bytes::from_static(b"value");
        assert_eq!(
            parse_cache_get_response(Some(Message::CacheValue {
                tenant_id: "t1".into(),
                namespace: "default".into(),
                cache: "primary".into(),
                key: "k".into(),
                value: Some(value.clone()),
                request_id: None
            }))
            .expect("ok"),
            Some(value)
        );
        assert!(parse_cache_get_response(Some(Message::Ok)).is_err());
        assert!(parse_cache_get_response(None).is_err());
    }

    #[test]
    fn ensure_event_order_variants() {
        ensure_event_order(&[b"alpha".to_vec(), b"beta".to_vec()]).expect("ok");
        assert!(ensure_event_order(&[b"beta".to_vec(), b"alpha".to_vec()]).is_err());
    }

    #[test]
    fn ensure_cache_value_variants() {
        let expected = Bytes::from_static(b"value");
        ensure_cache_value(Some(expected.clone()), expected.clone(), "cache get").expect("ok");
        assert!(ensure_cache_value(None, expected.clone(), "cache get").is_err());
        assert!(
            ensure_cache_value(Some(Bytes::from_static(b"nope")), expected, "cache get").is_err()
        );
    }

    #[test]
    fn ensure_cache_expired_variants() {
        ensure_cache_expired(None, "expired").expect("ok");
        assert!(ensure_cache_expired(Some(Bytes::from_static(b"value")), "expired").is_err());
    }

    #[test]
    fn ensure_client_event_variants() {
        ensure_client_event(&Bytes::from_static(b"alpha"), Bytes::from_static(b"alpha"))
            .expect("ok");
        assert!(
            ensure_client_event(&Bytes::from_static(b"alpha"), Bytes::from_static(b"beta"))
                .is_err()
        );
    }

    #[tokio::test]
    async fn read_frame_success_and_errors() {
        let payload = b"hello";
        let bytes = encode_frame_bytes(0, payload);
        let mut reader = TestReader::new(bytes);
        let frame = read_frame(&mut reader).await.expect("ok").expect("frame");
        assert_eq!(frame.payload, Bytes::from_static(payload));

        let mut early = TestReader::new(Vec::new());
        assert!(read_frame(&mut early).await.expect("ok").is_none());

        let mut error = TestReader::new(Vec::new())
            .with_error_on_call(1, ReadExactError::ReadError(quinn::ReadError::ClosedStream));
        assert!(read_frame(&mut error).await.is_err());

        let bad_header = encode_frame_bytes(0, payload);
        let mut bad_magic = TestReader::new(bad_header);
        bad_magic.data[0] = 0x00;
        assert!(read_frame(&mut bad_magic).await.is_err());

        let mut payload_error = TestReader::new(encode_frame_bytes(0, payload))
            .with_error_on_call(2, ReadExactError::ReadError(quinn::ReadError::ClosedStream));
        assert!(read_frame(&mut payload_error).await.is_err());

        let mut short_payload = TestReader::new(encode_frame_bytes(0, payload));
        short_payload.data.truncate(FrameHeader::LEN + 2);
        assert!(read_frame(&mut short_payload).await.is_err());
    }

    #[test]
    fn handle_event_frame_binary_paths() {
        let mut pending = VecDeque::new();
        let frame = binary_event_frame(7, &[Bytes::from_static(b"one")]);
        assert!(handle_event_frame(Some(7), frame.clone(), &mut pending, true).is_err());

        let mut pending = VecDeque::new();
        assert!(handle_event_frame(Some(8), frame.clone(), &mut pending, false).is_err());

        let mut pending = VecDeque::new();
        handle_event_frame(Some(7), frame, &mut pending, false).expect("ok");
        assert_eq!(pending.pop_front(), Some(b"one".to_vec()));

        let mut pending = VecDeque::new();
        let bad = Frame {
            header: FrameHeader::new(FLAG_BINARY_EVENT_BATCH, 2),
            payload: Bytes::from_static(b"hi"),
        };
        assert!(handle_event_frame(None, bad, &mut pending, false).is_err());
    }

    #[test]
    fn handle_event_frame_message_paths() {
        let mut pending = VecDeque::new();
        let hello = message_frame(Message::EventStreamHello { subscription_id: 9 });
        assert!(handle_event_frame(Some(9), hello.clone(), &mut pending, false).is_err());
        assert!(handle_event_frame(Some(8), hello.clone(), &mut pending, true).is_err());
        assert!(handle_event_frame(None, hello.clone(), &mut pending, true).is_err());
        handle_event_frame(Some(9), hello, &mut pending, true).expect("ok");

        let mut pending = VecDeque::new();
        let event = message_frame(Message::Event {
            tenant_id: "t1".into(),
            namespace: "default".into(),
            stream: "conformance".into(),
            payload: b"payload".to_vec(),
        });
        assert!(handle_event_frame(Some(1), event.clone(), &mut pending, true).is_err());
        handle_event_frame(None, event, &mut pending, true).expect("ok");
        assert_eq!(pending.pop_front(), Some(b"payload".to_vec()));

        let mut pending = VecDeque::new();
        let batch = message_frame(Message::EventBatch {
            tenant_id: "t1".into(),
            namespace: "default".into(),
            stream: "conformance".into(),
            payloads: vec![b"a".to_vec(), b"b".to_vec()],
        });
        assert!(handle_event_frame(Some(1), batch.clone(), &mut pending, true).is_err());
        handle_event_frame(None, batch, &mut pending, false).expect("ok");
        assert_eq!(pending.pop_front(), Some(b"a".to_vec()));
        assert_eq!(pending.pop_front(), Some(b"b".to_vec()));

        let mut pending = VecDeque::new();
        let other = message_frame(Message::Ok);
        assert!(handle_event_frame(None, other, &mut pending, false).is_err());
    }
}
