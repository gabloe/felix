//! Transport behaviour under reuse, restart, loss, overload, and shutdown.
//!
//! Everything here drives the real endpoints over loopback. A stub would let the
//! two halves drift: the pool could send a frame the server never reads, or wait
//! on a response shape the server never sends, and every test would still pass.
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use felix_wire::internal::{
    AckMode, ErrorCode, ForwardPublish, ForwardPublishOk, InternalMessage, ShardRef,
};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::peer::server::PeerRequestHandler;

const PEER: &str = "broker-b";

fn config() -> PeerTransportConfig {
    PeerTransportConfig {
        // Port 0: every test binds its own listener, so nothing here depends on
        // a fixed port being free.
        bind: "127.0.0.1:0".parse().expect("addr"),
        request_timeout: Duration::from_millis(500),
        handshake_timeout: Duration::from_millis(500),
        reconnect_base: Duration::from_millis(20),
        reconnect_max: Duration::from_millis(40),
        ..Default::default()
    }
}

fn shard() -> ShardRef {
    ShardRef {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        generation: 1,
    }
}

fn forward() -> InternalMessage {
    InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 0,
        shard: shard(),
        ack: AckMode::OnCommit,
        payloads: vec![bytes::Bytes::from_static(b"payload")],
    })
}

/// Answers every forward with `Ok`, and counts what it saw.
#[derive(Default)]
struct CountingHandler {
    seen: AtomicUsize,
    /// Delay before answering, for the overload and timeout cases.
    delay: Duration,
}

#[async_trait::async_trait]
impl PeerRequestHandler for CountingHandler {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        self.seen.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        InternalMessage::ForwardPublishOk(ForwardPublishOk {
            correlation_id: request.correlation_id(),
            first_offset: 10,
            last_offset: 10,
        })
    }
}

/// A running internal listener, with the handle needed to stop it.
struct Listener {
    addr: SocketAddr,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Listener {
    async fn start(node_id: &str, handler: Arc<dyn PeerRequestHandler>) -> Self {
        Self::start_on(node_id, handler, config()).await
    }

    async fn start_on(
        node_id: &str,
        handler: Arc<dyn PeerRequestHandler>,
        config: PeerTransportConfig,
    ) -> Self {
        let server = Self::bind_with_retry(node_id, handler, &config).await;
        let addr = server.local_addr().expect("addr");
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(server.serve(shutdown.clone()));
        Self {
            addr,
            shutdown,
            task,
        }
    }

    /// The restart case rebinds the port the previous listener held, and quinn
    /// releases it only once that endpoint's connections have finished draining.
    ///
    /// The wait must yield to the runtime. `#[tokio::test]` is single-threaded,
    /// so a blocking sleep here would stop the very tasks that close those
    /// connections, and the port would never come free however long we waited.
    async fn bind_with_retry(
        node_id: &str,
        handler: Arc<dyn PeerRequestHandler>,
        config: &PeerTransportConfig,
    ) -> PeerServer {
        let mut last = None;
        for _ in 0..100 {
            match PeerServer::bind(node_id.to_string(), config, Arc::clone(&handler)) {
                Ok(server) => return server,
                Err(err) => {
                    last = Some(err);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        panic!("bind never succeeded: {:#}", last.expect("an error"));
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), self.task).await;
    }
}

fn pool(config: PeerTransportConfig) -> Arc<PeerPool> {
    PeerPool::new("broker-a".to_string(), config, CancellationToken::new()).expect("pool")
}

/// Repeated requests to one peer must not redial it. One connection, and the
/// handshake happens once.
#[tokio::test]
async fn repeated_requests_reuse_one_connection() {
    let handler = Arc::new(CountingHandler::default());
    let listener = Listener::start(PEER, handler.clone()).await;
    let pool = pool(config());

    for _ in 0..20 {
        let response = pool
            .request(PEER, listener.addr, forward())
            .await
            .expect("request");
        assert!(matches!(response, InternalMessage::ForwardPublishOk(_)));
    }

    assert_eq!(pool.connection_count().await, 1, "one peer, one connection");
    assert_eq!(
        handler.seen.load(Ordering::SeqCst),
        20,
        "the handshake must not be re-run per request",
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// Responses are matched by correlation id, so concurrent requests on shared
/// streams must not cross.
#[tokio::test]
async fn concurrent_requests_do_not_cross_responses() {
    /// Echoes the shard's `generation` back as `first_offset`, so a crossed
    /// response is detectable rather than merely plausible.
    struct EchoHandler;

    #[async_trait::async_trait]
    impl PeerRequestHandler for EchoHandler {
        async fn handle(&self, request: InternalMessage) -> InternalMessage {
            let generation = match &request {
                InternalMessage::ForwardPublish(publish) => publish.shard.generation,
                _ => 0,
            };
            // A staggered delay guarantees responses are produced out of the
            // order the requests arrived in.
            tokio::time::sleep(Duration::from_millis(generation % 7)).await;
            InternalMessage::ForwardPublishOk(ForwardPublishOk {
                correlation_id: request.correlation_id(),
                first_offset: generation,
                last_offset: generation,
            })
        }
    }

    let listener = Listener::start(PEER, Arc::new(EchoHandler)).await;
    let pool = pool(config());

    let mut tasks = Vec::new();
    for generation in 0..64u64 {
        let pool = Arc::clone(&pool);
        let addr = listener.addr;
        tasks.push(tokio::spawn(async move {
            let mut message = shard();
            message.generation = generation;
            let request = InternalMessage::ForwardPublish(ForwardPublish {
                correlation_id: 0,
                shard: message,
                ack: AckMode::None,
                payloads: vec![],
            });
            let response = pool.request(PEER, addr, request).await.expect("request");
            match response {
                InternalMessage::ForwardPublishOk(ok) => (generation, ok.first_offset),
                other => panic!("unexpected {other:?}"),
            }
        }));
    }

    for task in tasks {
        let (sent, echoed) = task.await.expect("join");
        assert_eq!(sent, echoed, "response landed on the wrong request");
    }

    pool.shutdown().await;
    listener.stop().await;
}

/// A peer that restarts at the same address must be usable again, without the
/// pool needing to be told.
#[tokio::test]
async fn a_restarted_peer_is_reconnected_to() {
    // A fixed port, because the restarted listener has to come back at the
    // address the pool already holds a dead connection to.
    let first = Listener::start_on(
        PEER,
        Arc::new(CountingHandler::default()),
        PeerTransportConfig {
            bind: "127.0.0.1:0".parse().expect("addr"),
            ..config()
        },
    )
    .await;
    let addr = first.addr;
    let pool = pool(config());

    pool.request(PEER, addr, forward()).await.expect("first");
    first.stop().await;

    // The pool cannot know the peer is gone until it tries, so the request that
    // spans the restart is allowed to fail. What must not happen is failing
    // forever.
    let second = Listener::start_on(
        PEER,
        Arc::new(CountingHandler::default()),
        PeerTransportConfig {
            bind: addr,
            ..config()
        },
    )
    .await;

    let mut recovered = false;
    for _ in 0..50 {
        if pool.request(PEER, addr, forward()).await.is_ok() {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        recovered,
        "the pool never reconnected to the restarted peer"
    );

    pool.shutdown().await;
    second.stop().await;
}

/// A connection that drops with requests in flight must fail them at that
/// moment. `docs/internal-protocol.md` requires no forwarded publish is left
/// pending, and waiting out the request timeout is not the same thing.
#[tokio::test]
async fn connection_loss_fails_in_flight_requests_immediately() {
    let listener = Listener::start(
        PEER,
        Arc::new(CountingHandler {
            seen: AtomicUsize::new(0),
            // Longer than the test waits, so nothing answers on its own.
            delay: Duration::from_secs(30),
        }),
    )
    .await;
    let pool = pool(PeerTransportConfig {
        // Far longer than the connection will live: if the request only fails
        // when this expires, the test times out instead of passing.
        request_timeout: Duration::from_secs(30),
        ..config()
    });

    let addr = listener.addr;
    let requester = {
        let pool = Arc::clone(&pool);
        tokio::spawn(async move { pool.request(PEER, addr, forward()).await })
    };

    // Let the request reach the peer before taking the listener away. The pool
    // is deliberately *not* shut down here: cancelling it would end the request
    // for its own reason and prove nothing about the connection dropping.
    tokio::time::sleep(Duration::from_millis(100)).await;
    listener.stop().await;

    let result = tokio::time::timeout(Duration::from_secs(5), requester)
        .await
        .expect("the in-flight request was left pending")
        .expect("join");
    let err = result.expect_err("a lost connection must fail its requests");
    assert!(
        matches!(err, PeerError::Disconnected { .. }),
        "the loss must be reported as a loss, not as a timeout: {err:?}",
    );

    pool.shutdown().await;
}

/// An unhealthy peer must not be able to consume this broker. At the in-flight
/// limit, requests are shed rather than queued.
#[tokio::test]
async fn a_peer_at_its_inflight_limit_sheds_rather_than_queues() {
    let listener = Listener::start(
        PEER,
        Arc::new(CountingHandler {
            seen: AtomicUsize::new(0),
            delay: Duration::from_secs(30),
        }),
    )
    .await;
    let pool = pool(PeerTransportConfig {
        max_inflight_per_peer: 4,
        request_timeout: Duration::from_secs(10),
        ..config()
    });

    let mut inflight = Vec::new();
    for _ in 0..4 {
        let pool = Arc::clone(&pool);
        let addr = listener.addr;
        inflight.push(tokio::spawn(async move {
            pool.request(PEER, addr, forward()).await
        }));
    }
    // Let the four occupy their permits before the fifth arrives.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let shed = tokio::time::timeout(
        Duration::from_millis(500),
        pool.request(PEER, listener.addr, forward()),
    )
    .await
    .expect("the request over the limit blocked instead of shedding")
    .expect_err("it must be refused");

    assert!(
        matches!(shed, PeerError::Unavailable { .. }),
        "shedding is not the same as failing: {shed:?}",
    );
    assert!(
        shed.is_retryable(),
        "nothing was sent, so this is safe to retry elsewhere",
    );

    pool.shutdown().await;
    for task in inflight {
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }
    listener.stop().await;
}

/// A peer that accepts a request and never answers must not hold a waiter open
/// past the request timeout.
#[tokio::test]
async fn a_silent_peer_times_out() {
    let listener = Listener::start(
        PEER,
        Arc::new(CountingHandler {
            seen: AtomicUsize::new(0),
            delay: Duration::from_secs(30),
        }),
    )
    .await;
    let pool = pool(PeerTransportConfig {
        request_timeout: Duration::from_millis(200),
        ..config()
    });

    let err = pool
        .request(PEER, listener.addr, forward())
        .await
        .expect_err("a silent peer must time out");
    assert!(matches!(err, PeerError::Timeout { .. }), "{err:?}");
    assert!(
        !err.is_retryable(),
        "the peer may have applied the write, so a retry is a duplicate",
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// A peer that cannot be reached at all must back off rather than dial in a
/// loop, and must say so without sending anything.
#[tokio::test]
async fn an_unreachable_peer_backs_off() {
    // Nothing is listening here: a bound-then-dropped socket leaves an address
    // that is routable and refuses.
    let dead: SocketAddr = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind");
        socket.local_addr().expect("addr")
    };
    let pool = pool(PeerTransportConfig {
        handshake_timeout: Duration::from_millis(100),
        reconnect_base: Duration::from_millis(200),
        reconnect_max: Duration::from_millis(200),
        ..config()
    });

    let first = pool.request(PEER, dead, forward()).await;
    assert!(first.is_err(), "an unreachable peer must fail");

    // Inside the backoff window the next request must be refused without a dial,
    // which is what makes a dead peer cheap rather than a dial loop.
    let started = std::time::Instant::now();
    let second = pool
        .request(PEER, dead, forward())
        .await
        .expect_err("still unreachable");
    assert!(
        started.elapsed() < Duration::from_millis(50),
        "the backoff window was not honoured; it dialled again",
    );
    assert!(
        matches!(second, PeerError::Unavailable { .. }),
        "{second:?}"
    );

    pool.shutdown().await;
}

/// Connecting to a broker that is not the one the catalog named is a wrong
/// connection even if it would have answered.
#[tokio::test]
async fn a_peer_that_identifies_as_someone_else_is_refused() {
    let listener = Listener::start("broker-c", Arc::new(CountingHandler::default())).await;
    let pool = pool(config());

    let err = pool
        .request(PEER, listener.addr, forward())
        .await
        .expect_err("identity mismatch must be refused");
    assert!(matches!(err, PeerError::Handshake { .. }), "{err:?}");
    assert!(
        err.to_string().contains("broker-c"),
        "the error should name who actually answered: {err}",
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// A client-facing client must not be able to reach the internal listener.
///
/// The refusal happens in the TLS handshake: the internal listener requires
/// `felix-internal/1` and the client-facing role negotiates no ALPN at all, so
/// there is no protocol in common. That is what makes the two roles impossible
/// to confuse by pointing a client at the wrong port — it fails before a frame
/// is ever read, and before any broker state is touched.
#[tokio::test]
async fn the_internal_listener_refuses_a_client_facing_connection() {
    use felix_transport::{QuicClient, TransportConfig};

    let listener = Listener::start(PEER, Arc::new(CountingHandler::default())).await;

    // A client-shaped endpoint: no ALPN, exactly like the client-facing role.
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(rustls::ALL_VERSIONS)
    .expect("protocol versions")
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AcceptAnything))
    .with_no_client_auth();
    tls.alpn_protocols.clear();
    let quinn = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("crypto"),
    ));
    let client = QuicClient::bind(
        "127.0.0.1:0".parse().expect("addr"),
        quinn,
        TransportConfig::default(),
    )
    .expect("bind");

    let refused = client.connect(listener.addr, "felix-internal").await;
    assert!(
        refused.is_err(),
        "a client with no ALPN reached the internal listener",
    );

    // The listener is still serving: the refusal is per connection, not a
    // failure that takes this broker out of the cluster.
    let pool = pool(config());
    pool.request(PEER, listener.addr, forward())
        .await
        .expect("a real peer must still be served");

    pool.shutdown().await;
    listener.stop().await;
}

#[derive(Debug)]
struct AcceptAnything;

impl rustls::client::danger::ServerCertVerifier for AcceptAnything {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Shutdown must close connections and leave nothing running. A leaked pump
/// task holds a stream handle, which keeps the connection alive after the pool
/// has forgotten it.
#[tokio::test]
async fn shutdown_closes_connections_and_refuses_new_requests() {
    let listener = Listener::start(PEER, Arc::new(CountingHandler::default())).await;
    let pool = pool(config());
    pool.request(PEER, listener.addr, forward())
        .await
        .expect("request");
    assert_eq!(pool.connection_count().await, 1);

    pool.shutdown().await;
    assert_eq!(
        pool.connection_count().await,
        0,
        "connections were not closed"
    );

    let err = pool
        .request(PEER, listener.addr, forward())
        .await
        .expect_err("a shut-down pool must not dial");
    assert!(matches!(err, PeerError::ShuttingDown), "{err:?}");

    listener.stop().await;
}

/// Until forwarding is wired (#106), the listener must say so rather than
/// leaving a peer to time out.
#[tokio::test]
async fn the_default_handler_answers_unavailable() {
    let listener = Listener::start(PEER, Arc::new(UnavailableHandler)).await;
    let pool = pool(config());

    let response = pool
        .request(PEER, listener.addr, forward())
        .await
        .expect("request");
    match response {
        InternalMessage::ForwardPublishError(err) => {
            assert_eq!(err.code, ErrorCode::Unavailable);
            assert!(err.code.is_retryable());
        }
        other => panic!("unexpected {other:?}"),
    }

    pool.shutdown().await;
    listener.stop().await;
}

/// Backoff is jittered so that every broker noticing the same peer restart does
/// not redial it in step.
#[test]
fn reconnect_backoff_grows_and_is_jittered() {
    let config = PeerTransportConfig {
        reconnect_base: Duration::from_millis(100),
        reconnect_max: Duration::from_millis(5_000),
        ..Default::default()
    };

    for attempt in 0..6 {
        let ceiling = config
            .reconnect_base
            .saturating_mul(1 << attempt)
            .min(config.reconnect_max);
        for _ in 0..32 {
            assert!(
                config.reconnect_delay(attempt) <= ceiling,
                "attempt {attempt} exceeded its ceiling",
            );
        }
    }

    // Capped, and never longer than the configured maximum however many
    // attempts have failed.
    assert!(config.reconnect_delay(40) <= config.reconnect_max);

    let samples: std::collections::HashSet<_> =
        (0..64).map(|_| config.reconnect_delay(4)).collect();
    assert!(
        samples.len() > 1,
        "an unjittered backoff has every broker redial in step",
    );
}
