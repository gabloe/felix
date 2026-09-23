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
        credential: String::new(),
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
                credential: String::new(),
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
    // A window no CI stall outlives, so the second request below is inside it
    // however slowly this test is scheduled. The proof of "no dial" is the
    // error variant, not the clock: a refusal inside the window is
    // `Unavailable`, while an actual dial to this address fails as a connect
    // error. Asserting on elapsed time was a proxy for the same thing and
    // flaked whenever the runner stalled past the deadline.
    let pool = pool(PeerTransportConfig {
        handshake_timeout: Duration::from_millis(100),
        reconnect_base: Duration::from_secs(60),
        reconnect_max: Duration::from_secs(60),
        ..config()
    });

    let first = pool.request(PEER, dead, forward()).await;
    assert!(first.is_err(), "an unreachable peer must fail");

    // Inside the backoff window the next request must be refused without a dial,
    // which is what makes a dead peer cheap rather than a dial loop.
    let second = pool
        .request(PEER, dead, forward())
        .await
        .expect_err("still unreachable");
    assert!(
        matches!(second, PeerError::Unavailable { .. }),
        "the backoff window was not honoured; it dialled again: {second:?}"
    );
    assert!(
        second.to_string().contains("reconnecting in"),
        "the refusal should say it is backing off: {second}"
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

/// **A peer connection gives up before a request on it does.** A broker that
/// was killed leaves its peers holding connections nothing will tear down; if
/// QUIC outlasts the request timeout, every forwarded publish and every
/// replication pass sent over one waits the request timeout out in full.
#[test]
fn a_peer_connection_fails_before_the_request_on_it_does() {
    let config = PeerTransportConfig::default();
    assert!(
        config.peer_idle_timeout() < config.request_timeout,
        "idle window {:?} does not close before a request gives up at {:?}",
        config.peer_idle_timeout(),
        config.request_timeout,
    );
    let transport = config.quic_transport();
    assert_eq!(
        transport.max_idle_timeout,
        Some(config.peer_idle_timeout()),
        "the transport does not use the derived idle window",
    );
}

/// The keep-alive has to fit inside the idle window, or a healthy but quiet
/// peer connection is closed on the idle timer for having nothing to say.
#[test]
fn a_quiet_peer_connection_is_kept_alive_inside_its_idle_window() {
    let transport = PeerTransportConfig::default().quic_transport();
    let keep_alive = transport.keep_alive_interval.expect("a keep-alive");
    let idle = transport.max_idle_timeout.expect("an idle window");
    assert!(
        keep_alive * 3 <= idle,
        "keep-alive {keep_alive:?} leaves no margin inside idle window {idle:?}",
    );
}

/// **A kind this broker does not know is refused, and the stream lives.**
///
/// Adding a `Kind` is the protocol's sanctioned additive change, and it is only
/// additive if an older peer can refuse one frame instead of dropping the lane.
/// These streams are long-lived and multiplex every in-flight request, so
/// killing one on an unrecognised frame turns "the peer is newer than me" into
/// "every request in flight to that peer failed" — and made adding a kind a
/// cutover rather than an upgrade.
///
/// The frozen header is what makes stepping over it possible: it says how long
/// the body is, and every body begins with its correlation id, so the refusal
/// can name the request that caused it.
#[tokio::test]
async fn a_frame_kind_this_broker_does_not_know_is_refused_without_ending_the_stream() {
    use bytes::BufMut;
    use felix_transport::{QuicClient, TransportConfig};

    let listener = Listener::start(PEER, Arc::new(CountingHandler::default())).await;

    let client = QuicClient::bind(
        "127.0.0.1:0".parse().expect("addr"),
        crate::peer::tls::client_config(None).expect("client config"),
        TransportConfig::default(),
    )
    .expect("bind");
    let connection = client
        .connect(listener.addr, "felix-internal")
        .await
        .expect("connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("open");

    // A frame from some later build: our framing, our version, a kind that does
    // not exist yet, and a body whose first eight bytes are the correlation id
    // every body starts with.
    const FROM_THE_FUTURE: u16 = 60_000;
    const CORRELATION: u64 = 0x5eed_1234_5eed_1234;
    let mut frame = bytes::BytesMut::new();
    frame.put_u32(felix_wire::internal::INTERNAL_MAGIC);
    frame.put_u16(felix_wire::internal::INTERNAL_VERSION);
    frame.put_u16(FROM_THE_FUTURE);
    frame.put_u32(12);
    frame.put_u64(CORRELATION);
    frame.put_u32(0xabad_1dea);
    send.write_all(&frame).await.expect("write");

    let refusal = crate::peer::codec::read_frame(&mut recv)
        .await
        .expect("the stream must still be readable");
    let crate::peer::codec::Incoming::Message(InternalMessage::ForwardPublishError(err)) = refusal
    else {
        panic!("expected a typed refusal, got something else");
    };
    assert_eq!(err.code, ErrorCode::UnsupportedKind);
    assert_eq!(
        err.correlation_id, CORRELATION,
        "the refusal did not name the request, so a caller could not match it",
    );

    // The point of the whole thing: the same stream still serves.
    crate::peer::codec::write_frame(&mut send, &forward())
        .await
        .expect("write a request this broker does know");
    let answer = crate::peer::codec::read_frame(&mut recv)
        .await
        .expect("read");
    assert!(
        matches!(answer, crate::peer::codec::Incoming::Message(_)),
        "the stream was dropped after the unknown kind, so every request \
         sharing it would have failed",
    );

    connection.close(0u32.into(), b"done");
    listener.stop().await;
}

/// **The listener refuses past its inbound limit rather than accepting
/// everything.**
///
/// QUIC caps streams per connection and the decoder caps a frame, but nothing
/// capped *connections* — so one caller could make a broker hold 64 MiB × 1024
/// streams × however many it opened (#504). The cap survives peer
/// authentication too: an authenticated peer looping on a reconnect bug is
/// still unbounded, and is likelier than a hostile one.
#[tokio::test]
async fn the_listener_refuses_past_its_inbound_connection_limit() {
    use felix_transport::{QuicClient, TransportConfig};

    let mut config = config();
    config.max_inbound_connections = 2;
    config.max_inbound_per_source = 2;
    let listener = Listener::start_on(PEER, Arc::new(CountingHandler::default()), config).await;

    let client = QuicClient::bind(
        "127.0.0.1:0".parse().expect("addr"),
        crate::peer::tls::client_config(None).expect("client config"),
        TransportConfig::default(),
    )
    .expect("bind");

    // Two are served, and stay open.
    let mut held = Vec::new();
    for _ in 0..2 {
        let connection = client
            .connect(listener.addr, "felix-internal")
            .await
            .expect("within the limit");
        // Exercised, not merely established: a connection the listener has not
        // yet counted proves nothing about the limit.
        let (mut send, mut recv) = connection.open_bi().await.expect("open");
        crate::peer::codec::write_frame(&mut send, &forward())
            .await
            .expect("write");
        crate::peer::codec::read_frame(&mut recv)
            .await
            .expect("read");
        held.push(connection);
    }

    // The third is refused. The handshake succeeds — the listener closes it
    // after, which is what a QUIC peer at its limit can do — so the refusal
    // shows up on first use rather than on connect.
    let refused = client.connect(listener.addr, "felix-internal").await;
    let over_limit = match refused {
        Err(_) => true,
        Ok(connection) => {
            let opened = connection.open_bi().await;
            match opened {
                Err(_) => true,
                Ok((mut send, mut recv)) => {
                    let wrote = crate::peer::codec::write_frame(&mut send, &forward()).await;
                    wrote.is_err()
                        || !matches!(
                            crate::peer::codec::read_frame(&mut recv).await,
                            Ok(crate::peer::codec::Incoming::Message(_))
                        )
                }
            }
        }
    };
    assert!(
        over_limit,
        "a third connection was served against a limit of two, so the cap does \
         not bound anything",
    );

    // And dropping one gives its place back, or the broker stops accepting
    // peers after an uptime nobody can correlate with anything.
    held.pop();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after_release = client
        .connect(listener.addr, "felix-internal")
        .await
        .expect("connect after a release");
    let (mut send, mut recv) = after_release.open_bi().await.expect("open");
    crate::peer::codec::write_frame(&mut send, &forward())
        .await
        .expect("write");
    assert!(
        matches!(
            crate::peer::codec::read_frame(&mut recv).await,
            Ok(crate::peer::codec::Incoming::Message(_))
        ),
        "a released place was never given back",
    );

    listener.stop().await;
}

/// Peer authentication: who may connect to the internal listener, and as whom.
mod mtls {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::peer::config::PeerTlsConfig;
    use crate::peer::tls::PeerTls;

    /// A CA and the certificates it issued, on disk the way a deployment
    /// mounts them. Generated per test so nothing in the repo looks like a
    /// real credential.
    struct Pki {
        dir: tempfile::TempDir,
        ca: rcgen::Issuer<'static, rcgen::KeyPair>,
        ca_path: PathBuf,
    }

    impl Pki {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            let ca_key = rcgen::KeyPair::generate().expect("ca key");
            let mut params =
                rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let cert = params.self_signed(&ca_key).expect("ca cert");
            let ca_path = dir.path().join("ca.pem");
            std::fs::write(&ca_path, cert.pem()).expect("write ca");
            Self {
                ca: rcgen::Issuer::new(params, ca_key),
                dir,
                ca_path,
            }
        }

        /// Issue a certificate for `name`, valid now, under `label`.
        fn issue(&self, label: &str, name: &str) -> PeerTlsConfig {
            self.issue_with(label, name, |_| {})
        }

        fn issue_with(
            &self,
            label: &str,
            name: &str,
            adjust: impl FnOnce(&mut rcgen::CertificateParams),
        ) -> PeerTlsConfig {
            let key = rcgen::KeyPair::generate().expect("key");
            let mut params = rcgen::CertificateParams::new(vec![name.to_string()]).expect("params");
            adjust(&mut params);
            let cert = params.signed_by(&key, &self.ca).expect("sign");
            let cert_path = self.dir.path().join(format!("{label}.pem"));
            let key_path = self.dir.path().join(format!("{label}.key.pem"));
            std::fs::write(&cert_path, cert.pem()).expect("write cert");
            std::fs::write(&key_path, key.serialize_pem()).expect("write key");
            PeerTlsConfig {
                cert_path: cert_path.display().to_string(),
                key_path: key_path.display().to_string(),
                ca_path: self.ca_path.display().to_string(),
            }
        }

        fn path(&self) -> &Path {
            self.dir.path()
        }
    }

    /// A TLS refusal is seen by the dialler as a handshake that did not
    /// complete, or as a connection the listener closed before answering
    /// `Hello`. Either way no request was served.
    fn refused_at_handshake(err: &PeerError) -> bool {
        matches!(
            err,
            PeerError::Unavailable { .. }
                | PeerError::Handshake { .. }
                | PeerError::Disconnected { .. }
        )
    }

    fn tls(paths: &PeerTlsConfig) -> Arc<PeerTls> {
        Arc::new(PeerTls::load(paths).expect("load peer tls"))
    }

    async fn listener(node_id: &str, tls: Option<Arc<PeerTls>>) -> Listener {
        let handler: Arc<dyn PeerRequestHandler> = Arc::new(CountingHandler::default());
        let mut last = None;
        for _ in 0..100 {
            match PeerServer::bind_with_tls(
                node_id.to_string(),
                &config(),
                handler.clone(),
                tls.clone(),
            ) {
                Ok(server) => {
                    let addr = server.local_addr().expect("addr");
                    let shutdown = CancellationToken::new();
                    let task = tokio::spawn(server.serve(shutdown.clone()));
                    return Listener {
                        addr,
                        shutdown,
                        task,
                    };
                }
                Err(err) => {
                    last = Some(err);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        panic!("bind never succeeded: {:#}", last.expect("an error"));
    }

    fn pool_as(node_id: &str, tls: Option<Arc<PeerTls>>) -> Arc<PeerPool> {
        PeerPool::new_with_tls(node_id.to_string(), config(), CancellationToken::new(), tls)
            .expect("pool")
    }

    /// The whole thing working: both ends hold certificates from the CA
    /// issued to their node ids, and a forward goes through.
    #[tokio::test]
    async fn peers_with_certificates_from_the_ca_forward_to_each_other() {
        let pki = Pki::new();
        let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
        let pool = pool_as("broker-a", Some(tls(&pki.issue("a", "broker-a"))));

        let answer = pool
            .request(PEER, server.addr, forward())
            .await
            .expect("an authenticated peer is served");
        assert!(matches!(answer, InternalMessage::ForwardPublishOk(_)));

        pool.shutdown().await;
        server.stop().await;
    }

    /// **No certificate, no connection.** A dialler in the unauthenticated
    /// mode -- what a broker that predates mTLS, or anything that is not a
    /// broker, presents -- is refused in the handshake, before any frame.
    #[tokio::test]
    async fn a_peer_without_a_certificate_is_refused() {
        let pki = Pki::new();
        let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
        let pool = pool_as("broker-a", None);

        let err = pool
            .request(PEER, server.addr, forward())
            .await
            .expect_err("a connection with no client certificate was accepted");
        assert!(
            refused_at_handshake(&err),
            "refused in the handshake, not answered: {err:?}"
        );

        pool.shutdown().await;
        server.stop().await;
    }

    /// A certificate from a CA the listener does not trust is not a
    /// credential, however well-formed.
    #[tokio::test]
    async fn a_certificate_from_another_ca_is_refused() {
        let pki = Pki::new();
        let other = Pki::new();
        let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
        // Trusts the right CA, so the *server* verifies; presents a certificate
        // the server's CA never issued.
        let mut paths = other.issue("a", "broker-a");
        paths.ca_path = pki.ca_path.display().to_string();
        let pool = pool_as("broker-a", Some(tls(&paths)));

        let err = pool
            .request(PEER, server.addr, forward())
            .await
            .expect_err("a certificate from an untrusted CA was accepted");
        assert!(refused_at_handshake(&err), "{err:?}");

        pool.shutdown().await;
        server.stop().await;
    }

    /// An expired certificate is refused like an untrusted one.
    #[tokio::test]
    async fn an_expired_certificate_is_refused() {
        let pki = Pki::new();
        let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
        let expired = pki.issue_with("a", "broker-a", |params| {
            params.not_before = rcgen::date_time_ymd(2020, 1, 1);
            params.not_after = rcgen::date_time_ymd(2020, 1, 2);
        });
        let pool = pool_as("broker-a", Some(tls(&expired)));

        let err = pool
            .request(PEER, server.addr, forward())
            .await
            .expect_err("an expired certificate was accepted");
        assert!(refused_at_handshake(&err), "{err:?}");

        pool.shutdown().await;
        server.stop().await;
    }

    /// **The certificate's name is the identity.** A peer holding a valid
    /// certificate for one node id cannot claim another in `Hello`: the
    /// handshake proves it is *a* broker, the name check proves *which*.
    #[tokio::test]
    async fn a_peer_cannot_claim_a_node_id_its_certificate_does_not_carry() {
        let pki = Pki::new();
        let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
        // A real certificate from the right CA, issued to broker-c, presented
        // by a pool that says it is broker-a.
        let pool = pool_as("broker-a", Some(tls(&pki.issue("c", "broker-c"))));

        let err = pool
            .request(PEER, server.addr, forward())
            .await
            .expect_err("a peer was served under a name its certificate does not carry");
        assert!(refused_at_handshake(&err), "{err:?}");

        pool.shutdown().await;
        server.stop().await;
    }

    /// The other direction: a dialler verifies the listener's certificate
    /// against the node id it meant to reach, so a catalog entry that points
    /// at the wrong broker fails in the handshake.
    #[tokio::test]
    async fn a_listener_is_verified_against_the_node_id_being_dialled() {
        let pki = Pki::new();
        // The listener is broker-b, with broker-b's certificate.
        let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
        let pool = pool_as("broker-a", Some(tls(&pki.issue("a", "broker-a"))));

        // Dialled as broker-c at broker-b's address.
        let err = pool
            .request("broker-c", server.addr, forward())
            .await
            .expect_err("the wrong broker was accepted as the one dialled");
        assert!(refused_at_handshake(&err), "{err:?}");

        // And correctly as broker-b it works, on the same pool.
        pool.request(PEER, server.addr, forward())
            .await
            .expect("the right broker is served");

        pool.shutdown().await;
        server.stop().await;
    }

    /// **Rotation is a file swap.** Replacing the certificate on disk and
    /// reloading changes what the *next* handshake presents; a connection
    /// already up keeps working.
    #[tokio::test]
    async fn a_rotated_certificate_is_used_by_new_connections_without_dropping_old_ones() {
        let pki = Pki::new();
        let server_tls = tls(&pki.issue("b", PEER));
        let server = listener(PEER, Some(Arc::clone(&server_tls))).await;

        // The dialler's identity starts out issued to broker-c, so the
        // listener refuses it as broker-a ...
        let paths = pki.issue("a", "broker-c");
        let dialler_tls = tls(&paths);
        let pool = pool_as("broker-a", Some(Arc::clone(&dialler_tls)));
        pool.request(PEER, server.addr, forward())
            .await
            .expect_err("the pre-rotation certificate was accepted for the wrong name");

        // ... until the files are replaced with a certificate for broker-a
        // and reloaded. Same paths, new material: what a cert-manager renewal
        // looks like from the broker's side.
        let rotated = pki.issue("a", "broker-a");
        assert_eq!(rotated.cert_path, paths.cert_path);
        assert!(
            dialler_tls.reload().expect("reload"),
            "the new certificate was not noticed"
        );
        assert!(
            !dialler_tls.reload().expect("reload"),
            "an unchanged certificate was reported as rotated"
        );

        // The pool's endpoint is unchanged; only the resolver's answer moved.
        // The refused attempt above put the peer into reconnect backoff, so
        // give the pool a few tries.
        let mut last = None;
        for _ in 0..50 {
            match pool.request(PEER, server.addr, forward()).await {
                Ok(_) => {
                    last = None;
                    break;
                }
                Err(err) => {
                    last = Some(err);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        assert!(
            last.is_none(),
            "the rotated certificate was not presented on a new handshake: {last:?}"
        );

        pool.shutdown().await;
        server.stop().await;
        let _ = pki.path();
    }

    /// Missing or mismatched material is refused at load, before anything
    /// binds -- and the key never appears in the error.
    #[test]
    fn unreadable_material_fails_at_load_without_leaking_the_key() {
        let pki = Pki::new();
        let mut paths = pki.issue("a", "broker-a");
        let key_pem = std::fs::read_to_string(&paths.key_path).expect("key");

        paths.key_path = pki.path().join("missing.key.pem").display().to_string();
        let err = PeerTls::load(&paths).expect_err("a missing key loaded");
        assert!(
            format!("{err:#}").contains("FELIX_INTERNAL_TLS_KEY"),
            "{err:#}"
        );

        // A key that does not belong to the certificate.
        let other = pki.issue("x", "broker-x");
        paths.key_path = other.key_path;
        let err = PeerTls::load(&paths).expect_err("a mismatched key loaded");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("does not match"), "{rendered}");
        assert!(
            !rendered.contains(key_pem.trim()),
            "the key reached an error message"
        );
    }
}
