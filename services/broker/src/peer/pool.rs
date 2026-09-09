//! The outbound half of the internal transport: connections to peer brokers.
//!
//! # Shape
//!
//! One [`PeerPool`] per broker, holding at most `conns_per_peer` connections to
//! each peer it has been asked to reach. Each connection carries
//! `streams_per_conn` multiplexed bidirectional streams; requests are spread
//! across them round-robin and matched to responses by correlation id, which the
//! protocol scopes to the connection.
//!
//! Several streams rather than one because a QUIC stream is ordered: a large
//! forwarded batch on a single stream would hold up every smaller request queued
//! behind it.
//!
//! # What bounds it
//!
//! An unhealthy peer must not be able to consume this broker, so three limits
//! apply and each fails differently:
//!
//! - **In-flight requests per peer.** A peer that accepts frames and never
//!   answers otherwise accumulates one waiter per forwarded publish. At the
//!   limit, requests are shed immediately rather than queued.
//! - **Reconnect backoff.** A peer that refuses connections is dialled on a
//!   jittered exponential schedule, and requests arriving inside the backoff
//!   window fail without a dial.
//! - **Request timeout.** Every request has one, so no waiter is unbounded even
//!   if the connection stays open.
//!
//! # Every request ends
//!
//! `docs/internal-protocol.md` requires that a forwarded publish is never left
//! pending. A connection loss fails every waiter on it at the moment it drops,
//! which is what makes "the requester treats it as failed" true rather than
//! merely eventual.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use felix_transport::{QuicClient, QuicConnection};
use felix_wire::internal::{Hello, InternalMessage};
use parking_lot::Mutex;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::codec::read_frame;
use super::config::PeerTransportConfig;
use super::metrics;
use super::tls;

/// Why a request to a peer did not produce a response.
///
/// Typed so a caller can tell "this broker refused to try" from "the peer was
/// asked and did not answer" — the first is retryable elsewhere immediately,
/// the second is not necessarily safe to retry at all.
#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    /// The peer is in a reconnect backoff window, or at its in-flight limit.
    /// Nothing was sent.
    #[error("peer {node_id} is unavailable: {detail}")]
    Unavailable { node_id: String, detail: String },
    /// The connection could not be established or the handshake was refused.
    #[error("peer {node_id} handshake failed: {detail}")]
    Handshake { node_id: String, detail: String },
    /// The request was sent and the connection dropped before an answer.
    #[error("connection to peer {node_id} was lost")]
    Disconnected { node_id: String },
    /// The request was sent and no answer arrived in time.
    #[error("peer {node_id} did not respond within {timeout:?}")]
    Timeout { node_id: String, timeout: Duration },
    /// The pool is shutting down.
    #[error("peer transport is shutting down")]
    ShuttingDown,
}

impl PeerError {
    /// Whether the same request may be sent to the same peer again.
    ///
    /// `Disconnected` and `Timeout` are absent on purpose: the peer may have
    /// applied the write before the answer was lost, so retrying is a duplicate
    /// rather than a repair. Deciding that is the caller's, with the ack mode in
    /// hand.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }

    fn outcome(&self) -> &'static str {
        match self {
            Self::Unavailable { .. } | Self::ShuttingDown => metrics::OUTCOME_UNREACHABLE,
            Self::Handshake { .. } => metrics::OUTCOME_HANDSHAKE,
            Self::Disconnected { .. } => metrics::OUTCOME_DISCONNECTED,
            Self::Timeout { .. } => metrics::OUTCOME_TIMEOUT,
        }
    }
}

/// Connections to peer brokers.
pub struct PeerPool {
    config: PeerTransportConfig,
    local_node_id: String,
    client: QuicClient,
    peers: Mutex<HashMap<String, Arc<Peer>>>,
    shutdown: CancellationToken,
}

impl PeerPool {
    /// Bind the outbound endpoint. No peer is dialled until one is requested.
    pub fn new(
        local_node_id: String,
        config: PeerTransportConfig,
        shutdown: CancellationToken,
    ) -> Result<Arc<Self>> {
        let client = QuicClient::bind(
            "0.0.0.0:0".parse().expect("literal address"),
            tls::client_config()?,
            config.quic_transport(),
        )
        .context("bind peer QUIC endpoint")?;

        let pool = Arc::new(Self {
            config,
            local_node_id,
            client,
            peers: Mutex::new(HashMap::new()),
            shutdown,
        });
        pool.clone().spawn_reaper();
        Ok(pool)
    }

    /// Send `message` to `node_id` at `addr` and wait for its response.
    ///
    /// Establishes the connection if there is not one already. The address is
    /// passed per call rather than cached because the catalog owns it: a peer
    /// that re-registers at a new address must be reached there on the next
    /// request, not after this pool notices.
    pub async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let started = Instant::now();
        let result = self.request_inner(node_id, addr, message).await;
        match &result {
            Ok(_) => metrics::record_request(metrics::OUTCOME_OK, started.elapsed()),
            Err(err) => metrics::record_request(err.outcome(), started.elapsed()),
        }
        result
    }

    async fn request_inner(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        if self.shutdown.is_cancelled() {
            return Err(PeerError::ShuttingDown);
        }
        let peer = self.peer(node_id);

        // Taken before the connection is touched, so a peer at its limit is shed
        // without being dialled. Dropping the permit is what releases it, which
        // covers every exit below including a timeout.
        let Ok(_permit) = Arc::clone(&peer.inflight).try_acquire_owned() else {
            metrics::record_request_shed();
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: format!(
                    "{} requests already in flight",
                    self.config.max_inflight_per_peer
                ),
            });
        };

        let connection = peer.connection(self, addr).await?;
        let response = tokio::time::timeout(
            self.config.request_timeout,
            connection.request(message, &self.shutdown),
        )
        .await;

        match response {
            Ok(result) => result,
            Err(_) => Err(PeerError::Timeout {
                node_id: node_id.to_string(),
                timeout: self.config.request_timeout,
            }),
        }
    }

    fn peer(&self, node_id: &str) -> Arc<Peer> {
        let mut peers = self.peers.lock();
        Arc::clone(peers.entry(node_id.to_string()).or_insert_with(|| {
            Arc::new(Peer::new(
                node_id.to_string(),
                self.config.max_inflight_per_peer,
            ))
        }))
    }

    /// Live connections across all peers. Test and metrics surface.
    pub async fn connection_count(&self) -> usize {
        let peers: Vec<_> = self.peers.lock().values().cloned().collect();
        let mut total = 0;
        for peer in peers {
            total += peer.state.lock().await.live_connections();
        }
        total
    }

    /// Close every connection and stop the reaper.
    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        let peers: Vec<_> = self.peers.lock().drain().map(|(_, peer)| peer).collect();
        for peer in peers {
            peer.state.lock().await.close_all("broker shutting down");
        }
        metrics::set_connections(0);
        metrics::set_streams(0);
    }

    /// Close connections that have gone unused, and republish the gauges.
    ///
    /// Rebalancing changes which peers a broker forwards to, so a pool that only
    /// grew would keep a connection and its keep-alives alive for every peer it
    /// had ever talked to.
    fn spawn_reaper(self: Arc<Self>) {
        let interval = (self.config.idle_timeout / 2).max(Duration::from_secs(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = self.shutdown.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                self.reap().await;
            }
        });
    }

    async fn reap(&self) {
        let peers: Vec<_> = self
            .peers
            .lock()
            .iter()
            .map(|(id, peer)| (id.clone(), Arc::clone(peer)))
            .collect();

        let mut connections = 0;
        let mut streams = 0;
        let mut idle_peers = Vec::new();
        for (id, peer) in peers {
            let mut state = peer.state.lock().await;
            state.evict_idle(self.config.idle_timeout);
            let live = state.live_connections();
            connections += live;
            streams += live * self.config.streams_per_conn;
            if live == 0 && !state.in_backoff() {
                idle_peers.push(id);
            }
        }

        if !idle_peers.is_empty() {
            let mut peers = self.peers.lock();
            for id in idle_peers {
                // Only if nothing took a reference since: a request between the
                // scan and here would otherwise lose its peer entry, and with it
                // the in-flight limit that request is already counted against.
                if peers
                    .get(&id)
                    .is_some_and(|peer| Arc::strong_count(peer) == 1)
                {
                    peers.remove(&id);
                }
            }
        }

        metrics::set_connections(connections);
        metrics::set_streams(streams);
    }
}

/// One peer broker, and this broker's connections to it.
struct Peer {
    node_id: String,
    /// Async because establishing is held across a dial: concurrent requests to
    /// a cold peer must produce one connection attempt, not one each.
    state: tokio::sync::Mutex<PeerState>,
    inflight: Arc<Semaphore>,
}

impl Peer {
    fn new(node_id: String, max_inflight: usize) -> Self {
        Self {
            node_id,
            state: tokio::sync::Mutex::new(PeerState::default()),
            inflight: Arc::new(Semaphore::new(max_inflight)),
        }
    }

    /// A live connection to this peer, dialling if there is not one.
    async fn connection(
        &self,
        pool: &PeerPool,
        addr: SocketAddr,
    ) -> std::result::Result<Arc<PeerConnection>, PeerError> {
        let mut state = self.state.lock().await;
        state.drop_dead();

        if let Some(connection) = state.next(pool.config.conns_per_peer) {
            return Ok(connection);
        }

        if let Some(remaining) = state.backoff_remaining() {
            return Err(PeerError::Unavailable {
                node_id: self.node_id.clone(),
                detail: format!(
                    "reconnecting in {remaining:?} after {} failed attempts",
                    state.attempts
                ),
            });
        }

        if state.attempts > 0 {
            metrics::record_reconnect();
        }
        match pool.dial(&self.node_id, addr).await {
            Ok(connection) => {
                state.attempts = 0;
                state.backoff_until = None;
                state.connections.push(Arc::clone(&connection));
                Ok(connection)
            }
            Err(err) => {
                let delay = pool.config.reconnect_delay(state.attempts);
                state.attempts = state.attempts.saturating_add(1);
                state.backoff_until = Some(Instant::now() + delay);
                Err(err)
            }
        }
    }
}

#[derive(Default)]
struct PeerState {
    connections: Vec<Arc<PeerConnection>>,
    cursor: usize,
    attempts: u32,
    backoff_until: Option<Instant>,
}

impl PeerState {
    fn drop_dead(&mut self) {
        self.connections.retain(|connection| connection.is_live());
    }

    fn live_connections(&self) -> usize {
        self.connections
            .iter()
            .filter(|connection| connection.is_live())
            .count()
    }

    fn in_backoff(&self) -> bool {
        self.backoff_remaining().is_some()
    }

    fn backoff_remaining(&self) -> Option<Duration> {
        let until = self.backoff_until?;
        until.checked_duration_since(Instant::now())
    }

    /// The next connection to use, or `None` if the pool is not yet full.
    ///
    /// Growing to the configured size takes precedence over reuse: a pool of one
    /// connection would otherwise never reach two, since the first is always
    /// available.
    fn next(&mut self, target: usize) -> Option<Arc<PeerConnection>> {
        if self.connections.len() < target {
            return None;
        }
        let connection = self.connections.get(self.cursor % self.connections.len())?;
        self.cursor = self.cursor.wrapping_add(1);
        Some(Arc::clone(connection))
    }

    fn evict_idle(&mut self, idle_timeout: Duration) {
        self.connections.retain(|connection| {
            // A connection with work outstanding is not idle no matter how long
            // ago it was handed out.
            if connection.idle_for() < idle_timeout || connection.has_pending() {
                return true;
            }
            connection.close("idle");
            false
        });
    }

    fn close_all(&mut self, reason: &str) {
        for connection in self.connections.drain(..) {
            connection.close(reason);
        }
    }
}

impl PeerPool {
    /// Establish one connection and complete the handshake.
    async fn dial(
        &self,
        node_id: &str,
        addr: SocketAddr,
    ) -> std::result::Result<Arc<PeerConnection>, PeerError> {
        let connect = tokio::time::timeout(
            self.config.handshake_timeout,
            self.client.connect(addr, tls::INTERNAL_SERVER_NAME),
        );
        let connection = match connect.await {
            Ok(Ok(connection)) => connection,
            Ok(Err(err)) => {
                metrics::record_connect_attempt(metrics::OUTCOME_UNREACHABLE);
                return Err(PeerError::Unavailable {
                    node_id: node_id.to_string(),
                    detail: err.to_string(),
                });
            }
            Err(_) => {
                metrics::record_connect_attempt(metrics::OUTCOME_UNREACHABLE);
                return Err(PeerError::Unavailable {
                    node_id: node_id.to_string(),
                    detail: format!("no answer within {:?}", self.config.handshake_timeout),
                });
            }
        };

        let established = PeerConnection::establish(
            connection,
            node_id.to_string(),
            self.local_node_id.clone(),
            self.config.streams_per_conn,
            self.config.handshake_timeout,
        )
        .await;

        match established {
            Ok(connection) => {
                metrics::record_connect_attempt(metrics::OUTCOME_CONNECTED);
                tracing::debug!(peer = %node_id, %addr, "peer connection established");
                Ok(connection)
            }
            Err(err) => {
                metrics::record_connect_attempt(metrics::OUTCOME_HANDSHAKE);
                Err(PeerError::Handshake {
                    node_id: node_id.to_string(),
                    detail: format!("{err:#}"),
                })
            }
        }
    }
}

/// One established connection to a peer, with its multiplexed streams.
struct PeerConnection {
    node_id: String,
    connection: QuicConnection,
    lanes: Vec<mpsc::Sender<Bytes>>,
    cursor: AtomicUsize,
    pending: Arc<Mutex<Pending>>,
    next_correlation: AtomicU64,
    last_used: Mutex<Instant>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// Waiters for responses that have not arrived.
///
/// `closed` is what makes a lost connection fail its waiters rather than leave
/// them to time out, and it also closes the window where a request registers
/// itself just after the connection died.
#[derive(Default)]
struct Pending {
    waiters: HashMap<u64, oneshot::Sender<InternalMessage>>,
    closed: bool,
}

impl PeerConnection {
    /// Open the request streams and exchange the handshake.
    async fn establish(
        connection: QuicConnection,
        node_id: String,
        local_node_id: String,
        streams: usize,
        handshake_timeout: Duration,
    ) -> Result<Arc<Self>> {
        let pending = Arc::new(Mutex::new(Pending::default()));
        let mut lanes = Vec::with_capacity(streams);
        let mut tasks = Vec::new();

        for _ in 0..streams {
            let (mut send, mut recv) = connection.open_bi().await.context("open peer stream")?;
            let (tx, mut rx) = mpsc::channel::<Bytes>(64);

            tasks.push(connection.spawn_pump(async move {
                while let Some(frame) = rx.recv().await {
                    if send.write_all(&frame).await.is_err() {
                        // The reader half sees the same failure and fails every
                        // waiter, so there is nothing to report here.
                        break;
                    }
                }
                let _ = send.finish();
            }));

            let responses = Arc::clone(&pending);
            tasks.push(connection.spawn_pump(async move {
                loop {
                    match read_frame(&mut recv).await {
                        Ok(Some(message)) => {
                            let waiter = responses.lock().waiters.remove(&message.correlation_id());
                            // A response with no waiter is one whose request
                            // already timed out. Dropping it is correct; the
                            // caller has been told the request failed.
                            if let Some(waiter) = waiter {
                                let _ = waiter.send(message);
                            }
                        }
                        Ok(None) => break,
                        Err(err) => {
                            tracing::debug!(error = %err, "peer response stream ended");
                            break;
                        }
                    }
                }
            }));

            lanes.push(tx);
        }

        let peer = Arc::new(Self {
            node_id: node_id.clone(),
            connection,
            lanes,
            cursor: AtomicUsize::new(0),
            pending: Arc::clone(&pending),
            next_correlation: AtomicU64::new(1),
            last_used: Mutex::new(Instant::now()),
            tasks: Mutex::new(tasks),
        });

        // One watcher fails every waiter the moment the connection drops, which
        // is what makes "no forwarded publish is left pending" hold.
        //
        // It takes the pending map and a connection handle rather than the
        // `PeerConnection` itself: that would be a cycle through `tasks`, and the
        // connection would never drop.
        let watched = peer.connection.clone();
        let waiters = Arc::clone(&pending);
        let watched_id = node_id.clone();
        let watcher = peer.connection.spawn_pump(async move {
            let reason = watched.closed().await;
            tracing::debug!(peer = %watched_id, %reason, "peer connection closed");
            metrics::record_connection_loss(close_reason_label(&reason));
            let mut waiters = waiters.lock();
            waiters.closed = true;
            waiters.waiters.clear();
        });
        peer.tasks.lock().push(watcher);

        peer.handshake(local_node_id, handshake_timeout).await?;
        Ok(peer)
    }

    /// Exchange `Hello`, and check the peer is who the catalog said.
    async fn handshake(&self, local_node_id: String, timeout: Duration) -> Result<()> {
        let hello = InternalMessage::Hello(Hello {
            correlation_id: self.next_correlation.fetch_add(1, Ordering::Relaxed),
            node_id: local_node_id,
        });
        let shutdown = CancellationToken::new();
        let response = tokio::time::timeout(timeout, self.request(hello, &shutdown))
            .await
            .map_err(|_| anyhow!("no handshake response within {timeout:?}"))?
            .map_err(|err| anyhow!("{err}"))?;

        match response {
            InternalMessage::HelloOk(ok) if ok.node_id == self.node_id => Ok(()),
            InternalMessage::HelloOk(ok) => {
                self.close("peer identity mismatch");
                bail!(
                    "dialled {} but the broker there is {}",
                    self.node_id,
                    ok.node_id
                )
            }
            other => {
                self.close("unexpected handshake response");
                bail!("expected HelloOk, got {:?}", other.kind())
            }
        }
    }

    /// Send one request and wait for its terminal response.
    async fn request(
        &self,
        message: InternalMessage,
        shutdown: &CancellationToken,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let correlation_id = match &message {
            // The handshake picks its own id before the connection is usable.
            InternalMessage::Hello(hello) => hello.correlation_id,
            _ => self.next_correlation.fetch_add(1, Ordering::Relaxed),
        };
        let message = with_correlation(message, correlation_id);
        let frame = message.encode().map_err(|err| PeerError::Handshake {
            node_id: self.node_id.clone(),
            detail: err.to_string(),
        })?;

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock();
            if pending.closed {
                return Err(PeerError::Disconnected {
                    node_id: self.node_id.clone(),
                });
            }
            pending.waiters.insert(correlation_id, tx);
        }
        *self.last_used.lock() = Instant::now();

        let lane = self.cursor.fetch_add(1, Ordering::Relaxed) % self.lanes.len();
        if self.lanes[lane].send(frame).await.is_err() {
            self.pending.lock().waiters.remove(&correlation_id);
            return Err(PeerError::Disconnected {
                node_id: self.node_id.clone(),
            });
        }

        tokio::select! {
            response = rx => match response {
                Ok(message) => Ok(message),
                // The sender is dropped by `fail_all`, so this is the
                // connection dropping rather than a lost response.
                Err(_) => Err(PeerError::Disconnected { node_id: self.node_id.clone() }),
            },
            _ = shutdown.cancelled() => {
                self.pending.lock().waiters.remove(&correlation_id);
                Err(PeerError::ShuttingDown)
            }
        }
    }

    fn is_live(&self) -> bool {
        self.connection.close_reason().is_none() && !self.pending.lock().closed
    }

    fn has_pending(&self) -> bool {
        !self.pending.lock().waiters.is_empty()
    }

    fn idle_for(&self) -> Duration {
        self.last_used.lock().elapsed()
    }

    /// Fail every waiter and refuse new ones.
    fn fail_all(&self) {
        let mut pending = self.pending.lock();
        pending.closed = true;
        // Dropping each sender is the signal; the waiting side reads a dropped
        // sender as `Disconnected`.
        pending.waiters.clear();
    }

    fn close(&self, reason: &str) {
        self.fail_all();
        self.connection.close(0u32.into(), reason.as_bytes());
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }
    }
}

impl Drop for PeerConnection {
    fn drop(&mut self) {
        // Tasks hold `SendStream`/`RecvStream` handles, so leaving them running
        // would keep the connection alive after the pool has forgotten it.
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }
    }
}

/// Stamp the pool's correlation id onto a caller-supplied message.
///
/// Ids are the connection's to assign — a caller cannot know what is already in
/// flight on the stream its request lands on.
fn with_correlation(message: InternalMessage, correlation_id: u64) -> InternalMessage {
    use felix_wire::internal::*;
    match message {
        InternalMessage::ForwardPublish(m) => InternalMessage::ForwardPublish(ForwardPublish {
            correlation_id,
            ..m
        }),
        InternalMessage::ForwardPublishOk(m) => {
            InternalMessage::ForwardPublishOk(ForwardPublishOk {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ForwardPublishError(m) => {
            InternalMessage::ForwardPublishError(ForwardPublishError {
                correlation_id,
                ..m
            })
        }
        InternalMessage::NotLeader(m) => InternalMessage::NotLeader(NotLeader {
            correlation_id,
            ..m
        }),
        InternalMessage::Hello(m) => InternalMessage::Hello(Hello {
            correlation_id,
            ..m
        }),
        InternalMessage::HelloOk(m) => InternalMessage::HelloOk(HelloOk {
            correlation_id,
            ..m
        }),
    }
}

fn close_reason_label(reason: &quinn::ConnectionError) -> &'static str {
    match reason {
        quinn::ConnectionError::TimedOut => "timeout",
        quinn::ConnectionError::ApplicationClosed(_) => "closed_by_peer",
        quinn::ConnectionError::LocallyClosed => "closed_locally",
        quinn::ConnectionError::ConnectionClosed(_) => "transport_error",
        quinn::ConnectionError::Reset => "reset",
        _ => "other",
    }
}
