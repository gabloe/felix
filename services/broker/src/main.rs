//! Broker service main entry point.
//!
//! This binary is the runnable “broker node” for Felix. It wires together:
//! - **Core broker logic** (`felix_broker::Broker`) backed by an in-memory cache implementation
//!   in this MVP (`felix_storage::EphemeralCache`).
//! - **QUIC transport server** (`felix_transport::QuicServer`) and the broker’s QUIC accept loop
//!   (`broker::quic::serve`).
//! - **Observability**: a Prometheus metrics endpoint (and optionally tracing/OTel plumbing)
//!   via the local `observability` module.
//! - **Control-plane sync**: optional periodic synchronization of scope metadata from a
//!   configured control-plane endpoint.
//!
//! ## Process lifecycle
//! - The broker starts long-running background tasks (QUIC accept loop, metrics server,
//!   and optional control-plane sync).
//! - The process remains alive until the provided shutdown future completes. In
//!   production that is SIGTERM or SIGINT: SIGTERM is what Kubernetes, systemd, and
//!   `docker stop` send, so handling only SIGINT would abort in-flight work on every
//!   rolling update.
//! - Shutdown then runs a bounded drain, in order: readiness goes false so load
//!   balancers stop routing here, the listener stops admitting new connections,
//!   in-flight connections finish, and finally the metrics server stops. Anything
//!   still running when `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS` expires is force-cancelled
//!   and named in a warning. See `felix_common::lifecycle`.
//!
//! ## TLS note
//! `build_server_config()` currently creates a **dev-only self-signed** certificate for QUIC.
//! Production deployments should use a real certificate chain and should not re-generate keys
//! on each start.

mod controlplane;
mod observability;
#[cfg(test)]
mod test_support;

use anyhow::{Context, Result};
use broker::membership;
use broker::peer;
use broker::{auth::BrokerAuth, config, durable_config::DurableStorageConfig, quic};
use broker::{shard_lifecycle, shard_routing, shard_watch};
use felix_broker::{Broker, DurableStorage};
use felix_common::lifecycle::{self, DrainBudget, Readiness};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use quinn::ServerConfig;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::PrivatePkcs8KeyDer;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

// Tokio async runtime entry point. The broker is primarily I/O-bound (QUIC + HTTP metrics)
// and runs multiple background tasks concurrently.
#[tokio::main]
async fn main() -> Result<()> {
    // Default shutdown trigger: SIGTERM or SIGINT. SIGTERM is what Kubernetes,
    // systemd, and `docker stop` actually send; SIGINT only covers an interactive
    // Ctrl-C. `run_with_shutdown` is written so we can reuse the same startup logic
    // in tests or alternative hosting environments by passing a different future.
    run_with_shutdown(lifecycle::termination_signal()).await
}

/// Start the broker and run until the provided `shutdown` future resolves.
///
/// This indirection makes the process lifecycle explicit and testable:
/// - In production, `shutdown` is typically CTRL-C.
/// - In tests, callers can pass a bounded timer or a oneshot receiver.
///
/// The function is responsible for spawning background tasks and ensuring they are
/// cancelled when shutdown is requested.
async fn run_with_shutdown<F>(shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let metrics_handle = observability::init_observability("felix-broker");
    // Observability is initialized first so any subsequent startup logs/metrics are captured.

    let config = config::BrokerConfig::from_env_or_yaml()?;
    // Lifecycle coordination. `readiness` gates `/ready`; the two tokens separate
    // "stop admitting connections" from "stop serving probes", because the metrics
    // endpoint has to outlive the drain — that is how an operator watches the drain
    // happen. `connections` tracks per-connection tasks so the drain can wait for
    // in-flight work instead of aborting it.
    // Durable brokers start *not* ready. Their streams do not exist until the
    // control plane has been applied and each durable log recovered, and an
    // instance that reports ready before then invites an orchestrator to route
    // traffic at streams that are, from any client's point of view, missing.
    // An in-memory broker has nothing to recover and keeps the old behaviour.
    let durable_storage_configured = DurableStorageConfig::from_env()?.is_some();
    let gate_readiness_on_sync = durable_storage_configured && config.controlplane_url.is_some();
    let readiness = if gate_readiness_on_sync {
        Readiness::starting()
    } else {
        Readiness::ready()
    };
    let accept_shutdown = CancellationToken::new();
    // Cancelled the moment shutdown begins, so startup work in flight can tell
    // "not ready yet" from "no longer ready".
    let draining = CancellationToken::new();
    // Cancelled once the catalog is applied and every durable log recovered.
    // Both readiness and the accept loop wait on it, so a durable broker
    // neither advertises itself nor answers a direct client before its streams
    // exist.
    let seeded = CancellationToken::new();

    // Cluster ownership, when this broker has an identity. Built before the
    // accept loop because the publish path consults it, and `None` on a
    // single-node broker so that path is a null check.
    let cluster = config.membership.as_ref().map(|membership| {
        let router = Arc::new(felix_router::ShardRouter::new(
            membership.node_id.clone(),
            membership.region.clone(),
            felix_router::RegionRouter::new(membership.region.clone()),
        ));
        let ingress = Arc::new(shard_routing::IngressRouter::new(Arc::clone(&router)));
        let lifecycle = Arc::new(tokio::sync::Mutex::new(
            shard_lifecycle::ShardLifecycle::new(membership.node_id.clone()),
        ));
        let ownership = Arc::new(tokio::sync::RwLock::new(
            shard_watch::ShardOwnership::default(),
        ));
        (router, ingress, lifecycle, ownership)
    });
    let ingress_router = cluster
        .as_ref()
        .map(|(_, ingress, _, _)| Arc::clone(ingress));

    // This broker's authority to serve the shards it leads. Created here rather
    // than inside the membership task because the accept loop is built first and
    // the publish path reads it. Starts conservative and invalid: authority
    // arrives with the first accepted heartbeat, never before.
    let lease = config
        .membership
        .as_ref()
        .map(|_| Arc::new(peer_lease_state()));

    // Outbound peer connections. Built here because the publish path forwards
    // through it, and before the accept loop for the same reason the router is:
    // a publish must never arrive at a broker that can resolve a remote owner
    // and not reach it.
    let peer_shutdown = CancellationToken::new();
    let peers = match (&config.peer_transport, &config.membership) {
        (Some(peer_config), Some(membership_config)) => Some(
            peer::PeerPool::new(
                membership_config.node_id.clone(),
                peer_config.clone(),
                peer_shutdown.clone(),
            )
            .context("bind peer transport")?,
        ),
        _ => None,
    };
    let sync_shutdown = CancellationToken::new();
    let metrics_shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    tracing::info!(
        lanes = config.subscriber_writer_lanes.max(1),
        queue_bound = config.subscriber_lane_queue_depth.max(1),
        queue_mode = ?config.subscriber_lane_queue_policy,
        shard = ?config.subscriber_lane_shard,
        single_writer_per_conn = config.subscriber_single_writer_per_conn,
        "sub egress lanes ENABLED"
    );
    // Configuration is resolved from environment variables (and optionally a YAML file).
    // Keep this early so the remainder of startup is entirely driven by `config`.

    // Start an in-process broker. The cache backend is in-memory; stream
    // durability is separate and opt-in via `FELIX_DURABLE_STORAGE_DIR`.
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(config.subscriber_queue_capacity.max(1))
        .context("configure subscriber queue depth")?
        .with_subscriber_queue_policy(config.subscriber_queue_policy);

    // Durable storage is opened before the listener binds: recovering segments
    // can take time and can fail, and both are better surfaced as a startup
    // error than as a failed publish once traffic is arriving.
    let durable_storage = match DurableStorageConfig::from_env()? {
        Some(durable) => {
            tracing::info!(config = %durable.summary(), "opening durable stream storage");
            let storage = DurableStorage::open(&durable.root, durable.log.clone())
                .with_context(|| format!("open durable storage at {}", durable.root.display()))?;
            Some(storage)
        }
        None => {
            tracing::info!(
                "durable stream storage disabled (set FELIX_DURABLE_STORAGE_DIR to enable)"
            );
            None
        }
    };
    let broker = match durable_storage.clone() {
        Some(storage) => broker.with_durable_storage(storage),
        None => broker,
    };
    tracing::info!("broker started");
    let controlplane_url = config
        .controlplane_url
        .clone()
        .context("FELIX_CONTROLPLANE_URL must be set for auth")?;
    let auth = Arc::new(BrokerAuth::new(controlplane_url));

    // Start the Prometheus metrics HTTP server. This is separate from QUIC traffic and
    // intentionally lightweight so metrics remain available even under load.
    let metrics_task = {
        let metrics_shutdown = metrics_shutdown.clone();
        tokio::spawn(observability::serve_metrics(
            metrics_handle,
            config.metrics_bind,
            readiness.clone(),
            async move { metrics_shutdown.cancelled().await },
        ))
    };

    // Build and bind the QUIC listener. `build_server_config` currently uses a self-signed
    // certificate suitable for local development.
    let bind_addr = config.quic_bind;
    let server_config = build_server_config().context("build QUIC server config")?;

    // Apply transport-level configuration (flow control windows, pooling behavior, etc.)
    // derived from broker config.
    let transport = broker::transport::cache_transport_config(&config, TransportConfig::default());
    let quic_server = Arc::new(
        QuicServer::bind(bind_addr, server_config, transport).context("bind QUIC listener")?,
    );
    tracing::info!(addr = %quic_server.local_addr()?, "quic listener started");

    // Start accepting QUIC connections in a background task.
    // If the accept loop exits due to an error, we log and continue shutdown normally.
    let broker = Arc::new(broker);
    let accept_task = {
        let quic_server = Arc::clone(&quic_server);
        let broker = Arc::clone(&broker);
        let quic_config = config.clone();
        let auth = Arc::clone(&auth);
        let accept_shutdown = accept_shutdown.clone();
        let connections = connections.clone();
        let seeded = seeded.clone();
        let ingress_router = ingress_router.clone();
        let peers_for_accept = peers.clone();
        let lease_for_accept = lease.clone();
        tokio::spawn(async move {
            // A durable broker does not accept until its streams exist.
            // Readiness alone only steers orchestrated traffic; a client with
            // the address in hand would otherwise connect during recovery and
            // be told its durable stream does not exist. Waiting is the honest
            // answer, and shutdown still wins the race so a broker told to stop
            // during recovery stops.
            if gate_readiness_on_sync {
                tokio::select! {
                    biased;
                    _ = accept_shutdown.cancelled() => {
                        tracing::info!("shutdown before initial sync; not accepting");
                        return;
                    }
                    _ = seeded.cancelled() => {
                        tracing::info!("initial sync applied; accepting connections");
                    }
                }
            }
            if let Err(err) = quic::serve_with_shutdown(
                quic_server,
                broker,
                quic_config,
                auth,
                accept_shutdown,
                connections,
                quic::ClusterContext {
                    ingress: ingress_router,
                    peers: peers_for_accept,
                    lease: lease_for_accept,
                },
            )
            .await
            {
                tracing::warn!(error = %err, "quic accept loop exited");
            }
        })
    };

    // Optional: start a periodic control-plane sync to keep tenant/namespace/stream metadata
    // refreshed. When disabled, the broker relies solely on local registrations.
    let (seeded_tx, seeded_rx) = tokio::sync::oneshot::channel();
    let controlplane_task = if let Some(base_url) = config.controlplane_url.clone() {
        let interval_ms = config.controlplane_sync_interval_ms;
        let broker = Arc::clone(&broker);
        let sync_shutdown = sync_shutdown.clone();
        let seeded_tx = gate_readiness_on_sync.then_some(seeded_tx);
        Some(tokio::spawn(async move {
            // `start_sync` polls forever, so cancellation is what ends it. Dropping
            // it mid-iteration is safe: the sync is a read-only metadata refresh
            // whose cursor only advances on success, so an interrupted iteration is
            // the same case as a failed one and is simply re-fetched on next start.
            tokio::select! {
                _ = sync_shutdown.cancelled() => {
                    tracing::info!("control plane sync stopped");
                }
                result = controlplane::start_sync_with_signal(
                    broker,
                    base_url,
                    Duration::from_millis(interval_ms),
                    seeded_tx,
                ) => {
                    if let Err(err) = result {
                        tracing::warn!(error = %err, "control plane sync exited");
                    }
                }
            }
        }))
    } else {
        tracing::info!("control plane sync disabled (FELIX_CONTROLPLANE_URL not set)");
        None
    };

    // Flip to ready once the catalog has been applied and every durable stream
    // it named has been recovered.
    //
    // This has to be armed *before* the shutdown await, not after it: a task
    // spawned below that point would only start listening for the seed signal
    // once the broker was already draining, so it would never report ready
    // while serving — and could flip back to ready in the middle of a drain.
    //
    // A task rather than an inline await: `/ready` already reports false, so
    // blocking startup here would only delay the point at which an operator
    // can observe that state.
    //
    // The task holds a `Readiness` clone and ends when the signal resolves or
    // its sender is dropped, so nothing keeps it alive past shutdown.
    if gate_readiness_on_sync {
        let readiness = readiness.clone();
        let draining = draining.clone();
        let seeded = seeded.clone();
        tokio::spawn(async move {
            // `Readiness` is a single flag, so it cannot distinguish "never
            // ready yet" from "already drained" — both read false. The drain
            // token is what makes the difference observable, so a seed that
            // lands mid-drain cannot flip the broker back to ready.
            tokio::select! {
                biased;
                _ = draining.cancelled() => {
                    tracing::warn!("shutdown began before the initial sync; staying unready");
                }
                result = seeded_rx => {
                    if result.is_ok() {
                        // Release the accept loop first, so a connection that
                        // arrives the instant readiness flips is answered
                        // rather than dropped.
                        seeded.cancel();
                        readiness.mark_ready();
                        tracing::info!("initial control-plane sync applied; reporting ready");
                    }
                }
            }
        });
    }

    // Cluster membership, when this broker has an identity. Registration waits
    // for `serving`, because advertising a node placement can route to before
    // it can answer is worse than advertising it a moment late.
    let membership_client = reqwest::Client::new();
    let membership = match (&config.membership, &config.controlplane_url) {
        (Some(membership_config), Some(base_url)) => {
            let serving = if gate_readiness_on_sync {
                seeded.clone()
            } else {
                // Nothing to wait for: the accept loop is already running.
                let now = CancellationToken::new();
                now.cancel();
                now
            };
            let lease = Arc::clone(lease.as_ref().expect("a cluster member has a lease"));
            // Keeps the cheap admission flag in step with the clock, so a broker
            // that loses its lease stops accepting without waiting for a publish
            // to discover it.
            let refresh = Arc::clone(&lease).spawn_refresh(sync_shutdown.clone());
            drop(refresh);
            Some(membership::spawn(
                membership_client.clone(),
                base_url.clone(),
                membership_config.clone(),
                serving,
                sync_shutdown.clone(),
                lease,
            ))
        }
        _ => {
            tracing::info!("cluster membership disabled (FELIX_NODE_ID not set)");
            None
        }
    };

    // The broker-internal listener, when this broker is in a cluster. This is
    // the address `NodeSpec.advertise_addr` names, so it must be up for peers to
    // reach this node at all.
    let peer_task = match (&config.peer_transport, &config.membership, &cluster) {
        (Some(peer_config), Some(membership_config), Some((router, ingress, _, _))) => {
            let server = peer::PeerServer::bind(
                membership_config.node_id.clone(),
                peer_config,
                Arc::new(peer::ForwardingHandler::new(
                    Arc::clone(&broker),
                    Arc::clone(ingress),
                    Arc::clone(router),
                    membership_config.advertise_addr.clone(),
                )),
            )
            .context("bind broker-internal listener")?;
            tracing::info!(
                addr = %server.local_addr()?,
                "broker-internal listener started (peer connections are encrypted but \
                 not yet authenticated; see docs/internal-protocol.md)",
            );
            Some(tokio::spawn(server.serve(peer_shutdown.clone())))
        }
        _ => None,
    };

    // Shard ownership: follow the control plane's assignments, and keep local
    // state and the routing table in step with them.
    let shard_tasks = match (&cluster, &config.controlplane_url, &durable_storage) {
        (Some((router, ingress, lifecycle, ownership)), Some(base_url), storage) => {
            let store: Arc<dyn shard_lifecycle::ShardStore> = match storage {
                Some(storage) => Arc::new(shard_lifecycle::DurableShardStore::new(Arc::new(
                    storage.clone(),
                ))),
                // Without durable storage there is no log to open, so taking a
                // shard is bookkeeping only.
                None => Arc::new(shard_lifecycle::EphemeralShardStore),
            };
            let watch = tokio::spawn(shard_watch::run(
                membership_client.clone(),
                base_url.clone(),
                // The assignment feed is cluster metadata, so it is read with
                // the same credential the rest of membership uses.
                config.membership.as_ref().map(|m| m.token.clone()),
                Arc::clone(ownership),
                Duration::from_millis(config.controlplane_sync_interval_ms),
                sync_shutdown.clone(),
            ));
            let feed = shard_routing::spawn_feed(
                shard_routing::FeedState {
                    ownership: Arc::clone(ownership),
                    lifecycle: Arc::clone(lifecycle),
                    store,
                    ingress: Arc::clone(ingress),
                    router: Arc::clone(router),
                },
                Some(shard_routing::CatalogSource {
                    client: membership_client.clone(),
                    base_url: base_url.clone(),
                    token: config.membership.as_ref().map(|m| m.token.clone()),
                }),
                Duration::from_millis(config.controlplane_sync_interval_ms),
                sync_shutdown.clone(),
            );
            Some((watch, feed))
        }
        _ => None,
    };

    // Block until the shutdown signal resolves so the process stays alive.
    // A refused registration ends the process too: a broker that is not a
    // cluster member should say so and stop, not serve traffic nobody routes.
    let mut membership_rejected = false;
    match &membership {
        Some(task) => {
            tokio::select! {
                _ = shutdown => {}
                _ = task.fatal.cancelled() => {
                    membership_rejected = true;
                }
            }
        }
        None => shutdown.await,
    }

    // Step 1: stop advertising readiness. Load balancers and the Kubernetes
    // endpoints controller drop this instance from rotation while it can still
    // serve, so new traffic is steered elsewhere rather than hitting a closing
    // listener. This must happen before anything stops working.
    draining.cancel();
    readiness.begin_draining();
    tracing::info!("readiness set to draining");

    // Step 2: stop admitting new connections. In-flight ones are untouched.
    accept_shutdown.cancel();

    // Step 3: drain in-flight work against a single shared deadline.
    let mut budget = DrainBudget::new(Duration::from_millis(config.shutdown_drain_timeout_ms));
    tracing::info!(
        deadline_ms = config.shutdown_drain_timeout_ms,
        "draining in-flight work"
    );

    // Closing the tracker is what lets `wait()` resolve; without it the wait would
    // hang until the deadline even with no connections left.
    connections.close();
    budget.drain("quic_connections", connections.wait()).await;

    // Peers stop being served only after client work has drained, so a forwarded
    // publish this broker is still applying is not cut off by its own shutdown.
    // Cancelling closes the connections, which tells every peer immediately
    // rather than leaving each to wait out its request timeout.
    if let Some(pool) = &peers {
        pool.shutdown().await;
    }
    if let Some(peer_task) = peer_task {
        peer_shutdown.cancel();
        let mut peer_task = peer_task;
        if !budget
            .drain("peer_listener", async {
                let _ = (&mut peer_task).await;
            })
            .await
        {
            peer_task.abort();
        }
    }

    let mut accept_task = accept_task;
    if !budget
        .drain("quic_accept_loop", async {
            let _ = (&mut accept_task).await;
        })
        .await
    {
        accept_task.abort();
    }

    // Leave the cluster before draining connections, so nothing new is placed
    // here while in-flight work finishes.
    if let (Some(membership_config), Some(base_url)) =
        (&config.membership, &config.controlplane_url)
        && !membership_rejected
    {
        budget
            .drain("membership_deregister", async {
                membership::shutdown_membership(
                    &membership_client,
                    base_url,
                    &membership_config.node_id,
                    &membership_config.token,
                )
                .await;
            })
            .await;
    }

    if let Some((watch, feed)) = shard_tasks {
        sync_shutdown.cancel();
        let mut watch = watch;
        let mut feed = feed;
        if !budget
            .drain("shard_watch", async {
                let _ = (&mut watch).await;
            })
            .await
        {
            watch.abort();
        }
        if !budget
            .drain("shard_feed", async {
                let _ = (&mut feed).await;
            })
            .await
        {
            feed.abort();
        }
    }

    let mut membership = membership;
    if let Some(task) = &mut membership {
        sync_shutdown.cancel();
        if !budget
            .drain("membership_heartbeat", async {
                let _ = (&mut task.handle).await;
            })
            .await
        {
            task.handle.abort();
        }
    }

    let mut controlplane_task = controlplane_task;
    if let Some(task) = &mut controlplane_task {
        sync_shutdown.cancel();
        if !budget
            .drain("controlplane_sync", async {
                let _ = (&mut *task).await;
            })
            .await
        {
            task.abort();
        }
    }

    // Step 3b: flush durable logs while the drain deadline still applies.
    // Publishes have stopped by now, so this is the last chance to push
    // page-cache bytes to the device — under `Periodic` it is the difference
    // between losing up to one interval of writes and losing nothing.
    if let Some(storage) = &durable_storage {
        budget
            .drain("durable_storage_flush", async {
                if let Err(err) = storage.shutdown().await {
                    tracing::error!(error = %err, "failed to flush durable storage on shutdown");
                }
            })
            .await;
    }

    // Step 4: metrics last, so `/ready` keeps reporting "draining" and `/metrics`
    // stays scrapeable for the whole drain. This is the window in which an operator
    // can actually see what the broker is doing while it shuts down.
    metrics_shutdown.cancel();
    let mut metrics_task = metrics_task;
    if !budget
        .drain("metrics_server", async {
            let _ = (&mut metrics_task).await;
        })
        .await
    {
        metrics_task.abort();
    }

    budget.report();
    if membership_rejected {
        tracing::error!("broker stopped: the control plane refused this node identity");
        return Err(anyhow::anyhow!(
            "control plane refused this node identity; check FELIX_NODE_ID and FELIX_NODE_ADVERTISE_ADDR"
        ));
    }
    tracing::info!("broker stopped");
    Ok(())
}

/// Build the QUIC server TLS configuration.
///
/// Current behavior:
/// - Generates a fresh self-signed certificate for `localhost` at startup.
/// - Configures Quinn/Rustls with that certificate.
///
/// This is convenient for local development but **not appropriate for production**.
/// Production should load a real certificate chain and private key (and should avoid
/// regenerating keys on each start).
/// The initial lease: conservative, and invalid until the first heartbeat.
///
/// The real duration comes from the control plane's expiry window on the first
/// accepted heartbeat, so this value only bounds how long a broker could serve
/// if that window ever stopped being reported.
fn peer_lease_state() -> broker::lease::LeaseState {
    broker::lease::LeaseState::new(Duration::from_secs(10))
}

fn build_server_config() -> Result<ServerConfig> {
    // Dev-only self-signed TLS config for QUIC endpoints.
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    Ok(ServerConfig::with_single_cert(
        vec![cert_der],
        key_der.into(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, prev }
        }

        fn unset(key: &'static str) -> Self {
            let prev = std::env::var(key).ok();
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(value) => unsafe {
                    std::env::set_var(self.key, value);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }

    // Basic sanity check that TLS config generation succeeds.
    #[test]
    fn build_server_config_smoke() -> Result<()> {
        let _config = build_server_config()?;
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn run_with_shutdown_starts_and_stops() -> Result<()> {
        let _g1 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", "127.0.0.1:0");
        let _g2 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
        let _g3 = EnvGuard::unset("FELIX_CP_URL");
        let _g4 = EnvGuard::set("FELIX_CONTROLPLANE_URL", "http://127.0.0.1:1");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            run_with_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
        });

        let _ = shutdown_tx.send(());
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("shutdown timeout")?;
        result?;
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn run_with_shutdown_controlplane_enabled() -> Result<()> {
        let _g1 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", "127.0.0.1:0");
        let _g2 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
        let _g3 = EnvGuard::set("FELIX_CP_URL", "http://127.0.0.1:1");
        let _g4 = EnvGuard::set("FELIX_CP_SYNC_INTERVAL_MS", "1");
        let _g5 = EnvGuard::set("FELIX_CONTROLPLANE_URL", "http://127.0.0.1:1");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            run_with_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = shutdown_tx.send(());
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("shutdown timeout")?;
        result?;
        Ok(())
    }
}
