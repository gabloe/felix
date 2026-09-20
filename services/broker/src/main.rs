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
use broker::credential;
use broker::membership;
use broker::peer;
use broker::replication;
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
    // `--print-config` before anything is bound, so it can be run against a
    // live deployment's environment without a port conflict.
    //
    // It doubles as a pre-flight check: the config is loaded the same way
    // startup loads it, so a file that will not parse or a key the broker does
    // not know fails here — before a rollout — with the same message it would
    // have produced on the node.
    if std::env::args().any(|arg| arg == "--print-config") {
        return print_config();
    }

    // Default shutdown trigger: SIGTERM or SIGINT. SIGTERM is what Kubernetes,
    // systemd, and `docker stop` actually send; SIGINT only covers an interactive
    // Ctrl-C. Evaluated as an argument, so the handlers are installed before
    // `run_with_shutdown` binds anything — a signal arriving between binding and
    // awaiting would otherwise kill the process outright.
    // `run_with_shutdown` is written so we can reuse the same startup logic
    // in tests or alternative hosting environments by passing a different future.
    run_with_shutdown(lifecycle::termination_signal()).await
}

/// Print the configuration this broker would run with, and stop.
///
/// The question an operator actually has is "what is this process running
/// with", and until now the only answer was to read the environment, the config
/// file, and the defaults in the source, then combine them by hand.
///
/// YAML because that is what the config file is, so the shape is one an
/// operator already recognises. The credential is redacted: this output exists
/// to be pasted into an issue.
///
/// Unrecognised variables go to stderr rather than into the document, so the
/// warnings survive a `> config.yml` and the document stays a document.
fn print_config() -> Result<()> {
    for warning in felix_common::env_registry::unrecognised_warnings() {
        eprintln!("warning: {warning}");
    }
    let config = config::BrokerConfig::from_env_or_yaml().context("load broker configuration")?;
    println!(
        "{}",
        serde_yaml_ng::to_string(&config).context("render the configuration")?
    );
    Ok(())
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
    // A `FELIX_*` name nothing reads is a typo, and a typo is a default quietly
    // taking effect. Reported after logging is up so the warning is actually
    // seen, and as a warning rather than a refusal: an orchestrator may inject
    // variables meant for something else, and refusing to start over one is
    // worse than the mistake it guards against.
    for warning in felix_common::env_registry::unrecognised_warnings() {
        tracing::warn!("{warning}");
    }

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
    // Shared between the replication driver, which advances it, and the publish
    // path, which waits on it for `Quorum` streams.
    let quorum_marks = Arc::new(replication::quorum::QuorumMarks::new());
    // What replication has stopped for. Read by the admin listing, because the
    // metric is a bare count: a label per shard is a label per stream per
    // tenant, and a halt is useless to act on without knowing which replica.
    let halted_replicas = Arc::new(replication::halted::HaltedReplicas::new());
    // Empty until the first catalog refresh fills it, which is the honest
    // answer in the meantime: this broker has not yet been told where any
    // client may connect.
    let client_endpoints = Arc::new(broker::client_endpoints::ClientEndpoints::new());
    let peer_shutdown = CancellationToken::new();
    // One identity for both ends of the peer transport, so a rotation
    // reaches the listener and the dialler together. Loaded before either
    // binds: unreadable key material is a misconfiguration to refuse at
    // startup, not a handshake to fail later.
    let peer_tls = match config
        .peer_transport
        .as_ref()
        .and_then(|peer| peer.tls.as_ref())
    {
        Some(paths) => {
            let tls = Arc::new(peer::tls::PeerTls::load(paths).context("load peer mTLS material")?);
            drop(Arc::clone(&tls).spawn_reload(peer_shutdown.clone()));
            Some(tls)
        }
        None => None,
    };
    let peers = match (&config.peer_transport, &config.membership) {
        (Some(peer_config), Some(membership_config)) => Some(
            peer::PeerPool::new_with_tls(
                membership_config.node_id.clone(),
                peer_config.clone(),
                peer_shutdown.clone(),
                peer_tls.clone(),
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

    // Durable storage is opened before the listener binds: recovering segments
    // can take time and can fail, and both are better surfaced as a startup
    // error than as a failed publish once traffic is arriving.
    let durable_config = DurableStorageConfig::from_env()?;
    let durable_storage = match &durable_config {
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

    // The cache is a log when there is a disk to put one on.
    //
    // Under `caches/`, not the stream root: a shard directory is named from a
    // hash of its tenant, namespace and stream, so a cache and a stream sharing
    // a name would otherwise interleave their records. See
    // `docs/cache-on-log.md`.
    //
    // Without durable storage the cache stays in memory, which is the only
    // thing it can be: there is nowhere to write a log.
    let cache: Box<dyn felix_storage::StorageApi + Send> = match &durable_config {
        Some(durable) => {
            let root = durable.root.join("caches");
            tracing::info!(root = %root.display(), "opening the cache on durable storage");
            Box::new(
                felix_storage::LogCache::open(&root, durable.log.clone())
                    .with_context(|| format!("open the cache log at {}", root.display()))?,
            )
        }
        None => {
            tracing::info!("cache is in memory and is lost on restart");
            Box::new(EphemeralCache::new())
        }
    };

    // Consumer-group positions live on their own root, for the same reason the
    // cache does: a stream named `orders` and a cache named `orders` must not
    // share a directory, and neither must the group state for either.
    //
    // Only with durable storage. A group whose position is lost on restart
    // redelivers everything it had already processed, so an in-memory version
    // would be worse than not offering queues at all.
    let consumer_groups = match &durable_config {
        Some(durable) => {
            let root = durable.root.join("groups");
            tracing::info!(root = %root.display(), "opening consumer-group state");
            let dead_root = durable.root.join("dead-letters");
            Some((
                std::sync::Arc::new(
                    felix_broker::consumer_groups::ConsumerGroups::open(&root, durable.log.clone())
                        .with_context(|| {
                            format!("open the consumer-group log at {}", root.display())
                        })?,
                ),
                std::sync::Arc::new(
                    felix_broker::dead_letters::DeadLetters::open(&dead_root, durable.log.clone())
                        .with_context(|| {
                            format!("open the dead-letter log at {}", dead_root.display())
                        })?,
                ),
            ))
        }
        None => None,
    };

    let broker = Broker::new(cache)
        .with_topic_capacity(config.subscriber_queue_capacity.max(1))
        .context("configure subscriber queue depth")?
        .with_subscriber_queue_policy(config.subscriber_queue_policy);
    let broker = match consumer_groups {
        Some((groups, dead_letters)) => broker.with_consumer_groups(
            groups,
            dead_letters,
            Duration::from_millis(config.group_visibility_timeout_ms),
            config.group_max_attempts,
        ),
        None => broker,
    };
    let broker = match durable_storage.clone() {
        Some(storage) => broker.with_durable_storage(storage),
        None => broker,
    };
    // Counters on a root of their own, beside the cache's rather than inside
    // it: a counter record is a new durable shape, and a store of its own
    // keeps its blast radius to the counters. Only with durable storage — a
    // sum any restart resets is worse than refusing to count.
    let broker = match &durable_config {
        Some(durable) => {
            let root = durable.root.join("counters");
            tracing::info!(root = %root.display(), "opening counters");
            broker.with_counters(std::sync::Arc::new(
                felix_storage::CounterStore::open(&root, durable.log.clone())
                    .with_context(|| format!("open the counter log at {}", root.display()))?,
            ))
        }
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
            Arc::clone(&halted_replicas),
            async move { metrics_shutdown.cancelled().await },
        ))
    };

    // Build and bind the QUIC listener. `build_server_config` currently uses a self-signed
    // certificate suitable for local development.
    let server_config = build_server_config().context("build QUIC server config")?;

    // Apply transport-level configuration (flow control windows, pooling behavior, etc.)
    // derived from broker config.
    let transport = broker::transport::cache_transport_config(&config, TransportConfig::default());

    // One `QuicServer` per configured listener. Each owns its own UDP socket and
    // therefore its own `quinn` endpoint driver -- the single task that reads
    // every datagram for that socket and routes it by connection id. That task
    // is the per-broker throughput ceiling (#557): it cannot use more than one
    // core, and it saturates while the rest of the machine idles. N sockets are
    // N drivers. Default is one, so a deployment that has not asked for more
    // binds exactly what it always did.
    let mut quic_servers = Vec::with_capacity(config.quic_listeners);
    for bind_addr in config.quic_binds() {
        let server = QuicServer::bind(bind_addr, server_config.clone(), transport.clone())
            .with_context(|| format!("bind QUIC listener on {bind_addr}"))?;
        tracing::info!(addr = %server.local_addr()?, "quic listener started");
        quic_servers.push(Arc::new(server));
    }

    // Start accepting QUIC connections in a background task.
    // If the accept loop exits due to an error, we log and continue shutdown normally.
    let broker = Arc::new(broker);
    // One accept loop per listener. They share everything behind them -- the
    // same broker, the same connection registry, the same shutdown token -- and
    // differ only in the socket they read from.
    let accept_tasks: Vec<_> = quic_servers
        .iter()
        .map(|server| {
            let quic_server = Arc::clone(server);
            let broker = Arc::clone(&broker);
            let quic_config = config.clone();
            let auth = Arc::clone(&auth);
            let accept_shutdown = accept_shutdown.clone();
            let connections = connections.clone();
            let seeded = seeded.clone();
            let ingress_router = ingress_router.clone();
            let peers_for_accept = peers.clone();
            let lease_for_accept = lease.clone();
            let marks_for_accept = Arc::clone(&quorum_marks);
            let endpoints_for_accept = Arc::clone(&client_endpoints);
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
                        marks: Some(Arc::clone(&marks_for_accept)),
                        client_endpoints: Some(Arc::clone(&endpoints_for_accept)),
                    },
                )
                .await
                {
                    tracing::warn!(error = %err, "quic accept loop exited");
                }
            })
        })
        .collect();

    // Optional: start a periodic control-plane sync to keep tenant/namespace/stream metadata
    // refreshed. When disabled, the broker relies solely on local registrations.
    // One holder, shared by every control-plane caller: the metadata sync
    // here, and membership below. A refresh swaps what is inside it, so a
    // caller handed it at startup keeps presenting a current token for the
    // life of the process rather than the one token it was given.
    let credential = (!config.controlplane_token.is_empty())
        .then(|| credential::NodeCredential::new(config.controlplane_token.clone()));
    let (seeded_tx, seeded_rx) = tokio::sync::oneshot::channel();
    let controlplane_task = if let Some(base_url) = config.controlplane_url.clone() {
        let sync_credential = credential.clone();
        let interval_ms = config.controlplane_sync_interval_ms;
        let broker = Arc::clone(&broker);
        let sync_shutdown = sync_shutdown.clone();
        let seeded_tx = gate_readiness_on_sync.then_some(seeded_tx);
        // The feeds require `node.view:cluster:*`, so a sync with nothing to
        // present is refused on every poll. Said once here, at startup, rather
        // than discovered from a wall of 401s -- and as a warning, because the
        // JWKS fetch that verifies client tokens is unauthenticated and still
        // works.
        if sync_credential.is_none() {
            tracing::warn!(
                "FELIX_CONTROLPLANE_URL is set but no FELIX_NODE_TOKEN: the control \
                 plane will refuse the metadata sync, so no tenant, namespace, stream \
                 or cache will be learned from it"
            );
        }
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
                    sync_credential,
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
            let node_credential = credential
                .clone()
                .expect("a cluster member has a credential");
            // Refresh only when the operator provided somewhere to keep the
            // rotating half. Without it the broker behaves exactly as it did
            // before refresh existed: it runs on the token it was given, and
            // leaves the cluster when that expires.
            match membership_config.refresh_token_file.clone() {
                Some(refresh_token_file) => {
                    tokio::spawn(credential::refresh::run(
                        credential::refresh::RefreshConfig {
                            client: membership_client.clone(),
                            base_url: base_url.clone(),
                            credential: node_credential.clone(),
                            refresh_token_file,
                        },
                        sync_shutdown.clone(),
                    ));
                }
                None => tracing::info!(
                    "no FELIX_NODE_REFRESH_TOKEN_FILE: this broker will run on \
                     the credential it was given and leave the cluster when it \
                     expires",
                ),
            }

            // The other way a credential stays current: something outside the
            // broker rewrites the token file. Watched whenever the token came
            // from one, refresh loop or not -- the two are not alternatives, and
            // a deployment that runs both is a deployment where either can win.
            if let Some(node_token_file) = membership_config.node_token_file.clone() {
                tokio::spawn(credential::rotate::run(
                    node_token_file,
                    node_credential.clone(),
                    credential::rotate::POLL_INTERVAL,
                    sync_shutdown.clone(),
                ));
            }

            // Published once at startup so the series exists before the first
            // refresh or rotation, which on a long-lived token is hours away.
            credential::report_expiry(&node_credential);
            Some(membership::spawn(
                membership_client.clone(),
                base_url.clone(),
                membership_config.clone(),
                node_credential,
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
            let server = peer::PeerServer::bind_with_tls(
                membership_config.node_id.clone(),
                peer_config,
                Arc::new(peer::BrokerPeerHandler::new(
                    peer::ForwardingHandler::new(
                        Arc::clone(&broker),
                        Arc::clone(ingress),
                        Arc::clone(router),
                        membership_config.advertise_addr.clone(),
                        Some(Arc::clone(&quorum_marks)),
                        Duration::from_millis(config.publish_quorum_timeout_ms.max(1)),
                        Arc::clone(&auth),
                    ),
                    peer::ReplicaHandler::new(Arc::clone(&broker), Arc::clone(router)),
                )),
                peer_tls.clone(),
            )
            .context("bind broker-internal listener")?;
            match &peer_config.tls {
                Some(tls) => tracing::info!(
                    addr = %server.local_addr()?,
                    ca = %tls.ca_path,
                    "broker-internal listener started; peers must present a certificate \
                     from this CA issued to their node id",
                ),
                None => tracing::warn!(
                    addr = %server.local_addr()?,
                    "broker-internal listener started WITHOUT peer authentication: \
                     anything that can reach it is a peer. Set FELIX_INTERNAL_TLS_CERT, \
                     FELIX_INTERNAL_TLS_KEY and FELIX_INTERNAL_TLS_CA, or keep the port \
                     reachable from brokers only; see docs/internal-protocol.md",
                ),
            }
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
                credential.clone(),
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
                    client_endpoints: Some(Arc::clone(&client_endpoints)),
                },
                Some(shard_routing::CatalogSource {
                    client: membership_client.clone(),
                    base_url: base_url.clone(),
                    token: credential.clone(),
                }),
                Duration::from_millis(config.controlplane_sync_interval_ms),
                sync_shutdown.clone(),
            );
            // Shipping to followers, for the shards this broker leads. Only
            // when there is a peer transport to ship over: without one the
            // replica set is a plan nobody can act on.
            if let Some(pool) = &peers {
                replication::driver::spawn(
                    Arc::clone(pool),
                    Arc::clone(&broker),
                    Arc::clone(router),
                    replication::driver::Published {
                        marks: Arc::clone(&quorum_marks),
                        halted: Arc::clone(&halted_replicas),
                    },
                    // Only a broker that is a cluster member reports: the
                    // report is about shards the control plane assigned, and a
                    // broker it has never registered leads none of them.
                    //
                    // Reports from every shard in a pass share one request —
                    // see `replication::reporter`.
                    config.membership.as_ref().map(|membership| {
                        let (reporter, _task) = replication::reporter::Reporter::spawn(
                            replication::driver::ReportTo {
                                client: membership_client.clone(),
                                base_url: base_url.clone(),
                                node_id: membership.node_id.clone(),
                                token: credential.clone(),
                                incarnation: 0,
                            },
                            sync_shutdown.clone(),
                        );
                        reporter
                    }),
                    Duration::from_millis(config.controlplane_sync_interval_ms),
                    replication::RebuildPolicy {
                        max_concurrent: config.replication_rebuild_max_concurrent,
                        bytes_per_sec: config.replication_rebuild_bytes_per_sec,
                    },
                    sync_shutdown.clone(),
                );
            }
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

    let mut accept_tasks = accept_tasks;
    if !budget
        .drain("quic_accept_loop", async {
            for task in &mut accept_tasks {
                let _ = task.await;
            }
        })
        .await
    {
        for task in &accept_tasks {
            task.abort();
        }
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
                    // The current token, not the startup one: a broker that has
                    // been up for hours would otherwise deregister with an
                    // expired credential and be refused, leaving the control
                    // plane to expire it as if it had crashed.
                    &credential
                        .as_ref()
                        .map(|credential| credential.bearer().to_string())
                        .unwrap_or_default(),
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

    // A generated certificate nothing can name is a certificate only a client
    // that skips verification can use, which is how "just disable TLS
    // verification in dev" becomes a habit. Writing it out gives every client
    // -- including the ones that are not Rust and cannot reach into this
    // process -- a real CA file to trust.
    if let Ok(path) = std::env::var("FELIX_TLS_CERT_EXPORT")
        && !path.trim().is_empty()
    {
        export_certificate(&cert.cert.pem(), &path)?;
    }

    Ok(ServerConfig::with_single_cert(
        vec![cert_der],
        key_der.into(),
    )?)
}

/// Write the broker's certificate where a client can trust it from.
///
/// Fails startup rather than warning: a deployment that asked for the export
/// is a deployment whose clients are configured to read it, and coming up
/// without it produces connection failures whose cause is nowhere near the
/// symptom.
fn export_certificate(pem: &str, path: &str) -> Result<()> {
    if let Some(parent) = std::path::Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create the directory for FELIX_TLS_CERT_EXPORT {path}"))?;
    }
    std::fs::write(path, pem).with_context(|| format!("write the broker certificate to {path}"))?;
    tracing::info!(
        path,
        "wrote the broker's self-signed certificate for clients to trust"
    );
    Ok(())
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
