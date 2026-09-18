//! Felix control-plane HTTP service entry point.
//!
//! Wires configuration, storage, auth validators, and HTTP routers, then starts
//! the main API server and (optionally) the bootstrap server.
//!
//! The `build_state` helper keeps wiring testable and minimizes main setup logic.
use anyhow::Context;
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_bootstrap_router, build_router};
use controlplane::auth::oidc::UpstreamOidcValidator;
use controlplane::raft::{LeadershipGate, RaftHandle, RaftSettings};
use controlplane::store::raft_backend::RaftStore;
use controlplane::store::state_machine::MetadataStateMachine;
use controlplane::{config, membership, observability, placement, store};
use felix_common::lifecycle::{self, DrainBudget, Readiness};
use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::time::Duration;
use store::{ControlPlaneAuthStore, StoreConfig, memory::InMemoryStore, postgres::PostgresStore};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The migration tool rides the same binary so an operator's image has
    // it wherever the control plane runs; everything else stays env-driven.
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("migrate") {
        args.remove(0);
        return controlplane::migrate::run(args).await;
    }

    let config = config::ControlPlaneConfig::from_env_or_yaml().expect("control plane config");
    // SIGTERM is what Kubernetes, systemd, and `docker stop` send; SIGINT only
    // covers an interactive Ctrl-C. Evaluated as an argument, so the handlers are
    // installed before `run_with_shutdown` binds anything — a signal arriving
    // between binding and awaiting would otherwise kill the process outright.
    run_with_shutdown(config, lifecycle::termination_signal()).await
}

async fn run_with_shutdown<F>(config: config::ControlPlaneConfig, shutdown: F) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let metrics_handle = observability::init_observability("felix-controlplane");
    // A `FELIX_*` name nothing reads is a typo, and a typo is a default quietly
    // taking effect. Reported after logging is up so the warning is actually
    // seen, and as a warning rather than a refusal: an orchestrator may inject
    // variables meant for something else, and refusing to start over one is
    // worse than the mistake it guards against.
    for warning in felix_common::env_registry::unrecognised_warnings() {
        tracing::warn!("{warning}");
    }

    // One flag for the whole process. The metrics endpoint's `/ready` and the
    // API's `/v1/system/ready` both read it, so a drain is visible on both at
    // once rather than one of them insisting the instance is fine.
    //
    // It is created before the state so the state can hold it. The *tokens*
    // below stay separate because the metrics endpoint has to outlive the API
    // drain — that is how an operator watches the drain happen.
    let readiness = Readiness::ready();
    let (state, raft_handle) = build_state(config.clone(), readiness.clone()).await?;
    let _backend_name = state.store.backend_name();
    // Under Raft, singleton background work runs only on the (freshly
    // confirmed) leader; the other backends keep every instance sweeping,
    // which their stores make safe.
    let leadership = match &raft_handle {
        Some(handle) => LeadershipGate::Leader(handle.clone()),
        None => LeadershipGate::Always,
    };
    let api_shutdown = CancellationToken::new();
    let metrics_shutdown = CancellationToken::new();

    let metrics_task = {
        let metrics_shutdown = metrics_shutdown.clone();
        tokio::spawn(observability::serve_metrics(
            metrics_handle,
            config.metrics_bind,
            readiness.clone(),
            async move { metrics_shutdown.cancelled().await },
        ))
    };

    // Liveness expiry runs on the API token: it stops admitting work at the same
    // point the listener does, before anything drains.
    // Consensus position as gauges, on the API token like the other
    // background work.
    let raft_metrics_task = raft_handle
        .as_ref()
        .map(|handle| handle.spawn_metrics(api_shutdown.clone()));

    let expiry_task = membership::spawn_expiry_sweep(
        Arc::clone(&state.store) as Arc<dyn store::ControlPlaneStore + Send + Sync>,
        state.node_liveness.clone(),
        leadership.clone(),
        api_shutdown.clone(),
    );

    // Shard placement runs on the API token like expiry: it stops admitting work
    // at the same point the listener does.
    let reconcile_task = placement::spawn_reconciler(
        Arc::clone(&state.store) as Arc<dyn store::ControlPlaneStore + Send + Sync>,
        state.node_liveness.clone(),
        Duration::from_millis(state.node_liveness.shard_reconcile_interval_ms),
        leadership,
        api_shutdown.clone(),
    );

    let app = build_router(state.clone());
    // The Raft RPC routes ride the main listener — the design's "no new
    // port": one less listener to secure in M8.
    let app = match &raft_handle {
        Some(handle) => app.merge(handle.rpc_router()),
        None => app,
    };

    let bootstrap_task = if config.bootstrap.enabled {
        let bootstrap_addr = config.bootstrap.bind_addr;
        let bootstrap_app = build_bootstrap_router(state.clone());
        let api_shutdown = api_shutdown.clone();
        // Loaded before the task spawns: unreadable key material is a
        // misconfiguration that must fail startup, not a log line inside a
        // task nothing checks.
        let bootstrap_tls = config
            .bootstrap
            .tls
            .as_ref()
            .map(controlplane::tls::load_server_config)
            .transpose()?;
        Some(tokio::spawn(async move {
            tracing::info!(
                %bootstrap_addr,
                mtls = bootstrap_tls.is_some(),
                "bootstrap control plane listening"
            );
            match tokio::net::TcpListener::bind(bootstrap_addr).await {
                Ok(listener) => match bootstrap_tls {
                    Some(tls) => {
                        controlplane::tls::serve_mtls(listener, bootstrap_app, tls, api_shutdown)
                            .await;
                    }
                    None => {
                        let _ = axum::serve(listener, bootstrap_app.into_make_service())
                            .with_graceful_shutdown(async move { api_shutdown.cancelled().await })
                            .await;
                    }
                },
                Err(err) => {
                    tracing::warn!(error = %err, "failed to bind bootstrap listener");
                }
            }
        }))
    } else {
        None
    };

    let addr = config.bind_addr;
    tracing::info!(%addr, "control plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;

    // The API server drains itself: `with_graceful_shutdown` stops accepting new
    // connections and lets in-flight requests finish. Previously this was a
    // `select!` against the shutdown future, which dropped the server mid-request
    // and killed everything in flight.
    let mut api_task = {
        let api_shutdown = api_shutdown.clone();
        tokio::spawn(
            axum::serve(listener, app.into_make_service())
                .with_graceful_shutdown(async move { api_shutdown.cancelled().await })
                .into_future(),
        )
    };

    tokio::pin!(shutdown);
    tokio::select! {
        result = &mut api_task => {
            // The listener failed on its own; there is nothing left to drain.
            result??;
            return Ok(());
        }
        _ = &mut shutdown => {
            // Step 1: stop advertising readiness so load balancers remove this
            // instance while it can still serve. Must precede the listener stopping.
            readiness.begin_draining();
            tracing::info!("readiness set to draining");
        }
    }

    // Step 1b: keep serving while load balancers notice. Without this the
    // listener closes in the same breath as the readiness flip, and anything
    // still routed here is refused at the socket.
    if config.shutdown_predrain_ms > 0 {
        let hold_off = Duration::from_millis(config.shutdown_predrain_ms);
        tracing::info!(
            hold_off_ms = config.shutdown_predrain_ms,
            "serving while unready so load balancers can drop this instance"
        );
        tokio::select! {
            _ = tokio::time::sleep(hold_off) => {}
            // An operator who signals twice is asking to skip the wait.
            _ = lifecycle::termination_signal() => {
                tracing::info!("second termination signal; ending hold-off early");
            }
        }
    }

    // Step 2: stop admitting, then drain in-flight requests against one shared
    // deadline covering every subsystem.
    let mut budget = DrainBudget::new(Duration::from_millis(config.shutdown_drain_timeout_ms));
    tracing::info!(
        deadline_ms = config.shutdown_drain_timeout_ms,
        "draining in-flight requests"
    );
    api_shutdown.cancel();

    if !budget
        .drain("api_server", async {
            let _ = (&mut api_task).await;
        })
        .await
    {
        api_task.abort();
    }

    let mut reconcile_task = reconcile_task;
    if !budget
        .drain("shard_reconciler", async {
            let _ = (&mut reconcile_task).await;
        })
        .await
    {
        reconcile_task.abort();
    }

    let mut expiry_task = expiry_task;
    if !budget
        .drain("node_expiry_sweep", async {
            let _ = (&mut expiry_task).await;
        })
        .await
    {
        expiry_task.abort();
    }

    let mut bootstrap_task = bootstrap_task;
    if let Some(task) = &mut bootstrap_task
        && !budget
            .drain("bootstrap_server", async {
                let _ = (&mut *task).await;
            })
            .await
    {
        task.abort();
    }

    if let Some(task) = raft_metrics_task {
        task.abort();
    }
    if let Some(handle) = &raft_handle {
        // After the API stops (no new proposals), before metrics: a member
        // that leaves without shutting down looks like a failure to the
        // group and costs it an election timeout. Known fact, pre-0.10
        // openraft: there is no leadership-transfer API, so when the member
        // being deployed *is* the leader, the group pauses writes for one
        // election timeout before a successor takes over. Bounded (~1s at
        // defaults), recorded in docs/metadata-raft-design.md, and closed by
        // the 0.10 upgrade the seam exists to contain.
        if !budget
            .drain("raft_node", async {
                let _ = handle.shutdown().await;
            })
            .await
        {
            tracing::warn!("raft node did not shut down within the drain budget");
        }
    }

    // Step 3: metrics last, so `/ready` keeps reporting "draining" and `/metrics`
    // stays scrapeable for the whole drain.
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
    tracing::info!("control plane stopped");
    Ok(())
}

async fn build_state(
    config: config::ControlPlaneConfig,
    lifecycle_readiness: Readiness,
) -> anyhow::Result<(AppState, Option<RaftHandle>)> {
    let store_config = StoreConfig {
        changes_limit: config.changes_limit,
        change_retention_max_rows: config.change_retention_max_rows,
    };
    let mut raft_handle = None;
    let store: Arc<dyn ControlPlaneAuthStore + Send + Sync> = match config.storage {
        config::StorageBackend::Memory => Arc::new(InMemoryStore::new(store_config)),
        config::StorageBackend::Postgres => {
            let pg = config
                .postgres
                .as_ref()
                .context("postgres configuration missing")?;
            Arc::new(PostgresStore::connect(pg, store_config).await?)
        }
        config::StorageBackend::Raft => {
            let raft_cfg = config.raft.as_ref().context("raft configuration missing")?;
            let inner = Arc::new(InMemoryStore::new(store_config));
            let machine = Arc::new(MetadataStateMachine::new(inner));
            let mut settings = RaftSettings::new(raft_cfg.node_id, raft_cfg.data_dir.clone());
            if let Some(ms) = raft_cfg.heartbeat_ms {
                settings.heartbeat_interval = Duration::from_millis(ms);
            }
            if let Some(ms) = raft_cfg.election_timeout_min_ms {
                settings.election_timeout.0 = Duration::from_millis(ms);
            }
            if let Some(ms) = raft_cfg.election_timeout_max_ms {
                settings.election_timeout.1 = Duration::from_millis(ms);
            }
            if let Some(logs) = raft_cfg.snapshot_logs_since_last {
                settings.snapshot_logs_since_last = logs;
            }
            if let Some(logs) = raft_cfg.logs_kept_behind_snapshot {
                settings.logs_kept_behind_snapshot = logs;
            }
            if let Some(ms) = raft_cfg.write_timeout_ms {
                settings.write_timeout = Duration::from_millis(ms);
            }
            let handle = RaftHandle::start(
                settings,
                Arc::clone(&machine) as Arc<dyn controlplane::raft::AppStateMachine>,
            )
            .await?;
            // Every member initializes with the same configured group, which
            // openraft documents as safe; a member with prior state, or one
            // beaten to it by a peer, is told so and simply resumes. A real
            // failure surfaces as no leader, which readiness reports.
            if let Err(err) = handle.initialize(raft_cfg.peers.clone()).await {
                tracing::info!(error = %err, "raft group not initialized here (already formed, or resuming)");
            }
            raft_handle = Some(handle.clone());
            Arc::new(RaftStore::new(handle, machine))
        }
    };

    let readiness = Arc::new(controlplane::readiness::Readiness::with_lifecycle(
        // The same flag the metrics endpoint reads, so a drain is visible on
        // both ports at once.
        lifecycle_readiness.clone(),
        // The store, seen through the one method readiness needs.
        Arc::new(controlplane::readiness::StoreProbe(Arc::clone(&store))),
        std::time::Duration::from_millis(config.readiness_timeout_ms),
        std::time::Duration::from_millis(config.readiness_cache_ttl_ms),
    ));

    Ok((
        AppState {
            region: Region {
                region_id: config.region_id,
                display_name: "Local Region".to_string(),
            },
            api_version: "v1".to_string(),
            features: FeatureFlags {
                durable_storage: store.is_durable(),
                tiered_storage: false,
                bridges: false,
            },
            store,
            readiness,
            in_flight: Default::default(),
            oidc_validator: UpstreamOidcValidator::new_with_allowed_algorithms(
                std::time::Duration::from_secs(3600),
                std::time::Duration::from_secs(3600),
                60,
                config.oidc_allowed_algorithms,
            ),
            bootstrap_enabled: config.bootstrap.enabled,
            bootstrap_tokens: config.bootstrap.accepted_tokens(),
            node_liveness: config.node_liveness,
        },
        raft_handle,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    async fn build_state_memory_backend() {
        let config = config::ControlPlaneConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind"),
            metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
            region_id: "local".to_string(),
            storage: config::StorageBackend::Memory,
            postgres: None,
            raft: None,
            changes_limit: 10,
            change_retention_max_rows: Some(20),
            oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
            bootstrap: config::BootstrapConfig {
                enabled: false,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: None,
                previous_token: None,
                tls: None,
            },
            node_liveness: config::NodeLivenessConfig::default(),
            shutdown_drain_timeout_ms: 25_000,
            shutdown_predrain_ms: 0,
            readiness_timeout_ms: config::DEFAULT_READINESS_TIMEOUT_MS,
            readiness_cache_ttl_ms: config::DEFAULT_READINESS_CACHE_TTL_MS,
        };
        let (state, _raft) = build_state(config, Readiness::ready())
            .await
            .expect("state");
        assert_eq!(state.region.region_id, "local");
        assert!(!state.features.durable_storage);
    }

    #[tokio::test]
    async fn build_state_postgres_requires_config() {
        let config = config::ControlPlaneConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind"),
            metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
            region_id: "local".to_string(),
            storage: config::StorageBackend::Postgres,
            postgres: None,
            raft: None,
            changes_limit: 10,
            change_retention_max_rows: Some(20),
            oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
            bootstrap: config::BootstrapConfig {
                enabled: false,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: None,
                previous_token: None,
                tls: None,
            },
            node_liveness: config::NodeLivenessConfig::default(),
            shutdown_drain_timeout_ms: 25_000,
            shutdown_predrain_ms: 0,
            readiness_timeout_ms: config::DEFAULT_READINESS_TIMEOUT_MS,
            readiness_cache_ttl_ms: config::DEFAULT_READINESS_CACHE_TTL_MS,
        };
        let err = build_state(config, Readiness::ready())
            .await
            .err()
            .expect("missing postgres");
        assert!(err.to_string().contains("postgres configuration missing"));
    }

    #[tokio::test]
    async fn build_state_postgres_attempts_connection_when_config_present() {
        let config = config::ControlPlaneConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind"),
            metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
            region_id: "local".to_string(),
            storage: config::StorageBackend::Postgres,
            postgres: Some(config::PostgresConfig {
                url: "postgres://postgres:postgres@127.0.0.1:1/postgres".to_string(),
                max_connections: 1,
                connect_timeout_ms: 500,
                acquire_timeout_ms: 500,
            }),
            raft: None,
            changes_limit: 10,
            change_retention_max_rows: Some(20),
            oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
            bootstrap: config::BootstrapConfig {
                enabled: true,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: Some("bootstrap-token".to_string()),
                previous_token: None,
                tls: None,
            },
            node_liveness: config::NodeLivenessConfig::default(),
            shutdown_drain_timeout_ms: 25_000,
            shutdown_predrain_ms: 0,
            readiness_timeout_ms: config::DEFAULT_READINESS_TIMEOUT_MS,
            readiness_cache_ttl_ms: config::DEFAULT_READINESS_CACHE_TTL_MS,
        };
        let err = build_state(config, Readiness::ready())
            .await
            .err()
            .expect("connect should fail");
        let text = err.to_string();
        assert!(text.contains("pool") || text.contains("connect") || text.contains("Connection"));
    }

    #[tokio::test]
    #[serial]
    async fn run_with_shutdown_starts_and_stops_without_bootstrap() {
        let config = config::ControlPlaneConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind"),
            metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
            region_id: "local".to_string(),
            storage: config::StorageBackend::Memory,
            postgres: None,
            raft: None,
            changes_limit: 10,
            change_retention_max_rows: Some(20),
            oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
            bootstrap: config::BootstrapConfig {
                enabled: false,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: None,
                previous_token: None,
                tls: None,
            },
            node_liveness: config::NodeLivenessConfig::default(),
            shutdown_drain_timeout_ms: 25_000,
            shutdown_predrain_ms: 0,
            readiness_timeout_ms: config::DEFAULT_READINESS_TIMEOUT_MS,
            readiness_cache_ttl_ms: config::DEFAULT_READINESS_CACHE_TTL_MS,
        };
        run_with_shutdown(config, async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        })
        .await
        .expect("run should stop cleanly");
    }

    #[tokio::test]
    #[serial]
    async fn run_with_shutdown_starts_and_stops_with_bootstrap() {
        let config = config::ControlPlaneConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind"),
            metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
            region_id: "local".to_string(),
            storage: config::StorageBackend::Memory,
            postgres: None,
            raft: None,
            changes_limit: 10,
            change_retention_max_rows: Some(20),
            oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
            bootstrap: config::BootstrapConfig {
                enabled: true,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: Some("bootstrap-token".to_string()),
                previous_token: None,
                tls: None,
            },
            node_liveness: config::NodeLivenessConfig::default(),
            shutdown_drain_timeout_ms: 25_000,
            shutdown_predrain_ms: 0,
            readiness_timeout_ms: config::DEFAULT_READINESS_TIMEOUT_MS,
            readiness_cache_ttl_ms: config::DEFAULT_READINESS_CACHE_TTL_MS,
        };
        run_with_shutdown(config, async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        })
        .await
        .expect("run should stop cleanly");
    }
}
