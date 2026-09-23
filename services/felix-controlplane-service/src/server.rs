//! Running the control plane: build the state, start the listeners and the
//! background loops, then drain them in order when asked to stop.
//!
//! The drain order is the part to be careful with; `drain.rs` explains it.
mod drain;
mod observability;
mod state;
pub mod tls;

use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::time::Duration;

use felix_common::lifecycle::Readiness;
use tokio_util::sync::CancellationToken;

use crate::api::{build_bootstrap_router, build_router};
use crate::cluster::{membership, placement};
use crate::config::ControlPlaneConfig;
use crate::raft::LeadershipGate;
use crate::store;
use drain::Running;
use state::build_state;

/// Serve until `shutdown` resolves, then drain and return.
///
/// Returns early, with nothing to drain, if the API listener fails on its own.
pub async fn run<F>(config: ControlPlaneConfig, shutdown: F) -> anyhow::Result<()>
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
        placement::MovePolicy {
            max_concurrent: config.max_concurrent_shard_moves,
        },
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
            .map(tls::load_server_config)
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
                        tls::serve_mtls(listener, bootstrap_app, tls, api_shutdown).await;
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
    // connections and lets in-flight requests finish. Racing the server against
    // the shutdown future instead would drop it mid-request.
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

    Running {
        config,
        api_shutdown,
        metrics_shutdown,
        api_task,
        reconcile_task,
        expiry_task,
        bootstrap_task,
        raft_metrics_task,
        raft_handle,
        metrics_task,
    }
    .drain()
    .await
}

#[cfg(test)]
mod tests;
