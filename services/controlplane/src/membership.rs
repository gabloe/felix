//! Broker liveness expiry.
//!
//! A broker reports health on an interval. Nothing reports that a broker has
//! *stopped*, so silence is the only signal, and something has to notice it.
//! This is that something: a periodic sweep that marks nodes down once their
//! last heartbeat is older than the configured timeout.
//!
//! Running several control-plane instances is fine. The store moves each node
//! exactly once and only the instance that moved it publishes the change, so
//! duplicate sweeps cost a query and produce no duplicate events.
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config::NodeLivenessConfig;
use crate::store::ControlPlaneStore;

/// Run one expiry pass against `now_millis`.
///
/// Separate from the loop so tests can drive it at an exact time instead of
/// waiting for a timer.
pub async fn expire_once(
    store: &dyn ControlPlaneStore,
    liveness: &NodeLivenessConfig,
    now_millis: u64,
) -> usize {
    // Saturating: before the timeout has elapsed since the epoch nothing can be
    // stale, and wrapping would expire the whole cluster.
    let expiry_before = now_millis.saturating_sub(liveness.expiry_timeout_ms);

    match store.expire_stale_nodes(expiry_before).await {
        Ok(expired) => {
            for node in &expired {
                tracing::warn!(
                    node_id = %node.node_id,
                    last_heartbeat_at_millis = node.status.last_heartbeat_at_millis,
                    "broker missed its heartbeat window and was marked down",
                );
            }
            metrics::counter!("felix_node_expiry_total").increment(expired.len() as u64);
            expired.len()
        }
        Err(err) => {
            // Logged and retried on the next tick. A transient database error
            // must not leave liveness frozen for the rest of the process's life.
            tracing::error!(error = %err, "node expiry sweep failed");
            metrics::counter!("felix_node_expiry_failures_total").increment(1);
            0
        }
    }
}

/// Sweep for expired nodes until `shutdown` fires.
pub fn spawn_expiry_sweep(
    store: Arc<dyn ControlPlaneStore + Send + Sync>,
    liveness: NodeLivenessConfig,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(liveness.sweep_interval_ms));
        // A sweep that overruns its interval must not then run back-to-back
        // trying to catch up; the next tick is soon enough.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    expire_once(store.as_ref(), &liveness, crate::api::nodes::now_millis()).await;
                }
            }
        }
    })
}

#[cfg(test)]
#[path = "membership_tests.rs"]
mod tests;
