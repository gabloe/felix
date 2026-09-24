//! Waiting for shutdown, then the bounded drain.
//!
//! The drain runs in a fixed order: readiness goes false, the listener keeps
//! admitting for the optional hold-off, then stops; in-flight connections
//! finish, peers and background tasks stop, durable logs are flushed, and the
//! metrics server goes last so an operator can watch the whole thing.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_broker::DurableStorage;
use felix_common::lifecycle::{self, DrainBudget, Readiness};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::cluster::ShardTasks;
use crate::cluster::credential::NodeCredential;
use crate::cluster::membership::{self, MembershipTask};
use crate::config::BrokerConfig;
use crate::peer::PeerPool;

/// Everything a running node has to stop, in the fields the drain reads.
pub(super) struct Running {
    pub(super) config: BrokerConfig,
    pub(super) readiness: Readiness,
    pub(super) draining: CancellationToken,
    pub(super) accept_shutdown: CancellationToken,
    pub(super) connections: TaskTracker,
    pub(super) peers: Option<Arc<PeerPool>>,
    pub(super) peer_task: Option<JoinHandle<()>>,
    pub(super) peer_shutdown: CancellationToken,
    pub(super) accept_tasks: Vec<JoinHandle<()>>,
    pub(super) membership_client: reqwest::Client,
    pub(super) credential: Option<NodeCredential>,
    pub(super) membership: Option<MembershipTask>,
    pub(super) shard_tasks: Option<ShardTasks>,
    pub(super) sync_shutdown: CancellationToken,
    pub(super) controlplane_task: Option<JoinHandle<()>>,
    pub(super) durable_storage: Option<DurableStorage>,
    pub(super) metrics_task: JoinHandle<std::io::Result<()>>,
    pub(super) metrics_shutdown: CancellationToken,
}

impl Running {
    /// Block until the shutdown signal resolves so the process stays alive.
    /// Returns whether the control plane refused this node's registration.
    ///
    /// A refused registration ends the process too: a broker that is not a
    /// cluster member should say so and stop, not serve traffic nobody routes.
    pub(super) async fn wait<F>(&self, shutdown: F) -> bool
    where
        F: Future<Output = ()>,
    {
        let mut membership_rejected = false;
        match &self.membership {
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
        membership_rejected
    }

    /// Drain in order against one shared deadline, then report what was left.
    pub(super) async fn drain(self, membership_rejected: bool) -> Result<()> {
        let Running {
            config,
            readiness,
            draining,
            accept_shutdown,
            connections,
            peers,
            peer_task,
            peer_shutdown,
            accept_tasks,
            membership_client,
            credential,
            membership,
            shard_tasks,
            sync_shutdown,
            controlplane_task,
            durable_storage,
            metrics_task,
            metrics_shutdown,
        } = self;

        // Step 1: stop advertising readiness. Load balancers and the Kubernetes
        // endpoints controller drop this instance from rotation while it can still
        // serve, so new traffic is steered elsewhere rather than hitting a closing
        // listener. This must happen before anything stops working.
        draining.cancel();
        readiness.begin_draining();
        tracing::info!("readiness set to draining");

        // Step 1b: keep accepting while a load balancer polling `/ready` notices.
        // Without it the listener stops admitting in the same breath as the flip.
        if config.shutdown_predrain_ms > 0 {
            tracing::info!(
                hold_off_ms = config.shutdown_predrain_ms,
                "serving while unready so load balancers can drop this broker"
            );
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(config.shutdown_predrain_ms)) => {}
                // An operator who signals twice is asking to skip the wait.
                _ = lifecycle::termination_signal() => {
                    tracing::info!("second termination signal; ending hold-off early");
                }
            }
        }

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
}
