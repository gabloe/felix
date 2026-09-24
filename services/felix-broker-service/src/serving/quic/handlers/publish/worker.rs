//! The process-wide publish worker pool, and the publish context every
//! connection derives its own from.

use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use tokio::sync::mpsc;

use super::{
    PublishAdmission, PublishContext, PublishJob, PublishTarget, SubscriptionLimiter,
    decrement_depth,
};
use crate::config::BrokerConfig;
use crate::serving::quic::handlers::subscribe::WriterLaneManager;
use crate::serving::quic::{ClusterContext, GLOBAL_INGRESS_DEPTH};

pub(crate) fn build_publish_context(
    broker: Arc<Broker>,
    config: &BrokerConfig,
    cluster: ClusterContext,
) -> PublishContext {
    let ClusterContext {
        ingress,
        peers,
        lease,
        marks,
        client_endpoints,
    } = cluster;
    let quorum_timeout = std::time::Duration::from_millis(config.publish_quorum_timeout_ms.max(1));
    // Copied per worker below. A forward that outlasts this is cut off with its
    // own answer rather than the waiter's, which is the whole point -- see
    // `BrokerConfig::forward_budget`.
    let forward_budget = config.forward_budget();
    // NOTE: This is intentionally global for the process (not per-connection).
    // With per-connection worker pools, adding more publisher connections multiplied
    // concurrent broker.publish_batch callers and caused lock contention on shared broker state.
    let shards = crate::serving::core_shards::global_shards(config);
    // With core shards enabled, run exactly one publish worker per shard so
    // each stream's state has a single owning core (worker i lives on shard i,
    // and `publish_worker_index` maps handle id -> worker == shard).
    let worker_count = match &shards {
        Some(shards) => shards.len(),
        None => config.pub_workers_per_conn.max(1),
    };
    let publish_queue_depth = config.pub_queue_depth.max(1);
    let admission = Arc::new(PublishAdmission::new(config.pub_inflight_bytes));
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut worker_txs = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        #[cfg(not(feature = "perf_debug"))]
        let _ = worker_id;
        #[cfg(feature = "perf_debug")]
        let worker_label = worker_id.to_string();
        let (publish_tx, mut publish_rx) = mpsc::channel::<PublishJob>(publish_queue_depth);
        let queue_depth_worker = Arc::clone(&queue_depth);
        let broker_for_worker = Arc::clone(&broker);
        let peers_for_worker = peers.clone();
        let lease_for_worker = lease.clone();
        let marks_for_worker = marks.clone();
        let ingress_for_worker = ingress.clone();
        // How many durable publishes this worker may have awaiting their device
        // flush at once. The worker claims offsets serially -- that is what
        // keeps disk order equal to arrival order -- and then lets the flushes
        // overlap, because group commit only coalesces what is concurrently in
        // `ensure_durable`. At a fan-in of one the ceiling was one batch per
        // flush (#535).
        //
        // Bounded, not unbounded: the reason this pool is process-wide in the
        // first place is that unbounded concurrent `publish_batch` callers
        // contended on shared broker state (see the note above). This buys the
        // flush overlap without going back to that.
        let flush_slots = Arc::new(tokio::sync::Semaphore::new(
            config.pub_flush_concurrency.max(1),
        ));
        let worker_task = async move {
            while let Some(job) = publish_rx.recv().await {
                #[cfg(feature = "perf_debug")]
                metrics::counter!(
                    "felix_perf_publish_worker_wakeups_total",
                    "worker" => worker_label.clone()
                )
                .increment(1);
                let _ = decrement_depth(
                    &queue_depth_worker,
                    &GLOBAL_INGRESS_DEPTH,
                    "felix_broker_ingress_queue_depth",
                );
                #[cfg(feature = "perf_debug")]
                let worker_start = std::time::Instant::now();

                // Durable local publishes take the split path: claim here, in
                // queue order, then complete off-worker so the device flushes
                // overlap. Everything else -- forwards, idempotent sequences,
                // the single-node local target -- stays inline, because none of
                // them is waiting on a flush this worker could be sharing.
                if let PublishTarget::Resolved { handle, shard } = &job.target {
                    let lease_ok = match &lease_for_worker {
                        Some(lease) => lease.is_valid_now(),
                        None => true,
                    };
                    if lease_ok && handle.is_durable() {
                        // Serial, and the only ordered part: offsets are
                        // consumed here, so the order these return in is the
                        // order records land on disk.
                        match broker_for_worker.claim_publish(handle, &job.payloads).await {
                            Ok(claimed) => {
                                let permit = Arc::clone(&flush_slots)
                                    .acquire_owned()
                                    .await
                                    .expect("flush slots are never closed");
                                let broker = Arc::clone(&broker_for_worker);
                                let handle = handle.clone();
                                let shard = shard.clone();
                                let marks = marks_for_worker.clone();
                                let ingress = ingress_for_worker.clone();
                                let response = job.response;
                                tokio::spawn(async move {
                                    let result = match broker.complete_publish(claimed).await {
                                        Ok(outcome) => {
                                            crate::replication::quorum::await_quorum(
                                                &handle,
                                                shard.as_ref(),
                                                &outcome,
                                                marks.as_deref(),
                                                ingress.as_deref(),
                                                quorum_timeout,
                                            )
                                            .await
                                        }
                                        Err(err) => Err(err.into()),
                                    };
                                    if let Some(response) = response {
                                        let _ = response.send(result);
                                    }
                                    drop(permit);
                                });
                                continue;
                            }
                            Err(err) => {
                                if let Some(response) = job.response {
                                    let _ = response.send(Err(err.into()));
                                }
                                continue;
                            }
                        }
                    }
                }

                let result: Result<(), anyhow::Error> = match &job.target {
                    PublishTarget::Resolved { handle, shard } => {
                        // The commit fence, and the authoritative one. Everything
                        // between admission and here can take arbitrarily long --
                        // a full queue, a slow fsync, a suspended process -- so a
                        // lease that was valid on the way in may have lapsed. A
                        // broker that writes here after losing its lease is a
                        // broker writing a shard someone else may already lead.
                        match &lease_for_worker {
                            Some(lease) if !lease.is_valid_now() => {
                                crate::cluster::lease::metrics::record_refusal(
                                    crate::cluster::lease::metrics::BOUNDARY_COMMIT,
                                );
                                Err(anyhow::anyhow!(
                                    "lease lapsed before the record could be committed"
                                ))
                            }
                            _ => {
                                match broker_for_worker
                                    .publish_batch_with_outcome(handle, &job.payloads)
                                    .await
                                {
                                    Ok(outcome) => {
                                        crate::replication::quorum::await_quorum(
                                            handle,
                                            shard.as_ref(),
                                            &outcome,
                                            marks_for_worker.as_deref(),
                                            ingress_for_worker.as_deref(),
                                            quorum_timeout,
                                        )
                                        .await
                                    }
                                    Err(err) => Err(err.into()),
                                }
                            }
                        }
                    }
                    PublishTarget::Idempotent {
                        handle,
                        shard,
                        producer_id,
                        sequence,
                    } => match &lease_for_worker {
                        // The same commit fence as a plain publish: see above.
                        Some(lease) if !lease.is_valid_now() => {
                            crate::cluster::lease::metrics::record_refusal(
                                crate::cluster::lease::metrics::BOUNDARY_COMMIT,
                            );
                            Err(anyhow::anyhow!(
                                "lease lapsed before the record could be committed"
                            ))
                        }
                        _ => {
                            match broker_for_worker
                                .publish_batch_idempotent(
                                    handle,
                                    *producer_id,
                                    *sequence,
                                    &job.payloads,
                                )
                                .await
                            {
                                // A duplicate waits on the same quorum the
                                // original did: its offsets are the original's,
                                // and the answer must mean the same thing.
                                Ok(idempotent) => {
                                    crate::replication::quorum::await_quorum(
                                        handle,
                                        shard.as_ref(),
                                        &idempotent.outcome,
                                        marks_for_worker.as_deref(),
                                        ingress_for_worker.as_deref(),
                                        quorum_timeout,
                                    )
                                    .await
                                }
                                Err(err) => Err(err.into()),
                            }
                        }
                    },
                    #[cfg(test)]
                    PublishTarget::Named {
                        tenant_id,
                        namespace,
                        stream,
                    } => broker_for_worker
                        .publish_batch(tenant_id, namespace, stream, 0, &job.payloads)
                        .await
                        .map(|_| ())
                        .map_err(Into::into),
                    PublishTarget::Forward {
                        target,
                        key,
                        ack,
                        credential,
                    } => {
                        // Forwarding runs on the publish worker, not inline on
                        // the read loop, so a slow peer backs up the same queue
                        // a slow disk would and the existing backpressure and
                        // ack plumbing apply unchanged.
                        match &peers_for_worker {
                            Some(pool) => crate::serving::forward::forward_publish(
                                pool,
                                target,
                                key,
                                *ack,
                                credential,
                                job.payloads.clone(),
                                forward_budget,
                            )
                            .await
                            .map(|_| ())
                            .map_err(|err| anyhow::anyhow!("{err}")),
                            // The route said forward and there is nothing to
                            // forward with. Refusing beats writing another
                            // broker's shard locally.
                            None => Err(anyhow::anyhow!(
                                "no peer transport: this broker cannot forward to {}",
                                target.node_id
                            )),
                        }
                    }
                };
                #[cfg(feature = "perf_debug")]
                {
                    let ns = worker_start.elapsed().as_nanos() as u64;
                    metrics::histogram!("felix_perf_pub_worker_ns", "worker" => worker_label.clone())
                        .record(ns as f64);
                    metrics::counter!(
                        "felix_perf_publish_worker_jobs_total",
                        "worker" => worker_label.clone()
                    )
                    .increment(1);
                }
                if let Some(response) = job.response {
                    let _ = response.send(result);
                }
            }
        };
        match &shards {
            Some(shards) => {
                shards.handle_for(worker_id as u64).spawn(worker_task);
            }
            None => {
                tokio::spawn(worker_task);
            }
        }
        worker_txs.push(publish_tx);
    }
    PublishContext {
        ingress,
        peers,
        lease,
        client_endpoints,
        marks,
        quorum_timeout,
        workers: Arc::new(worker_txs),
        worker_count,
        depth: queue_depth,
        wait_timeout: Duration::from_millis(config.publish_queue_wait_timeout_ms),
        admission,
        // Placeholder; `handle_connection` replaces this (and `subscriptions`/`lane_manager`)
        // with fresh per-connection instances before this context is used by any stream on
        // that connection. These template values are never themselves shared across
        // connections.
        conn_admission: Arc::new(PublishAdmission::new(config.pub_conn_inflight_bytes)),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: WriterLaneManager::new(config),
        ingress_wait: config.pub_ingress_wait,
    }
}

#[cfg(test)]
mod tests;
