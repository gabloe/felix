// Ingress: worker sharding, the bounded enqueue path, and in-flight depth accounting.

use anyhow::{Result, anyhow};
use bytes::Bytes;
use felix_broker::StreamHandle;
#[cfg(test)]
use std::collections::hash_map::DefaultHasher;
#[cfg(test)]
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "perf_debug")]
use std::time::Instant;
use tokio::sync::mpsc;

use crate::transport::quic::GLOBAL_INGRESS_DEPTH;
use crate::transport::quic::handlers::publish::ack::EnqueuePolicy;
use crate::transport::quic::handlers::publish::admission::AdmissionPermit;
use crate::transport::quic::handlers::publish::{PublishContext, PublishJob};

pub(crate) enum PublishTarget {
    Resolved(StreamHandle),
    #[cfg(test)]
    Named {
        tenant_id: String,
        namespace: String,
        stream: String,
    },
}

/// Deterministically map (tenant, namespace, stream) to a publish worker index.
///
/// Goal: keep ordering locality and cache locality for a given stream by always hashing to
/// the same worker, while distributing streams across workers reasonably well.
///
/// This *must* be stable across processes for predictable performance; it does not need to be
/// cryptographically secure.
#[cfg(test)]
pub(crate) fn publish_worker_index(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    worker_count: usize,
) -> usize {
    if worker_count == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    tenant_id.hash(&mut hasher);
    namespace.hash(&mut hasher);
    stream.hash(&mut hasher);
    (hasher.finish() as usize) % worker_count
}

/// Enqueue a publish job into the appropriate worker queue with explicit overload semantics.
///
/// Return value:
/// - `Ok(true)`  → job enqueued
/// - `Ok(false)` → job intentionally dropped (policy = Drop)
/// - `Err(...)`  → failure to enqueue (policy = Fail, closed queue, timeout, etc.)
///
/// Implementation detail:
/// - We try `try_send` first to keep the common path allocation-free and to make overload observable
///   (`Full` vs `Closed`). Only `Wait` does an async `send` with a timeout.
/// - Any path that successfully enqueues must increment both local and global depth **exactly once**.
pub(crate) async fn enqueue_publish(
    publish_ctx: &PublishContext,
    mut job: PublishJob,
    policy: EnqueuePolicy,
) -> Result<bool> {
    let worker_index = match &job.target {
        PublishTarget::Resolved(handle) => handle.id() as usize % publish_ctx.worker_count.max(1),
        #[cfg(test)]
        PublishTarget::Named {
            tenant_id,
            namespace,
            stream,
        } => publish_worker_index(tenant_id, namespace, stream, publish_ctx.worker_count),
    };
    let worker = publish_ctx
        .workers
        .get(worker_index)
        .ok_or_else(|| anyhow!("publish worker index out of range"))?;

    // Byte-based admission gate, independent of the item-count queue depth: bounds total bytes
    // queued-or-processing so a handful of large payloads/batches can't blow past the intended
    // ingress memory budget. Two gates are applied: the connection's own share
    // (`conn_admission`) first, then the shared process-wide budget (`admission`). Gating on the
    // per-connection budget first means one connection maxing out its own share can't consume
    // global-budget accounting cycles meant for other connections. Both permits travel with the
    // job and are released together once the job is done (or dropped without ever being
    // enqueued).
    let job_bytes: usize = job.payloads.iter().map(Bytes::len).sum();
    let (conn_permit, permit) = match policy {
        EnqueuePolicy::Wait => {
            let acquire_both = async {
                let conn_permit = publish_ctx.conn_admission.acquire(job_bytes).await?;
                let permit = publish_ctx.admission.acquire(job_bytes).await?;
                Ok::<_, tokio::sync::AcquireError>((conn_permit, permit))
            };
            match tokio::time::timeout(publish_ctx.wait_timeout, acquire_both).await {
                Ok(Ok(permits)) => permits,
                Ok(Err(_)) => return Err(anyhow!("publish admission closed")),
                Err(_) => return Err(anyhow!("publish admission timed out")),
            }
        }
        EnqueuePolicy::Drop | EnqueuePolicy::Fail => {
            let conn_permit = match publish_ctx.conn_admission.try_acquire(job_bytes) {
                Ok(permit) => permit,
                Err(_) => {
                    t_counter!("felix_broker_ingress_conn_bytes_full_total").increment(1);
                    return match policy {
                        EnqueuePolicy::Drop => {
                            t_counter!("felix_broker_ingress_dropped_total").increment(1);
                            Ok(false)
                        }
                        EnqueuePolicy::Fail => {
                            t_counter!("felix_broker_ingress_rejected_total").increment(1);
                            Err(anyhow!(
                                "publish ingress per-connection byte budget exhausted"
                            ))
                        }
                        EnqueuePolicy::Wait => unreachable!("Wait handled above"),
                    };
                }
            };
            match publish_ctx.admission.try_acquire(job_bytes) {
                Ok(permit) => (conn_permit, permit),
                Err(_) => {
                    t_counter!("felix_broker_ingress_bytes_full_total").increment(1);
                    return match policy {
                        EnqueuePolicy::Drop => {
                            t_counter!("felix_broker_ingress_dropped_total").increment(1);
                            Ok(false)
                        }
                        EnqueuePolicy::Fail => {
                            t_counter!("felix_broker_ingress_rejected_total").increment(1);
                            Err(anyhow!("publish ingress byte budget exhausted"))
                        }
                        EnqueuePolicy::Wait => unreachable!("Wait handled above"),
                    };
                }
            }
        }
    };
    job.admission_permit = Some(AdmissionPermit {
        _conn: conn_permit,
        _global: permit,
    });

    #[cfg(feature = "perf_debug")]
    let enqueue_wait_start = Instant::now();
    // We use try_send first to keep the fast path allocation-free and to make overload observable
    // (Full vs Closed). Only the Wait policy performs an async send with a timeout.
    // IMPORTANT: Any code path that successfully enqueues MUST increment depth counters exactly once.
    // Best-effort enqueue with metrics and optional backpressure/err.
    match worker.try_send(job) {
        Ok(()) => {
            let _local = publish_ctx.depth.fetch_add(1, Ordering::Relaxed) + 1;
            let global = GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            t_gauge!("felix_broker_ingress_queue_depth").set(global as f64);
            #[cfg(feature = "perf_debug")]
            {
                let wait_ns = enqueue_wait_start.elapsed().as_nanos() as u64;
                metrics::histogram!(
                    "felix_perf_publish_enqueue_wait_ns",
                    "worker" => worker_index.to_string()
                )
                .record(wait_ns as f64);
                metrics::counter!(
                    "felix_perf_publish_enqueue_ok_total",
                    "worker" => worker_index.to_string()
                )
                .increment(1);
            }
            Ok(true)
        }
        Err(mpsc::error::TrySendError::Full(job)) => {
            t_counter!("felix_broker_ingress_queue_full_total").increment(1);
            match policy {
                EnqueuePolicy::Drop => {
                    t_counter!("felix_broker_ingress_dropped_total").increment(1);
                    Ok(false)
                }
                EnqueuePolicy::Fail => {
                    t_counter!("felix_broker_ingress_rejected_total").increment(1);
                    Err(anyhow!("publish queue full"))
                }
                EnqueuePolicy::Wait => {
                    // Acked publishes with ack_on_commit use Wait; otherwise we Fail/Drop.
                    t_counter!("felix_broker_ingress_waited_total").increment(1);
                    let send_result =
                        tokio::time::timeout(publish_ctx.wait_timeout, worker.send(job))
                            .await
                            .map_err(|_| anyhow!("publish enqueue timed out"))?;
                    send_result.map_err(|_| anyhow!("publish queue closed"))?;
                    // Local depth is per publish context; global depth is used for cross-connection observability.
                    let _local = publish_ctx.depth.fetch_add(1, Ordering::Relaxed) + 1;
                    let global = GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
                    t_gauge!("felix_broker_ingress_queue_depth").set(global as f64);
                    #[cfg(feature = "perf_debug")]
                    {
                        let wait_ns = enqueue_wait_start.elapsed().as_nanos() as u64;
                        metrics::histogram!(
                            "felix_perf_publish_enqueue_wait_ns",
                            "worker" => worker_index.to_string()
                        )
                        .record(wait_ns as f64);
                        metrics::counter!(
                            "felix_perf_publish_enqueue_wait_total",
                            "worker" => worker_index.to_string()
                        )
                        .increment(1);
                    }
                    Ok(true)
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(anyhow!("publish queue closed")),
    }
}

// Adjust queue depth gauges safely when send fails or work completes.
pub(crate) fn decrement_depth(
    depth: &Arc<AtomicUsize>,
    global: &AtomicUsize,
    gauge: &'static str,
) -> Option<(usize, usize)> {
    #[cfg(not(feature = "telemetry"))]
    let _ = gauge;
    // Depth tracking is intentionally best-effort: we avoid panicking on underflow and tolerate drift.
    // Drift can occur if a task exits unexpectedly or if multiple teardown paths reset counters.
    // We record drift metrics and rely on `reset_local_depth_only` to reconcile on teardown.
    if let Ok(prev) = depth.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        if value == 0 { None } else { Some(value - 1) }
    }) {
        let cur = prev.saturating_sub(1);
        let updated = match global.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            if value == 0 { None } else { Some(value - 1) }
        }) {
            Ok(value) => value - 1,
            Err(_) => {
                t_counter!("felix_queue_depth_drift_total", "queue" => gauge).increment(1);
                global.load(Ordering::Relaxed)
            }
        };
        t_gauge!(gauge).set(updated as f64);
        return Some((prev, cur));
    }
    None
}

pub(crate) fn reset_local_depth_only(
    depth: &Arc<AtomicUsize>,
    global: &AtomicUsize,
    gauge: &'static str,
) {
    #[cfg(not(feature = "telemetry"))]
    let _ = gauge;
    let remaining = depth.swap(0, Ordering::Relaxed);
    if remaining == 0 {
        return;
    }
    let mut prev = global.load(Ordering::Relaxed);
    loop {
        let next = prev.saturating_sub(remaining);
        match global.compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => {
                t_gauge!(gauge).set(next as f64);
                break;
            }
            Err(updated) => prev = updated,
        }
    }
}
