//! Cache and counter requests through a [`Client`].
//!
//! Each goes to one of the client's cache workers, round-robin; see
//! `crate::cache` for how a worker carries it.

use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_wire::Message;
use tokio::sync::oneshot;

use super::Client;
use crate::cache::{CacheRequest, CacheWorker};

impl Client {
    pub async fn cache_put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        // Cache ops are delegated to a pool of single-writer cache workers.
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CachePut {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            value,
            request_id: Some(request_id),
            ttl_ms,
        };
        let (response_tx, response_rx) = oneshot::channel();
        let (worker, conn_index) = self.cache_worker();
        worker
            .tx
            .send(CacheRequest::Put {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;

        // Track the connection since it counts as inflight.
        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
        .increment(1);
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache put response dropped"))?
    }

    pub async fn cache_get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<Bytes>> {
        // Cache ops are delegated to a pool of single-writer cache workers.
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CacheGet {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            request_id: Some(request_id),
        };
        let (response_tx, response_rx) = oneshot::channel();
        let (worker, conn_index) = self.cache_worker();
        worker
            .tx
            .send(CacheRequest::Get {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;

        // Now it's *actually* enqueued, so it counts as inflight.
        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
        .increment(1);
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache get response dropped"))?
    }

    /// Remove a key, reporting the value it held.
    ///
    /// `Ok(None)` means the key was not there. Both are answers: a delete is not
    /// an error just because there was nothing to remove.
    ///
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_CACHE_DELETE`]. An unrecognised message type is
    /// fatal to a broker's control loop, so probing one that predates this would
    /// cost the connection rather than return an error.
    pub async fn cache_delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<Bytes>> {
        if !felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_CACHE_DELETE) {
            return Err(anyhow::anyhow!("this broker does not support cache delete",));
        }
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CacheDelete {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            request_id: Some(request_id),
        };
        let (response_tx, response_rx) = oneshot::channel();
        let (worker, conn_index) = self.cache_worker();
        worker
            .tx
            .send(CacheRequest::Get {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;

        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
        .increment(1);
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache delete response dropped"))?
    }

    /// Add a signed delta to a counter, answering with the sum including it.
    ///
    /// A counter is scoped exactly as a cache key is — same registered cache
    /// scope, same key-to-shard routing, same owner — but lives beside the
    /// cache, not in it: a counter and a cache value may share a key and are
    /// unrelated. **Delivery is at least once**: a retry after a lost
    /// acknowledgement counts twice, because deltas carry no dedupe identity.
    /// An application that cannot tolerate that keeps its own idempotency key
    /// outside the counter.
    ///
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_COUNTERS`] — probing an older broker would cost
    /// the connection.
    pub async fn counter_add(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> Result<i64> {
        if !felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_COUNTERS) {
            return Err(anyhow::anyhow!("this broker does not support counters"));
        }
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CounterAdd {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            delta,
            request_id,
        };
        self.counter_round_trip(message, request_id)
            .await?
            .context("an add always answers with the sum it produced")
    }

    /// Read a counter's sum. `Ok(None)` means the counter has never been
    /// written — a different answer from a sum of zero, exactly as a cache
    /// miss differs from a stored empty value.
    pub async fn counter_get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<i64>> {
        if !felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_COUNTERS) {
            return Err(anyhow::anyhow!("this broker does not support counters"));
        }
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CounterGet {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            request_id,
        };
        self.counter_round_trip(message, request_id).await
    }

    pub fn cache_conn_counts(&self) -> Vec<usize> {
        self.cache_conn_counts
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect()
    }

    async fn counter_round_trip(&self, message: Message, request_id: u64) -> Result<Option<i64>> {
        let (response_tx, response_rx) = oneshot::channel();
        let (worker, conn_index) = self.cache_worker();
        worker
            .tx
            .send(CacheRequest::Counter {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;
        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
        .increment(1);
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("counter response dropped"))?
    }

    fn cache_worker(&self) -> (&CacheWorker, usize) {
        // Round-robin pick only.
        //
        // IMPORTANT: do not mutate metrics/counters here.
        // We only consider an op "in-flight" once it is successfully enqueued.
        let index = self.cache_worker_rr.fetch_add(1, Ordering::Relaxed) % self.cache_workers.len();
        let worker = &self.cache_workers[index];
        (worker, worker.conn_index)
    }
}
