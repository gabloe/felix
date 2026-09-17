//! Python bindings for the Felix client.
//!
//! This is a **wrapper over `felix-client`, not a second implementation of the
//! protocol.** Reconnection, redirect-following, retry classification, offset
//! bookkeeping and the frame codec all live in the Rust client and are shared
//! by every language that binds to it. A Python-native client would be a
//! second place for those to be subtly wrong, in exactly the areas — failover
//! and delivery accounting — where subtly wrong is most expensive.
//!
//! The surface is synchronous. Calls release the GIL and block on a shared
//! Tokio runtime, so a thread pool or `asyncio.to_thread` gives concurrency
//! today; a native `async def` layer can be added over these same primitives
//! without changing their semantics.
//!
//! A cache entry is named by tenant, namespace, cache, and key before its
//! value and options ever appear, so several of these methods carry more
//! arguments than clippy's default. Bundling them into a struct would move
//! the argument list rather than shorten it, and every Python caller has the
//! parts separately anyway — the same reasoning `StorageApi` records.
#![allow(clippy::too_many_arguments)]
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use bytes::Bytes;
use felix_client::{ClientConfig, ClusterClient, Subscription};
use felix_wire::{AckMode, StartPosition};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::Mutex;

mod asyncio;
mod errors;
mod runtime;
mod tls;
mod types;

use errors::to_py_err;
use runtime::block_on;

/// One record delivered to a subscriber.
#[pyclass(module = "felix", frozen, get_all)]
pub struct Event {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub payload: Py<PyBytes>,
    /// Log offset on a durable stream; `None` on an in-memory stream or from a
    /// broker that did not negotiate offsets.
    ///
    /// Two uses. Checkpoint `offset + 1` to resume after a reconnect. And
    /// because offsets are contiguous, a jump between consecutive events means
    /// the subscriber queue dropped something — which is otherwise invisible.
    pub offset: Option<u64>,
}

#[pymethods]
impl Event {
    fn __repr__(&self, py: Python<'_>) -> String {
        let len = self.payload.bind(py).as_bytes().len();
        match self.offset {
            Some(offset) => format!(
                "Event(stream='{}/{}/{}', {len} bytes, offset={offset})",
                self.tenant_id, self.namespace, self.stream
            ),
            None => format!(
                "Event(stream='{}/{}/{}', {len} bytes)",
                self.tenant_id, self.namespace, self.stream
            ),
        }
    }
}

/// A live subscription. Iterate it, or call `next_event()`.
#[pyclass(module = "felix")]
pub struct SubscriptionHandle {
    inner: Arc<Mutex<Option<Subscription>>>,
    /// Kept alive because the subscription's event stream belongs to it:
    /// dropping the client would close the stream underneath the iterator.
    _client: Arc<felix_client::Client>,
}

#[pymethods]
impl SubscriptionHandle {
    /// The next event, or `None` once the broker closes the stream.
    ///
    /// Blocks until one arrives. Pass `timeout` (seconds) to bound the wait;
    /// a timeout returns `None`, which is *not* the same as the stream ending
    /// — check `closed` to tell them apart.
    #[pyo3(signature = (timeout=None))]
    fn next_event(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Event>> {
        let inner = Arc::clone(&self.inner);
        let event = block_on(py, async move {
            let mut guard = inner.lock().await;
            let Some(subscription) = guard.as_mut() else {
                return Ok(None);
            };
            let next = match timeout {
                Some(seconds) => {
                    let duration = std::time::Duration::from_secs_f64(seconds.max(0.0));
                    match tokio::time::timeout(duration, subscription.next_event()).await {
                        Ok(result) => result.map_err(to_py_err)?,
                        // A timeout is a "nothing yet", not an end: the
                        // subscription stays usable and the caller may ask
                        // again.
                        Err(_elapsed) => return Ok(None),
                    }
                }
                None => subscription.next_event().await.map_err(to_py_err)?,
            };
            Ok(next)
        })?;

        Ok(event.map(|event| Event {
            tenant_id: event.tenant_id.to_string(),
            namespace: event.namespace.to_string(),
            stream: event.stream.to_string(),
            payload: PyBytes::new(py, &event.payload).unbind(),
            offset: event.offset,
        }))
    }

    /// Stop receiving. Idempotent.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            inner.lock().await.take();
            Ok(())
        })
    }

    #[getter]
    fn closed(&self, py: Python<'_>) -> PyResult<bool> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move { Ok(inner.lock().await.is_none()) })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Event>> {
        self.next_event(py, None)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

/// A live cache watch. Iterate it, or call `recv()`.
///
/// Yields `CacheChange` for each applied write and, if the watch falls behind,
/// a single `CacheWatchLagged` before ending. The lag is a value rather than
/// an exception because it is not a failure: the watch did its job by telling
/// you, and re-watching from `resume_from` is gapless.
#[pyclass(module = "felix")]
pub struct CacheWatchHandle {
    inner: Arc<Mutex<Option<felix_client::CacheWatch>>>,
    #[pyo3(get)]
    resume_offset: u64,
    #[pyo3(get)]
    resnapshot: bool,
    #[pyo3(get)]
    retained_count: Option<u64>,
}

#[pymethods]
impl CacheWatchHandle {
    /// The next change, or `None` once the watch ends.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<PyObject>> {
        let inner = Arc::clone(&self.inner);
        let item = block_on(py, async move {
            let mut guard = inner.lock().await;
            let Some(watch) = guard.as_mut() else {
                return Ok(None);
            };
            let next = match timeout {
                Some(seconds) => {
                    let duration = std::time::Duration::from_secs_f64(seconds.max(0.0));
                    match tokio::time::timeout(duration, watch.recv()).await {
                        Ok(item) => item,
                        Err(_elapsed) => return Ok(None),
                    }
                }
                None => watch.recv().await,
            };
            Ok(next.map(types::OwnedWatchItem::from))
        })?;
        item.map(|item| item.into_pyobject(py).map(|bound| bound.unbind()))
            .transpose()
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            inner.lock().await.take();
            Ok(())
        })
    }

    #[getter]
    fn closed(&self, py: Python<'_>) -> PyResult<bool> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move { Ok(inner.lock().await.is_none()) })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        self.recv(py, None)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

/// Every shard of a stream, merged into one iterator.
///
/// Yields `ShardRecord`, and also `ShardLost` / `ShardRecovered` — a shard
/// going away is surfaced rather than swallowed, because the other shards
/// carry on and a consumer that ignored it would be reading part of the
/// stream while believing it read all of it.
#[pyclass(module = "felix")]
pub struct ShardedSubscriptionHandle {
    inner: Arc<Mutex<Option<felix_client::ShardedSubscription>>>,
    #[pyo3(get)]
    shards: u32,
}

#[pymethods]
impl ShardedSubscriptionHandle {
    /// The next event from any shard, or `None` once every shard has ended.
    #[pyo3(signature = (timeout=None))]
    fn next_event(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<PyObject>> {
        let inner = Arc::clone(&self.inner);
        let event = block_on(py, async move {
            let mut guard = inner.lock().await;
            let Some(subscription) = guard.as_mut() else {
                return Ok(None);
            };
            let next = match timeout {
                Some(seconds) => {
                    let duration = std::time::Duration::from_secs_f64(seconds.max(0.0));
                    match tokio::time::timeout(duration, subscription.next()).await {
                        Ok(event) => event,
                        Err(_elapsed) => return Ok(None),
                    }
                }
                None => subscription.next().await,
            };
            Ok(next.map(types::OwnedShardEvent::from))
        })?;
        event
            .map(|event| event.into_pyobject(py).map(|bound| bound.unbind()))
            .transpose()
    }

    /// The highest offset handled per shard, for resuming.
    ///
    /// Pass it back as `resume=` and each listed shard continues at
    /// `offset + 1`.
    fn positions(&self, py: Python<'_>) -> PyResult<std::collections::BTreeMap<u32, u64>> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            let guard = inner.lock().await;
            Ok(guard
                .as_ref()
                .map(felix_client::ShardedSubscription::positions)
                .unwrap_or_default())
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            inner.lock().await.take();
            Ok(())
        })
    }

    #[getter]
    fn closed(&self, py: Python<'_>) -> PyResult<bool> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move { Ok(inner.lock().await.is_none()) })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        self.next_event(py, None)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

/// Four scope strings at once — the shape every group call starts with.
fn owned4(a: &str, b: &str, c: &str, d: &str) -> (String, String, String, String) {
    (a.to_string(), b.to_string(), c.to_string(), d.to_string())
}

/// A connection to a Felix cluster.
///
/// Holds the seed addresses it was given plus every broker the cluster has
/// since advertised, and moves itself to a healthy broker when the one in use
/// goes away — the reconnect and redirect logic is the Rust client's, not a
/// reimplementation.
#[pyclass(module = "felix")]
pub struct Client {
    inner: Arc<ClusterClient>,
}

#[pymethods]
impl Client {
    /// Connect to a cluster.
    ///
    /// `addrs` is one `"host:port"` or a list of them; any reachable one is
    /// enough, and the client discovers the rest. `server_name` is the name
    /// the broker's certificate is expected to carry.
    ///
    /// TLS is not optional — QUIC has no unencrypted mode. Supply `ca_file`
    /// to trust a specific CA (what the demos and a self-signed development
    /// broker need); omit it to use the operating system's trust store.
    #[new]
    #[pyo3(signature = (
        addrs,
        *,
        tenant_id,
        token,
        server_name="localhost",
        ca_file=None,
    ))]
    fn new(
        py: Python<'_>,
        addrs: PyObject,
        tenant_id: &str,
        token: &str,
        server_name: &str,
        ca_file: Option<&str>,
    ) -> PyResult<Self> {
        let seeds = parse_addrs(py, &addrs)?;
        if seeds.is_empty() {
            return Err(PyValueError::new_err(
                "at least one broker address is required",
            ));
        }
        let quinn = tls::client_config(ca_file)?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id.to_string());
        config.auth_token = Some(token.to_string());

        let server_name = server_name.to_string();
        let inner = block_on(py, async move {
            ClusterClient::connect(&seeds, &server_name, config)
                .await
                .map_err(to_py_err)
        })?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Publish one record.
    ///
    /// `ack` selects what the broker must have done before this returns:
    /// `"none"` (fire and forget), `"per_message"`, or `"per_batch"`. With
    /// `at_least_once=True` a publish that fails because the broker went away
    /// is **re-sent** to its replacement, which can duplicate the record — use
    /// it only where consumers tolerate that.
    ///
    /// `key` is the routing key, and it decides the shard. Without one every
    /// record lands on shard 0, so a multi-shard stream behaves like a
    /// single-shard one. Records sharing a key share a shard and stay ordered
    /// with respect to each other; records with different keys do not.
    #[pyo3(signature = (
        tenant_id,
        namespace,
        stream,
        payload,
        *,
        key=None,
        ack="per_message",
        at_least_once=false,
    ))]
    fn publish(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: &[u8],
        key: Option<&[u8]>,
        ack: &str,
        at_least_once: bool,
    ) -> PyResult<()> {
        let ack = parse_ack(ack)?;
        if key.is_some() && at_least_once {
            return Err(PyValueError::new_err(
                "at_least_once does not carry a routing key yet; publish the \
                 keyed record without it, or drop the key",
            ));
        }
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let payload = payload.to_vec();
        let key = key.map(Bytes::copy_from_slice);
        block_on(py, async move {
            match (key, at_least_once) {
                (Some(key), _) => {
                    inner
                        .publish_keyed(&tenant_id, &namespace, &stream, payload, key, ack)
                        .await
                }
                (None, true) => {
                    inner
                        .publish_at_least_once(&tenant_id, &namespace, &stream, payload, ack)
                        .await
                }
                (None, false) => {
                    inner
                        .publish(&tenant_id, &namespace, &stream, payload, ack)
                        .await
                }
            }
            .map_err(to_py_err)
        })
    }

    /// Subscribe to a stream.
    ///
    /// `start` is `"latest"` (default), `"earliest"`, or an integer offset —
    /// the first record you have *not* seen, so a resuming client passes the
    /// offset it last handled plus one.
    ///
    /// A subscription reads **one shard**. For a multi-shard stream this is
    /// shard 0; consuming every shard needs one subscription per shard, which
    /// this binding does not yet wrap.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None))]
    fn subscribe(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<PyObject>,
    ) -> PyResult<SubscriptionHandle> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let (client, subscription) = block_on(py, async move {
            inner
                .subscribe_from(&tenant_id, &namespace, &stream, start)
                .await
                .map_err(to_py_err)
        })?;
        Ok(SubscriptionHandle {
            inner: Arc::new(Mutex::new(Some(subscription))),
            _client: client,
        })
    }

    /// Store a value, optionally with a time-to-live in seconds.
    #[pyo3(signature = (tenant_id, namespace, cache, key, value, *, ttl=None))]
    fn cache_put(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: &[u8],
        ttl: Option<f64>,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        let value = Bytes::copy_from_slice(value);
        // The wire carries a TTL in milliseconds; seconds is what a Python
        // caller reaches for, so the conversion happens here rather than
        // leaking the unit into the signature.
        let ttl_ms = ttl.map(|seconds| (seconds.max(0.0) * 1000.0) as u64);
        block_on(py, async move {
            let client = inner.client().await;
            client
                .cache_put(&tenant_id, &namespace, &cache, &key, value, ttl_ms)
                .await
                .map_err(to_py_err)
        })
    }

    /// Read a value, or `None` if the key is absent or expired.
    fn cache_get(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Option<Py<PyBytes>>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        let value = block_on(py, async move {
            let client = inner.client().await;
            client
                .cache_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })?;
        Ok(value.map(|bytes| PyBytes::new(py, &bytes).unbind()))
    }

    /// Remove a key, returning the value it held, or `None` if it held none.
    fn cache_delete(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Option<Py<PyBytes>>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        let value = block_on(py, async move {
            let client = inner.client().await;
            client
                .cache_delete(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })?;
        Ok(value.map(|bytes| PyBytes::new(py, &bytes).unbind()))
    }

    /// Apply a signed delta to a counter and return the sum *including* it.
    ///
    /// One round trip: incrementing and learning where you stand are the same
    /// call. Delivery is at least once — a retried add after a lost
    /// acknowledgement counts twice, so an application that cannot tolerate a
    /// double count keeps its own idempotency key.
    fn counter_add(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> PyResult<i64> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        block_on(py, async move {
            let client = inner.client().await;
            client
                .counter_add(&tenant_id, &namespace, &cache, &key, delta)
                .await
                .map_err(to_py_err)
        })
    }

    /// Read a counter. `None` means never written, which is not zero.
    fn counter_get(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Option<i64>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        block_on(py, async move {
            let client = inner.client().await;
            client
                .counter_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })
    }

    // ---- consumer groups -------------------------------------------------
    //
    // A queue rather than a stream: records are *pulled*, because only the
    // consumer knows when it has capacity, and each one is claimed by one
    // member until it is settled or its visibility timeout lapses.

    /// Take up to `max_records` for this group, waiting up to `wait` seconds
    /// for work to appear.
    ///
    /// An empty list is an answer, not an error: the log has nothing unclaimed
    /// for this group right now. `wait=0` polls without blocking, which spins
    /// if you loop on it — prefer a few seconds so the broker holds the
    /// request open instead.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, group, *, max_records=32, wait=5.0))]
    fn group_poll(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
        wait: f64,
    ) -> PyResult<Vec<Py<types::GroupRecord>>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        let wait = std::time::Duration::from_secs_f64(wait.max(0.0));
        let records = block_on(py, async move {
            let client = inner.client().await;
            client
                .group_poll_wait(
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
                    max_records,
                    wait,
                )
                .await
                .map_err(to_py_err)
        })?;
        records
            .into_iter()
            .map(|record| {
                types::OwnedGroupRecord::from(record)
                    .into_pyobject(py)
                    .map(Bound::unbind)
            })
            .collect()
    }

    /// Finish one record. Everything below the group's cursor stays finished.
    fn group_ack(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            let client = inner.client().await;
            client
                .group_ack(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Hand one record back for immediate redelivery, rather than waiting out
    /// the visibility timeout.
    fn group_nack(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            let client = inner.client().await;
            client
                .group_nack(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Offsets this group gave up on, lowest first.
    ///
    /// The records are still in the log at these offsets: this is a list of
    /// what to look at, not a copy of it.
    fn group_dead_letters(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> PyResult<Vec<u64>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            let client = inner.client().await;
            client
                .group_dead_letters(&tenant_id, &namespace, &stream, shard, &group)
                .await
                .map_err(to_py_err)
        })
    }

    /// Drop one dead letter, having decided the record is not worth
    /// reprocessing. Does not touch the record itself.
    fn group_discard(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            let client = inner.client().await;
            client
                .group_discard(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Put one dead letter back in the queue with its attempt count reset.
    ///
    /// For when the reason it failed has been fixed. The group's cursor does
    /// not move backwards — the record is owed again, which is a different
    /// thing: everything the group finished stays finished.
    fn group_redrive(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            let client = inner.client().await;
            client
                .group_redrive(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    // ---- cache watch -----------------------------------------------------

    /// Watch a key or prefix for changes.
    ///
    /// `start` is the first cache-log offset you have *not* seen, so a
    /// resuming watcher passes the offset it last handled plus one. `None`
    /// means from now: live changes only.
    ///
    /// With `retained=True` the watch delivers each matching key's *current*
    /// value first and then live changes — join a room and immediately hold
    /// the roster. Mutually exclusive with `start`, whose replay already
    /// reconstructs the state that shortcuts.
    #[pyo3(signature = (tenant_id, namespace, cache, filter, *, start=None, retained=false))]
    fn watch_cache(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: &types::CacheWatchFilter,
        start: Option<u64>,
        retained: bool,
    ) -> PyResult<CacheWatchHandle> {
        if retained && start.is_some() {
            return Err(PyValueError::new_err(
                "retained and start are mutually exclusive: a resume already \
                 replays the state a retained start shortcuts",
            ));
        }
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
        );
        let filter = filter.to_client();
        let watch = block_on(py, async move {
            // Through the cluster client, so the watch follows the redirect to
            // whichever broker owns the key's shard.
            if retained {
                inner
                    .watch_cache_retained(&tenant_id, &namespace, &cache, filter)
                    .await
            } else {
                inner
                    .watch_cache(&tenant_id, &namespace, &cache, filter, start)
                    .await
            }
            .map_err(to_py_err)
        })?;
        Ok(CacheWatchHandle {
            resume_offset: watch.resume_offset(),
            resnapshot: watch.resnapshot(),
            retained_count: watch.retained_count(),
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    // ---- multi-shard subscribe -------------------------------------------

    /// How many shards this stream was placed with.
    ///
    /// A subscription reads one shard, so consuming a whole stream means
    /// knowing how many there are. `0` means the broker knows nothing of the
    /// stream.
    fn stream_shards(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<u32> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        block_on(py, async move {
            let client = inner.client().await;
            client
                .stream_shards(&tenant_id, &namespace, &stream)
                .await
                .map_err(to_py_err)
        })
    }

    /// Subscribe to **every** shard of a stream and merge them.
    ///
    /// One subscription per shard underneath, each following its own shard's
    /// owner, because two shards of one stream can live on two brokers and an
    /// answer about one says nothing about the other.
    ///
    /// Ordering holds *within* a shard, not across them — records with the
    /// same routing key share a shard and stay ordered; unrelated records do
    /// not. A consumer that needs total order wants a single-shard stream.
    ///
    /// `resume` is a mapping of shard to the offset last handled; each listed
    /// shard resumes at `offset + 1`, and a shard not listed starts wherever
    /// `start` says.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None, resume=None))]
    fn subscribe_sharded(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<PyObject>,
        resume: Option<std::collections::BTreeMap<u32, u64>>,
    ) -> PyResult<ShardedSubscriptionHandle> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let subscription = block_on(py, async move {
            match resume {
                Some(positions) => {
                    inner
                        .resubscribe_sharded(&tenant_id, &namespace, &stream, positions, start)
                        .await
                }
                None => {
                    inner
                        .subscribe_sharded(&tenant_id, &namespace, &stream, start)
                        .await
                }
            }
            .map_err(to_py_err)
        })?;
        Ok(ShardedSubscriptionHandle {
            shards: subscription.shards(),
            inner: Arc::new(Mutex::new(Some(subscription))),
        })
    }

    /// Every broker this client would try, seeds included.
    fn endpoints(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let inner = Arc::clone(&self.inner);
        let addrs = block_on(py, async move { Ok(inner.endpoints().await) })?;
        Ok(addrs.iter().map(SocketAddr::to_string).collect())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        "Client(...)".to_string()
    }
}

pub(crate) fn parse_addrs(py: Python<'_>, addrs: &PyObject) -> PyResult<Vec<SocketAddr>> {
    let bound = addrs.bind(py);
    let items: Vec<String> = if let Ok(single) = bound.extract::<String>() {
        vec![single]
    } else {
        bound.extract::<Vec<String>>().map_err(|_| {
            PyValueError::new_err("addrs must be a 'host:port' string or a list of them")
        })?
    };

    let mut resolved = Vec::with_capacity(items.len());
    for item in items {
        // Resolve names, not just literal addresses: a Kubernetes service name
        // is the normal way to reach a broker.
        let mut iter = item
            .to_socket_addrs()
            .map_err(|err| PyValueError::new_err(format!("could not resolve {item:?}: {err}")))?;
        match iter.next() {
            Some(addr) => resolved.push(addr),
            None => {
                return Err(PyValueError::new_err(format!(
                    "{item:?} resolved to no addresses"
                )));
            }
        }
    }
    Ok(resolved)
}

pub(crate) fn parse_ack(ack: &str) -> PyResult<AckMode> {
    match ack {
        "none" => Ok(AckMode::None),
        "per_message" => Ok(AckMode::PerMessage),
        "per_batch" => Ok(AckMode::PerBatch),
        other => Err(PyValueError::new_err(format!(
            "ack must be 'none', 'per_message', or 'per_batch', not {other:?}"
        ))),
    }
}

pub(crate) fn parse_start(
    py: Python<'_>,
    start: Option<PyObject>,
) -> PyResult<Option<StartPosition>> {
    let Some(start) = start else {
        return Ok(None);
    };
    let bound = start.bind(py);
    if let Ok(offset) = bound.extract::<u64>() {
        return Ok(Some(StartPosition::Offset(offset)));
    }
    match bound.extract::<String>()?.as_str() {
        "latest" => Ok(Some(StartPosition::Latest)),
        "earliest" => Ok(Some(StartPosition::Earliest)),
        other => Err(PyValueError::new_err(format!(
            "start must be 'latest', 'earliest', or an integer offset, not {other:?}"
        ))),
    }
}

#[pymodule]
fn _felix(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_class::<Client>()?;
    module.add_class::<SubscriptionHandle>()?;
    module.add_class::<Event>()?;
    module.add_class::<CacheWatchHandle>()?;
    module.add_class::<ShardedSubscriptionHandle>()?;
    module.add_class::<types::GroupRecord>()?;
    module.add_class::<types::CacheChange>()?;
    module.add_class::<types::CacheWatchLagged>()?;
    module.add_class::<types::CacheWatchFilter>()?;
    module.add_class::<types::ShardRecord>()?;
    module.add_class::<types::ShardLost>()?;
    module.add_class::<types::ShardRecovered>()?;
    module.add_class::<asyncio::AsyncClient>()?;
    module.add_class::<asyncio::AsyncSubscription>()?;
    module.add_class::<asyncio::AsyncCacheWatch>()?;
    module.add_class::<asyncio::AsyncShardedSubscription>()?;
    errors::register(module)?;
    Ok(())
}
