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
    #[pyo3(signature = (
        tenant_id,
        namespace,
        stream,
        payload,
        *,
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
        ack: &str,
        at_least_once: bool,
    ) -> PyResult<()> {
        let ack = parse_ack(ack)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let payload = payload.to_vec();
        block_on(py, async move {
            if at_least_once {
                inner
                    .publish_at_least_once(&tenant_id, &namespace, &stream, payload, ack)
                    .await
            } else {
                inner
                    .publish(&tenant_id, &namespace, &stream, payload, ack)
                    .await
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
    module.add_class::<asyncio::AsyncClient>()?;
    module.add_class::<asyncio::AsyncSubscription>()?;
    errors::register(module)?;
    Ok(())
}
