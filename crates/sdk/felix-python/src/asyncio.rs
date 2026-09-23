//! The `async def` surface.
//!
//! Python realtime backends are asyncio backends, so this is the API most
//! callers should reach for: every method returns an awaitable driven by the
//! caller's own event loop, and a subscription is an `async for`.
//!
//! It wraps exactly the same [`ClusterClient`] the synchronous surface does,
//! so reconnection, redirect-following and retry classification behave
//! identically whichever one an application picks. The only difference is who
//! waits: the sync surface blocks a thread with the GIL released, this one
//! yields to the event loop.
//!
//! The awaited work runs on the shared Tokio runtime rather than the Python
//! loop — `pyo3_async_runtimes` moves the result back — which is what lets a
//! subscription's background reader keep draining while the coroutine that
//! owns it is parked.
//!
//! Scope arguments outnumber clippy's default here for the same reason they do
//! on the synchronous surface; see `lib.rs`.
#![allow(clippy::too_many_arguments)]
use std::sync::Arc;

use bytes::Bytes;
use felix_client::{ClientConfig, ClusterClient, Subscription};
use pyo3::exceptions::{PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::Mutex;

use crate::errors::to_py_err;
use crate::types::{CacheWatchFilter, OwnedGroupRecord, OwnedShardEvent, OwnedWatchItem};
use crate::{Event, parse_ack, parse_addrs, parse_start};

/// Which settle a group call is making. One helper serves all four because
/// they differ only in the client method they reach.
#[derive(Clone, Copy)]
enum Settle {
    Ack,
    Nack,
    Discard,
    Redrive,
}

/// A subscription consumed with `async for`.
#[pyclass(module = "felix", name = "AsyncSubscription")]
pub struct AsyncSubscription {
    inner: Arc<Mutex<Option<Subscription>>>,
    _client: Arc<felix_client::Client>,
}

#[pymethods]
impl AsyncSubscription {
    /// The next event, or `None` once the broker closes the stream.
    ///
    /// `timeout` (seconds) bounds the wait and yields `None` on expiry, which
    /// is *not* the same as the stream ending — check `closed` to tell them
    /// apart.
    #[pyo3(signature = (timeout=None))]
    fn next_event<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let Some(subscription) = guard.as_mut() else {
                return Ok(None);
            };
            let next = match timeout {
                Some(seconds) => {
                    let duration = std::time::Duration::from_secs_f64(seconds.max(0.0));
                    match tokio::time::timeout(duration, subscription.next_event()).await {
                        Ok(result) => result.map_err(to_py_err)?,
                        Err(_elapsed) => return Ok(None),
                    }
                }
                None => subscription.next_event().await.map_err(to_py_err)?,
            };
            Ok(next.map(OwnedEvent::from))
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.take();
            Ok(())
        })
    }

    #[getter]
    fn closed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            async move { Ok(inner.lock().await.is_none()) },
        )
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// `async for` ends when the broker closes the stream: the iterator raises
    /// `StopAsyncIteration` rather than yielding `None`, so a loop terminates
    /// the way Python expects instead of the caller testing for a sentinel.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let Some(subscription) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err("subscription closed"));
            };
            match subscription.next_event().await.map_err(to_py_err)? {
                Some(event) => Ok(OwnedEvent::from(event)),
                None => Err(PyStopAsyncIteration::new_err("stream ended")),
            }
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle: Py<Self> = slf.into();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(handle) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.take();
            Ok(false)
        })
    }
}

/// An event carried back to the Python loop.
///
/// A separate owned type because the payload has to cross a thread boundary
/// before there is a `Python<'_>` to build a `PyBytes` with; the conversion to
/// [`Event`] happens once the result reaches the loop.
struct OwnedEvent {
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Vec<u8>,
    offset: Option<u64>,
}

impl From<felix_client::Event> for OwnedEvent {
    fn from(event: felix_client::Event) -> Self {
        Self {
            tenant_id: event.tenant_id.to_string(),
            namespace: event.namespace.to_string(),
            stream: event.stream.to_string(),
            payload: event.payload.to_vec(),
            offset: event.offset,
        }
    }
}

impl<'py> IntoPyObject<'py> for OwnedEvent {
    type Target = Event;
    type Output = Bound<'py, Event>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(
            py,
            Event {
                tenant_id: self.tenant_id,
                namespace: self.namespace,
                stream: self.stream,
                payload: PyBytes::new(py, &self.payload).unbind(),
                offset: self.offset,
            },
        )
    }
}

/// A connection to a Felix cluster, for asyncio.
///
/// Construct with `await AsyncClient.connect(...)` rather than `__init__`:
/// connecting reaches the network, and a constructor that blocked an event
/// loop to do it would be the wrong shape.
#[pyclass(module = "felix", name = "AsyncClient")]
pub struct AsyncClient {
    inner: Arc<ClusterClient>,
}

#[pymethods]
impl AsyncClient {
    #[staticmethod]
    #[pyo3(signature = (
        addrs,
        *,
        tenant_id,
        token,
        server_name="localhost",
        ca_file=None,
    ))]
    fn connect<'py>(
        py: Python<'py>,
        addrs: Py<PyAny>,
        tenant_id: &str,
        token: &str,
        server_name: &str,
        ca_file: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let seeds = parse_addrs(py, &addrs)?;
        if seeds.is_empty() {
            return Err(PyValueError::new_err(
                "at least one broker address is required",
            ));
        }
        let quinn = crate::tls::client_config(ca_file)?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id.to_string());
        config.auth_token = Some(token.to_string());
        let server_name = server_name.to_string();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = ClusterClient::connect(&seeds, &server_name, config)
                .await
                .map_err(to_py_err)?;
            Ok(AsyncClient {
                inner: Arc::new(inner),
            })
        })
    }

    /// Publish one record. See the synchronous `Client.publish` for what `ack`
    /// and `at_least_once` mean — they are the same options.
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
    fn publish<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: &[u8],
        key: Option<&[u8]>,
        ack: &str,
        at_least_once: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
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
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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

    /// Subscribe to a stream, awaiting an `AsyncSubscription`.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None))]
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (client, subscription) = inner
                .subscribe_from(&tenant_id, &namespace, &stream, start)
                .await
                .map_err(to_py_err)?;
            Ok(AsyncSubscription {
                inner: Arc::new(Mutex::new(Some(subscription))),
                _client: client,
            })
        })
    }

    #[pyo3(signature = (tenant_id, namespace, cache, key, value, *, ttl=None))]
    fn cache_put<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: &[u8],
        ttl: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        let value = Bytes::copy_from_slice(value);
        let ttl_ms = ttl.map(|seconds| (seconds.max(0.0) * 1000.0) as u64);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = inner.client().await;
            client
                .cache_put(&tenant_id, &namespace, &cache, &key, value, ttl_ms)
                .await
                .map_err(to_py_err)
        })
    }

    fn cache_get<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = inner.client().await;
            let value = client
                .cache_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)?;
            Ok(value.map(|bytes| bytes.to_vec()))
        })
    }

    fn cache_delete<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = inner.client().await;
            let value = client
                .cache_delete(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)?;
            Ok(value.map(|bytes| bytes.to_vec()))
        })
    }

    fn counter_add<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = inner.client().await;
            client
                .counter_add(&tenant_id, &namespace, &cache, &key, delta)
                .await
                .map_err(to_py_err)
        })
    }

    fn counter_get<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = inner.client().await;
            client
                .counter_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })
    }

    // ---- consumer groups -------------------------------------------------

    /// Take up to `max_records` for this group, waiting up to `wait` seconds.
    /// See the synchronous `Client.group_poll` for what the options mean.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, group, *, max_records=32, wait=5.0))]
    fn group_poll<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
        wait: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        let wait = std::time::Duration::from_secs_f64(wait.max(0.0));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = inner.client().await;
            let records = client
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
                .map_err(to_py_err)?;
            Ok(records
                .into_iter()
                .map(OwnedGroupRecord::from)
                .collect::<Vec<_>>())
        })
    }

    /// Finish one record.
    fn group_ack<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle(
            py,
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Ack,
        )
    }

    /// Hand one record back for immediate redelivery.
    fn group_nack<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle(
            py,
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Nack,
        )
    }

    /// Offsets this group gave up on, lowest first.
    fn group_dead_letters<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = inner.client().await;
            client
                .group_dead_letters(&tenant_id, &namespace, &stream, shard, &group)
                .await
                .map_err(to_py_err)
        })
    }

    /// Drop one dead letter without touching the record.
    fn group_discard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle(
            py,
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Discard,
        )
    }

    /// Put one dead letter back in the queue, attempts reset.
    fn group_redrive<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle(
            py,
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Redrive,
        )
    }

    // ---- cache watch -----------------------------------------------------

    /// Watch a key or prefix for changes. See the synchronous
    /// `Client.watch_cache` for what `start` and `retained` mean.
    #[pyo3(signature = (tenant_id, namespace, cache, filter, *, start=None, retained=false))]
    fn watch_cache<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: &CacheWatchFilter,
        start: Option<u64>,
        retained: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
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
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Through the cluster client, so the watch follows the redirect to
            // whichever broker owns the key's shard.
            let watch = if retained {
                inner
                    .watch_cache_retained(&tenant_id, &namespace, &cache, filter)
                    .await
            } else {
                inner
                    .watch_cache(&tenant_id, &namespace, &cache, filter, start)
                    .await
            }
            .map_err(to_py_err)?;
            Ok(AsyncCacheWatch {
                resume_offset: watch.resume_offset(),
                resnapshot: watch.resnapshot(),
                retained_count: watch.retained_count(),
                inner: Arc::new(Mutex::new(Some(watch))),
            })
        })
    }

    // ---- multi-shard subscribe -------------------------------------------

    /// How many shards this stream was placed with.
    fn stream_shards<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = inner.client().await;
            client
                .stream_shards(&tenant_id, &namespace, &stream)
                .await
                .map_err(to_py_err)
        })
    }

    /// Subscribe to every shard of a stream and merge them. See the
    /// synchronous `Client.subscribe_sharded` for the ordering caveat.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None, resume=None))]
    fn subscribe_sharded<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<Py<PyAny>>,
        resume: Option<std::collections::BTreeMap<u32, u64>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let subscription = match resume {
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
            .map_err(to_py_err)?;
            Ok(AsyncShardedSubscription {
                shards: subscription.shards(),
                inner: Arc::new(Mutex::new(Some(subscription))),
            })
        })
    }

    fn endpoints<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(inner
                .endpoints()
                .await
                .iter()
                .map(std::net::SocketAddr::to_string)
                .collect::<Vec<_>>())
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle: Py<Self> = slf.into();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(handle) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(false) })
    }
}

impl AsyncClient {
    /// The four group settles differ only in which client method they call.
    #[allow(clippy::too_many_arguments)]
    fn settle<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
        what: Settle,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = inner.client().await;
            match what {
                Settle::Ack => {
                    client
                        .group_ack(&tenant_id, &namespace, &stream, shard, &group, offset)
                        .await
                }
                Settle::Nack => {
                    client
                        .group_nack(&tenant_id, &namespace, &stream, shard, &group, offset)
                        .await
                }
                Settle::Discard => {
                    client
                        .group_discard(&tenant_id, &namespace, &stream, shard, &group, offset)
                        .await
                }
                Settle::Redrive => {
                    client
                        .group_redrive(&tenant_id, &namespace, &stream, shard, &group, offset)
                        .await
                }
            }
            .map_err(to_py_err)
        })
    }
}

/// A cache watch consumed with `async for`.
#[pyclass(module = "felix", name = "AsyncCacheWatch")]
pub struct AsyncCacheWatch {
    inner: Arc<Mutex<Option<felix_client::CacheWatch>>>,
    #[pyo3(get)]
    resume_offset: u64,
    #[pyo3(get)]
    resnapshot: bool,
    #[pyo3(get)]
    retained_count: Option<u64>,
}

#[pymethods]
impl AsyncCacheWatch {
    #[pyo3(signature = (timeout=None))]
    fn recv<'py>(&self, py: Python<'py>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
            Ok(next.map(OwnedWatchItem::from))
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.take();
            Ok(())
        })
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let Some(watch) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err("watch closed"));
            };
            match watch.recv().await {
                Some(item) => Ok(OwnedWatchItem::from(item)),
                None => Err(PyStopAsyncIteration::new_err("watch ended")),
            }
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle: Py<Self> = slf.into();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(handle) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.take();
            Ok(false)
        })
    }
}

/// Every shard of a stream, merged, consumed with `async for`.
#[pyclass(module = "felix", name = "AsyncShardedSubscription")]
pub struct AsyncShardedSubscription {
    inner: Arc<Mutex<Option<felix_client::ShardedSubscription>>>,
    #[pyo3(get)]
    shards: u32,
}

#[pymethods]
impl AsyncShardedSubscription {
    #[pyo3(signature = (timeout=None))]
    fn next_event<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
            Ok(next.map(OwnedShardEvent::from))
        })
    }

    /// The highest offset handled per shard, for resuming.
    fn positions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard
                .as_ref()
                .map(felix_client::ShardedSubscription::positions)
                .unwrap_or_default())
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.take();
            Ok(())
        })
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let Some(subscription) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err("subscription closed"));
            };
            match subscription.next().await {
                Some(event) => Ok(OwnedShardEvent::from(event)),
                None => Err(PyStopAsyncIteration::new_err("every shard ended")),
            }
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle: Py<Self> = slf.into();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(handle) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.take();
            Ok(false)
        })
    }
}
