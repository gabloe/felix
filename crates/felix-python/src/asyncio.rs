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
use crate::{Event, parse_ack, parse_addrs, parse_start};

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
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
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
        addrs: PyObject,
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
        ack: &str,
        at_least_once: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ack = parse_ack(ack)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let payload = payload.to_vec();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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

    /// Subscribe to a stream, awaiting an `AsyncSubscription`.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None))]
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<PyObject>,
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
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(false) })
    }
}
