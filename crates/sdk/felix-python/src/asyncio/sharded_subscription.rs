//! Every shard of a stream, merged, consumed with `async for`.

use std::sync::Arc;

use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::types::OwnedShardEvent;

/// Every shard of a stream, merged, consumed with `async for`.
#[pyclass(module = "felix", name = "AsyncShardedSubscription")]
pub struct AsyncShardedSubscription {
    inner: Arc<Mutex<Option<felix_client::ShardedSubscription>>>,
    #[pyo3(get)]
    shards: u32,
}

impl AsyncShardedSubscription {
    pub(crate) fn new(subscription: felix_client::ShardedSubscription) -> Self {
        Self {
            shards: subscription.shards(),
            inner: Arc::new(Mutex::new(Some(subscription))),
        }
    }
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
