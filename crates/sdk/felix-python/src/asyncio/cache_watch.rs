//! A cache watch consumed with `async for`.

use std::sync::Arc;

use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::types::OwnedWatchItem;

/// A cache watch consumed with `async for`.
#[pyclass(module = "felix", name = "AsyncCacheWatch")]
pub struct AsyncCacheWatch {
    inner: Arc<Mutex<Option<felix_client::ClusterCacheWatch>>>,
    #[pyo3(get)]
    resume_offset: u64,
    #[pyo3(get)]
    resnapshot: bool,
    #[pyo3(get)]
    retained_count: Option<u64>,
}

impl AsyncCacheWatch {
    pub(crate) fn new(watch: felix_client::ClusterCacheWatch) -> Self {
        Self {
            resume_offset: watch.resume_offset(),
            resnapshot: watch.resnapshot(),
            retained_count: watch.retained_count(),
            inner: Arc::new(Mutex::new(Some(watch))),
        }
    }
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
