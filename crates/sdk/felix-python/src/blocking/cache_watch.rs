//! A cache watch read from a thread.

use std::sync::Arc;

use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::runtime::block_on;
use crate::types::OwnedWatchItem;

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

impl CacheWatchHandle {
    pub(crate) fn new(watch: felix_client::CacheWatch) -> Self {
        Self {
            resume_offset: watch.resume_offset(),
            resnapshot: watch.resnapshot(),
            retained_count: watch.retained_count(),
            inner: Arc::new(Mutex::new(Some(watch))),
        }
    }
}

#[pymethods]
impl CacheWatchHandle {
    /// The next change, or `None` once the watch ends.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Py<PyAny>>> {
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
            Ok(next.map(OwnedWatchItem::from))
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

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.recv(py, None)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}
