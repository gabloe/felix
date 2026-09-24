//! Every shard of a stream, read from a thread.

use std::sync::Arc;

use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::runtime::block_on;
use crate::types::OwnedShardEvent;

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

impl ShardedSubscriptionHandle {
    pub(crate) fn new(subscription: felix_client::ShardedSubscription) -> Self {
        Self {
            shards: subscription.shards(),
            inner: Arc::new(Mutex::new(Some(subscription))),
        }
    }
}

#[pymethods]
impl ShardedSubscriptionHandle {
    /// The next event from any shard, or `None` once every shard has ended.
    #[pyo3(signature = (timeout=None))]
    fn next_event(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Py<PyAny>>> {
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
            Ok(next.map(OwnedShardEvent::from))
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

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.next_event(py, None)
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
