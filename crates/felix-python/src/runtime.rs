//! The Tokio runtime the bindings block on.
//!
//! One runtime for the process, not one per client: a Felix client owns
//! connection pools and background tasks, and giving each its own runtime
//! would multiply the I/O threads for no benefit. Created on first use so
//! importing the module costs nothing.
//!
//! Every call into it is wrapped in `Python::allow_threads`, which releases
//! the GIL for the duration. Without that a blocking publish would stall every
//! other Python thread in the process, which is exactly the behaviour that
//! makes people distrust native extensions.
use std::sync::OnceLock;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub(crate) fn runtime() -> PyResult<&'static Runtime> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let built = Runtime::new().map_err(|err| {
        PyRuntimeError::new_err(format!("could not start the Felix runtime: {err}"))
    })?;
    // A racing thread may have installed one already; theirs is as good as ours.
    let _ = RUNTIME.set(built);
    RUNTIME
        .get()
        .ok_or_else(|| PyRuntimeError::new_err("the Felix runtime vanished after being set"))
}

/// Run a future to completion with the GIL released.
pub(crate) fn block_on<F, T>(py: Python<'_>, future: F) -> PyResult<T>
where
    F: std::future::Future<Output = PyResult<T>> + Send,
    T: Send,
{
    let runtime = runtime()?;
    py.allow_threads(|| runtime.block_on(future))
}
