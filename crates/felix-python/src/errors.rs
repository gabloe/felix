//! Typed exceptions, so Python can branch on *why* something failed.
//!
//! A single `FelixError` carrying prose would push callers into matching on
//! message text, which breaks the moment a message is reworded. The split here
//! mirrors the distinction the Rust client already makes and the one that
//! actually changes what an application does: a `NotLeaderError` or a
//! connection failure is worth retrying, an authorization failure or a bad
//! argument is not.
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    _felix,
    FelixError,
    PyException,
    "Base class for every Felix error."
);
create_exception!(
    _felix,
    ConnectionError,
    FelixError,
    "The broker could not be reached, or the connection was lost mid-call."
);
create_exception!(
    _felix,
    AuthError,
    FelixError,
    "The token was rejected, or it does not carry the permission this call needs."
);
create_exception!(
    _felix,
    NotFoundError,
    FelixError,
    "The tenant, namespace, stream, or cache does not exist on the broker."
);
create_exception!(
    _felix,
    CursorError,
    FelixError,
    "The requested start offset is no longer available (retention discarded it)."
);

/// Classify an error from the Rust client into one of the typed exceptions.
///
/// Deliberately conservative: anything unrecognised becomes the base
/// `FelixError` rather than being forced into a category, because a wrong
/// category is worse than a general one — it tells an application to retry
/// something that cannot succeed, or to give up on something that could.
pub(crate) fn to_py_err(err: anyhow::Error) -> PyErr {
    let text = format!("{err:#}");
    let lower = text.to_ascii_lowercase();

    if lower.contains("unauthorized")
        || lower.contains("permission")
        || lower.contains("forbidden")
        || lower.contains("token")
    {
        return AuthError::new_err(text);
    }
    if lower.contains("unknown tenant")
        || lower.contains("unknown stream")
        || lower.contains("unknown cache")
        || lower.contains("not found")
    {
        return NotFoundError::new_err(text);
    }
    if lower.contains("cursor") || lower.contains("trimmed") || lower.contains("too old") {
        return CursorError::new_err(text);
    }
    if lower.contains("connect")
        || lower.contains("connection")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("no broker")
        || lower.contains("not leader")
    {
        return ConnectionError::new_err(text);
    }
    FelixError::new_err(text)
}

/// Register the exception types on the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("FelixError", py.get_type::<FelixError>())?;
    module.add("ConnectionError", py.get_type::<ConnectionError>())?;
    module.add("AuthError", py.get_type::<AuthError>())?;
    module.add("NotFoundError", py.get_type::<NotFoundError>())?;
    module.add("CursorError", py.get_type::<CursorError>())?;
    Ok(())
}
