//! Typed errors, so JavaScript can branch on *why* something failed.
//!
//! A single error carrying prose would push callers into matching on message
//! text, which breaks the moment a message is reworded. The split here mirrors
//! the one the Python binding makes, and the one that actually changes what an
//! application does: a connection failure is worth retrying, an authorization
//! failure or a bad argument is not.
//!
//! The identity rides on `err.code` — the idiom Node's own errors use
//! (`ENOENT`, `ERR_MODULE_NOT_FOUND`) — but it cannot be set from here: napi
//! puts an error's *status* on `err.code`, and `#[napi]` requires that status
//! to be its own fixed `Status` enum. So the code travels as a message prefix
//! and `index.js` lifts it onto a typed `FelixError`. The two halves ship as
//! one package, so the prefix is an internal detail rather than something a
//! caller is expected to parse.

use napi::{Error, Status};

/// The separator between the code and the human text.
///
/// napi puts an error's *status* on `err.code`, and `Status` is a fixed enum
/// with no room for a Felix code — so the code travels in the message and
/// `index.js` lifts it onto a typed error. That prefix is an internal detail
/// between the two halves of this package, not something a caller parses.
pub(crate) const CODE_SEPARATOR: &str = ": ";

/// The broker rejected the token, or it lacks the permission this call needs.
/// Not retryable.
pub(crate) const CODE_AUTH: &str = "FELIX_AUTH";
/// The tenant, namespace, stream or cache does not exist. Not retryable.
pub(crate) const CODE_NOT_FOUND: &str = "FELIX_NOT_FOUND";
/// The requested start offset is gone — retention discarded it. Recover by
/// restarting from `earliest` and accepting the gap.
pub(crate) const CODE_CURSOR: &str = "FELIX_CURSOR";
/// The broker could not be reached, or the connection was lost mid-call.
/// Worth retrying, and against a different broker.
pub(crate) const CODE_CONNECTION: &str = "FELIX_CONNECTION";
/// Something this binding could not classify. Deliberately not a category.
pub(crate) const CODE_GENERIC: &str = "FELIX_ERROR";
/// A bad argument to this binding, rather than a failure of the call.
pub(crate) const CODE_INVALID: &str = "FELIX_INVALID";

/// Classify an error from the Rust client into one of the typed codes.
///
/// Deliberately conservative: anything unrecognised gets `FELIX_ERROR` rather
/// than being forced into a category, because a wrong category is worse than a
/// general one — it tells an application to retry something that cannot
/// succeed, or to give up on something that could.
pub(crate) fn classify<E: std::fmt::Display>(err: E) -> Error {
    let text = format!("{err:#}");
    let lower = text.to_ascii_lowercase();

    let code = if lower.contains("unauthorized")
        || lower.contains("permission")
        || lower.contains("forbidden")
        || lower.contains("token")
    {
        CODE_AUTH
    } else if lower.contains("unknown tenant")
        || lower.contains("unknown stream")
        || lower.contains("unknown cache")
        || lower.contains("unknown namespace")
        || lower.contains("not found")
    {
        CODE_NOT_FOUND
    } else if lower.contains("cursor") || lower.contains("trimmed") || lower.contains("too old") {
        CODE_CURSOR
    } else if lower.contains("connect")
        || lower.contains("connection")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("no broker")
        || lower.contains("not leader")
    {
        CODE_CONNECTION
    } else {
        CODE_GENERIC
    };

    Error::new(
        Status::GenericFailure,
        format!("{code}{CODE_SEPARATOR}{text}"),
    )
}

/// An error raised by this binding rather than by the client beneath it.
pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::new(
        Status::InvalidArg,
        format!("{CODE_INVALID}{CODE_SEPARATOR}{}", message.into()),
    )
}
