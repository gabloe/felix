//! API error types and helpers.
//!
//! # Purpose and responsibility
//! Centralizes HTTP error response construction to keep error shapes uniform
//! across control-plane endpoints.
//!
//! # Where it fits in Felix
//! All API handlers use these helpers to return structured errors to clients
//! and to translate store failures into HTTP responses.
//!
//! # Key invariants and assumptions
//! - Error responses must include a stable `code` and human-readable `message`.
//! - Status codes must align with the error category.
//!
//! # Security considerations
//! - Internal errors log details server-side but return generic messages.
//! - Request IDs are optional; avoid leaking sensitive details in messages.
use crate::api::types::ErrorResponse;
use crate::store::StoreError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;

/// Structured API error returned by handlers.
///
/// # What it does
/// Couples an HTTP status code with a JSON error body.
///
/// # Why it exists
/// Provides a single error type that implements `IntoResponse` for Axum.
///
/// # Invariants
/// - `status` must match the semantics of `body.code`.
///
/// # Example
/// ```rust
/// use axum::http::StatusCode;
/// use controlplane::api::error::ApiError;
/// use controlplane::api::types::ErrorResponse;
///
/// let err = ApiError {
///     status: StatusCode::NOT_FOUND,
///     body: ErrorResponse {
///         code: "not_found".to_string(),
///         message: "missing".to_string(),
///         request_id: None,
///     },
/// };
/// ```
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: ErrorResponse,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(self.body)).into_response()
    }
}

/// Build a 404 Not Found error.
///
/// # What it does
/// Returns an `ApiError` with code `not_found` and the provided message.
///
/// # Errors
/// - Does not fail.
pub fn api_not_found(message: &str) -> ApiError {
    // Return a consistent not-found error shape.
    ApiError {
        status: StatusCode::NOT_FOUND,
        body: ErrorResponse {
            code: "not_found".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// Build a 404 error for disabled features.
///
/// # What it does
/// Uses a `not_enabled` code to indicate the endpoint is disabled.
///
/// # Errors
/// - Does not fail.
pub fn api_not_enabled(message: &str) -> ApiError {
    // Use NOT_FOUND to avoid exposing disabled feature presence.
    ApiError {
        status: StatusCode::NOT_FOUND,
        body: ErrorResponse {
            code: "not_enabled".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// Build a 409 Conflict error.
///
/// # What it does
/// Returns an `ApiError` with a caller-provided conflict code.
///
/// # Errors
/// - Does not fail.
pub fn api_conflict(code: &str, message: &str) -> ApiError {
    // Caller provides a specific conflict code for precise client handling.
    ApiError {
        status: StatusCode::CONFLICT,
        body: ErrorResponse {
            code: code.to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// Build a 500 Internal Server Error from a store error.
///
/// # What it does
/// Logs the store error and returns a generic internal error response.
///
/// # Errors
/// - Does not fail.
pub fn api_internal(message: &str, err: &StoreError) -> ApiError {
    // Log internal details server-side for debugging; return generic message.
    tracing::error!(error = ?err, "controlplane storage error");
    ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        body: ErrorResponse {
            code: "internal".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// Build a 500 Internal Server Error without a store error.
///
/// # What it does
/// Returns a generic internal error response with the provided message.
///
/// # Errors
/// - Does not fail.
pub fn api_internal_message(message: &str) -> ApiError {
    // Internal error without a concrete store error to log.
    ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        body: ErrorResponse {
            code: "internal".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// Build a 401 Unauthorized error.
///
/// # What it does
/// Returns an `ApiError` with code `unauthorized`.
///
/// # Errors
/// - Does not fail.
pub fn api_unauthorized(message: &str) -> ApiError {
    // Authentication failed or missing.
    ApiError {
        status: StatusCode::UNAUTHORIZED,
        body: ErrorResponse {
            code: "unauthorized".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// Build a 403 Forbidden error.
///
/// # What it does
/// Returns an `ApiError` with code `forbidden`.
///
/// # Errors
/// - Does not fail.
pub fn api_forbidden(message: &str) -> ApiError {
    // Authorization failed despite authentication.
    ApiError {
        status: StatusCode::FORBIDDEN,
        body: ErrorResponse {
            code: "forbidden".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// Build a 400 Bad Request validation error.
///
/// # What it does
/// Returns an `ApiError` with code `validation_error`.
///
/// # Errors
/// - Does not fail.
pub fn api_validation_error(message: &str) -> ApiError {
    // Client input failed validation or was malformed.
    ApiError {
        status: StatusCode::BAD_REQUEST,
        body: ErrorResponse {
            code: "validation_error".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_error_helpers_build_expected_codes() {
        let not_found = api_not_found("missing");
        assert_eq!(not_found.status, StatusCode::NOT_FOUND);
        assert_eq!(not_found.body.code, "not_found");

        let not_enabled = api_not_enabled("disabled");
        assert_eq!(not_enabled.status, StatusCode::NOT_FOUND);
        assert_eq!(not_enabled.body.code, "not_enabled");

        let conflict = api_conflict("already_exists", "conflict");
        assert_eq!(conflict.status, StatusCode::CONFLICT);
        assert_eq!(conflict.body.code, "already_exists");

        let internal = api_internal_message("oops");
        assert_eq!(internal.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(internal.body.code, "internal");

        let unauthorized = api_unauthorized("nope");
        assert_eq!(unauthorized.status, StatusCode::UNAUTHORIZED);
        assert_eq!(unauthorized.body.code, "unauthorized");

        let forbidden = api_forbidden("nope");
        assert_eq!(forbidden.status, StatusCode::FORBIDDEN);
        assert_eq!(forbidden.body.code, "forbidden");

        let validation = api_validation_error("bad");
        assert_eq!(validation.status, StatusCode::BAD_REQUEST);
        assert_eq!(validation.body.code, "validation_error");
    }

    #[test]
    fn api_internal_logs_and_wraps_store_error() {
        let err = StoreError::Unexpected(anyhow::anyhow!("boom"));
        let api = api_internal("storage failed", &err);
        assert_eq!(api.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(api.body.code, "internal");
        assert_eq!(api.body.message, "storage failed");
    }
}
