//! Uniform HTTP error shapes for the API. Every response body carries a
//! stable `code` plus a human-readable `message`; internal failures are
//! logged server-side and returned as generic messages so store details never
//! leak to clients.
use crate::api::types::ErrorResponse;
use crate::store::StoreError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;

/// An HTTP status paired with the JSON error body.
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

/// 404 with code `not_found`.
pub fn api_not_found(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::NOT_FOUND,
        body: ErrorResponse {
            code: "not_found".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// 404 with code `not_enabled` — deliberately a 404, so callers can't probe
/// which features a deployment has disabled.
pub fn api_not_enabled(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::NOT_FOUND,
        body: ErrorResponse {
            code: "not_enabled".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// 409 with a caller-chosen conflict code.
pub fn api_conflict(code: &str, message: &str) -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        body: ErrorResponse {
            code: code.to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// 500 from a store error: the error is logged here, the client gets only
/// `message`.
pub fn api_internal(message: &str, err: &StoreError) -> ApiError {
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

/// An error with a status the caller chose.
///
/// For the cases where the status *is* the answer rather than a fault report —
/// readiness answering 503 is a statement about this instance, not a bug.
pub fn api_error(status: StatusCode, code: &str, message: &str) -> ApiError {
    ApiError {
        status,
        body: ErrorResponse {
            code: code.to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// 500 without a store error to log.
pub fn api_internal_message(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        body: ErrorResponse {
            code: "internal".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// 401 with code `unauthorized`.
pub fn api_unauthorized(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::UNAUTHORIZED,
        body: ErrorResponse {
            code: "unauthorized".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// 403 with code `forbidden`.
pub fn api_forbidden(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::FORBIDDEN,
        body: ErrorResponse {
            code: "forbidden".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

/// 400 with code `validation_error`.
pub fn api_validation_error(message: &str) -> ApiError {
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
