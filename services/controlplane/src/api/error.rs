//! API error types and helpers.
//!
//! # Purpose
//! Centralizes HTTP error response construction for consistent error shapes.
use crate::api::types::ErrorResponse;
use crate::store::StoreError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;

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
