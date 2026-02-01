//! Error types for Felix authorization and token processing.
//!
//! # Purpose
//! Centralizes error variants for authz validation, key handling, and JWT work.
//!
//! # How it fits
//! All authz modules return [`AuthzResult`] with these variants to keep error
//! handling consistent across broker and control-plane code.
//!
//! # Key invariants
//! - Variants are stable; external callers may match on them.
//! - JWT errors are wrapped to preserve context from `jsonwebtoken`.
//!
//! # Important configuration
//! - None.
//!
//! # Examples
//! ```rust
//! use felix_authz::{AuthzError, AuthzResult};
//!
//! fn validate(flag: bool) -> AuthzResult<()> {
//!     if !flag {
//!         return Err(AuthzError::InvalidPermission("missing".to_string()));
//!     }
//!     Ok(())
//! }
//! assert!(validate(false).is_err());
//! ```
//!
//! # Common pitfalls
//! - Dropping the underlying JWT error loses the root cause in logs.
//! - Converting all errors to strings makes it hard to match on variants in tests.
//!
//! # Future work
//! - Add richer error context for policy evaluation (subject, action, resource).
use thiserror::Error;

/// Errors emitted by Felix authorization helpers.
///
/// # Summary
/// Enumerates the failure modes for token, key, and permission handling.
///
/// # Invariants
/// - Variants that carry tenant IDs must include the raw tenant string.
///
/// # Example
/// ```rust
/// use felix_authz::AuthzError;
///
/// let err = AuthzError::MissingSigningKey("tenant-1".to_string());
/// assert!(err.to_string().contains("tenant-1"));
/// ```
#[derive(Debug, Error)]
pub enum AuthzError {
    #[error("invalid action: {0}")]
    InvalidAction(String),
    #[error("invalid permission: {0}")]
    InvalidPermission(String),
    #[error("missing signing key for tenant {0}")]
    MissingSigningKey(String),
    #[error("missing verification keys for tenant {0}")]
    MissingVerificationKeys(String),
    #[error("jwt error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("key error: {0}")]
    Key(String),
    #[error("claims tenant mismatch: expected {expected}, got {actual}")]
    TenantMismatch { expected: String, actual: String },
    #[error("jwks not available for tenant {0}")]
    MissingJwks(String),
}

/// Result alias for authorization operations.
///
/// # Summary
/// Standardizes return types for authz helpers and key stores.
///
/// # Example
/// ```rust
/// use felix_authz::{AuthzResult, AuthzError};
///
/// fn ok() -> AuthzResult<()> {
///     Ok(())
/// }
/// let _ = ok();
/// ```
pub type AuthzResult<T> = Result<T, AuthzError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_variants() {
        let errors = vec![
            AuthzError::InvalidAction("bad".to_string()),
            AuthzError::InvalidPermission("bad".to_string()),
            AuthzError::MissingSigningKey("tenant".to_string()),
            AuthzError::MissingVerificationKeys("tenant".to_string()),
            AuthzError::Key("bad".to_string()),
            AuthzError::TenantMismatch {
                expected: "a".to_string(),
                actual: "b".to_string(),
            },
            AuthzError::MissingJwks("tenant".to_string()),
        ];

        for error in errors {
            let rendered = error.to_string();
            assert!(!rendered.is_empty());
        }
    }
}
