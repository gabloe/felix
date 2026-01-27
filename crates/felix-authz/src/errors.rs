use thiserror::Error;

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
    #[error("claims tenant mismatch: expected {expected}, got {actual}")]
    TenantMismatch { expected: String, actual: String },
    #[error("jwks not available for tenant {0}")]
    MissingJwks(String),
}

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
