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
