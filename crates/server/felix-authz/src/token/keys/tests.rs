use std::sync::Arc;

use super::*;
use crate::token::tests::{TEST_PRIVATE_KEY, test_key_store};

#[test]
fn key_store_jwks() {
    let key_store = test_key_store();
    let jwks = key_store.jwks(&TenantId::new("tenant-a")).expect("jwks");
    assert_eq!(jwks.keys.len(), 1);
}

#[test]
fn key_store_missing_jwks() {
    let key_store: Arc<dyn TenantKeyStore> = Arc::new(HashMap::new());
    let err = key_store
        .jwks(&TenantId::new("tenant-a"))
        .expect_err("missing jwks");
    assert!(matches!(err, AuthzError::MissingJwks(_)));
}

#[test]
fn invalid_key_algorithm_rejected() {
    let mut keys = HashMap::new();
    keys.insert(
        "tenant-a".to_string(),
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::RS256,
            private_key: TEST_PRIVATE_KEY,
            public_key: TEST_PRIVATE_KEY,
            jwks: Jwks { keys: vec![] },
        },
    );
    let key_store: Arc<dyn TenantKeyStore> = Arc::new(keys);
    let signing = key_store.current_signing_key(&TenantId::new("tenant-a"));
    assert!(matches!(signing, Err(AuthzError::Key(_))));

    let verification = key_store.verification_keys(&TenantId::new("tenant-a"));
    assert!(matches!(verification, Err(AuthzError::Key(_))));
}
