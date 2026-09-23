use super::*;
use std::error::Error;

const TEST_PRIVATE_KEY: [u8; 32] = [5u8; 32];

fn signing_keys() -> TenantSigningKeys {
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    TenantSigningKeys {
        current: SigningKey {
            kid: "k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TEST_PRIVATE_KEY,
            public_key,
        },
        previous: vec![],
    }
}

#[test]
fn mint_roundtrip_contains_claims() {
    let keys = signing_keys();
    let token = mint_token(
        &keys,
        "tenant-a",
        "principal",
        vec!["stream.publish:stream:tenant-a/payments/orders/*".to_string()],
        Duration::from_secs(900),
    )
    .expect("mint");
    assert!(!token.is_empty());
}

#[test]
fn signing_key_rejects_non_eddsa_algorithm() {
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let key = SigningKey {
        kid: "k1".to_string(),
        alg: Algorithm::ES256,
        private_key: TEST_PRIVATE_KEY,
        public_key: signing_key.verifying_key().to_bytes(),
    };
    let err = key.validate().expect_err("expected invalid algorithm");
    assert!(matches!(err, TokenError::Key(_)));
}

#[test]
fn signing_key_rejects_mismatched_public_key() {
    let key = SigningKey {
        kid: "k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key: TEST_PRIVATE_KEY,
        public_key: [9u8; 32],
    };
    let err = key.validate().expect_err("expected mismatched key");
    assert!(matches!(err, TokenError::Key(_)));
}

#[test]
fn tenant_signing_keys_validate_checks_previous() {
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let current = SigningKey {
        kid: "k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key: TEST_PRIVATE_KEY,
        public_key: signing_key.verifying_key().to_bytes(),
    };
    let previous = SigningKey {
        kid: "k0".to_string(),
        alg: Algorithm::EdDSA,
        private_key: TEST_PRIVATE_KEY,
        public_key: [1u8; 32],
    };
    let keys = TenantSigningKeys {
        current,
        previous: vec![previous],
    };
    let err = keys.validate().expect_err("expected invalid previous key");
    assert!(matches!(err, TokenError::Key(_)));
}

#[test]
fn tenant_signing_keys_all_keys_returns_current_first() {
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let keys = TenantSigningKeys {
        current: SigningKey {
            kid: "k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TEST_PRIVATE_KEY,
            public_key: signing_key.verifying_key().to_bytes(),
        },
        previous: vec![SigningKey {
            kid: "k0".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TEST_PRIVATE_KEY,
            public_key: signing_key.verifying_key().to_bytes(),
        }],
    };
    let mut iter = keys.all_keys();
    assert_eq!(iter.next().map(|k| k.kid.as_str()), Some("k1"));
    assert_eq!(iter.next().map(|k| k.kid.as_str()), Some("k0"));
}

#[test]
fn token_error_source_is_none_for_key() {
    let err = TokenError::Key("bad".to_string());
    assert!(err.source().is_none());
}
