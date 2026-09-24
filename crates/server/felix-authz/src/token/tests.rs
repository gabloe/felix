//! Shared key fixtures for the token tests, and the round trip through
//! issuer and verifier.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use jsonwebtoken::{Algorithm, EncodingKey};

use super::*;
use crate::jwks::{Jwk, KeyUse};
use crate::{Jwks, TenantId};

pub(super) const TEST_PRIVATE_KEY: [u8; 32] = [7u8; 32];

pub(super) fn test_key_material() -> TenantKeyMaterial {
    let signing_key = SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    TenantKeyMaterial {
        kid: "k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key: TEST_PRIVATE_KEY,
        public_key,
        jwks: Jwks {
            keys: vec![Jwk {
                kty: "OKP".to_string(),
                kid: "k1".to_string(),
                alg: "EdDSA".to_string(),
                use_field: KeyUse::Sig,
                crv: Some("Ed25519".to_string()),
                x: Some("test".to_string()),
            }],
        },
    }
}

pub(super) fn test_key_store() -> Arc<dyn TenantKeyStore> {
    let mut keys = HashMap::new();
    keys.insert("tenant-a".to_string(), test_key_material());
    Arc::new(keys)
}

// Mirrors the production PKCS8 conversion so hand-built tokens verify.
pub(super) fn encoding_key_from_seed(seed: &[u8; 32]) -> EncodingKey {
    let signing_key = SigningKey::from_bytes(seed);
    let der = signing_key.to_pkcs8_der().expect("pkcs8 der");
    EncodingKey::from_ed_der(der.as_bytes())
}

#[test]
fn mint_and_verify_roundtrip() {
    let key_store = test_key_store();
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(600),
        key_store.clone(),
    );
    let token = issuer
        .mint(
            &TenantId::new("tenant-a"),
            "principal",
            vec!["stream.publish:stream:tenant-a/payments/orders.*".to_string()],
        )
        .expect("token mint");

    let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store.clone());
    let claims = verifier
        .verify(&TenantId::new("tenant-a"), &token)
        .expect("verify token");
    assert_eq!(claims.sub, "principal");
    assert_eq!(claims.tid, "tenant-a");
}
