//! Tenant Ed25519 signing-key generation for Felix JWTs.
//!
//! Ed25519 only, and it has to stay that way: signing is constant-time by
//! design, which sidesteps the RSA timing pitfalls, and Felix verification
//! rejects every other algorithm anyway.
//!
//! The private key is a raw 32-byte seed, *not* PKCS8 DER, and the public key
//! is derived from that seed rather than stored alongside it — a stored pair
//! can drift, a derived one cannot. `kid` is random, used for rotation and
//! cache lookup, and is not a secret. Private material must never be
//! serialized or logged outside the control-plane store; only public keys
//! cross the boundary, via JWKS.
use anyhow::Result;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use jsonwebtoken::Algorithm;
use rand::Rng;

use crate::auth::felix_token::{SigningKey, TenantSigningKeys};

/// Generate a fresh Ed25519 key set for a tenant: one current key, no
/// previous keys. Call during provisioning or rotation and persist the result.
pub fn generate_signing_keys() -> Result<TenantSigningKeys> {
    let mut private_key = [0u8; 32];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut private_key);
    let signing_key = Ed25519SigningKey::from_bytes(&private_key);
    let public_key = signing_key.verifying_key().to_bytes();

    let mut kid_bytes = [0u8; 16];
    rng.fill_bytes(&mut kid_bytes);
    let kid = hex::encode(kid_bytes);

    Ok(TenantSigningKeys {
        current: SigningKey {
            kid,
            alg: Algorithm::EdDSA,
            private_key,
            public_key,
        },
        previous: Vec::new(),
    })
}
