//! Generation of tenant Ed25519 signing keys for Felix JWTs.
//!
//! Ed25519 only, and it has to stay that way: signing is constant-time by
//! design, which sidesteps the RSA timing pitfalls, and Felix verification
//! rejects every other algorithm anyway.
//!
//! The private key is a raw 32-byte seed, *not* PKCS8 DER, and the public key is
//! derived from that seed rather than stored alongside it -- a stored pair can
//! drift, a derived one cannot. `kid` is random, used for rotation and cache
//! lookup, and is not a secret.
//!
//! Generation is pure and stateless, so it is safe to call concurrently. The
//! private material it returns must never be serialized or logged outside the
//! control-plane store; only public keys cross the boundary, via JWKS.
//!
//! Call [`generate_signing_keys`] during provisioning or rotation and persist
//! the returned [`TenantSigningKeys`].
use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use anyhow::Result;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use jsonwebtoken::Algorithm;
use rand::Rng;

/// Generate a fresh Ed25519 signing key set for a tenant.
///
/// Returns a [`TenantSigningKeys`] with a single current Ed25519 key and no
/// previous keys. The key material uses a raw 32-byte Ed25519 seed to keep the
/// storage format compact and explicit.
/// - `Ok(TenantSigningKeys)` with a current signing key and empty rotation list.
///
/// # Errors
/// - Propagates any RNG failure surfaced by the OS (via [`anyhow::Result`]).
/// # Examples
/// ```rust
/// use controlplane::auth::keys::generate_signing_keys;
///
/// let keys = generate_signing_keys().expect("keys");
/// assert_eq!(keys.previous.len(), 0);
/// assert_eq!(keys.current.alg, jsonwebtoken::Algorithm::EdDSA);
/// ```
///
/// - Never log or serialize the returned private key outside secure storage.
/// - The algorithm is fixed to EdDSA to avoid accidental RSA fallback.
pub fn generate_signing_keys() -> Result<TenantSigningKeys> {
    // Step 1: Generate a 32-byte Ed25519 seed.
    // We store raw seeds (not PKCS8) to keep storage compact and to make
    // derivation of the public key deterministic for integrity checks.
    let mut private_key = [0u8; 32];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut private_key);
    let signing_key = Ed25519SigningKey::from_bytes(&private_key);
    let public_key = signing_key.verifying_key().to_bytes();

    // Step 2: Generate a random `kid` for rotation and cache lookup.
    // The `kid` is not a secret; it allows brokers to select the right public
    // key quickly while we still verify all keys if needed.
    let mut kid_bytes = [0u8; 16];
    rng.fill_bytes(&mut kid_bytes);
    let kid = hex::encode(kid_bytes);

    // IMPORTANT:
    // `alg` must stay EdDSA to prevent accidental RSA usage in Felix-issued
    // tokens. Changing this would break verification and reintroduce RSA.

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
