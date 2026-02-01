//! Signing key generation helpers for Felix tenant JWTs.
//!
//! # Purpose
//! Produce tenant-scoped Ed25519 signing keys that the control-plane stores and
//! later exposes (public parts only) via JWKS for broker verification.
//!
//! # Architectural role
//! This module is the single source of truth for Felix tenant signing key
//! generation. It is invoked by control-plane provisioning and rotation flows
//! and feeds downstream JWKS publication and broker verification.
//!
//! # Callers / consumers
//! - Control-plane tenant provisioning and rotation logic.
//! - Tests that need deterministic key material for Felix JWTs.
//!
//! # Key invariants
//! - Keys are always Ed25519 and must remain that way for Felix-issued tokens.
//! - The private key is a raw 32-byte Ed25519 seed (not PKCS8 DER), and the
//!   public key is derived from that seed to avoid mismatches.
//! - Private key material must never be serialized or logged outside storage.
//!
//! # Concurrency model
//! Pure, stateless key generation with no shared mutable state; safe to call
//! concurrently from multiple async tasks.
//!
//! # Security boundary
//! This module generates private key material and must only be used inside the
//! control-plane trust boundary. Public keys may cross the boundary via JWKS,
//! but private keys must never leave storage.
//!
//! # Security model and threat assumptions
//! - We assume attackers can observe tokens and JWKS but cannot read the
//!   control-plane's private key storage.
//! - Ed25519 is used to avoid RSA timing pitfalls and because it is constant-time
//!   by design for signing, reducing side-channel risk.
//! - Key IDs (`kid`) are random and used for rotation and cache lookups, not as
//!   a secret.
//!
//! # How to use
//! Call [`generate_signing_keys`] during tenant provisioning or key rotation and
//! persist the returned [`TenantSigningKeys`] in the control-plane store.
use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use anyhow::Result;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use jsonwebtoken::Algorithm;
use rand::RngCore;

/// Generate a fresh Ed25519 signing key set for a tenant.
///
/// # Overview
/// Returns a [`TenantSigningKeys`] with a single current Ed25519 key and no
/// previous keys. The key material uses a raw 32-byte Ed25519 seed to keep the
/// storage format compact and explicit.
///
/// # Arguments
/// - None.
///
/// # Returns
/// - `Ok(TenantSigningKeys)` with a current signing key and empty rotation list.
///
/// # Errors
/// - Propagates any RNG failure surfaced by the OS (via [`anyhow::Result`]).
///
/// # Panics
/// - Does not panic under normal conditions.
///
/// # Examples
/// ```rust
/// use controlplane::auth::keys::generate_signing_keys;
///
/// let keys = generate_signing_keys().expect("keys");
/// assert_eq!(keys.previous.len(), 0);
/// assert_eq!(keys.current.alg, jsonwebtoken::Algorithm::EdDSA);
/// ```
///
/// # Security
/// - Never log or serialize the returned private key outside secure storage.
/// - The algorithm is fixed to EdDSA to avoid accidental RSA fallback.
pub fn generate_signing_keys() -> Result<TenantSigningKeys> {
    // Step 1: Generate a 32-byte Ed25519 seed.
    // We store raw seeds (not PKCS8) to keep storage compact and to make
    // derivation of the public key deterministic for integrity checks.
    let mut private_key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut private_key);
    let signing_key = Ed25519SigningKey::from_bytes(&private_key);
    let public_key = signing_key.verifying_key().to_bytes();

    // Step 2: Generate a random `kid` for rotation and cache lookup.
    // The `kid` is not a secret; it allows brokers to select the right public
    // key quickly while we still verify all keys if needed.
    let mut kid_bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut kid_bytes);
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
