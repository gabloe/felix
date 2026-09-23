//! Tenant key material and where it comes from.

use std::collections::HashMap;

use jsonwebtoken::Algorithm;

use crate::{AuthzError, AuthzResult, Jwks, TenantId};

const ED25519_KEY_LEN: usize = 32;

/// A tenant's signing key: raw Ed25519 seed and public key plus `kid`.
/// Never serialize or log `private_key`.
#[derive(Clone)]
pub struct TenantSigningKey {
    pub kid: String,
    pub alg: Algorithm,
    pub private_key: [u8; ED25519_KEY_LEN],
    pub public_key: [u8; ED25519_KEY_LEN],
}

/// A tenant's verification key: Ed25519 public key plus `kid`.
#[derive(Clone)]
pub struct TenantVerificationKey {
    pub kid: String,
    pub alg: Algorithm,
    pub public_key: [u8; ED25519_KEY_LEN],
}

/// Source of signing keys, verification keys, and JWKS for a tenant.
/// Implementations must be `Send + Sync` and must never let private keys out
/// of trusted storage.
pub trait TenantKeyStore: Send + Sync {
    /// The active signing key for a tenant.
    ///
    /// # Errors
    /// [`AuthzError::MissingSigningKey`] when absent, [`AuthzError::Key`] for
    /// a non-EdDSA key.
    fn current_signing_key(&self, tenant_id: &TenantId) -> AuthzResult<TenantSigningKey>;

    /// All verification keys for a tenant — more than one during rotation.
    ///
    /// # Errors
    /// [`AuthzError::MissingVerificationKeys`] when absent, [`AuthzError::Key`]
    /// for a non-EdDSA key.
    fn verification_keys(&self, tenant_id: &TenantId) -> AuthzResult<Vec<TenantVerificationKey>>;

    /// The tenant's JWKS. Public key material only.
    ///
    /// # Errors
    /// [`AuthzError::MissingJwks`] when absent.
    fn jwks(&self, tenant_id: &TenantId) -> AuthzResult<Jwks>;
}

/// In-memory tenant key material, mostly for tests and simple stores.
#[derive(Clone)]
pub struct TenantKeyMaterial {
    pub kid: String,
    pub alg: Algorithm,
    pub private_key: [u8; ED25519_KEY_LEN],
    pub public_key: [u8; ED25519_KEY_LEN],
    pub jwks: Jwks,
}

impl TenantKeyStore for HashMap<String, TenantKeyMaterial> {
    fn current_signing_key(&self, tenant_id: &TenantId) -> AuthzResult<TenantSigningKey> {
        let entry = self
            .get(tenant_id.as_str())
            .ok_or_else(|| AuthzError::MissingSigningKey(tenant_id.to_string()))?;
        validate_alg(entry.alg)?;
        Ok(TenantSigningKey {
            kid: entry.kid.clone(),
            alg: entry.alg,
            private_key: entry.private_key,
            public_key: entry.public_key,
        })
    }

    fn verification_keys(&self, tenant_id: &TenantId) -> AuthzResult<Vec<TenantVerificationKey>> {
        let entry = self
            .get(tenant_id.as_str())
            .ok_or_else(|| AuthzError::MissingVerificationKeys(tenant_id.to_string()))?;
        validate_alg(entry.alg)?;
        Ok(vec![TenantVerificationKey {
            kid: entry.kid.clone(),
            alg: entry.alg,
            public_key: entry.public_key,
        }])
    }

    fn jwks(&self, tenant_id: &TenantId) -> AuthzResult<Jwks> {
        let entry = self
            .get(tenant_id.as_str())
            .ok_or_else(|| AuthzError::MissingJwks(tenant_id.to_string()))?;
        Ok(entry.jwks.clone())
    }
}

pub(super) fn validate_alg(alg: Algorithm) -> AuthzResult<()> {
    if alg == Algorithm::EdDSA {
        Ok(())
    } else {
        Err(AuthzError::Key(format!(
            "invalid Felix signing algorithm: {alg:?}"
        )))
    }
}

#[cfg(test)]
mod tests;
