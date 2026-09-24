//! Verifying tokens.

use std::sync::Arc;

use jsonwebtoken::{Algorithm, Validation};

use super::FelixClaims;
use super::key_cache::TenantKeyCache;
use super::keys::{TenantKeyStore, validate_alg};
use crate::{AuthzError, AuthzResult, TenantId};

/// Verifies Felix JWTs: signature, `iss`, `aud`, expiry (with leeway), and a
/// `tid` match against the expected tenant. Algorithm is pinned to EdDSA.
pub struct FelixTokenVerifier {
    issuer: String,
    audience: String,
    leeway: u64,
    key_store: Arc<dyn TenantKeyStore>,
    cache: Arc<TenantKeyCache>,
}

impl FelixTokenVerifier {
    pub fn new(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        leeway: u64,
        key_store: Arc<dyn TenantKeyStore>,
    ) -> Self {
        Self {
            issuer: issuer.into(),
            audience: audience.into(),
            leeway,
            key_store,
            cache: Arc::new(TenantKeyCache::default()),
        }
    }

    /// Share a key cache with other components. The cache must be invalidated
    /// on key rotation.
    pub fn with_cache(mut self, cache: Arc<TenantKeyCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Verify a token and return its claims.
    ///
    /// Keys are tried in `kid`-preferred order so the common case is one
    /// signature check even mid-rotation; an unknown or missing `kid` just
    /// means trying every key.
    ///
    /// # Errors
    /// Missing verification keys, signature/claim validation failures, or a
    /// `tid` that names a different tenant.
    pub fn verify(&self, tenant_id: &TenantId, token: &str) -> AuthzResult<FelixClaims> {
        let header = jsonwebtoken::decode_header(token)?;
        let keys = self.key_store.verification_keys(tenant_id)?;
        let mut ordered_keys = Vec::with_capacity(keys.len());
        if let Some(kid) = header.kid.as_deref() {
            if let Some(pos) = keys.iter().position(|entry| entry.kid == kid) {
                ordered_keys.push(keys[pos].clone());
                for (idx, entry) in keys.into_iter().enumerate() {
                    if idx != pos {
                        ordered_keys.push(entry);
                    }
                }
            } else {
                ordered_keys.extend(keys);
            }
        } else {
            ordered_keys.extend(keys);
        }

        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.leeway = self.leeway;

        let mut last_err = None;
        for key in ordered_keys {
            validate_alg(key.alg)?;
            let decoding_key = self.cache.decoding_key(tenant_id, &key)?;
            match jsonwebtoken::decode::<FelixClaims>(token, &decoding_key, &validation) {
                Ok(data) => {
                    // The signature only proves who signed it; the tid check is
                    // what stops a valid tenant-a token being used as tenant-b.
                    if data.claims.tid != tenant_id.as_str() {
                        return Err(AuthzError::TenantMismatch {
                            expected: tenant_id.to_string(),
                            actual: data.claims.tid.clone(),
                        });
                    }
                    return Ok(data.claims);
                }
                Err(err) => last_err = Some(err),
            }
        }

        // Keep the last JWT error so the caller sees why verification failed.
        Err(last_err.map(AuthzError::Jwt).unwrap_or_else(|| {
            AuthzError::Jwt(jsonwebtoken::errors::Error::from(
                jsonwebtoken::errors::ErrorKind::InvalidToken,
            ))
        }))
    }
}

#[cfg(test)]
mod tests;
