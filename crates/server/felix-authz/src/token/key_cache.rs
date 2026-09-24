//! Derived `jsonwebtoken` keys, cached per tenant and `kid`.

use std::collections::HashMap;
use std::sync::RwLock;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use jsonwebtoken::{DecodingKey, EncodingKey};

use super::keys::{TenantSigningKey, TenantVerificationKey, validate_alg};
use crate::{AuthzError, AuthzResult, TenantId};

/// Caches derived `jsonwebtoken` encoding/decoding keys by tenant and `kid`,
/// so PKCS8/base64url conversion happens once per key instead of per token.
/// Invalidate on rotation or stale keys will keep verifying.
#[derive(Default)]
pub struct TenantKeyCache {
    encoding: RwLock<HashMap<String, EncodingKey>>,
    decoding: RwLock<HashMap<String, DecodingKey>>,
}

impl TenantKeyCache {
    /// Drop every cached key for a tenant. Call this on key rotation.
    pub fn invalidate_tenant(&self, tenant_id: &TenantId) {
        // Entries are keyed "tenant:kid", so a prefix match catches them all.
        let mut prefix = String::with_capacity(tenant_id.as_str().len() + 1);
        prefix.push_str(tenant_id.as_str());
        prefix.push(':');
        if let Ok(mut map) = self.encoding.write() {
            map.retain(|key, _| !key.starts_with(&prefix));
        }
        if let Ok(mut map) = self.decoding.write() {
            map.retain(|key, _| !key.starts_with(&prefix));
        }
    }

    pub(super) fn encoding_key(
        &self,
        tenant_id: &TenantId,
        key: &TenantSigningKey,
    ) -> AuthzResult<EncodingKey> {
        validate_alg(key.alg)?;
        let cache_key = cache_key(tenant_id, &key.kid);
        if let Ok(map) = self.encoding.read()
            && let Some(found) = map.get(&cache_key)
        {
            return Ok(found.clone());
        }
        // jsonwebtoken wants PKCS8 DER; build it in memory only.
        let signing_key = SigningKey::from_bytes(&key.private_key);
        let der = signing_key
            .to_pkcs8_der()
            .map_err(|err| AuthzError::Key(format!("encode Ed25519 key: {err}")))?;
        let encoding_key = EncodingKey::from_ed_der(der.as_bytes());
        if let Ok(mut map) = self.encoding.write() {
            map.insert(cache_key, encoding_key.clone());
        }
        Ok(encoding_key)
    }

    pub(super) fn decoding_key(
        &self,
        tenant_id: &TenantId,
        key: &TenantVerificationKey,
    ) -> AuthzResult<DecodingKey> {
        validate_alg(key.alg)?;
        let cache_key = cache_key(tenant_id, &key.kid);
        if let Ok(map) = self.decoding.read()
            && let Some(found) = map.get(&cache_key)
        {
            return Ok(found.clone());
        }
        let x = URL_SAFE_NO_PAD.encode(key.public_key);
        let decoding_key = DecodingKey::from_ed_components(&x).map_err(AuthzError::Jwt)?;
        if let Ok(mut map) = self.decoding.write() {
            map.insert(cache_key, decoding_key.clone());
        }
        Ok(decoding_key)
    }
}

fn cache_key(tenant_id: &TenantId, kid: &str) -> String {
    let tenant = tenant_id.as_str();
    let mut key = String::with_capacity(tenant.len() + 1 + kid.len());
    key.push_str(tenant);
    key.push(':');
    key.push_str(kid);
    key
}

#[cfg(test)]
mod tests;
