//! Felix token minting and verification, shared across the broker and the
//! control plane.
//!
//! EdDSA (Ed25519) only — RSA and HS algorithms are rejected rather than
//! supported, because the caller supplies the token and accepting a second
//! algorithm means accepting whichever is weakest. `iss`, `aud`, `tid` and the
//! signature are all checked before a token is trusted.
//!
//! Private keys never leave this module serialized; PKCS8 DER is built in
//! memory only to hand to `jsonwebtoken`. `kid` drives rotation and cache
//! lookup and is not a secret.
//!
//! [`FelixTokenIssuer`] mints; [`FelixTokenVerifier`] verifies; both need a
//! [`TenantKeyStore`] for key access.
//!
//! ```rust
//! use felix_authz::{FelixTokenIssuer, TenantId, TenantKeyMaterial, Jwks};
//! use jsonwebtoken::Algorithm;
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! let mut store = HashMap::new();
//! store.insert("t1".to_string(), TenantKeyMaterial {
//!     kid: "k1".to_string(),
//!     alg: Algorithm::EdDSA,
//!     private_key: [1u8; 32],
//!     public_key: [2u8; 32],
//!     jwks: Jwks { keys: vec![] },
//! });
//! let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", Duration::from_secs(60), Arc::new(store));
//! let _ = issuer.mint(&TenantId::new("t1"), "principal", vec![]);
//! ```
use crate::{AuthzError, AuthzResult, Jwks, TenantId};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ED25519_KEY_LEN: usize = 32;

/// Claims carried by Felix-issued JWTs. `tid` and `perms` are what the broker
/// enforces with, so both are checked, never trusted as-is.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FelixClaims {
    pub iss: String,
    pub aud: String,
    pub sub: String,
    pub tid: String,
    pub exp: i64,
    pub iat: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jti: Option<String>,
    pub perms: Vec<String>,
}

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

/// Mints Felix JWTs. Issuer/audience/TTL are fixed at construction and must
/// match the verifier's configuration.
pub struct FelixTokenIssuer {
    issuer: String,
    audience: String,
    ttl: Duration,
    key_store: Arc<dyn TenantKeyStore>,
    cache: Arc<TenantKeyCache>,
}

impl FelixTokenIssuer {
    pub fn new(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        ttl: Duration,
        key_store: Arc<dyn TenantKeyStore>,
    ) -> Self {
        Self {
            issuer: issuer.into(),
            audience: audience.into(),
            ttl,
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

    /// Mint a token for a tenant/principal with the given permissions.
    /// Treat the result as a secret; don't log it.
    ///
    /// # Errors
    /// Key-store lookup failures and JWT encoding errors.
    pub fn mint(
        &self,
        tenant_id: &TenantId,
        principal_id: &str,
        perms: Vec<String>,
    ) -> AuthzResult<String> {
        let now = now_epoch_seconds();
        let exp = now + self.ttl.as_secs() as i64;
        let claims = FelixClaims {
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            sub: principal_id.to_string(),
            tid: tenant_id.to_string(),
            exp,
            iat: now,
            jti: None,
            perms,
        };
        let signing_key = self.key_store.current_signing_key(tenant_id)?;
        let encoding_key = self.cache.encoding_key(tenant_id, &signing_key)?;
        let mut header = Header::new(signing_key.alg);
        header.kid = Some(signing_key.kid);
        let token = jsonwebtoken::encode(&header, &claims, &encoding_key)?;
        Ok(token)
    }
}

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

fn now_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}

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

    fn encoding_key(
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

    fn decoding_key(
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

fn validate_alg(alg: Algorithm) -> AuthzResult<()> {
    if alg == Algorithm::EdDSA {
        Ok(())
    } else {
        Err(AuthzError::Key(format!(
            "invalid Felix signing algorithm: {alg:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jwks::Jwk;

    const TEST_PRIVATE_KEY: [u8; 32] = [7u8; 32];

    fn test_key_material() -> TenantKeyMaterial {
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
                    use_field: crate::jwks::KeyUse::Sig,
                    crv: Some("Ed25519".to_string()),
                    x: Some("test".to_string()),
                }],
            },
        }
    }

    fn test_key_store() -> Arc<dyn TenantKeyStore> {
        let mut keys = HashMap::new();
        keys.insert("tenant-a".to_string(), test_key_material());
        Arc::new(keys)
    }

    // Mirrors the production PKCS8 conversion so hand-built tokens verify.
    fn encoding_key_from_seed(seed: &[u8; 32]) -> EncodingKey {
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

    #[test]
    fn mint_fails_without_signing_key() {
        let key_store: Arc<dyn TenantKeyStore> = Arc::new(HashMap::new());
        let issuer = FelixTokenIssuer::new(
            "felix-auth",
            "felix-broker",
            Duration::from_secs(600),
            key_store.clone(),
        );
        let err = issuer
            .mint(&TenantId::new("tenant-missing"), "principal", vec![])
            .expect_err("missing signing key");
        assert!(matches!(err, AuthzError::MissingSigningKey(_)));
    }

    #[test]
    fn verify_fails_without_verification_keys() {
        let key_store = test_key_store();
        let issuer = FelixTokenIssuer::new(
            "felix-auth",
            "felix-broker",
            Duration::from_secs(600),
            key_store.clone(),
        );
        let token = issuer
            .mint(&TenantId::new("tenant-a"), "principal", vec![])
            .expect("token mint");

        let empty_store: Arc<dyn TenantKeyStore> = Arc::new(HashMap::new());
        let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, empty_store);
        let err = verifier
            .verify(&TenantId::new("tenant-a"), &token)
            .expect_err("missing verification keys");
        assert!(matches!(err, AuthzError::MissingVerificationKeys(_)));
    }

    #[test]
    fn verify_fails_on_tenant_mismatch() {
        let mut keys = HashMap::new();
        let material = test_key_material();
        keys.insert("tenant-a".to_string(), material.clone());
        keys.insert("tenant-b".to_string(), material);
        let key_store: Arc<dyn TenantKeyStore> = Arc::new(keys);

        let issuer = FelixTokenIssuer::new(
            "felix-auth",
            "felix-broker",
            Duration::from_secs(600),
            key_store.clone(),
        );
        let token = issuer
            .mint(&TenantId::new("tenant-a"), "principal", vec![])
            .expect("token mint");

        let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store);
        let err = verifier
            .verify(&TenantId::new("tenant-b"), &token)
            .expect_err("tenant mismatch");
        assert!(matches!(err, AuthzError::TenantMismatch { .. }));
    }

    #[test]
    fn verify_uses_first_key_when_no_kid() {
        let key_store = test_key_store();
        let signing = key_store
            .current_signing_key(&TenantId::new("tenant-a"))
            .expect("signing key");

        let claims = FelixClaims {
            iss: "felix-auth".to_string(),
            aud: "felix-broker".to_string(),
            sub: "principal".to_string(),
            tid: "tenant-a".to_string(),
            exp: now_epoch_seconds() + 600,
            iat: now_epoch_seconds(),
            jti: None,
            perms: vec![],
        };
        let header = Header::new(signing.alg);
        let encoding_key = encoding_key_from_seed(&signing.private_key);
        let token = jsonwebtoken::encode(&header, &claims, &encoding_key).expect("encode token");

        let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store);
        let verified = verifier
            .verify(&TenantId::new("tenant-a"), &token)
            .expect("verify token");
        assert_eq!(verified.sub, "principal");
    }

    #[test]
    fn verify_succeeds_when_kid_unknown() {
        let key_store = test_key_store();
        let signing = key_store
            .current_signing_key(&TenantId::new("tenant-a"))
            .expect("signing key");

        let claims = FelixClaims {
            iss: "felix-auth".to_string(),
            aud: "felix-broker".to_string(),
            sub: "principal".to_string(),
            tid: "tenant-a".to_string(),
            exp: now_epoch_seconds() + 600,
            iat: now_epoch_seconds(),
            jti: None,
            perms: vec![],
        };
        let mut header = Header::new(signing.alg);
        header.kid = Some("unknown".to_string());
        let encoding_key = encoding_key_from_seed(&signing.private_key);
        let token = jsonwebtoken::encode(&header, &claims, &encoding_key).expect("encode token");

        let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store);
        let verified = verifier
            .verify(&TenantId::new("tenant-a"), &token)
            .expect("verify token");
        assert_eq!(verified.sub, "principal");
    }

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

    #[test]
    fn tenant_key_cache_invalidate_tenant_evicts_cached_entries() {
        let key_store = test_key_store();
        let cache = TenantKeyCache::default();
        let tenant = TenantId::new("tenant-a");
        let signing = key_store.current_signing_key(&tenant).expect("signing key");
        let verify = key_store
            .verification_keys(&tenant)
            .expect("verification keys")
            .into_iter()
            .next()
            .expect("verification key");

        let _first_encoding = cache.encoding_key(&tenant, &signing).expect("encoding key");
        let _first_decoding = cache.decoding_key(&tenant, &verify).expect("decoding key");
        let _second_encoding = cache.encoding_key(&tenant, &signing).expect("encoding key");
        let _second_decoding = cache.decoding_key(&tenant, &verify).expect("decoding key");

        cache.invalidate_tenant(&tenant);
        let _refreshed_encoding = cache.encoding_key(&tenant, &signing).expect("encoding key");
        let _refreshed_decoding = cache.decoding_key(&tenant, &verify).expect("decoding key");
    }

    #[test]
    fn cache_key_and_validate_alg_helpers() {
        let tenant = TenantId::new("tenant-a");
        assert_eq!(cache_key(&tenant, "k9"), "tenant-a:k9");
        assert!(validate_alg(Algorithm::EdDSA).is_ok());
        let err = validate_alg(Algorithm::HS256).expect_err("unsupported alg");
        assert!(matches!(err, AuthzError::Key(_)));
    }
}
