//! Broker-side authentication: JWKS caching and Felix token verification.
//!
//! Felix tokens are EdDSA (Ed25519); RSA is never accepted here. JWKS fetched
//! from the control plane carries public key material only, and is treated as
//! untrusted input — key shape and length are checked before use. `kid` guides
//! which key is tried first, but verification still tries all of them, so a
//! rotation in progress does not reject valid tokens.
//!
//! The JWKS cache is a `DashMap`; invalidation is per-tenant and coordinated
//! with refresh. Nothing here logs a token or a key.
//!
//! Construct [`BrokerAuth`] with the control-plane URL and call
//! [`BrokerAuth::authenticate`] to get an [`AuthContext`].
use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use dashmap::DashMap;
use felix_authz::{
    AuthzError, AuthzResult, FelixTokenVerifier, Jwks, PermissionMatcher, TenantId, TenantKeyCache,
    TenantKeyStore, TenantVerificationKey,
};
use jsonwebtoken::Algorithm;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Verifies Felix tokens against control-plane JWKS. Cloning shares the
/// verifier and key store.
#[derive(Clone)]
pub struct BrokerAuth {
    verifier: Arc<FelixTokenVerifier>,
    key_store: Arc<ControlPlaneKeyStore>,
}

impl BrokerAuth {
    pub fn new(controlplane_url: String) -> Self {
        // The verifier and the JWKS store share one key cache, so a JWKS
        // refresh invalidates the derived decoding keys with it.
        let key_cache = Arc::new(TenantKeyCache::default());
        let key_store = Arc::new(ControlPlaneKeyStore::new(
            controlplane_url,
            key_cache.clone(),
        ));
        let verifier = Arc::new(
            FelixTokenVerifier::new("felix-auth", "felix-broker", 60, key_store.clone())
                .with_cache(key_cache),
        );
        Self {
            verifier,
            key_store,
        }
    }

    /// Build from an existing key store, reusing its key cache.
    pub fn with_key_store(key_store: Arc<ControlPlaneKeyStore>) -> Self {
        let key_cache = key_store.key_cache.clone();
        let verifier = Arc::new(
            FelixTokenVerifier::new("felix-auth", "felix-broker", 60, key_store.clone())
                .with_cache(key_cache),
        );
        Self {
            verifier,
            key_store,
        }
    }

    /// Verify a token for a tenant and return an [`AuthContext`] with a
    /// precomputed permission matcher. Fetches JWKS if not already cached.
    ///
    /// # Errors
    /// JWKS fetch failures (control plane unreachable) and token verification
    /// failures.
    pub async fn authenticate(&self, tenant_id: &str, token: &str) -> Result<AuthContext> {
        ensure_jwks_cached(&self.key_store, tenant_id).await?;
        let tenant = TenantId::new(tenant_id);
        let claims = self.verifier.verify(&tenant, token)?;
        let matcher = PermissionMatcher::from_strings(&claims.perms)?;
        Ok(AuthContext {
            tenant_id: tenant_id.to_string(),
            matcher,
            token: token.to_string(),
        })
    }
}

/// Tenant scope plus the permission matcher from a verified token.
#[derive(Clone)]
pub struct AuthContext {
    pub tenant_id: String,
    pub matcher: PermissionMatcher,
    /// The token itself, kept so a request this broker forwards carries it
    /// and the owner can verify it again. Never logged.
    pub token: String,
}

/// Fetches and caches per-tenant JWKS from the control plane, and exposes the
/// verification keys to `felix_authz`. Holds public keys only.
#[derive(Clone)]
pub struct ControlPlaneKeyStore {
    base_url: String,
    client: reqwest::Client,
    cache: Arc<DashMap<String, CachedJwks>>,
    ttl: Duration,
    key_cache: Arc<TenantKeyCache>,
}

#[derive(Clone)]
struct CachedJwks {
    jwks: Jwks,
    expires_at: Instant,
}

impl ControlPlaneKeyStore {
    pub fn new(base_url: String, key_cache: Arc<TenantKeyCache>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
            cache: Arc::new(DashMap::new()),
            ttl: Duration::from_secs(3600),
            key_cache,
        }
    }

    /// Cache JWKS for a tenant, replacing any existing entry and invalidating
    /// the derived decoding keys so a rotation takes effect immediately.
    pub fn insert_jwks(&self, tenant_id: &TenantId, jwks: Jwks) {
        self.cache.insert(
            tenant_id.to_string(),
            CachedJwks {
                jwks,
                expires_at: Instant::now() + self.ttl,
            },
        );
        self.key_cache.invalidate_tenant(tenant_id);
    }

    /// Fetch JWKS from the control plane and cache it.
    ///
    /// # Errors
    /// Network or JSON-decode failures.
    pub async fn refresh(&self, tenant_id: &TenantId) -> Result<Jwks> {
        let url = format!(
            "{}/v1/tenants/{}/.well-known/jwks.json",
            self.base_url,
            tenant_id.as_str()
        );
        // The URL is deliberately not logged: a deployment may embed
        // credentials in the base URL.
        let jwks: Jwks = self
            .client
            .get(url)
            .send()
            .await
            .context("fetch jwks")?
            .json()
            .await
            .context("decode jwks")?;
        self.cache.insert(
            tenant_id.to_string(),
            CachedJwks {
                jwks: jwks.clone(),
                expires_at: Instant::now() + self.ttl,
            },
        );
        self.key_cache.invalidate_tenant(tenant_id);
        Ok(jwks)
    }

    fn cached_jwks(&self, tenant_id: &TenantId) -> Option<Jwks> {
        self.cache.get(tenant_id.as_str()).and_then(|entry| {
            if entry.expires_at > Instant::now() {
                Some(entry.jwks.clone())
            } else {
                None
            }
        })
    }
}

impl TenantKeyStore for ControlPlaneKeyStore {
    fn current_signing_key(
        &self,
        _tenant_id: &TenantId,
    ) -> AuthzResult<felix_authz::TenantSigningKey> {
        // Brokers never mint Felix tokens, so there is no signing key to give.
        Err(AuthzError::MissingSigningKey("broker".to_string()))
    }

    fn verification_keys(&self, tenant_id: &TenantId) -> AuthzResult<Vec<TenantVerificationKey>> {
        // Cache only — this runs on the auth path and must not block on
        // network IO. `ensure_jwks_cached` fills the cache beforehand.
        let jwks = self
            .cached_jwks(tenant_id)
            .ok_or_else(|| AuthzError::MissingJwks(tenant_id.to_string()))?;
        jwks_to_keys(&jwks)
    }

    fn jwks(&self, tenant_id: &TenantId) -> AuthzResult<Jwks> {
        self.cached_jwks(tenant_id)
            .ok_or_else(|| AuthzError::MissingJwks(tenant_id.to_string()))
    }
}

fn jwks_to_keys(jwks: &Jwks) -> AuthzResult<Vec<TenantVerificationKey>> {
    let mut keys = Vec::new();
    for key in &jwks.keys {
        let public_key = key
            .x
            .as_ref()
            .ok_or_else(|| AuthzError::Key("missing jwk x".to_string()))?;
        let decoded = URL_SAFE_NO_PAD
            .decode(public_key.as_bytes())
            .map_err(|err| AuthzError::Key(format!("invalid jwk x: {err}")))?;
        let key_bytes: [u8; 32] = decoded
            .as_slice()
            .try_into()
            .map_err(|_| AuthzError::Key("invalid Ed25519 public key length".to_string()))?;
        // The algorithm is pinned to EdDSA regardless of what the JWKS claims;
        // honoring a JWKS-supplied alg would reopen the RSA/HS downgrade.
        keys.push(TenantVerificationKey {
            kid: key.kid.clone(),
            alg: Algorithm::EdDSA,
            public_key: key_bytes,
        });
    }
    Ok(keys)
}

/// Fetch JWKS for a tenant only if the cache is missing or expired.
///
/// # Errors
/// Network or decode failures from the fetch.
pub async fn ensure_jwks_cached(key_store: &ControlPlaneKeyStore, tenant_id: &str) -> Result<()> {
    let tenant = TenantId::new(tenant_id);
    if key_store.cached_jwks(&tenant).is_none() {
        key_store.refresh(&tenant).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::get};
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use serde_json::json;
    use tokio::net::TcpListener;

    #[test]
    fn jwks_to_keys_rejects_invalid_components() {
        let jwks = Jwks {
            keys: vec![felix_authz::Jwk {
                kty: "OKP".to_string(),
                kid: "k1".to_string(),
                alg: "EdDSA".to_string(),
                use_field: felix_authz::KeyUse::Sig,
                crv: Some("Ed25519".to_string()),
                x: Some("not-base64".to_string()),
            }],
        };
        assert!(matches!(jwks_to_keys(&jwks), Err(AuthzError::Key(_))));
    }

    #[test]
    fn jwks_to_keys_accepts_valid_ed25519_key() {
        let x = URL_SAFE_NO_PAD.encode(vec![9u8; 32]);
        let jwks = Jwks {
            keys: vec![felix_authz::Jwk {
                kty: "OKP".to_string(),
                kid: "k1".to_string(),
                alg: "EdDSA".to_string(),
                use_field: felix_authz::KeyUse::Sig,
                crv: Some("Ed25519".to_string()),
                x: Some(x),
            }],
        };
        let keys = jwks_to_keys(&jwks).expect("jwks to keys");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].kid, "k1");
        assert_eq!(keys[0].alg, Algorithm::EdDSA);
        assert_eq!(keys[0].public_key, [9u8; 32]);
    }

    #[tokio::test]
    async fn ensure_jwks_cached_refreshes_when_expired() -> Result<()> {
        let x = URL_SAFE_NO_PAD.encode(vec![7u8; 32]);
        let jwks_body = json!({
            "keys": [{
                "kty": "OKP",
                "kid": "k1",
                "alg": "EdDSA",
                "use": "sig",
                "crv": "Ed25519",
                "x": x,
            }]
        });

        let app = Router::new().route(
            "/v1/tenants/t1/.well-known/jwks.json",
            get(move || async move { Json(jwks_body.clone()) }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server =
            tokio::spawn(async move { axum::serve(listener, app).await.context("serve jwks") });

        let key_store = ControlPlaneKeyStore::new(
            format!("http://{addr}"),
            Arc::new(TenantKeyCache::default()),
        );
        let tenant = TenantId::new("t1");
        key_store.cache.insert(
            tenant.to_string(),
            CachedJwks {
                jwks: Jwks { keys: vec![] },
                expires_at: Instant::now() - Duration::from_secs(1),
            },
        );

        ensure_jwks_cached(&key_store, "t1").await?;
        let cached = key_store.cached_jwks(&tenant).expect("cached jwks");
        assert_eq!(cached.keys.len(), 1);

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn ensure_jwks_cached_uses_existing_cache() -> Result<()> {
        let key_store = ControlPlaneKeyStore::new(
            "http://127.0.0.1:1".to_string(),
            Arc::new(TenantKeyCache::default()),
        );
        let tenant = TenantId::new("t1");
        let x = URL_SAFE_NO_PAD.encode(vec![1; 32]);
        key_store.insert_jwks(
            &tenant,
            Jwks {
                keys: vec![felix_authz::Jwk {
                    kty: "OKP".to_string(),
                    kid: "k1".to_string(),
                    alg: "EdDSA".to_string(),
                    use_field: felix_authz::KeyUse::Sig,
                    crv: Some("Ed25519".to_string()),
                    x: Some(x),
                }],
            },
        );
        ensure_jwks_cached(&key_store, "t1").await?;
        Ok(())
    }

    #[tokio::test]
    async fn ensure_jwks_cached_fetch_fails_when_missing() {
        let key_store = ControlPlaneKeyStore::new(
            "http://127.0.0.1:1".to_string(),
            Arc::new(TenantKeyCache::default()),
        );
        let result = ensure_jwks_cached(&key_store, "t1").await;
        assert!(result.is_err());
    }

    #[test]
    fn verification_keys_missing_jwks_returns_error() {
        let key_store = ControlPlaneKeyStore::new(
            "http://127.0.0.1:1".to_string(),
            Arc::new(TenantKeyCache::default()),
        );
        let tenant = TenantId::new("t1");
        assert!(matches!(
            key_store.verification_keys(&tenant),
            Err(AuthzError::MissingJwks(_))
        ));
    }

    #[test]
    fn jwks_missing_returns_error() {
        let key_store = ControlPlaneKeyStore::new(
            "http://127.0.0.1:1".to_string(),
            Arc::new(TenantKeyCache::default()),
        );
        let tenant = TenantId::new("t1");
        assert!(matches!(
            key_store.jwks(&tenant),
            Err(AuthzError::MissingJwks(_))
        ));
    }

    #[test]
    fn cached_jwks_expires_after_ttl() {
        let key_store = ControlPlaneKeyStore::new(
            "http://127.0.0.1:1".to_string(),
            Arc::new(TenantKeyCache::default()),
        );
        let tenant = TenantId::new("t1");
        key_store.cache.insert(
            tenant.to_string(),
            CachedJwks {
                jwks: Jwks { keys: vec![] },
                expires_at: Instant::now() - Duration::from_secs(5),
            },
        );
        assert!(key_store.cached_jwks(&tenant).is_none());
    }

    #[test]
    fn current_signing_key_is_unavailable_for_broker() {
        let key_store = ControlPlaneKeyStore::new(
            "http://127.0.0.1:1".to_string(),
            Arc::new(TenantKeyCache::default()),
        );
        let tenant = TenantId::new("t1");
        assert!(matches!(
            key_store.current_signing_key(&tenant),
            Err(AuthzError::MissingSigningKey(_))
        ));
    }

    #[test]
    fn broker_auth_new_builds_shared_store_and_normalizes_url() {
        let auth = BrokerAuth::new("http://controlplane/".to_string());
        assert_eq!(auth.key_store.base_url, "http://controlplane");

        let clone = auth.clone();
        assert!(Arc::ptr_eq(&auth.key_store, &clone.key_store));
        assert!(Arc::ptr_eq(&auth.verifier, &clone.verifier));
    }
}
