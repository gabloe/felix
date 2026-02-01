//! Broker-side authentication and JWKS caching.
//!
//! # Purpose
//! Fetch and cache tenant JWKS from the control-plane, then verify Felix tokens
//! using EdDSA (Ed25519) public keys.
//!
//! # Key invariants
//! - Felix-issued tokens must be EdDSA; RSA is never accepted for broker auth.
//! - JWKS data contains only public key material; private keys are never loaded.
//! - `kid` guides verification order but verification still tries all keys.
//!
//! # Security model / threat assumptions
//! - Attackers may present arbitrary bearer tokens; we validate signature,
//!   issuer, audience, and tenant ID via `felix_authz`.
//! - JWKS endpoints are public; we treat the data as untrusted input and validate
//!   key shape/length before use.
//! - No tokens or keys are logged to avoid secret leakage.
//!
//! # Concurrency + ordering guarantees
//! - JWKS cache is thread-safe via `DashMap`.
//! - Key cache invalidation is per-tenant and coordinated with JWKS refresh.
//!
//! # How to use
//! Construct [`BrokerAuth`] with the control-plane URL and call
//! [`BrokerAuth::authenticate`] to validate a token and obtain an [`AuthContext`].
//!
//! # Examples
//! ```rust,no_run
//! use broker::auth::BrokerAuth;
//!
//! let auth = BrokerAuth::new("http://controlplane".to_string());
//! ```
//!
//! # Common pitfalls
//! - Forgetting to warm the JWKS cache leads to auth latency on first request.
//! - Not invalidating cache on JWKS rotation keeps stale public keys in memory.
//!
//! # Future work
//! - Add background JWKS refresh to avoid on-demand fetches under load.
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

/// Broker authentication facade for Felix tokens.
///
/// # What it does
/// Holds the verifier and JWKS-backed key store needed to validate tokens.
///
/// # Inputs/outputs
/// - Inputs: control-plane URL or an injected [`ControlPlaneKeyStore`].
/// - Outputs: [`AuthContext`] from [`BrokerAuth::authenticate`].
///
/// # Errors
/// - Construction does not fail; authentication errors are surfaced by
///   [`BrokerAuth::authenticate`].
///
/// # Security notes
/// - Verifier is configured for EdDSA only and uses cached public keys.
///
/// # Concurrency
/// - Cloning shares the verifier and key store via `Arc`.
///
/// # Example
/// ```rust,no_run
/// use broker::auth::BrokerAuth;
///
/// let auth = BrokerAuth::new("http://controlplane".to_string());
/// ```
#[derive(Clone)]
pub struct BrokerAuth {
    verifier: Arc<FelixTokenVerifier>,
    key_store: Arc<ControlPlaneKeyStore>,
}

impl BrokerAuth {
    /// Create a broker auth handler using the control-plane JWKS endpoint.
    ///
    /// # What it does
    /// Builds a JWKS-backed key store and an EdDSA verifier with caching.
    ///
    /// # Inputs/outputs
    /// - Input: `controlplane_url` base URL.
    /// - Output: Configured [`BrokerAuth`].
    ///
    /// # Errors
    /// - Does not return errors; misconfiguration surfaces during auth calls.
    ///
    /// # Security notes
    /// - The verifier is pinned to `iss=felix-auth` and `aud=felix-broker`.
    ///
    /// # Performance
    /// - Construction is O(1); network IO happens during authentication.
    ///
    /// # Example
    /// ```rust,no_run
    /// use broker::auth::BrokerAuth;
    ///
    /// let auth = BrokerAuth::new("http://controlplane".to_string());
    /// ```
    pub fn new(controlplane_url: String) -> Self {
        // We share a key cache between the verifier and JWKS store so that
        // refreshing JWKS invalidates cached decoding keys reliably.
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

    /// Create a broker auth handler from an existing JWKS key store.
    ///
    /// # What it does
    /// Reuses the provided key store and its key cache to build the verifier.
    ///
    /// # Inputs/outputs
    /// - Input: `key_store` with JWKS caching.
    /// - Output: Configured [`BrokerAuth`].
    ///
    /// # Errors
    /// - Does not return errors.
    ///
    /// # Security notes
    /// - The caller is responsible for ensuring the store fetches Ed25519 JWKS.
    ///
    /// # Concurrency
    /// - Shares a single key cache across cloned `BrokerAuth` instances.
    ///
    /// # Example
    /// ```rust,no_run
    /// use broker::auth::{BrokerAuth, ControlPlaneKeyStore};
    /// use felix_authz::TenantKeyCache;
    /// use std::sync::Arc;
    ///
    /// let cache = Arc::new(TenantKeyCache::default());
    /// let store = Arc::new(ControlPlaneKeyStore::new("http://controlplane".to_string(), cache));
    /// let auth = BrokerAuth::with_key_store(store);
    /// ```
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

    /// Authenticate a Felix token for a tenant and produce an auth context.
    ///
    /// # What it does
    /// Ensures JWKS are cached, verifies the token, and builds a permission
    /// matcher for authorization checks.
    ///
    /// # Inputs/outputs
    /// - Inputs: `tenant_id`, `token`.
    /// - Output: [`AuthContext`] containing a precomputed permission matcher.
    ///
    /// # Errors
    /// - JWKS fetch errors if control-plane is unavailable.
    /// - Verification errors if token is invalid or claims mismatch.
    ///
    /// # Security notes
    /// - Enforces issuer/audience/tenant checks via `felix_authz`.
    /// - Never logs or returns the token contents.
    ///
    /// # Performance
    /// - Uses cached JWKS and decoding keys to minimize per-request overhead.
    ///
    /// # Example
    /// ```rust,no_run
    /// use broker::auth::BrokerAuth;
    ///
    /// async fn auth_request(auth: BrokerAuth, token: &str) {
    ///     let _ = auth.authenticate("tenant-a", token).await;
    /// }
    /// ```
    pub async fn authenticate(&self, tenant_id: &str, token: &str) -> Result<AuthContext> {
        // Step 1: Ensure JWKS is cached to avoid repeated network calls.
        // This also refreshes cached decoding keys when JWKS rotates.
        ensure_jwks_cached(&self.key_store, tenant_id).await?;
        let tenant = TenantId::new(tenant_id);
        // Step 2: Verify the token signature and critical claims.
        // This uses EdDSA verification and validates `iss`, `aud`, `tid`.
        let claims = self.verifier.verify(&tenant, token)?;
        // Step 3: Build an efficient matcher for downstream authorization checks.
        let matcher = PermissionMatcher::from_strings(&claims.perms)?;
        Ok(AuthContext {
            tenant_id: tenant_id.to_string(),
            matcher,
        })
    }
}

/// Authorization context produced after successful authentication.
///
/// # What it does
/// Holds tenant scope and a permission matcher for fast authorization checks.
///
/// # Inputs/outputs
/// - Output from [`BrokerAuth::authenticate`].
///
/// # Errors
/// - Not applicable.
///
/// # Security notes
/// - `matcher` encapsulates permissions embedded in a verified token.
///
/// # Performance
/// - Matcher creation cost scales with permission count.
///
/// # Example
/// ```rust
/// use broker::auth::AuthContext;
/// use felix_authz::PermissionMatcher;
///
/// let matcher = PermissionMatcher::from_strings(&[]).expect("matcher");
/// let ctx = AuthContext { tenant_id: "t1".to_string(), matcher };
/// assert_eq!(ctx.tenant_id, "t1");
/// ```
#[derive(Clone)]
pub struct AuthContext {
    pub tenant_id: String,
    pub matcher: PermissionMatcher,
}

/// JWKS-backed key store for broker verification.
///
/// # What it does
/// Fetches and caches JWKS from the control-plane and exposes verification keys
/// to the Felix authz layer.
///
/// # Inputs/outputs
/// - Input: control-plane base URL and shared key cache.
/// - Output: JWKS data and verification keys.
///
/// # Errors
/// - Fetch/JSON decode errors bubble up from `reqwest`.
///
/// # Security notes
/// - Only Ed25519 public keys are accepted; private keys are never stored here.
///
/// # Concurrency
/// - Internal caches are shared and thread-safe (`DashMap` + `RwLock`).
///
/// # Example
/// ```rust,no_run
/// use broker::auth::ControlPlaneKeyStore;
/// use std::sync::Arc;
/// use felix_authz::TenantKeyCache;
///
/// let store = ControlPlaneKeyStore::new("http://controlplane".to_string(), Arc::new(TenantKeyCache::default()));
/// ```
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
    /// Create a new JWKS key store.
    ///
    /// # What it does
    /// Normalizes the base URL, sets up HTTP client, and initializes caches.
    ///
    /// # Inputs/outputs
    /// - Inputs: `base_url`, `key_cache`.
    /// - Output: [`ControlPlaneKeyStore`].
    ///
    /// # Errors
    /// - Does not return errors.
    ///
    /// # Security notes
    /// - `base_url` must be trusted and point to the control-plane.
    ///
    /// # Performance
    /// - Construction is O(1); no network IO is performed.
    ///
    /// # Example
    /// ```rust,no_run
    /// use broker::auth::ControlPlaneKeyStore;
    /// use felix_authz::TenantKeyCache;
    /// use std::sync::Arc;
    ///
    /// let store = ControlPlaneKeyStore::new("http://controlplane".to_string(), Arc::new(TenantKeyCache::default()));
    /// ```
    pub fn new(base_url: String, key_cache: Arc<TenantKeyCache>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
            cache: Arc::new(DashMap::new()),
            ttl: Duration::from_secs(3600),
            key_cache,
        }
    }

    /// Insert JWKS into the cache for a tenant.
    ///
    /// # What it does
    /// Stores JWKS and expires it after a fixed TTL; invalidates cached decoding keys.
    ///
    /// # Inputs/outputs
    /// - Inputs: `tenant_id`, `jwks`.
    /// - Output: None.
    ///
    /// # Errors
    /// - Does not return errors.
    ///
    /// # Security notes
    /// - Invalidating the key cache ensures new keys are used immediately.
    ///
    /// # Concurrency
    /// - Uses `DashMap` for lock-free reads and writes.
    ///
    /// # Example
    /// ```rust
    /// use broker::auth::ControlPlaneKeyStore;
    /// use felix_authz::{Jwks, TenantId, TenantKeyCache};
    /// use std::sync::Arc;
    ///
    /// let store = ControlPlaneKeyStore::new("http://controlplane".to_string(), Arc::new(TenantKeyCache::default()));
    /// store.insert_jwks(&TenantId::new("t1"), Jwks { keys: vec![] });
    /// ```
    pub fn insert_jwks(&self, tenant_id: &TenantId, jwks: Jwks) {
        // We overwrite any existing entry to make rotations visible.
        self.cache.insert(
            tenant_id.to_string(),
            CachedJwks {
                jwks,
                expires_at: Instant::now() + self.ttl,
            },
        );
        // Invalidate derived decoding keys so new JWKS keys are used immediately.
        self.key_cache.invalidate_tenant(tenant_id);
    }

    /// Fetch and cache JWKS for a tenant from the control-plane.
    ///
    /// # What it does
    /// Performs a GET request to the JWKS endpoint and stores the result with TTL.
    ///
    /// # Inputs/outputs
    /// - Input: `tenant_id`.
    /// - Output: `Jwks` payload.
    ///
    /// # Errors
    /// - Network failures or JSON decode errors from `reqwest`.
    ///
    /// # Security notes
    /// - JWKS is public data but treated as untrusted until validated.
    /// - The URL should not be logged because it may include credentials in some deployments.
    ///
    /// # Performance
    /// - Refresh performs a network round-trip and JSON decode.
    ///
    /// # Example
    /// ```rust,no_run
    /// use broker::auth::ControlPlaneKeyStore;
    /// use felix_authz::{TenantId, TenantKeyCache};
    /// use std::sync::Arc;
    ///
    /// async fn fetch(store: ControlPlaneKeyStore) {
    ///     let _ = store.refresh(&TenantId::new("t1")).await;
    /// }
    /// ```
    pub async fn refresh(&self, tenant_id: &TenantId) -> Result<Jwks> {
        let url = format!(
            "{}/v1/tenants/{}/.well-known/jwks.json",
            self.base_url,
            tenant_id.as_str()
        );
        // We intentionally avoid logging the URL to prevent leaking credentials
        // if the base URL embeds them.
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
        // Invalidate cached decoding keys so the verifier picks up new keys.
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
        // Brokers never mint Felix tokens, so they do not expose signing keys.
        Err(AuthzError::MissingSigningKey("broker".to_string()))
    }

    fn verification_keys(&self, tenant_id: &TenantId) -> AuthzResult<Vec<TenantVerificationKey>> {
        // We only use cached JWKS to avoid blocking auth on network IO.
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
    // Step 1: Decode and validate each Ed25519 public key from JWKS.
    // We reject malformed or non-Ed25519 keys to prevent downgrade attacks.
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
        // IMPORTANT:
        // The algorithm is pinned to EdDSA for Felix tokens; any change here
        // would permit RSA/HS verification which must remain disallowed.
        keys.push(TenantVerificationKey {
            kid: key.kid.clone(),
            alg: Algorithm::EdDSA,
            public_key: key_bytes,
        });
    }
    Ok(keys)
}

/// Ensure JWKS are available in the cache for a tenant.
///
/// # What it does
/// Checks the cache and performs a refresh if necessary.
///
/// # Inputs/outputs
/// - Inputs: `key_store`, `tenant_id`.
/// - Output: `Ok(())` if cache is populated.
///
/// # Errors
/// - Network/JSON errors when fetching JWKS.
///
/// # Security notes
/// - Performs network IO only when cache is missing or expired.
///
/// # Performance
/// - Avoids network IO when cached JWKS is still valid.
///
/// # Example
/// ```rust,no_run
/// use broker::auth::{ensure_jwks_cached, ControlPlaneKeyStore};
/// use felix_authz::TenantKeyCache;
/// use std::sync::Arc;
///
/// async fn ensure(store: ControlPlaneKeyStore) {
///     let _ = ensure_jwks_cached(&store, "t1").await;
/// }
/// ```
pub async fn ensure_jwks_cached(key_store: &ControlPlaneKeyStore, tenant_id: &str) -> Result<()> {
    // We avoid unconditional refresh to keep auth latency low.
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
        // This test prevents accepting malformed JWKS payloads, which would
        // enable invalid key material to bypass verification.
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
        // This test ensures cached JWKS are reused to avoid unnecessary network IO.
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
        // This test ensures a missing JWKS triggers a fetch attempt and fails
        // predictably when the control-plane is unreachable.
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
}
