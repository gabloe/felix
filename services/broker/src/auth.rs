use anyhow::{Context, Result};
use dashmap::DashMap;
use felix_authz::{
    AuthzError, AuthzResult, FelixTokenVerifier, Jwks, PermissionMatcher, TenantId, TenantKeyStore,
    TenantVerificationKey,
};
use jsonwebtoken::{Algorithm, DecodingKey};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct BrokerAuth {
    verifier: Arc<FelixTokenVerifier>,
    key_store: Arc<ControlPlaneKeyStore>,
}

impl BrokerAuth {
    pub fn new(controlplane_url: String) -> Self {
        let key_store = Arc::new(ControlPlaneKeyStore::new(controlplane_url));
        let verifier = Arc::new(FelixTokenVerifier::new(
            "felix-auth",
            "felix-broker",
            60,
            key_store.clone(),
        ));
        Self {
            verifier,
            key_store,
        }
    }

    pub fn with_key_store(key_store: Arc<ControlPlaneKeyStore>) -> Self {
        let verifier = Arc::new(FelixTokenVerifier::new(
            "felix-auth",
            "felix-broker",
            60,
            key_store.clone(),
        ));
        Self {
            verifier,
            key_store,
        }
    }

    pub async fn authenticate(&self, tenant_id: &str, token: &str) -> Result<AuthContext> {
        ensure_jwks_cached(&self.key_store, tenant_id).await?;
        let tenant = TenantId::new(tenant_id);
        let claims = self.verifier.verify(&tenant, token)?;
        let matcher = PermissionMatcher::from_strings(&claims.perms)?;
        Ok(AuthContext {
            tenant_id: tenant_id.to_string(),
            matcher,
        })
    }
}

#[derive(Clone)]
pub struct AuthContext {
    pub tenant_id: String,
    pub matcher: PermissionMatcher,
}

#[derive(Clone)]
pub struct ControlPlaneKeyStore {
    base_url: String,
    client: reqwest::Client,
    cache: Arc<DashMap<String, CachedJwks>>,
    ttl: Duration,
}

#[derive(Clone)]
struct CachedJwks {
    jwks: Jwks,
    expires_at: Instant,
}

impl ControlPlaneKeyStore {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
            cache: Arc::new(DashMap::new()),
            ttl: Duration::from_secs(3600),
        }
    }

    pub fn insert_jwks(&self, tenant_id: &TenantId, jwks: Jwks) {
        self.cache.insert(
            tenant_id.to_string(),
            CachedJwks {
                jwks,
                expires_at: Instant::now() + self.ttl,
            },
        );
    }

    pub async fn refresh(&self, tenant_id: &TenantId) -> Result<Jwks> {
        let url = format!(
            "{}/v1/tenants/{}/.well-known/jwks.json",
            self.base_url,
            tenant_id.as_str()
        );
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
        Err(AuthzError::MissingSigningKey("broker".to_string()))
    }

    fn verification_keys(&self, tenant_id: &TenantId) -> AuthzResult<Vec<TenantVerificationKey>> {
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
        let decoding_key =
            DecodingKey::from_rsa_components(&key.n, &key.e).map_err(AuthzError::Jwt)?;
        keys.push(TenantVerificationKey {
            kid: key.kid.clone(),
            alg: Algorithm::RS256,
            decoding_key,
        });
    }
    Ok(keys)
}

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
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    #[test]
    fn jwks_to_keys_rejects_invalid_components() {
        let jwks = Jwks {
            keys: vec![felix_authz::Jwk {
                kty: "RSA".to_string(),
                kid: "k1".to_string(),
                alg: "RS256".to_string(),
                use_field: felix_authz::KeyUse::Sig,
                n: "not-base64".to_string(),
                e: "AQAB".to_string(),
            }],
        };
        let err = match jwks_to_keys(&jwks) {
            Ok(_) => panic!("expected error"),
            Err(err) => err,
        };
        match err {
            AuthzError::Jwt(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn ensure_jwks_cached_uses_existing_cache() -> Result<()> {
        let key_store = ControlPlaneKeyStore::new("http://127.0.0.1:1".to_string());
        let tenant = TenantId::new("t1");
        let n = URL_SAFE_NO_PAD.encode(vec![1, 2, 3]);
        let e = URL_SAFE_NO_PAD.encode(vec![1, 0, 1]);
        key_store.insert_jwks(
            &tenant,
            Jwks {
                keys: vec![felix_authz::Jwk {
                    kty: "RSA".to_string(),
                    kid: "k1".to_string(),
                    alg: "RS256".to_string(),
                    use_field: felix_authz::KeyUse::Sig,
                    n,
                    e,
                }],
            },
        );
        ensure_jwks_cached(&key_store, "t1").await?;
        Ok(())
    }

    #[tokio::test]
    async fn ensure_jwks_cached_fetch_fails_when_missing() {
        let key_store = ControlPlaneKeyStore::new("http://127.0.0.1:1".to_string());
        let result = ensure_jwks_cached(&key_store, "t1").await;
        assert!(result.is_err());
    }
}
