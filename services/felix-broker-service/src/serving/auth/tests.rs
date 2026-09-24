use axum::{Json, Router, routing::get};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::json;
use tokio::net::TcpListener;

use super::*;

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
