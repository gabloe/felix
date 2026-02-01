//! Integration test for broker JWKS refresh and authentication flow.
//!
//! # Purpose
//! Ensure the broker refreshes JWKS from the control-plane and successfully
//! authenticates a Felix EdDSA token.
//!
//! # Key invariants
//! - Felix tokens are EdDSA and validated against Ed25519 JWKS.
//! - JWKS refresh invalidates cached decoding keys.
//!
//! # Security model / threat assumptions
//! - Test uses local JWKS server with public keys only.
//! - Tokens and private keys are test fixtures and must not be logged.
//!
//! # Concurrency + ordering guarantees
//! - Local JWKS server binds to 127.0.0.1:0 for deterministic, isolated tests.
//! - Async ordering is controlled by awaiting server bind before use.
//!
//! # How to use
//! Run with `cargo test -p broker auth_jwks_refresh` to execute this test.
use anyhow::Result;
use axum::{Json, Router, routing::get};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use broker::auth::{BrokerAuth, ControlPlaneKeyStore, ensure_jwks_cached};
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
    TenantKeyStore,
};
use jsonwebtoken::Algorithm;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

const TEST_PRIVATE_KEY: [u8; 32] = [8u8; 32];

fn jwks_from_public_key(public_key: &[u8], kid: &str) -> Jwks {
    // Public key is base64url encoded into JWK `x` per RFC 8037.
    let x = URL_SAFE_NO_PAD.encode(public_key);
    Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    }
}

async fn serve_jwks(jwks: Jwks) -> SocketAddr {
    // Deterministic local JWKS server prevents network flakiness in CI.
    let app = Router::new().route(
        "/v1/tenants/t1/.well-known/jwks.json",
        get(move || {
            let jwks = jwks.clone();
            async move { Json(jwks) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });
    addr
}

#[tokio::test]
async fn jwks_refresh_and_authenticate() -> Result<()> {
    // This test ensures the broker refreshes JWKS and accepts EdDSA tokens.
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let jwks = jwks_from_public_key(&public_key, "k1");
    let addr = serve_jwks(jwks).await;

    let key_store = ControlPlaneKeyStore::new(
        format!("http://{addr}"),
        Arc::new(TenantKeyCache::default()),
    );
    ensure_jwks_cached(&key_store, "t1").await?;
    let keys = key_store.verification_keys(&TenantId::new("t1"))?;
    assert_eq!(keys.len(), 1);

    let key_material = TenantKeyMaterial {
        kid: "k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key: TEST_PRIVATE_KEY,
        public_key,
        jwks: jwks_from_public_key(&public_key, "k1"),
    };
    let mut map = HashMap::new();
    map.insert("t1".to_string(), key_material);
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(map),
    );
    let token = issuer.mint(
        &TenantId::new("t1"),
        "p:test",
        vec!["stream.publish:stream:*/*".to_string()],
    )?;

    let auth = BrokerAuth::new(format!("http://{addr}"));
    let ctx = auth.authenticate("t1", &token).await?;
    assert!(
        ctx.matcher
            .allows(felix_authz::Action::StreamPublish, "stream:default/orders")
    );
    Ok(())
}
