use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use jsonwebtoken::Algorithm;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::{BrokerAuth, ControlPlaneKeyStore};

const DEMO_PRIVATE_KEY: [u8; 32] = [42u8; 32];

pub struct DemoAuth {
    pub auth: Arc<BrokerAuth>,
    pub tenant_id: String,
    pub token: String,
}

pub fn demo_auth_for_tenant(tenant_id: &str) -> Result<DemoAuth> {
    let jwks = build_jwks("demo-k1")?;
    let signing_key = Ed25519SigningKey::from_bytes(&DEMO_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let key_material = TenantKeyMaterial {
        kid: "demo-k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key: DEMO_PRIVATE_KEY,
        public_key,
        jwks: jwks.clone(),
    };

    let mut key_store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    key_store.insert(tenant_id.to_string(), key_material);
    let key_store = Arc::new(key_store);

    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        key_store,
    );

    let perms = vec![
        "tenant.admin:tenant:*".to_string(),
        "ns.manage:ns:*".to_string(),
        "stream.publish:stream:*/*".to_string(),
        "stream.subscribe:stream:*/*".to_string(),
        "cache.read:cache:*/*".to_string(),
        "cache.write:cache:*/*".to_string(),
    ];

    let token = issuer
        .mint(&TenantId::new(tenant_id), "p:demo", perms)
        .context("mint demo token")?;

    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://127.0.0.1".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    let auth = Arc::new(BrokerAuth::with_key_store(key_store));

    Ok(DemoAuth {
        auth,
        tenant_id: tenant_id.to_string(),
        token,
    })
}

fn build_jwks(kid: &str) -> Result<Jwks> {
    let signing_key = Ed25519SigningKey::from_bytes(&DEMO_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let x = URL_SAFE_NO_PAD.encode(public_key);
    Ok(Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use felix_authz::Action;

    #[test]
    fn build_jwks_includes_kid_and_key() {
        let jwks = build_jwks("demo-k1").expect("jwks");
        assert_eq!(jwks.keys.len(), 1);
        assert_eq!(jwks.keys[0].kid, "demo-k1");
        assert!(jwks.keys[0].x.as_ref().expect("x").len() > 10);
    }

    #[tokio::test]
    async fn demo_auth_for_tenant_mints_and_verifies() -> Result<()> {
        let demo = demo_auth_for_tenant("t1")?;
        let ctx = demo.auth.authenticate(&demo.tenant_id, &demo.token).await?;
        assert!(
            ctx.matcher
                .allows(Action::StreamPublish, "stream:default/orders")
        );
        assert!(
            ctx.matcher
                .allows(Action::CacheRead, "cache:default/primary")
        );
        Ok(())
    }
}
