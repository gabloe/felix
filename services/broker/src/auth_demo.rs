use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use felix_authz::{FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyMaterial};
use jsonwebtoken::Algorithm;
use rsa::RsaPublicKey;
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::traits::PublicKeyParts;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::{BrokerAuth, ControlPlaneKeyStore};

const DEMO_PRIVATE_KEY_PEM: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL
UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V
rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H
oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI
Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/
by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd
WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ
Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j
E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn
LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS
bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE
8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl
xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY
2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1
9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0
7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ
mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7
MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4
t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam
QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA
2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ
4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn
PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH
UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8
oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==
-----END RSA PRIVATE KEY-----"#;

const DEMO_PUBLIC_KEY_PEM: &str = r#"-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4
l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2VrUyW
yj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG
/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4l
QzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/by2h
3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQAB
-----END RSA PUBLIC KEY-----"#;

pub struct DemoAuth {
    pub auth: Arc<BrokerAuth>,
    pub tenant_id: String,
    pub token: String,
}

pub fn demo_auth_for_tenant(tenant_id: &str) -> Result<DemoAuth> {
    let jwks = build_jwks("demo-k1")?;
    let key_material = TenantKeyMaterial {
        kid: "demo-k1".to_string(),
        alg: Algorithm::RS256,
        private_key_pem: DEMO_PRIVATE_KEY_PEM.as_bytes().to_vec(),
        public_key_pem: DEMO_PUBLIC_KEY_PEM.as_bytes().to_vec(),
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

    let key_store = Arc::new(ControlPlaneKeyStore::new("http://127.0.0.1".to_string()));
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    let auth = Arc::new(BrokerAuth::with_key_store(key_store));

    Ok(DemoAuth {
        auth,
        tenant_id: tenant_id.to_string(),
        token,
    })
}

fn build_jwks(kid: &str) -> Result<Jwks> {
    let public_key =
        RsaPublicKey::from_pkcs1_pem(DEMO_PUBLIC_KEY_PEM).context("parse demo public key")?;
    let n = URL_SAFE_NO_PAD.encode(public_key.n().to_bytes_be());
    let e = URL_SAFE_NO_PAD.encode(public_key.e().to_bytes_be());
    Ok(Jwks {
        keys: vec![Jwk {
            kty: "RSA".to_string(),
            kid: kid.to_string(),
            alg: "RS256".to_string(),
            use_field: KeyUse::Sig,
            n,
            e,
        }],
    })
}
