mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use common::read_json;
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_router};
use controlplane::auth::felix_token::{FelixClaims, SigningKey, TenantSigningKeys};
use controlplane::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use controlplane::auth::oidc::OidcValidator;
use controlplane::auth::principal::principal_id;
use controlplane::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use controlplane::model::Tenant;
use controlplane::store::{AuthStore, ControlPlaneStore, StoreConfig, memory::InMemoryStore};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tower::ServiceExt;

const FELIX_PRIVATE_KEY: &str = r#"-----BEGIN RSA PRIVATE KEY-----
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

const FELIX_PUBLIC_KEY: &str = r#"-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4
l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2VrUyW
yj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG
/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4l
QzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/by2h
3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQAB
-----END RSA PUBLIC KEY-----"#;

fn verify_token(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    token: &str,
) -> Result<FelixClaims, jsonwebtoken::errors::Error> {
    let mut last_err = None;
    for key in keys.all_keys() {
        let decoding_key = DecodingKey::from_rsa_pem(&key.public_key_pem)?;
        let mut validation = Validation::new(key.alg);
        validation.set_audience(&["felix-broker"]);
        validation.set_issuer(&["felix-auth"]);
        match decode::<FelixClaims>(token, &decoding_key, &validation) {
            Ok(data) => {
                if data.claims.tid != tenant_id {
                    return Err(jsonwebtoken::errors::Error::from(
                        jsonwebtoken::errors::ErrorKind::InvalidToken,
                    ));
                }
                return Ok(data.claims);
            }
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        jsonwebtoken::errors::Error::from(jsonwebtoken::errors::ErrorKind::InvalidToken)
    }))
}

#[tokio::test]
async fn exchange_returns_tenant_scoped_token() {
    let idp_key = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).expect("key");
    let idp_public = RsaPublicKey::from(&idp_key);
    let jwks = jwks_for_key(&idp_public, "kid-1");
    let (addr, _handle) = spawn_jwks_server(jwks).await;

    let issuer = format!("http://{addr}");
    let token = mint_upstream_token(&idp_key, &issuer, "aud-1", "kid-1");

    let store = InMemoryStore::new(StoreConfig {
        changes_limit: 200,
        change_retention_max_rows: Some(200),
    });
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");

    store
        .upsert_idp_issuer(
            "t1",
            IdpIssuerConfig {
                issuer: issuer.clone(),
                audiences: vec!["aud-1".to_string()],
                discovery_url: None,
                jwks_url: Some(format!("{issuer}/jwks")),
                claim_mappings: ClaimMappings::default(),
            },
        )
        .await
        .expect("issuer");

    let principal = principal_id(&issuer, "user-1");
    store
        .add_rbac_policy(
            "t1",
            PolicyRule {
                subject: "role:ns-admin".to_string(),
                object: "ns:payments".to_string(),
                action: "ns.manage".to_string(),
            },
        )
        .await
        .expect("policy");
    store
        .add_rbac_grouping(
            "t1",
            GroupingRule {
                user: principal.clone(),
                role: "role:ns-admin".to_string(),
            },
        )
        .await
        .expect("grouping");

    store
        .set_tenant_signing_keys(
            "t1",
            TenantSigningKeys {
                current: SigningKey {
                    kid: "k1".to_string(),
                    alg: Algorithm::RS256,
                    private_key_pem: FELIX_PRIVATE_KEY.as_bytes().to_vec(),
                    public_key_pem: FELIX_PUBLIC_KEY.as_bytes().to_vec(),
                },
                previous: vec![],
            },
        )
        .await
        .expect("keys");

    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::new(store),
        oidc_validator: OidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_token: None,
    };
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
        build_router(state).into_service();

    let req = Request::builder()
        .method("POST")
        .uri("/v1/tenants/t1/token/exchange")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .expect("request");
    let response = app.oneshot(req).await.expect("exchange");
    assert_eq!(response.status(), StatusCode::OK);

    let payload = read_json(response).await;
    let felix_token = payload["felix_token"].as_str().expect("token");
    let keys = TenantSigningKeys {
        current: SigningKey {
            kid: "k1".to_string(),
            alg: Algorithm::RS256,
            private_key_pem: FELIX_PRIVATE_KEY.as_bytes().to_vec(),
            public_key_pem: FELIX_PUBLIC_KEY.as_bytes().to_vec(),
        },
        previous: vec![],
    };
    let claims = verify_token(&keys, "t1", felix_token).expect("verify");
    assert_eq!(claims.tid, "t1");
    assert!(
        claims
            .perms
            .contains(&"stream.publish:stream:payments/*".to_string())
    );
}

#[tokio::test]
async fn exchange_forbidden_without_policies() {
    let idp_key = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).expect("key");
    let idp_public = RsaPublicKey::from(&idp_key);
    let jwks = jwks_for_key(&idp_public, "kid-1");
    let (addr, _handle) = spawn_jwks_server(jwks).await;

    let issuer = format!("http://{addr}");
    let token = mint_upstream_token(&idp_key, &issuer, "aud-1", "kid-1");

    let store = InMemoryStore::new(StoreConfig {
        changes_limit: 200,
        change_retention_max_rows: Some(200),
    });
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .upsert_idp_issuer(
            "t1",
            IdpIssuerConfig {
                issuer: issuer.clone(),
                audiences: vec!["aud-1".to_string()],
                discovery_url: None,
                jwks_url: Some(format!("{issuer}/jwks")),
                claim_mappings: ClaimMappings::default(),
            },
        )
        .await
        .expect("issuer");
    store
        .set_tenant_signing_keys(
            "t1",
            TenantSigningKeys {
                current: SigningKey {
                    kid: "k1".to_string(),
                    alg: Algorithm::RS256,
                    private_key_pem: FELIX_PRIVATE_KEY.as_bytes().to_vec(),
                    public_key_pem: FELIX_PUBLIC_KEY.as_bytes().to_vec(),
                },
                previous: vec![],
            },
        )
        .await
        .expect("keys");

    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::new(store),
        oidc_validator: OidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_token: None,
    };
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
        build_router(state).into_service();

    let req = Request::builder()
        .method("POST")
        .uri("/v1/tenants/t1/token/exchange")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .expect("request");
    let response = app.oneshot(req).await.expect("exchange");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

async fn spawn_jwks_server(jwks: serde_json::Value) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    use axum::{Json, Router, routing::get};
    use tokio::net::TcpListener;

    let app = Router::new().route(
        "/jwks",
        get({
            let jwks = jwks.clone();
            move || {
                let jwks = jwks.clone();
                async move { Json(jwks) }
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = axum::serve(listener, app.into_make_service());
    let handle = tokio::spawn(async move {
        let _ = server.await;
    });
    (addr, handle)
}

fn jwks_for_key(key: &RsaPublicKey, kid: &str) -> serde_json::Value {
    let n = URL_SAFE_NO_PAD.encode(key.n().to_bytes_be());
    let e = URL_SAFE_NO_PAD.encode(key.e().to_bytes_be());
    json!({
        "keys": [{
            "kty": "RSA",
            "kid": kid,
            "alg": "RS256",
            "use": "sig",
            "n": n,
            "e": e
        }]
    })
}

fn mint_upstream_token(key: &RsaPrivateKey, issuer: &str, audience: &str, kid: &str) -> String {
    let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
    header.kid = Some(kid.to_string());
    let now = chrono::Utc::now().timestamp();
    let claims = json!({
        "iss": issuer,
        "sub": "user-1",
        "aud": audience,
        "iat": now,
        "exp": now + 300
    });
    let pem = key.to_pkcs1_pem(Default::default()).expect("pem");
    jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).expect("enc"),
    )
    .expect("token")
}
