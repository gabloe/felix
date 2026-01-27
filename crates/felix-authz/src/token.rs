use crate::{AuthzError, AuthzResult, Jwks, TenantId};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

pub struct TenantSigningKey {
    pub kid: String,
    pub alg: Algorithm,
    pub encoding_key: EncodingKey,
}

pub struct TenantVerificationKey {
    pub kid: String,
    pub alg: Algorithm,
    pub decoding_key: DecodingKey,
}

pub trait TenantKeyStore: Send + Sync {
    fn current_signing_key(&self, tenant_id: &TenantId) -> AuthzResult<TenantSigningKey>;
    fn verification_keys(&self, tenant_id: &TenantId) -> AuthzResult<Vec<TenantVerificationKey>>;
    fn jwks(&self, tenant_id: &TenantId) -> AuthzResult<Jwks>;
}

#[derive(Clone)]
pub struct TenantKeyMaterial {
    pub kid: String,
    pub alg: Algorithm,
    pub private_key_pem: Vec<u8>,
    pub public_key_pem: Vec<u8>,
    pub jwks: Jwks,
}

impl TenantKeyStore for HashMap<String, TenantKeyMaterial> {
    fn current_signing_key(&self, tenant_id: &TenantId) -> AuthzResult<TenantSigningKey> {
        let entry = self
            .get(tenant_id.as_str())
            .ok_or_else(|| AuthzError::MissingSigningKey(tenant_id.to_string()))?;
        let encoding_key =
            EncodingKey::from_rsa_pem(&entry.private_key_pem).map_err(AuthzError::Jwt)?;
        Ok(TenantSigningKey {
            kid: entry.kid.clone(),
            alg: entry.alg,
            encoding_key,
        })
    }

    fn verification_keys(&self, tenant_id: &TenantId) -> AuthzResult<Vec<TenantVerificationKey>> {
        let entry = self
            .get(tenant_id.as_str())
            .ok_or_else(|| AuthzError::MissingVerificationKeys(tenant_id.to_string()))?;
        let decoding_key =
            DecodingKey::from_rsa_pem(&entry.public_key_pem).map_err(AuthzError::Jwt)?;
        Ok(vec![TenantVerificationKey {
            kid: entry.kid.clone(),
            alg: entry.alg,
            decoding_key,
        }])
    }

    fn jwks(&self, tenant_id: &TenantId) -> AuthzResult<Jwks> {
        let entry = self
            .get(tenant_id.as_str())
            .ok_or_else(|| AuthzError::MissingJwks(tenant_id.to_string()))?;
        Ok(entry.jwks.clone())
    }
}

pub struct FelixTokenIssuer {
    issuer: String,
    audience: String,
    ttl: Duration,
    key_store: Arc<dyn TenantKeyStore>,
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
        }
    }

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
        let mut header = Header::new(signing_key.alg);
        header.kid = Some(signing_key.kid);
        let token = jsonwebtoken::encode(&header, &claims, &signing_key.encoding_key)?;
        Ok(token)
    }
}

pub struct FelixTokenVerifier {
    issuer: String,
    audience: String,
    leeway: u64,
    key_store: Arc<dyn TenantKeyStore>,
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
        }
    }

    pub fn verify(&self, tenant_id: &TenantId, token: &str) -> AuthzResult<FelixClaims> {
        let header = jsonwebtoken::decode_header(token)?;
        let keys = self.key_store.verification_keys(tenant_id)?;
        let key = match header.kid.as_deref() {
            Some(kid) => keys
                .into_iter()
                .find(|entry| entry.kid == kid)
                .ok_or_else(|| AuthzError::MissingVerificationKeys(tenant_id.to_string()))?,
            None => keys
                .into_iter()
                .next()
                .ok_or_else(|| AuthzError::MissingVerificationKeys(tenant_id.to_string()))?,
        };

        let mut validation = Validation::new(key.alg);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.leeway = self.leeway;
        let token = jsonwebtoken::decode::<FelixClaims>(token, &key.decoding_key, &validation)?;
        if token.claims.tid != tenant_id.as_str() {
            return Err(AuthzError::TenantMismatch {
                expected: tenant_id.to_string(),
                actual: token.claims.tid.clone(),
            });
        }
        Ok(token.claims)
    }
}

fn now_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jwks::Jwk;

    const TEST_PRIVATE_KEY: &str = r#"-----BEGIN RSA PRIVATE KEY-----
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

    const TEST_PUBLIC_KEY: &str = r#"-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4
l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2VrUyW
yj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG
/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4l
QzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/by2h
3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQAB
-----END RSA PUBLIC KEY-----"#;

    fn test_key_material() -> TenantKeyMaterial {
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::RS256,
            private_key_pem: TEST_PRIVATE_KEY.as_bytes().to_vec(),
            public_key_pem: TEST_PUBLIC_KEY.as_bytes().to_vec(),
            jwks: Jwks {
                keys: vec![Jwk {
                    kty: "RSA".to_string(),
                    kid: "k1".to_string(),
                    alg: "RS256".to_string(),
                    use_field: crate::jwks::KeyUse::Sig,
                    n: "test".to_string(),
                    e: "AQAB".to_string(),
                }],
            },
        }
    }

    fn test_key_store() -> Arc<dyn TenantKeyStore> {
        let mut keys = HashMap::new();
        keys.insert("tenant-a".to_string(), test_key_material());
        Arc::new(keys)
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
                vec!["stream.publish:stream:payments/orders.*".to_string()],
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
        let token =
            jsonwebtoken::encode(&header, &claims, &signing.encoding_key).expect("encode token");

        let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store);
        let verified = verifier
            .verify(&TenantId::new("tenant-a"), &token)
            .expect("verify token");
        assert_eq!(verified.sub, "principal");
    }

    #[test]
    fn verify_fails_on_unknown_kid() {
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
        let token =
            jsonwebtoken::encode(&header, &claims, &signing.encoding_key).expect("encode token");

        let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store);
        let err = verifier
            .verify(&TenantId::new("tenant-a"), &token)
            .expect_err("unknown kid");
        assert!(matches!(err, AuthzError::MissingVerificationKeys(_)));
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
    fn invalid_key_material_produces_jwt_error() {
        let mut keys = HashMap::new();
        keys.insert(
            "tenant-a".to_string(),
            TenantKeyMaterial {
                kid: "k1".to_string(),
                alg: Algorithm::RS256,
                private_key_pem: b"not-a-key".to_vec(),
                public_key_pem: b"not-a-key".to_vec(),
                jwks: Jwks { keys: vec![] },
            },
        );
        let key_store: Arc<dyn TenantKeyStore> = Arc::new(keys);
        let signing = key_store.current_signing_key(&TenantId::new("tenant-a"));
        assert!(matches!(signing, Err(AuthzError::Jwt(_))));

        let verification = key_store.verification_keys(&TenantId::new("tenant-a"));
        assert!(matches!(verification, Err(AuthzError::Jwt(_))));
    }
}
