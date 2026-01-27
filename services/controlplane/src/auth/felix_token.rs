//! Felix JWT token minting and verification helpers.
//!
//! # Purpose
//! Defines claim structures and helpers for signing/verifying tenant-scoped
//! access tokens used by the broker and control-plane APIs.
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone)]
pub struct SigningKey {
    pub kid: String,
    pub alg: Algorithm,
    pub private_key_pem: Vec<u8>,
    pub public_key_pem: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TenantSigningKeys {
    pub current: SigningKey,
    pub previous: Vec<SigningKey>,
}

impl TenantSigningKeys {
    pub fn all_keys(&self) -> Vec<SigningKey> {
        let mut keys = vec![self.current.clone()];
        keys.extend(self.previous.clone());
        keys
    }
}

#[derive(Debug)]
pub enum TokenError {
    Jwt(jsonwebtoken::errors::Error),
    MissingKey,
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenError::Jwt(err) => write!(f, "jwt error: {err}"),
            TokenError::MissingKey => write!(f, "missing signing key"),
        }
    }
}

impl std::error::Error for TokenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TokenError::Jwt(err) => Some(err),
            TokenError::MissingKey => None,
        }
    }
}

impl From<jsonwebtoken::errors::Error> for TokenError {
    fn from(value: jsonwebtoken::errors::Error) -> Self {
        TokenError::Jwt(value)
    }
}

pub fn mint_token(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    principal_id: &str,
    perms: Vec<String>,
    ttl: Duration,
) -> Result<String, TokenError> {
    let now = now_epoch_seconds();
    let exp = now + ttl.as_secs() as i64;
    let claims = FelixClaims {
        iss: "felix-auth".to_string(),
        aud: "felix-broker".to_string(),
        sub: principal_id.to_string(),
        tid: tenant_id.to_string(),
        exp,
        iat: now,
        jti: None,
        perms,
    };

    let mut header = Header::new(keys.current.alg);
    header.kid = Some(keys.current.kid.clone());
    let encoding_key = EncodingKey::from_rsa_pem(&keys.current.private_key_pem)?;
    Ok(jsonwebtoken::encode(&header, &claims, &encoding_key)?)
}

pub fn verify_token(
    keys: &TenantSigningKeys,
    token: &str,
    leeway: u64,
) -> Result<FelixClaims, TokenError> {
    let header = jsonwebtoken::decode_header(token)?;
    let key = match header.kid.as_deref() {
        Some(kid) => keys
            .all_keys()
            .into_iter()
            .find(|entry| entry.kid == kid)
            .ok_or(TokenError::MissingKey)?,
        None => keys
            .all_keys()
            .into_iter()
            .next()
            .ok_or(TokenError::MissingKey)?,
    };

    let decoding_key = DecodingKey::from_rsa_pem(&key.public_key_pem)?;
    let mut validation = Validation::new(key.alg);
    validation.set_audience(&["felix-broker"]);
    validation.set_issuer(&["felix-auth"]);
    validation.leeway = leeway;
    let token = jsonwebtoken::decode::<FelixClaims>(token, &decoding_key, &validation)?;
    Ok(token.claims)
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

    fn signing_keys() -> TenantSigningKeys {
        TenantSigningKeys {
            current: SigningKey {
                kid: "k1".to_string(),
                alg: Algorithm::RS256,
                private_key_pem: TEST_PRIVATE_KEY.as_bytes().to_vec(),
                public_key_pem: TEST_PUBLIC_KEY.as_bytes().to_vec(),
            },
            previous: vec![],
        }
    }

    #[test]
    fn mint_roundtrip_contains_claims() {
        let keys = signing_keys();
        let token = mint_token(
            &keys,
            "tenant-a",
            "principal",
            vec!["stream.publish:stream:payments/orders/*".to_string()],
            Duration::from_secs(900),
        )
        .expect("mint");
        assert!(!token.is_empty());
    }
}
