//! OIDC token validation with cached discovery and JWKS fetching.
//!
//! # Purpose
//! Validates inbound bearer tokens against configured IdP issuers, using cached
//! discovery documents and JWKS with TTL-based refresh.
use crate::auth::idp_registry::IdpIssuerConfig;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use dashmap::DashMap;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct OidcValidator {
    client: reqwest::Client,
    jwks_cache: Arc<DashMap<String, CachedJwks>>,
    discovery_cache: Arc<DashMap<String, CachedDiscovery>>,
    jwks_ttl: Duration,
    discovery_ttl: Duration,
    clock_skew_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct ValidatedToken {
    pub issuer: String,
    pub subject: String,
    pub groups: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum OidcError {
    #[error("missing issuer")]
    MissingIssuer,
    #[error("issuer not allowed")]
    IssuerNotAllowed,
    #[error("missing subject")]
    MissingSubject,
    #[error("unsupported algorithm")]
    UnsupportedAlgorithm,
    #[error("jwks key not found")]
    JwksKeyNotFound,
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("jwt error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("invalid claim: {0}")]
    InvalidClaim(String),
}

#[derive(Debug, Clone)]
struct CachedJwks {
    jwks: JwkSet,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct CachedDiscovery {
    jwks_url: String,
    expires_at: Instant,
}

#[derive(Debug, Deserialize)]
struct DiscoveryDocument {
    jwks_uri: String,
}

impl Default for OidcValidator {
    fn default() -> Self {
        Self::new(Duration::from_secs(3600), Duration::from_secs(3600), 60)
    }
}

impl OidcValidator {
    pub fn new(jwks_ttl: Duration, discovery_ttl: Duration, clock_skew_seconds: u64) -> Self {
        Self {
            client: reqwest::Client::new(),
            jwks_cache: Arc::new(DashMap::new()),
            discovery_cache: Arc::new(DashMap::new()),
            jwks_ttl,
            discovery_ttl,
            clock_skew_seconds,
        }
    }

    pub async fn validate(
        &self,
        token: &str,
        issuers: &[IdpIssuerConfig],
    ) -> Result<ValidatedToken, OidcError> {
        let header = decode_header(token)?;
        if header.alg != Algorithm::RS256 {
            return Err(OidcError::UnsupportedAlgorithm);
        }

        let unsafe_claims = decode_unverified_claims(token)?;
        let issuer = extract_string_claim(&unsafe_claims, "iss").ok_or(OidcError::MissingIssuer)?;
        let issuer_cfg = issuers
            .iter()
            .find(|cfg| cfg.issuer == issuer)
            .ok_or(OidcError::IssuerNotAllowed)?;

        let jwks_url = self.resolve_jwks_url(&issuer, issuer_cfg).await?;
        let jwks = self.get_jwks(&jwks_url).await?;
        let decoding_key = match find_jwk(&jwks, header.kid.as_deref()) {
            Some(key) => DecodingKey::from_jwk(key)?,
            None => {
                let refreshed = self.refresh_jwks(&jwks_url).await?;
                let key = find_jwk(&refreshed, header.kid.as_deref())
                    .ok_or(OidcError::JwksKeyNotFound)?;
                DecodingKey::from_jwk(key)?
            }
        };
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[issuer_cfg.issuer.as_str()]);
        validation.set_audience(&issuer_cfg.audiences);
        validation.leeway = self.clock_skew_seconds;

        let token = decode::<Value>(token, &decoding_key, &validation)?;
        let subject = extract_string_claim(&token.claims, &issuer_cfg.claim_mappings.subject_claim)
            .ok_or(OidcError::MissingSubject)?;
        let groups = extract_groups_claim(
            &token.claims,
            issuer_cfg.claim_mappings.groups_claim.as_deref(),
        );

        Ok(ValidatedToken {
            issuer,
            subject,
            groups,
        })
    }

    async fn resolve_jwks_url(
        &self,
        issuer: &str,
        issuer_cfg: &IdpIssuerConfig,
    ) -> Result<String, OidcError> {
        if let Some(url) = &issuer_cfg.jwks_url {
            return Ok(url.to_string());
        }
        let discovery_url = issuer_cfg.discovery_url.clone().unwrap_or_else(|| {
            format!(
                "{}/.well-known/openid-configuration",
                issuer.trim_end_matches('/')
            )
        });

        if let Some(entry) = self.discovery_cache.get(&discovery_url)
            && entry.expires_at > Instant::now()
        {
            return Ok(entry.jwks_url.clone());
        }

        let doc: DiscoveryDocument = self.client.get(&discovery_url).send().await?.json().await?;
        self.discovery_cache.insert(
            discovery_url,
            CachedDiscovery {
                jwks_url: doc.jwks_uri.clone(),
                expires_at: Instant::now() + self.discovery_ttl,
            },
        );
        Ok(doc.jwks_uri)
    }

    async fn get_jwks(&self, jwks_url: &str) -> Result<JwkSet, OidcError> {
        if let Some(entry) = self.jwks_cache.get(jwks_url)
            && entry.expires_at > Instant::now()
        {
            return Ok(entry.jwks.clone());
        }
        self.refresh_jwks(jwks_url).await
    }

    async fn refresh_jwks(&self, jwks_url: &str) -> Result<JwkSet, OidcError> {
        let jwks: JwkSet = self.client.get(jwks_url).send().await?.json().await?;
        self.jwks_cache.insert(
            jwks_url.to_string(),
            CachedJwks {
                jwks: jwks.clone(),
                expires_at: Instant::now() + self.jwks_ttl,
            },
        );
        Ok(jwks)
    }
}

fn find_jwk<'a>(jwks: &'a JwkSet, kid: Option<&str>) -> Option<&'a jsonwebtoken::jwk::Jwk> {
    match kid {
        Some(kid) => jwks
            .keys
            .iter()
            .find(|key| key.common.key_id.as_deref() == Some(kid)),
        None => jwks.keys.first(),
    }
}

fn decode_unverified_claims(token: &str) -> Result<Value, OidcError> {
    let mut parts = token.split('.');
    let _header = parts.next();
    let payload = parts
        .next()
        .ok_or_else(|| OidcError::InvalidClaim("token format".to_string()))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| OidcError::InvalidClaim("token payload".to_string()))?;
    serde_json::from_slice(&bytes)
        .map_err(|err| OidcError::InvalidClaim(format!("token payload: {err}")))
}

fn extract_string_claim(claims: &Value, name: &str) -> Option<String> {
    claims
        .get(name)
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn extract_groups_claim(claims: &Value, name: Option<&str>) -> Vec<String> {
    let Some(name) = name else {
        return Vec::new();
    };
    let Some(value) = claims.get(name) else {
        return Vec::new();
    };
    if let Some(values) = value.as_array() {
        return values
            .iter()
            .filter_map(|item| item.as_str().map(|val| val.to_string()))
            .collect();
    }
    if let Some(value) = value.as_str() {
        return vec![value.to_string()];
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::idp_registry::ClaimMappings;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::traits::PublicKeyParts;
    use rsa::{RsaPrivateKey, RsaPublicKey};
    use serde_json::json;
    use std::net::SocketAddr;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn validates_against_mock_jwks() {
        let key = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).expect("key");
        let public_key = RsaPublicKey::from(&key);
        let private_pem = key.to_pkcs1_pem(Default::default()).unwrap();

        let jwk_n = URL_SAFE_NO_PAD.encode(public_key.n().to_bytes_be());
        let jwk_e = URL_SAFE_NO_PAD.encode(public_key.e().to_bytes_be());

        let jwks = json!({
            "keys": [{
                "kty": "RSA",
                "kid": "kid-1",
                "alg": "RS256",
                "use": "sig",
                "n": jwk_n,
                "e": jwk_e
            }]
        });

        let (addr, _handle) = spawn_jwks_server(jwks).await;
        let issuer = format!("http://{addr}");

        let token = mint_upstream_token(&private_pem, &issuer, "aud1", "kid-1");
        let issuer_cfg = IdpIssuerConfig {
            issuer: issuer.clone(),
            audiences: vec!["aud1".to_string()],
            discovery_url: None,
            jwks_url: Some(format!("{issuer}/jwks")),
            claim_mappings: ClaimMappings {
                subject_claim: "sub".to_string(),
                groups_claim: None,
            },
        };

        let validator = OidcValidator::default();
        let validated = validator
            .validate(&token, &[issuer_cfg])
            .await
            .expect("valid");
        assert_eq!(validated.issuer, issuer);
        assert_eq!(validated.subject, "user-1");
    }

    #[tokio::test]
    async fn rejects_wrong_audience() {
        let key = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).expect("key");
        let public_key = RsaPublicKey::from(&key);
        let private_pem = key.to_pkcs1_pem(Default::default()).unwrap();

        let jwk_n = URL_SAFE_NO_PAD.encode(public_key.n().to_bytes_be());
        let jwk_e = URL_SAFE_NO_PAD.encode(public_key.e().to_bytes_be());
        let jwks = json!({
            "keys": [{
                "kty": "RSA",
                "kid": "kid-1",
                "alg": "RS256",
                "use": "sig",
                "n": jwk_n,
                "e": jwk_e
            }]
        });

        let (addr, _handle) = spawn_jwks_server(jwks).await;
        let issuer = format!("http://{addr}");
        let token = mint_upstream_token(&private_pem, &issuer, "aud-wrong", "kid-1");
        let issuer_cfg = IdpIssuerConfig {
            issuer: issuer.clone(),
            audiences: vec!["aud1".to_string()],
            discovery_url: None,
            jwks_url: Some(format!("{issuer}/jwks")),
            claim_mappings: ClaimMappings::default(),
        };

        let validator = OidcValidator::default();
        let err = validator.validate(&token, &[issuer_cfg]).await.unwrap_err();
        assert!(matches!(err, OidcError::Jwt(_)));
    }

    async fn spawn_jwks_server(jwks: Value) -> (SocketAddr, JoinHandle<()>) {
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

    fn mint_upstream_token(private_pem: &str, issuer: &str, audience: &str, kid: &str) -> String {
        let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        let now = chrono::Utc::now().timestamp();
        let claims = serde_json::json!({
            "iss": issuer,
            "sub": "user-1",
            "aud": audience,
            "iat": now,
            "exp": now + 300
        });
        jsonwebtoken::encode(
            &header,
            &claims,
            &jsonwebtoken::EncodingKey::from_rsa_pem(private_pem.as_bytes()).expect("key"),
        )
        .expect("token")
    }
}
