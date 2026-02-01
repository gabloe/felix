//! OIDC token validation with cached discovery and JWKS fetching.
//!
//! # Purpose
//! Validate inbound IdP bearer tokens against configured issuers using cached
//! discovery documents and JWKS with TTL-based refresh.
//!
//! # Architectural role
//! Provides the IdP boundary for the control-plane: it verifies upstream tokens
//! (typically RS256 or ES256) before issuing Felix EdDSA tokens elsewhere.
//!
//! # Callers / consumers
//! - Token exchange endpoint (`/token/exchange`) validates IdP tokens.
//! - Tests that exercise JWKS caching and issuer validation.
//!
//! # Key invariants
//! - Only ES256 is accepted by default; RS256 is allowed only when the
//!   `oidc-rsa` feature is enabled. Felix tokens are EdDSA and handled in a
//!   separate module.
//! - RS256 is not accepted by default because of the Marvin side-channel attack
//!   which currently has no known mitigations in Rust's crypto libraries.
//! - Issuer and audience claims are validated against configuration.
//! - JWKS and discovery caches are time-bounded and refreshed on demand.
//!
//! # Concurrency model
//! Shared caches are stored in `DashMap` for concurrent read/write access across
//! async tasks without global locks.
//!
//! # Security boundary
//! This module is the boundary between external IdP tokens and the internal
//! authorization system. It must reject unsupported algorithms and issuers.
//!
//! # Security model and threat assumptions
//! - Attackers may craft tokens; we validate issuer, audience, and signature.
//! - RS256 is feature-gated because many IdPs publish RSA keys via JWKS.
//! - We decode claims without verification only to locate the issuer; all other
//!   validation happens after signature verification.
//!
//! # How to use
//! Construct an [`UpstreamOidcValidator`] and call [`UpstreamOidcValidator::validate`]
//! with the bearer token and configured issuers.
use crate::auth::idp_registry::IdpIssuerConfig;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use dashmap::DashMap;
use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, JwkSet, KeyAlgorithm};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Validator for upstream OIDC bearer tokens with cached discovery/JWKS.
///
/// # Overview
/// Maintains HTTP client and in-memory caches for discovery and JWKS, enabling
/// efficient validation for configured issuers.
///
/// # Arguments
/// - Constructed via [`UpstreamOidcValidator::new`] or `Default`.
///
/// # Returns
/// - Not applicable (stateful validator).
///
/// # Errors
/// - Not applicable.
///
/// # Panics
/// - Does not panic.
///
/// # Examples
/// ```rust
/// use controlplane::auth::oidc::UpstreamOidcValidator;
/// use std::time::Duration;
///
/// let validator = UpstreamOidcValidator::new(Duration::from_secs(300), Duration::from_secs(300), 60);
/// ```
///
/// # Security
/// - ES256 is accepted by default; RS256 is allowed only with `oidc-rsa`.
#[derive(Debug, Clone)]
pub struct UpstreamOidcValidator {
    client: reqwest::Client,
    jwks_cache: Arc<DashMap<String, CachedJwks>>,
    discovery_cache: Arc<DashMap<String, CachedDiscovery>>,
    jwks_ttl: Duration,
    discovery_ttl: Duration,
    clock_skew_seconds: u64,
}

/// Claims extracted from a validated upstream OIDC token.
///
/// # Overview
/// Minimal identity payload used to derive a Felix principal.
///
/// # Arguments
/// - Populated by [`UpstreamOidcValidator::validate`].
///
/// # Returns
/// - Not applicable (data container).
///
/// # Errors
/// - Not applicable.
///
/// # Panics
/// - Does not panic.
///
/// # Examples
/// ```rust
/// use controlplane::auth::oidc::ValidatedToken;
///
/// let token = ValidatedToken {
///     issuer: "https://issuer.example".to_string(),
///     subject: "user-1".to_string(),
///     groups: vec!["group-a".to_string()],
/// };
/// assert_eq!(token.subject, "user-1");
/// ```
///
/// # Security
/// - The fields are derived from a verified token and should not be tampered with.
#[derive(Debug, Clone)]
pub struct ValidatedToken {
    pub issuer: String,
    pub subject: String,
    pub groups: Vec<String>,
}

/// Errors returned during upstream OIDC validation.
///
/// # Overview
/// Enumerates validation failures such as issuer mismatch, unsupported
/// algorithms, JWKS lookup errors, or JWT validation errors.
///
/// # Arguments
/// - Variants carry contextual error information.
///
/// # Returns
/// - Not applicable.
///
/// # Errors
/// - Not applicable.
///
/// # Panics
/// - Does not panic.
///
/// # Examples
/// ```rust
/// use controlplane::auth::oidc::OidcError;
///
/// let err = OidcError::IssuerNotAllowed;
/// assert!(matches!(err, OidcError::IssuerNotAllowed));
/// ```
///
/// # Security
/// - Error details must not include sensitive token contents.
#[derive(Debug, thiserror::Error)]
pub enum OidcError {
    #[error("missing issuer")]
    MissingIssuer,
    #[error("issuer not allowed")]
    IssuerNotAllowed,
    #[error("missing subject")]
    MissingSubject,
    #[error("missing key id")]
    MissingKeyId,
    #[error("unsupported algorithm")]
    UnsupportedAlgorithm,
    #[error("invalid jwk: {0}")]
    InvalidJwk(String),
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

impl Default for UpstreamOidcValidator {
    fn default() -> Self {
        Self::new(Duration::from_secs(3600), Duration::from_secs(3600), 60)
    }
}

impl UpstreamOidcValidator {
    /// Create a new validator with explicit cache TTLs and clock skew.
    ///
    /// # Overview
    /// Configures cache durations for JWKS and discovery documents and sets
    /// allowable clock skew for JWT validation.
    ///
    /// # Arguments
    /// - `jwks_ttl`: Duration to cache JWKS responses.
    /// - `discovery_ttl`: Duration to cache OIDC discovery documents.
    /// - `clock_skew_seconds`: Allowed time skew for `iat/exp` validation.
    ///
    /// # Returns
    /// - A configured [`UpstreamOidcValidator`].
    ///
    /// # Errors
    /// - Not applicable.
    ///
    /// # Panics
    /// - Does not panic.
    ///
    /// # Examples
    /// ```rust
    /// use controlplane::auth::oidc::UpstreamOidcValidator;
    /// use std::time::Duration;
    ///
    /// let validator = UpstreamOidcValidator::new(Duration::from_secs(600), Duration::from_secs(600), 30);
    /// ```
    ///
    /// # Security
    /// - Shorter TTLs reduce exposure to stale keys but increase fetch volume.
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

    /// Validate an upstream OIDC bearer token against configured issuers.
    ///
    /// # Overview
    /// Enforces allowed algorithms, resolves issuer configuration, fetches JWKS as needed,
    /// and validates issuer/audience/subject claims.
    ///
    /// # Arguments
    /// - `token`: The JWT bearer token to validate.
    /// - `issuers`: Allowed issuer configurations for the tenant.
    ///
    /// # Returns
    /// - `Ok(ValidatedToken)` containing issuer, subject, and groups.
    ///
    /// # Errors
    /// - `OidcError::UnsupportedAlgorithm` if token is not an allowed algorithm.
    /// - `OidcError::IssuerNotAllowed` if `iss` is not configured.
    /// - `OidcError::MissingKeyId` if the token header lacks a `kid`.
    /// - `OidcError::JwksKeyNotFound` if the signing key cannot be found.
    /// - `OidcError::InvalidJwk` if the JWK metadata does not match the algorithm.
    /// - `OidcError::InvalidClaim` if `iat` is missing or invalid.
    /// - `OidcError::Jwt` for signature/claim validation failures.
    ///
    /// # Panics
    /// - Does not panic.
    ///
    /// # Examples
    /// ```rust,no_run
    /// use controlplane::auth::oidc::UpstreamOidcValidator;
    /// use controlplane::auth::idp_registry::IdpIssuerConfig;
    ///
    /// async fn validate_token(validator: UpstreamOidcValidator, token: &str, issuers: Vec<IdpIssuerConfig>) {
    ///     let _ = validator.validate(token, &issuers).await;
    /// }
    /// ```
    ///
    /// # Security
    /// - The algorithm is pinned to the allowlist for IdP validation only.
    /// - `iss` and `aud` must match configured values.
    pub async fn validate(
        &self,
        token: &str,
        issuers: &[IdpIssuerConfig],
    ) -> Result<ValidatedToken, OidcError> {
        // Step 1: Check header algorithm before any heavy work.
        // This avoids accepting EdDSA Felix tokens or other algorithms here.
        let header = decode_header(token)?;
        if !is_algorithm_allowed(header.alg) {
            return Err(OidcError::UnsupportedAlgorithm);
        }
        let kid = header.kid.as_deref().ok_or(OidcError::MissingKeyId)?;

        // Step 2: Decode claims without verification to locate the issuer.
        // We only trust these claims after signature verification later.
        let unsafe_claims = decode_unverified_claims(token)?;
        let issuer = extract_string_claim(&unsafe_claims, "iss").ok_or(OidcError::MissingIssuer)?;
        let issuer_cfg = issuers
            .iter()
            .find(|cfg| cfg.issuer == issuer)
            .ok_or(OidcError::IssuerNotAllowed)?;

        // Step 3: Resolve and fetch JWKS, retrying once on a miss.
        // This handles key rotation and transient cache inconsistencies.
        let jwks_url = self.resolve_jwks_url(&issuer, issuer_cfg).await?;
        let jwks = self.get_jwks(&jwks_url).await?;
        let decoding_key = match find_jwk(&jwks, kid) {
            Some(key) => {
                ensure_jwk_matches_algorithm(key, header.alg)?;
                DecodingKey::from_jwk(key)?
            }
            None => {
                let refreshed = self.refresh_jwks(&jwks_url).await?;
                let key = find_jwk(&refreshed, kid).ok_or(OidcError::JwksKeyNotFound)?;
                ensure_jwk_matches_algorithm(key, header.alg)?;
                DecodingKey::from_jwk(key)?
            }
        };
        // Step 4: Enforce issuer and audience validation.
        // This prevents token substitution across tenants or clients.
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[issuer_cfg.issuer.as_str()]);
        validation.set_audience(&issuer_cfg.audiences);
        validation
            .required_spec_claims
            .extend(["iss".to_string(), "aud".to_string()]);
        validation.leeway = self.clock_skew_seconds;

        // Step 5: Verify the token signature and claims.
        let token = decode::<Value>(token, &decoding_key, &validation)?;
        validate_iat(&token.claims, self.clock_skew_seconds)?;
        // Step 6: Extract mapped subject and groups for downstream RBAC.
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
        // Step 1: Use explicit JWKS URL when configured.
        // This avoids discovery for issuers with custom endpoints.
        if let Some(url) = &issuer_cfg.jwks_url {
            return Ok(url.to_string());
        }
        // Step 2: Build the discovery URL from the issuer if not provided.
        let discovery_url = issuer_cfg.discovery_url.clone().unwrap_or_else(|| {
            format!(
                "{}/.well-known/openid-configuration",
                issuer.trim_end_matches('/')
            )
        });

        // Step 3: Serve cached discovery results when still valid.
        if let Some(entry) = self.discovery_cache.get(&discovery_url)
            && entry.expires_at > Instant::now()
        {
            return Ok(entry.jwks_url.clone());
        }

        // Step 4: Fetch discovery and cache it for the configured TTL.
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
        // Step 1: Use cached JWKS when it hasn't expired.
        if let Some(entry) = self.jwks_cache.get(jwks_url)
            && entry.expires_at > Instant::now()
        {
            return Ok(entry.jwks.clone());
        }
        // Step 2: Refresh JWKS on cache miss or expiry.
        self.refresh_jwks(jwks_url).await
    }

    async fn refresh_jwks(&self, jwks_url: &str) -> Result<JwkSet, OidcError> {
        // We always fetch JWKS over HTTPS (assumed) using reqwest.
        // A refresh can be triggered by cache expiry or missing `kid`.
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

fn is_algorithm_allowed(alg: Algorithm) -> bool {
    matches!(alg, Algorithm::ES256)
        || (cfg!(feature = "oidc-rsa") && matches!(alg, Algorithm::RS256))
}

fn ensure_jwk_matches_algorithm(
    jwk: &jsonwebtoken::jwk::Jwk,
    alg: Algorithm,
) -> Result<(), OidcError> {
    let key_alg = jwk
        .common
        .key_algorithm
        .ok_or_else(|| OidcError::InvalidJwk("missing alg".to_string()))?;
    match (key_alg, alg) {
        (KeyAlgorithm::RS256, Algorithm::RS256) => {}
        (KeyAlgorithm::ES256, Algorithm::ES256) => {}
        _ => return Err(OidcError::InvalidJwk("alg mismatch".to_string())),
    }
    match (&jwk.algorithm, alg) {
        (AlgorithmParameters::RSA(_), Algorithm::RS256) => Ok(()),
        (AlgorithmParameters::EllipticCurve(params), Algorithm::ES256) => {
            if params.curve != EllipticCurve::P256 {
                return Err(OidcError::InvalidJwk("unexpected EC curve".to_string()));
            }
            Ok(())
        }
        _ => Err(OidcError::InvalidJwk("kty mismatch".to_string())),
    }
}

fn find_jwk<'a>(jwks: &'a JwkSet, kid: &str) -> Option<&'a jsonwebtoken::jwk::Jwk> {
    jwks.keys
        .iter()
        .find(|key| key.common.key_id.as_deref() == Some(kid))
}

fn decode_unverified_claims(token: &str) -> Result<Value, OidcError> {
    // We decode claims without verification only to locate the issuer.
    // Signature validation still happens later using the resolved JWKS.
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
    // Only accept string-valued claims; other types are ignored.
    claims
        .get(name)
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn validate_iat(claims: &Value, leeway_seconds: u64) -> Result<(), OidcError> {
    // Require `iat` and ensure it is not unreasonably in the future.
    let iat = claims
        .get("iat")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| OidcError::InvalidClaim("iat".to_string()))?;
    let now = Utc::now().timestamp();
    let leeway = leeway_seconds as i64;
    if iat > now + leeway {
        return Err(OidcError::InvalidClaim("iat in future".to_string()));
    }
    Ok(())
}

fn extract_groups_claim(claims: &Value, name: Option<&str>) -> Vec<String> {
    // Groups may be encoded as either a string or array of strings.
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
    #[cfg(feature = "oidc-rsa")]
    use crate::auth::idp_registry::ClaimMappings;
    use ed25519_dalek::SigningKey as Ed25519SigningKey;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use jsonwebtoken::EncodingKey;
    use serde_json::json;
    #[cfg(feature = "oidc-rsa")]
    use std::net::SocketAddr;
    #[cfg(feature = "oidc-rsa")]
    use tokio::task::JoinHandle;

    #[cfg(feature = "oidc-rsa")]
    const TEST_PRIVATE_KEY_PEM: &str = r#"-----BEGIN RSA PRIVATE KEY-----
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

    #[cfg(feature = "oidc-rsa")]
    const TEST_JWK_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
    #[cfg(feature = "oidc-rsa")]
    const TEST_JWK_E: &str = "AQAB";

    #[cfg(feature = "oidc-rsa")]
    #[tokio::test]
    async fn validates_against_mock_jwks() {
        // This test ensures our JWKS lookup + RS256 validation flow works end-to-end.
        // The RSA key here is a test-only IdP fixture; Felix tokens remain EdDSA.
        let jwks = json!({
            "keys": [{
                "kty": "RSA",
                "kid": "kid-1",
                "alg": "RS256",
                "use": "sig",
                "n": TEST_JWK_N,
                "e": TEST_JWK_E
            }]
        });

        let (addr, _handle) = spawn_jwks_server(jwks).await;
        let issuer = format!("http://{addr}");

        let token = mint_upstream_token(TEST_PRIVATE_KEY_PEM, &issuer, "aud1", "kid-1");
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

        let validator = UpstreamOidcValidator::default();
        let validated = validator
            .validate(&token, &[issuer_cfg])
            .await
            .expect("valid");
        assert_eq!(validated.issuer, issuer);
        assert_eq!(validated.subject, "user-1");
    }

    #[cfg(feature = "oidc-rsa")]
    #[tokio::test]
    async fn rejects_wrong_audience() {
        // This test prevents regressions where audience validation is skipped.
        // The RSA key is a test-only IdP fixture.
        let jwks = json!({
            "keys": [{
                "kty": "RSA",
                "kid": "kid-1",
                "alg": "RS256",
                "use": "sig",
                "n": TEST_JWK_N,
                "e": TEST_JWK_E
            }]
        });

        let (addr, _handle) = spawn_jwks_server(jwks).await;
        let issuer = format!("http://{addr}");
        let token = mint_upstream_token(TEST_PRIVATE_KEY_PEM, &issuer, "aud-wrong", "kid-1");
        let issuer_cfg = IdpIssuerConfig {
            issuer: issuer.clone(),
            audiences: vec!["aud1".to_string()],
            discovery_url: None,
            jwks_url: Some(format!("{issuer}/jwks")),
            claim_mappings: ClaimMappings::default(),
        };

        let validator = UpstreamOidcValidator::default();
        let err = validator.validate(&token, &[issuer_cfg]).await.unwrap_err();
        assert!(matches!(err, OidcError::Jwt(_)));
    }

    #[tokio::test]
    async fn rejects_non_allowed_tokens() {
        // This test ensures Felix EdDSA tokens are rejected by the IdP validator.
        // It prevents accidental acceptance of non-allowed algorithms.
        let signing_key = Ed25519SigningKey::from_bytes(&[1u8; 32]);
        let der = signing_key.to_pkcs8_der().expect("pkcs8 der");
        let now = chrono::Utc::now().timestamp();
        let claims = json!({
            "iss": "https://issuer.example",
            "sub": "user-1",
            "aud": "aud1",
            "iat": now,
            "exp": now + 300
        });
        let header = jsonwebtoken::Header::new(Algorithm::EdDSA);
        let token =
            jsonwebtoken::encode(&header, &claims, &EncodingKey::from_ed_der(der.as_bytes()))
                .expect("token");
        let validator = UpstreamOidcValidator::default();
        let err = validator.validate(&token, &[]).await.unwrap_err();
        assert!(matches!(err, OidcError::UnsupportedAlgorithm));
    }

    #[cfg(feature = "oidc-rsa")]
    async fn spawn_jwks_server(jwks: Value) -> (SocketAddr, JoinHandle<()>) {
        // We spawn a deterministic local JWKS server for tests to avoid flakiness.
        // Binding to 127.0.0.1:0 lets the OS choose a free port safely.
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

    #[cfg(feature = "oidc-rsa")]
    fn mint_upstream_token(private_pem: &str, issuer: &str, audience: &str, kid: &str) -> String {
        // This helper mints RS256 tokens for IdP validation tests only.
        // It must never be used for Felix-issued tokens (which are EdDSA).
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
