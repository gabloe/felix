//! Validation of upstream IdP tokens -- the boundary between an external
//! identity provider and Felix's own authorization.
//!
//! ES256 is the default and only algorithm. RS* and PS* can be enabled by
//! configuration but are off by default because of the Marvin side-channel
//! attack, which has no known mitigation in Rust's crypto libraries today; many
//! IdPs publish RSA keys via JWKS, so the allowlist is opt-in rather than
//! absent. Felix's own EdDSA tokens are a separate path entirely.
//!
//! Claims are decoded once *without* verification purely to find the issuer, so
//! the right JWKS can be fetched. Everything else -- issuer, audience,
//! signature -- is checked only after that, against configuration.
//!
//! Discovery documents and JWKS are cached with a TTL and refreshed on demand,
//! in a `DashMap` so concurrent tasks share them without a global lock.
//!
//! Construct an [`UpstreamOidcValidator`] and call
//! [`UpstreamOidcValidator::validate`].
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

/// Validates upstream OIDC bearer tokens, with cached discovery documents and
/// JWKS. ES256 by default; RS*/PS* only when explicitly allowlisted.
#[derive(Debug, Clone)]
pub struct UpstreamOidcValidator {
    client: reqwest::Client,
    jwks_cache: Arc<DashMap<String, CachedJwks>>,
    discovery_cache: Arc<DashMap<String, CachedDiscovery>>,
    jwks_ttl: Duration,
    discovery_ttl: Duration,
    clock_skew_seconds: u64,
    allowed_algorithms: Arc<Vec<Algorithm>>,
}

/// The identity extracted from a verified upstream token — just enough to
/// derive a Felix principal.
#[derive(Debug, Clone)]
pub struct ValidatedToken {
    pub issuer: String,
    pub subject: String,
    pub groups: Vec<String>,
}

/// Upstream validation failures. Messages never include token contents.
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
    /// ES256-only validator with the given cache TTLs and clock skew.
    /// Shorter TTLs reduce exposure to stale keys at the cost of more fetches.
    pub fn new(jwks_ttl: Duration, discovery_ttl: Duration, clock_skew_seconds: u64) -> Self {
        Self::new_with_allowed_algorithms(
            jwks_ttl,
            discovery_ttl,
            clock_skew_seconds,
            vec![Algorithm::ES256],
        )
    }

    /// Create a new validator with an explicit upstream JWT algorithm allowlist.
    ///
    /// The allowlist is checked against the JWT header `alg` before any key
    /// lookup or signature verification. If empty, defaults to ES256-only.
    pub fn new_with_allowed_algorithms(
        jwks_ttl: Duration,
        discovery_ttl: Duration,
        clock_skew_seconds: u64,
        mut allowed_algorithms: Vec<Algorithm>,
    ) -> Self {
        if allowed_algorithms.is_empty() {
            allowed_algorithms.push(Algorithm::ES256);
        }
        allowed_algorithms.sort_unstable_by_key(|alg| *alg as u8);
        allowed_algorithms.dedup();
        Self {
            client: reqwest::Client::new(),
            jwks_cache: Arc::new(DashMap::new()),
            discovery_cache: Arc::new(DashMap::new()),
            jwks_ttl,
            discovery_ttl,
            clock_skew_seconds,
            allowed_algorithms: Arc::new(allowed_algorithms),
        }
    }

    /// Validate an upstream bearer token against the tenant's configured
    /// issuers: allowlisted algorithm, resolved JWKS, then signature and
    /// `iss`/`aud`/`iat` checks.
    ///
    /// # Errors
    /// An [`OidcError`] naming the specific failure; issuer-not-configured is
    /// distinguished so the caller can answer 403 instead of 401.
    pub async fn validate(
        &self,
        token: &str,
        issuers: &[IdpIssuerConfig],
    ) -> Result<ValidatedToken, OidcError> {
        // Algorithm check first — this is also what keeps EdDSA Felix tokens
        // out of the upstream path.
        let header = decode_header(token)?;
        if !self.is_algorithm_allowed(header.alg) {
            return Err(OidcError::UnsupportedAlgorithm);
        }
        let kid = header.kid.as_deref().ok_or(OidcError::MissingKeyId)?;

        // Unverified decode, purely to find which issuer's JWKS to fetch;
        // nothing from it is trusted until the signature check below.
        let unsafe_claims = decode_unverified_claims(token)?;
        let issuer = extract_string_claim(&unsafe_claims, "iss").ok_or(OidcError::MissingIssuer)?;
        let issuer_cfg = issuers
            .iter()
            .find(|cfg| cfg.issuer == issuer)
            .ok_or(OidcError::IssuerNotAllowed)?;

        // On a `kid` miss, refresh once and retry — the miss usually means the
        // IdP rotated keys since our cached fetch.
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
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[issuer_cfg.issuer.as_str()]);
        validation.set_audience(&issuer_cfg.audiences);
        validation
            .required_spec_claims
            .extend(["iss".to_string(), "aud".to_string()]);
        validation.leeway = self.clock_skew_seconds;

        let token = decode::<Value>(token, &decoding_key, &validation)?;
        validate_iat(&token.claims, self.clock_skew_seconds)?;
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
        // An explicit JWKS URL skips discovery entirely.
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

    fn is_algorithm_allowed(&self, alg: Algorithm) -> bool {
        self.allowed_algorithms.contains(&alg)
    }
}

fn ensure_jwk_matches_algorithm(
    jwk: &jsonwebtoken::jwk::Jwk,
    alg: Algorithm,
) -> Result<(), OidcError> {
    // `alg` is OPTIONAL in a JWK (RFC 7517 §4.4), and major IdPs — Microsoft
    // Entra among them — publish signing keys without it. Requiring it here
    // rejected every token those IdPs issue. When the member is present it must
    // match the token's algorithm; when absent, the key-type/params check below
    // is what binds the key to the algorithm (an RSA key cannot verify an EC
    // token or vice versa, and the header `alg` is already allowlisted upstream).
    if let Some(key_alg) = jwk.common.key_algorithm {
        let expected = expected_key_algorithm(alg)
            .ok_or_else(|| OidcError::InvalidJwk("unsupported algorithm".to_string()))?;
        if key_alg != expected {
            return Err(OidcError::InvalidJwk("alg mismatch".to_string()));
        }
    }

    match (&jwk.algorithm, alg) {
        (AlgorithmParameters::EllipticCurve(params), Algorithm::ES256) => {
            if params.curve != EllipticCurve::P256 {
                return Err(OidcError::InvalidJwk("unexpected EC curve".to_string()));
            }
            if params.x.is_empty() || params.y.is_empty() {
                return Err(OidcError::InvalidJwk("missing EC coordinates".to_string()));
            }
            Ok(())
        }
        (AlgorithmParameters::RSA(params), Algorithm::RS256)
        | (AlgorithmParameters::RSA(params), Algorithm::RS384)
        | (AlgorithmParameters::RSA(params), Algorithm::RS512)
        | (AlgorithmParameters::RSA(params), Algorithm::PS256)
        | (AlgorithmParameters::RSA(params), Algorithm::PS384)
        | (AlgorithmParameters::RSA(params), Algorithm::PS512) => {
            if params.n.is_empty() || params.e.is_empty() {
                return Err(OidcError::InvalidJwk(
                    "missing RSA modulus/exponent".to_string(),
                ));
            }
            Ok(())
        }
        _ => Err(OidcError::InvalidJwk("kty mismatch".to_string())),
    }
}

fn expected_key_algorithm(alg: Algorithm) -> Option<KeyAlgorithm> {
    match alg {
        Algorithm::ES256 => Some(KeyAlgorithm::ES256),
        Algorithm::RS256 => Some(KeyAlgorithm::RS256),
        Algorithm::RS384 => Some(KeyAlgorithm::RS384),
        Algorithm::RS512 => Some(KeyAlgorithm::RS512),
        Algorithm::PS256 => Some(KeyAlgorithm::PS256),
        Algorithm::PS384 => Some(KeyAlgorithm::PS384),
        Algorithm::PS512 => Some(KeyAlgorithm::PS512),
        _ => None,
    }
}

fn find_jwk<'a>(jwks: &'a JwkSet, kid: &str) -> Option<&'a jsonwebtoken::jwk::Jwk> {
    jwks.keys
        .iter()
        .find(|key| key.common.key_id.as_deref() == Some(kid))
}

// Unverified decode, only ever used to locate the issuer before the real
// signature check.
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

fn validate_iat(claims: &Value, leeway_seconds: u64) -> Result<(), OidcError> {
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

// IdPs encode groups as either a string or an array of strings.
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
mod tests;
