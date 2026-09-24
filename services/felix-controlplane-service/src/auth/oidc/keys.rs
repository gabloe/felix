//! Finding the key a token was signed with: discovery, the JWKS behind it,
//! both cached with a TTL, and the checks that the key fits the algorithm.
use std::time::Instant;

use jsonwebtoken::Algorithm;
use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, JwkSet, KeyAlgorithm};
use serde::Deserialize;

use super::{OidcError, UpstreamOidcValidator};
use crate::auth::idp_registry::IdpIssuerConfig;

#[derive(Debug, Clone)]
pub(super) struct CachedJwks {
    pub(super) jwks: JwkSet,
    pub(super) expires_at: Instant,
}

#[derive(Debug, Clone)]
pub(super) struct CachedDiscovery {
    pub(super) jwks_url: String,
    pub(super) expires_at: Instant,
}

#[derive(Debug, Deserialize)]
struct DiscoveryDocument {
    jwks_uri: String,
}

impl UpstreamOidcValidator {
    pub(super) async fn resolve_jwks_url(
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

    pub(super) async fn get_jwks(&self, jwks_url: &str) -> Result<JwkSet, OidcError> {
        if let Some(entry) = self.jwks_cache.get(jwks_url)
            && entry.expires_at > Instant::now()
        {
            return Ok(entry.jwks.clone());
        }
        self.refresh_jwks(jwks_url).await
    }

    pub(super) async fn refresh_jwks(&self, jwks_url: &str) -> Result<JwkSet, OidcError> {
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

pub(super) fn ensure_jwk_matches_algorithm(
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

pub(super) fn expected_key_algorithm(alg: Algorithm) -> Option<KeyAlgorithm> {
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

pub(super) fn find_jwk<'a>(jwks: &'a JwkSet, kid: &str) -> Option<&'a jsonwebtoken::jwk::Jwk> {
    jwks.keys
        .iter()
        .find(|key| key.common.key_id.as_deref() == Some(kid))
}
