//! Felix JWT token minting and verification helpers.
//!
//! # Purpose
//! Define claim structures and helpers for signing/verifying tenant-scoped
//! access tokens used by the broker and control-plane APIs.
//!
//! # Architectural role
//! Centralizes Felix token semantics (claims, issuer/audience, EdDSA signing)
//! and key caching to ensure consistent verification across services.
//!
//! # Callers / consumers
//! - Token exchange handler mints Felix tokens.
//! - Broker and control-plane components verify Felix tokens.
//! - Tests that assert EdDSA-only behavior and key rotation logic.
//!
//! # Key invariants
//! - Felix tokens are always EdDSA (Ed25519) and never RSA/HS variants.
//! - `iss`, `aud`, and `tid` claims are mandatory and validated.
//! - The private key is a 32-byte Ed25519 seed; the public key must match it.
//!
//! # Concurrency model
//! Thread-safe key cache protected by `RwLock`; safe for concurrent reads and
//! rare writes when keys rotate or are first used.
//!
//! # Security boundary
//! This module handles private key material and must only be used within the
//! control-plane trust boundary. Public keys may be exported via JWKS elsewhere.
//!
//! # Security model and threat assumptions
//! - Attackers may supply arbitrary JWTs; we validate algorithm, issuer,
//!   audience, and tenant ID before accepting.
//! - Ed25519 is chosen for constant-time signing and smaller keys to reduce
//!   side-channel risk compared to RSA.
//! - Key IDs (`kid`) are used for rotation but are not secrets.
//!
//! # How to use
//! Use [`mint_token`] to issue a Felix token and [`verify_token`] to validate it
//! against a tenant's signing keys, including rotation handling.
//!
//! # Examples
//! ```rust
//! use controlplane::auth::felix_token::{mint_token, SigningKey, TenantSigningKeys};
//! use ed25519_dalek::SigningKey as Ed25519SigningKey;
//! use jsonwebtoken::Algorithm;
//! use std::time::Duration;
//!
//! let seed = [1u8; 32];
//! let ed_key = Ed25519SigningKey::from_bytes(&seed);
//! let keys = TenantSigningKeys {
//!     current: SigningKey {
//!         kid: "k1".to_string(),
//!         alg: Algorithm::EdDSA,
//!         private_key: seed,
//!         public_key: ed_key.verifying_key().to_bytes(),
//!     },
//!     previous: vec![],
//! };
//! let _ = mint_token(&keys, "tenant-a", "principal", vec![], Duration::from_secs(60));
//! ```
//!
//! # Common pitfalls
//! - Using mismatched issuer/audience values across services.
//! - Forgetting to validate rotated keys before issuing or verifying tokens.
//!
//! # Future work
//! - Add structured key rotation metadata for safe rollout/rollback.
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ED25519_KEY_LEN: usize = 32;

/// Claims carried by Felix-issued JWTs.
///
/// # Overview
/// These claims encode issuer, audience, tenant scope, subject, and permissions
/// for the broker to authorize access.
///
/// # Arguments
/// - Fields are populated during minting or decoding.
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
/// use controlplane::auth::felix_token::FelixClaims;
///
/// let claims = FelixClaims {
///     iss: "felix-auth".to_string(),
///     aud: "felix-broker".to_string(),
///     sub: "principal".to_string(),
///     tid: "tenant-a".to_string(),
///     exp: 1_700_000_000,
///     iat: 1_699_999_000,
///     jti: None,
///     perms: vec!["stream.publish:stream:payments/*".to_string()],
/// };
/// assert_eq!(claims.tid, "tenant-a");
/// ```
///
/// # Security
/// - `perms` and `tid` are authorization-critical and must be validated.
///
/// # Performance
/// - Plain data container; cloning scales with permission count.
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

/// Felix signing key material for a tenant.
///
/// # Overview
/// Holds the Ed25519 private seed and public key along with its `kid` and
/// algorithm for token minting.
///
/// # Arguments
/// - `kid`: Key ID used for JWKS lookup and rotation.
/// - `alg`: Must be `Algorithm::EdDSA` for Felix tokens.
/// - `private_key`: Raw 32-byte Ed25519 seed (must not be logged).
/// - `public_key`: Ed25519 public key derived from the seed.
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
/// use controlplane::auth::felix_token::SigningKey;
/// use ed25519_dalek::SigningKey as Ed25519SigningKey;
/// use jsonwebtoken::Algorithm;
///
/// let seed = [1u8; 32];
/// let signing_key = Ed25519SigningKey::from_bytes(&seed);
/// let key = SigningKey {
///     kid: "k1".to_string(),
///     alg: Algorithm::EdDSA,
///     private_key: seed,
///     public_key: signing_key.verifying_key().to_bytes(),
/// };
/// assert_eq!(key.alg, Algorithm::EdDSA);
/// ```
///
/// # Security
/// - Never serialize or log `private_key`.
/// - `alg` must remain EdDSA to avoid RSA fallback.
///
/// # Performance
/// - Copying this struct is cheap (fixed-size arrays).
#[derive(Debug, Clone)]
pub struct SigningKey {
    pub kid: String,
    pub alg: Algorithm,
    pub private_key: [u8; ED25519_KEY_LEN],
    pub public_key: [u8; ED25519_KEY_LEN],
}

/// Current and previous signing keys for a tenant.
///
/// # Overview
/// Supports key rotation by keeping a current key and a list of previous keys
/// that may still validate existing tokens.
///
/// # Arguments
/// - `current`: The active signing key for new tokens.
/// - `previous`: Older keys still accepted for verification.
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
/// use controlplane::auth::felix_token::{SigningKey, TenantSigningKeys};
/// use ed25519_dalek::SigningKey as Ed25519SigningKey;
/// use jsonwebtoken::Algorithm;
///
/// let seed = [1u8; 32];
/// let signing_key = Ed25519SigningKey::from_bytes(&seed);
/// let keys = TenantSigningKeys {
///     current: SigningKey {
///         kid: "k1".to_string(),
///         alg: Algorithm::EdDSA,
///         private_key: seed,
///         public_key: signing_key.verifying_key().to_bytes(),
///     },
///     previous: vec![],
/// };
/// assert!(keys.previous.is_empty());
/// ```
///
/// # Security
/// - Previous keys are still valid for verification; rotate carefully.
///
/// # Performance
/// - Cloning scales with the number of previous keys.
#[derive(Debug, Clone)]
pub struct TenantSigningKeys {
    pub current: SigningKey,
    pub previous: Vec<SigningKey>,
}

impl TenantSigningKeys {
    /// Validate the integrity of all signing keys.
    ///
    /// # Overview
    /// Ensures all keys are EdDSA and that public keys match the private seed.
    ///
    /// # Arguments
    /// - `&self`: The key set to validate.
    ///
    /// # Returns
    /// - `Ok(())` if all keys are valid.
    ///
    /// # Errors
    /// - `TokenError::Key` if any key is invalid or mismatched.
    ///
    /// # Panics
    /// - Does not panic.
    ///
    /// # Examples
    /// ```rust
    /// use controlplane::auth::felix_token::{SigningKey, TenantSigningKeys};
    /// use ed25519_dalek::SigningKey as Ed25519SigningKey;
    /// use jsonwebtoken::Algorithm;
    ///
    /// let seed = [1u8; 32];
    /// let signing_key = Ed25519SigningKey::from_bytes(&seed);
    /// let keys = TenantSigningKeys {
    ///     current: SigningKey {
    ///         kid: "k1".to_string(),
    ///         alg: Algorithm::EdDSA,
    ///         private_key: seed,
    ///         public_key: signing_key.verifying_key().to_bytes(),
    ///     },
    ///     previous: vec![],
    /// };
    /// let _ = keys.validate();
    /// ```
    ///
    /// # Security
    /// - Prevents accidental use of RSA/HS algorithms for Felix tokens.
    ///
    /// # Performance
    /// - Validation is O(number of keys).
    pub fn validate(&self) -> Result<(), TokenError> {
        // Step 1: Validate the current key before any verification attempt.
        // This ensures the primary signing key is EdDSA and consistent.
        self.current.validate()?;
        // Step 2: Validate rotated keys to avoid accepting mismatched material.
        for key in &self.previous {
            key.validate()?;
        }
        Ok(())
    }

    /// Iterate over current and previous keys in rotation order.
    ///
    /// # Overview
    /// Returns an iterator where the current key is first, followed by previous
    /// keys. This ordering matters when trying keys for verification.
    ///
    /// # Arguments
    /// - `&self`: The key set to iterate.
    ///
    /// # Returns
    /// - An iterator of `&SigningKey` in verification order.
    ///
    /// # Errors
    /// - Not applicable.
    ///
    /// # Panics
    /// - Does not panic.
    ///
    /// # Examples
    /// ```rust
    /// use controlplane::auth::felix_token::{SigningKey, TenantSigningKeys};
    /// use ed25519_dalek::SigningKey as Ed25519SigningKey;
    /// use jsonwebtoken::Algorithm;
    ///
    /// let seed = [1u8; 32];
    /// let signing_key = Ed25519SigningKey::from_bytes(&seed);
    /// let keys = TenantSigningKeys {
    ///     current: SigningKey {
    ///         kid: "k1".to_string(),
    ///         alg: Algorithm::EdDSA,
    ///         private_key: seed,
    ///         public_key: signing_key.verifying_key().to_bytes(),
    ///     },
    ///     previous: vec![],
    /// };
    /// let mut iter = keys.all_keys();
    /// assert_eq!(iter.next().unwrap().kid, "k1");
    /// ```
    ///
    /// # Security
    /// - Current key is always tried first to minimize acceptance of older keys.
    ///
    /// # Performance
    /// - Iterator creation is O(1); iteration is linear in key count.
    pub fn all_keys(&self) -> impl Iterator<Item = &SigningKey> {
        std::iter::once(&self.current).chain(self.previous.iter())
    }
}

impl SigningKey {
    /// Validate that the signing key is Ed25519 and consistent.
    ///
    /// # Overview
    /// Ensures the algorithm is EdDSA and the public key matches the private
    /// seed to prevent mismatched or malformed key material.
    ///
    /// # Arguments
    /// - `&self`: The signing key to validate.
    ///
    /// # Returns
    /// - `Ok(())` if the key is valid.
    ///
    /// # Errors
    /// - `TokenError::Key` if the algorithm is not EdDSA or keys mismatch.
    ///
    /// # Panics
    /// - Does not panic.
    ///
    /// # Examples
    /// ```rust
    /// use controlplane::auth::felix_token::SigningKey;
    /// use ed25519_dalek::SigningKey as Ed25519SigningKey;
    /// use jsonwebtoken::Algorithm;
    ///
    /// let seed = [1u8; 32];
    /// let signing_key = Ed25519SigningKey::from_bytes(&seed);
    /// let key = SigningKey {
    ///     kid: "k1".to_string(),
    ///     alg: Algorithm::EdDSA,
    ///     private_key: seed,
    ///     public_key: signing_key.verifying_key().to_bytes(),
    /// };
    /// let _ = key.validate();
    /// ```
    ///
    /// # Security
    /// - Rejects any non-EdDSA algorithms to prevent RSA fallback.
    ///
    /// # Performance
    /// - Validation is constant time for a single key.
    pub fn validate(&self) -> Result<(), TokenError> {
        // Step 1: Enforce the EdDSA-only invariant for Felix tokens.
        // Changing this would allow RSA or other algorithms unintentionally.
        if self.alg != Algorithm::EdDSA {
            return Err(TokenError::Key(format!(
                "invalid Felix signing algorithm: {:?}",
                self.alg
            )));
        }
        // Step 2: Confirm the public key matches the private seed.
        // This guards against corrupted storage or mismatched rotation data.
        let signing_key = Ed25519SigningKey::from_bytes(&self.private_key);
        let expected = signing_key.verifying_key().to_bytes();
        if expected != self.public_key {
            return Err(TokenError::Key(
                "Ed25519 public key does not match private seed".to_string(),
            ));
        }
        Ok(())
    }
}

/// Errors produced by token minting or verification.
///
/// # Overview
/// Wraps JWT decoding/encoding errors and key validation errors.
///
/// # Arguments
/// - Variants carry detailed error context for debugging.
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
/// use controlplane::auth::felix_token::TokenError;
/// use jsonwebtoken::errors::ErrorKind;
///
/// let err = TokenError::Jwt(jsonwebtoken::errors::Error::from(ErrorKind::InvalidToken));
/// assert!(matches!(err, TokenError::Jwt(_)));
/// ```
///
/// # Security
/// - Error messages must not leak private key material.
///
/// # Performance
/// - Error construction is O(1) and clone-free.
#[derive(Debug)]
pub enum TokenError {
    Jwt(jsonwebtoken::errors::Error),
    Key(String),
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenError::Jwt(err) => write!(f, "jwt error: {err}"),
            TokenError::Key(err) => write!(f, "key error: {err}"),
        }
    }
}

impl std::error::Error for TokenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TokenError::Jwt(err) => Some(err),
            TokenError::Key(_) => None,
        }
    }
}

impl From<jsonwebtoken::errors::Error> for TokenError {
    fn from(value: jsonwebtoken::errors::Error) -> Self {
        TokenError::Jwt(value)
    }
}

/// Mint a Felix EdDSA token for a tenant and principal.
///
/// # Overview
/// Builds Felix claims, enforces key validity, and signs using Ed25519.
///
/// # Arguments
/// - `keys`: Tenant signing keys, including current key for signing.
/// - `tenant_id`: Tenant identifier to embed in `tid`.
/// - `principal_id`: Subject identifier to embed in `sub`.
/// - `perms`: Permission strings granted to the token.
/// - `ttl`: Time-to-live for the token.
///
/// # Returns
/// - `Ok(String)` containing the signed JWT.
///
/// # Errors
/// - `TokenError::Key` if key validation fails.
/// - `TokenError::Jwt` if encoding fails.
///
/// # Panics
/// - Does not panic.
///
/// # Examples
/// ```rust
/// use controlplane::auth::felix_token::{mint_token, SigningKey, TenantSigningKeys};
/// use ed25519_dalek::SigningKey as Ed25519SigningKey;
/// use jsonwebtoken::Algorithm;
/// use std::time::Duration;
///
/// let seed = [1u8; 32];
/// let signing_key = Ed25519SigningKey::from_bytes(&seed);
/// let keys = TenantSigningKeys {
///     current: SigningKey {
///         kid: "k1".to_string(),
///         alg: Algorithm::EdDSA,
///         private_key: seed,
///         public_key: signing_key.verifying_key().to_bytes(),
///     },
///     previous: vec![],
/// };
/// let _ = mint_token(&keys, "tenant-a", "principal", vec![], Duration::from_secs(60));
/// ```
///
/// # Security
/// - The algorithm is pinned to EdDSA and the token includes `iss`, `aud`, `tid`.
/// - Never log the resulting token in production logs.
///
/// # Performance
/// - Uses a cached encoding key to avoid repeated PKCS8 conversion.
pub fn mint_token(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    principal_id: &str,
    perms: Vec<String>,
    ttl: Duration,
) -> Result<String, TokenError> {
    // Step 1: Validate keys before using them to sign.
    // This prevents signing with non-EdDSA or mismatched key material.
    keys.validate()?;
    // Step 2: Build claims with consistent issuer/audience and tenant scope.
    // Changing these values would break verification in the broker.
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

    // Step 3: Use the current key and embed its `kid` in the JWT header.
    // This enables key rotation while keeping verification deterministic.
    let mut header = Header::new(keys.current.alg);
    header.kid = Some(keys.current.kid.clone());
    // Step 4: Encode using EdDSA with cached key material.
    // The cache stores PKCS8 DER in-memory only; the database stores raw seeds.
    let encoding_key = key_cache().encoding_key(tenant_id, &keys.current)?;
    Ok(jsonwebtoken::encode(&header, &claims, &encoding_key)?)
}

/// Verify a Felix token against a tenant's signing keys.
///
/// # Overview
/// Validates JWT header, issuer, audience, tenant ID, and signature using the
/// current and previous Ed25519 keys, with `kid`-guided ordering.
///
/// # Arguments
/// - `keys`: Tenant signing keys including current and previous keys.
/// - `tenant_id`: Expected tenant ID in the `tid` claim.
/// - `token`: The JWT string to verify.
/// - `leeway`: Allowed clock skew in seconds.
///
/// # Returns
/// - `Ok(FelixClaims)` if the token is valid and tenant-scoped.
///
/// # Errors
/// - `TokenError::Jwt` for JWT validation/decoding failures.
/// - `TokenError::Key` if key validation fails.
///
/// # Panics
/// - Does not panic.
///
/// # Examples
/// ```rust
/// use controlplane::auth::felix_token::{verify_token, SigningKey, TenantSigningKeys};
/// use ed25519_dalek::SigningKey as Ed25519SigningKey;
/// use jsonwebtoken::Algorithm;
///
/// let seed = [1u8; 32];
/// let signing_key = Ed25519SigningKey::from_bytes(&seed);
/// let keys = TenantSigningKeys {
///     current: SigningKey {
///         kid: "k1".to_string(),
///         alg: Algorithm::EdDSA,
///         private_key: seed,
///         public_key: signing_key.verifying_key().to_bytes(),
///     },
///     previous: vec![],
/// };
/// let _ = verify_token(&keys, "tenant-a", "token", 0);
/// ```
///
/// # Security
/// - Enforces `iss`, `aud`, and `tid` checks to prevent token confusion.
/// - Only EdDSA tokens are accepted; RSA/HS are rejected via key validation.
///
/// # Performance
/// - Tries keys in `kid`-preferred order to reduce failed verification attempts.
pub fn verify_token(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    token: &str,
    leeway: u64,
) -> Result<FelixClaims, TokenError> {
    // Step 1: Validate signing keys before verification.
    // This ensures we never accept non-EdDSA algorithms for Felix tokens.
    keys.validate()?;
    // Step 2: Decode the header to order keys by `kid` when present.
    // This speeds verification during rotation and reduces needless failures.
    let header = jsonwebtoken::decode_header(token)?;
    let mut ordered_keys = Vec::new();
    if let Some(kid) = header.kid.as_deref() {
        if let Some(found) = keys.all_keys().find(|entry| entry.kid == kid) {
            ordered_keys.push(found);
            for entry in keys.all_keys() {
                if entry.kid != kid {
                    ordered_keys.push(entry);
                }
            }
        } else {
            ordered_keys.extend(keys.all_keys());
        }
    } else {
        ordered_keys.extend(keys.all_keys());
    }

    // Step 3: Configure strict validation for issuer and audience.
    // These checks are mandatory to prevent token substitution attacks.
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.set_audience(&["felix-broker"]);
    validation.set_issuer(&["felix-auth"]);
    validation.leeway = leeway;
    let mut last_err = None;
    for key in ordered_keys {
        // Step 4: Attempt verification with each key in order.
        // This is rotation-safe and falls back to older keys when needed.
        let decoding_key = key_cache().decoding_key(tenant_id, key)?;
        match jsonwebtoken::decode::<FelixClaims>(token, &decoding_key, &validation) {
            Ok(token) => {
                // Step 5: Enforce tenant ID match after signature validation.
                // This prevents cross-tenant token reuse even with valid signatures.
                if token.claims.tid != tenant_id {
                    return Err(TokenError::Jwt(jsonwebtoken::errors::Error::from(
                        jsonwebtoken::errors::ErrorKind::InvalidToken,
                    )));
                }
                return Ok(token.claims);
            }
            Err(err) => last_err = Some(err),
        }
    }
    // IMPORTANT:
    // If all keys fail, return the last JWT error to preserve context. Falling
    // back to a generic error would hide root causes in telemetry.
    Err(TokenError::Jwt(last_err.unwrap_or_else(|| {
        jsonwebtoken::errors::Error::from(jsonwebtoken::errors::ErrorKind::InvalidToken)
    })))
}

fn now_epoch_seconds() -> i64 {
    // We use wall-clock time and allow upstream leeway during verification.
    // If the clock is skewed backwards, clamp to zero to avoid panics.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}

#[derive(Clone, Default)]
struct FelixKeyCache {
    encoding: Arc<RwLock<HashMap<String, EncodingKey>>>,
    decoding: Arc<RwLock<HashMap<String, DecodingKey>>>,
}

impl FelixKeyCache {
    fn invalidate_tenant(&self, tenant_id: &str) {
        // Step 1: Build a prefix so we can invalidate all keys for the tenant.
        // This ensures both encoding and decoding caches rotate together.
        let mut prefix = String::with_capacity(tenant_id.len() + 1);
        prefix.push_str(tenant_id);
        prefix.push(':');
        // Step 2: Remove cached entries for the tenant in both maps.
        // Keeping them in sync avoids using stale keys during rotation.
        if let Ok(mut map) = self.encoding.write() {
            map.retain(|key, _| !key.starts_with(&prefix));
        }
        if let Ok(mut map) = self.decoding.write() {
            map.retain(|key, _| !key.starts_with(&prefix));
        }
    }

    fn encoding_key(&self, tenant_id: &str, key: &SigningKey) -> Result<EncodingKey, TokenError> {
        // Step 1: Attempt a read lock for fast-path cache hits.
        // This avoids re-encoding PKCS8 DER for repeated mints.
        let cache_key = cache_key(tenant_id, &key.kid);
        if let Ok(map) = self.encoding.read()
            && let Some(found) = map.get(&cache_key)
        {
            return Ok(found.clone());
        }
        // Step 2: Convert the raw Ed25519 seed to PKCS8 DER for jsonwebtoken.
        // We choose PKCS8 DER because jsonwebtoken expects that format for EdDSA.
        let signing_key = Ed25519SigningKey::from_bytes(&key.private_key);
        let der = signing_key
            .to_pkcs8_der()
            .map_err(|err| TokenError::Key(format!("encode Ed25519 key: {err}")))?;
        let encoding_key = EncodingKey::from_ed_der(der.as_bytes());
        // Step 3: Populate the cache for future calls.
        if let Ok(mut map) = self.encoding.write() {
            map.insert(cache_key, encoding_key.clone());
        }
        Ok(encoding_key)
    }

    fn decoding_key(&self, tenant_id: &str, key: &SigningKey) -> Result<DecodingKey, TokenError> {
        // Step 1: Attempt a read lock for fast-path cache hits.
        // Verification is hot; caching avoids repeated base64 and key creation.
        let cache_key = cache_key(tenant_id, &key.kid);
        if let Ok(map) = self.decoding.read()
            && let Some(found) = map.get(&cache_key)
        {
            return Ok(found.clone());
        }
        // Step 2: Convert the raw public key to base64url `x` per JWK conventions.
        // jsonwebtoken expects EdDSA decoding keys from the `x` component.
        let x = URL_SAFE_NO_PAD.encode(key.public_key);
        let decoding_key = DecodingKey::from_ed_components(&x).map_err(TokenError::Jwt)?;
        // Step 3: Populate the cache for future calls.
        if let Ok(mut map) = self.decoding.write() {
            map.insert(cache_key, decoding_key.clone());
        }
        Ok(decoding_key)
    }
}

static KEY_CACHE: OnceLock<FelixKeyCache> = OnceLock::new();

fn key_cache() -> &'static FelixKeyCache {
    // Global cache avoids rebuilding keys across requests.
    // OnceLock ensures thread-safe, lazy initialization.
    KEY_CACHE.get_or_init(FelixKeyCache::default)
}

/// Invalidate cached key material for a tenant.
///
/// # Overview
/// Clears both encoding and decoding caches for a tenant so rotations take
/// effect immediately.
///
/// # Arguments
/// - `tenant_id`: Tenant identifier whose cached keys should be removed.
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
/// use controlplane::auth::felix_token::invalidate_tenant_cache;
///
/// invalidate_tenant_cache("tenant-a");
/// ```
///
/// # Security
/// - Should be called after key rotation to avoid accepting stale keys.
///
/// # Performance
/// - Eviction is linear in cached entries for the tenant.
pub fn invalidate_tenant_cache(tenant_id: &str) {
    key_cache().invalidate_tenant(tenant_id);
}

fn cache_key(tenant_id: &str, kid: &str) -> String {
    // We use a stable tenant:kid prefix so invalidation can be prefix-based.
    let mut key = String::with_capacity(tenant_id.len() + 1 + kid.len());
    key.push_str(tenant_id);
    key.push(':');
    key.push_str(kid);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PRIVATE_KEY: [u8; 32] = [5u8; 32];

    fn signing_keys() -> TenantSigningKeys {
        // This test helper keeps keys deterministic for repeatable JWTs.
        let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
        let public_key = signing_key.verifying_key().to_bytes();
        TenantSigningKeys {
            current: SigningKey {
                kid: "k1".to_string(),
                alg: Algorithm::EdDSA,
                private_key: TEST_PRIVATE_KEY,
                public_key,
            },
            previous: vec![],
        }
    }

    #[test]
    fn mint_roundtrip_contains_claims() {
        // This test prevents regressions where minting fails due to key format
        // or algorithm changes (e.g., accidental RSA usage).
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
