//! Felix token minting and verification for the broker/control-plane boundary.
//!
//! # Purpose
//! Provide shared, EdDSA-only JWT minting and verification logic for Felix
//! tokens, plus key caching to reduce per-request overhead.
//!
//! # Key invariants
//! - Felix tokens are always EdDSA (Ed25519); RSA/HS algorithms are rejected.
//! - `iss`, `aud`, and `tid` claims are mandatory and validated.
//! - Public keys must match the stored 32-byte Ed25519 seed.
//!
//! # Security model / threat assumptions
//! - Attackers may present arbitrary JWTs; we validate algorithm, issuer,
//!   audience, tenant ID, and signature before accepting.
//! - Private keys are never serialized in this module; only in-memory PKCS8
//!   DER is constructed for `jsonwebtoken`.
//! - `kid` is used for rotation and caching, not as a secret.
//!
//! # Concurrency + ordering guarantees
//! - Encoding/decoding key caches use `RwLock` for concurrent readers.
//! - Verification attempts keys in `kid`-preferred order to reduce failures.
//!
//! # How to use
//! Use [`FelixTokenIssuer`] to mint tokens and [`FelixTokenVerifier`] to verify
//! them, providing a [`TenantKeyStore`] implementation for key access.
//!
//! # Examples
//! ```rust
//! use felix_authz::{FelixTokenIssuer, TenantId, TenantKeyMaterial, Jwks};
//! use jsonwebtoken::Algorithm;
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! let mut store = HashMap::new();
//! store.insert("t1".to_string(), TenantKeyMaterial {
//!     kid: "k1".to_string(),
//!     alg: Algorithm::EdDSA,
//!     private_key: [1u8; 32],
//!     public_key: [2u8; 32],
//!     jwks: Jwks { keys: vec![] },
//! });
//! let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", Duration::from_secs(60), Arc::new(store));
//! let _ = issuer.mint(&TenantId::new("t1"), "principal", vec![]);
//! ```
//!
//! # Common pitfalls
//! - Using mismatched issuer/audience values between issuer and verifier.
//! - Forgetting to invalidate the cache when rotating keys.
//! - Passing non-Ed25519 keys, which are explicitly rejected.
//!
//! # Future work
//! - Add background JWKS refresh and cache eviction policies.
use crate::{AuthzError, AuthzResult, Jwks, TenantId};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ED25519_KEY_LEN: usize = 32;

/// Claims carried by Felix-issued JWTs.
///
/// # What it does
/// Encodes issuer, audience, tenant scope, subject, and permissions for broker
/// authorization.
///
/// # Inputs/outputs
/// - Inputs: populated during minting or decoding.
/// - Outputs: used by the broker to enforce permissions.
///
/// # Errors
/// - Not applicable (data container).
///
/// # Security notes
/// - `tid` and `perms` are authorization-critical and must be validated.
///
/// # Performance
/// - Plain data container; cloning scales with permission count.
///
/// # Example
/// ```rust
/// use felix_authz::FelixClaims;
///
/// let claims = FelixClaims {
///     iss: "felix-auth".to_string(),
///     aud: "felix-broker".to_string(),
///     sub: "principal".to_string(),
///     tid: "tenant-a".to_string(),
///     exp: 1_700_000_000,
///     iat: 1_699_999_000,
///     jti: None,
///     perms: vec!["stream.publish:stream:tenant-a/payments/*".to_string()],
/// };
/// assert_eq!(claims.tid, "tenant-a");
/// ```
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

/// Tenant signing key material for Felix token minting.
///
/// # What it does
/// Holds Ed25519 private seed and public key along with `kid` and algorithm.
///
/// # Inputs/outputs
/// - Inputs: loaded from a [`TenantKeyStore`].
/// - Outputs: used to create encoding keys for JWT signing.
///
/// # Errors
/// - Not applicable (data container).
///
/// # Security notes
/// - Never serialize or log `private_key`.
/// - `alg` must remain `Algorithm::EdDSA` for Felix tokens.
///
/// # Performance
/// - Copying this struct is cheap (fixed-size arrays).
///
/// # Example
/// ```rust
/// use ed25519_dalek::SigningKey;
/// use felix_authz::TenantSigningKey;
/// use jsonwebtoken::Algorithm;
///
/// let seed = [1u8; 32];
/// let signing_key = SigningKey::from_bytes(&seed);
/// let key = TenantSigningKey {
///     kid: "k1".to_string(),
///     alg: Algorithm::EdDSA,
///     private_key: seed,
///     public_key: signing_key.verifying_key().to_bytes(),
/// };
/// assert_eq!(key.alg, Algorithm::EdDSA);
/// ```
#[derive(Clone)]
pub struct TenantSigningKey {
    pub kid: String,
    pub alg: Algorithm,
    pub private_key: [u8; ED25519_KEY_LEN],
    pub public_key: [u8; ED25519_KEY_LEN],
}

/// Tenant verification key material for Felix token verification.
///
/// # What it does
/// Holds Ed25519 public key and metadata used to verify Felix tokens.
///
/// # Inputs/outputs
/// - Inputs: derived from JWKS or a key store.
/// - Outputs: used to build decoding keys for verification.
///
/// # Errors
/// - Not applicable (data container).
///
/// # Security notes
/// - `alg` must be EdDSA; RSA/HS are rejected.
///
/// # Performance
/// - Copying this struct is cheap (fixed-size arrays).
///
/// # Example
/// ```rust
/// use felix_authz::TenantVerificationKey;
/// use jsonwebtoken::Algorithm;
///
/// let key = TenantVerificationKey {
///     kid: "k1".to_string(),
///     alg: Algorithm::EdDSA,
///     public_key: [2u8; 32],
/// };
/// assert_eq!(key.kid, "k1");
/// ```
#[derive(Clone)]
pub struct TenantVerificationKey {
    pub kid: String,
    pub alg: Algorithm,
    pub public_key: [u8; ED25519_KEY_LEN],
}

/// Key store abstraction for Felix token signing and verification.
///
/// # What it does
/// Provides signing keys, verification keys, and JWKS for a tenant.
///
/// # Inputs/outputs
/// - Inputs: tenant identifier.
/// - Outputs: key material or JWKS for that tenant.
///
/// # Errors
/// - Implementations return [`AuthzError`] for missing or invalid key material.
///
/// # Security notes
/// - Implementations must never expose private keys outside trusted boundaries.
///
/// # Concurrency
/// - Implementations must be thread-safe (`Send + Sync`) for shared use.
///
/// # Example
/// ```rust
/// use felix_authz::{TenantId, TenantKeyStore, TenantKeyMaterial, Jwks};
/// use jsonwebtoken::Algorithm;
/// use std::collections::HashMap;
///
/// let mut store: HashMap<String, TenantKeyMaterial> = HashMap::new();
/// store.insert("t1".to_string(), TenantKeyMaterial {
///     kid: "k1".to_string(),
///     alg: Algorithm::EdDSA,
///     private_key: [1u8; 32],
///     public_key: [2u8; 32],
///     jwks: Jwks { keys: vec![] },
/// });
/// let _ = store.jwks(&TenantId::new("t1"));
/// ```
pub trait TenantKeyStore: Send + Sync {
    /// Return the current signing key for a tenant.
    ///
    /// # What it does
    /// Provides the active Ed25519 signing key used to mint new Felix tokens.
    ///
    /// # Inputs/outputs
    /// - Input: `tenant_id`.
    /// - Output: [`TenantSigningKey`].
    ///
    /// # Errors
    /// - [`AuthzError::MissingSigningKey`] when the key is absent.
    /// - [`AuthzError::Key`] if the key is invalid or uses a disallowed algorithm.
    ///
    /// # Security notes
    /// - Private keys must not leave trusted storage boundaries.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::{TenantId, TenantKeyStore, TenantKeyMaterial, Jwks};
    /// use jsonwebtoken::Algorithm;
    /// use std::collections::HashMap;
    ///
    /// let mut store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    /// store.insert("t1".to_string(), TenantKeyMaterial {
    ///     kid: "k1".to_string(),
    ///     alg: Algorithm::EdDSA,
    ///     private_key: [1u8; 32],
    ///     public_key: [2u8; 32],
    ///     jwks: Jwks { keys: vec![] },
    /// });
    /// let _ = store.current_signing_key(&TenantId::new("t1"));
    /// ```
    fn current_signing_key(&self, tenant_id: &TenantId) -> AuthzResult<TenantSigningKey>;
    /// Return verification keys for a tenant.
    ///
    /// # What it does
    /// Provides Ed25519 public keys used to verify Felix tokens.
    ///
    /// # Inputs/outputs
    /// - Input: `tenant_id`.
    /// - Output: list of [`TenantVerificationKey`].
    ///
    /// # Errors
    /// - [`AuthzError::MissingVerificationKeys`] when keys are absent.
    /// - [`AuthzError::Key`] if any key is invalid.
    ///
    /// # Security notes
    /// - Only EdDSA keys should be returned.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::{TenantId, TenantKeyStore, TenantKeyMaterial, Jwks};
    /// use jsonwebtoken::Algorithm;
    /// use std::collections::HashMap;
    ///
    /// let mut store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    /// store.insert("t1".to_string(), TenantKeyMaterial {
    ///     kid: "k1".to_string(),
    ///     alg: Algorithm::EdDSA,
    ///     private_key: [1u8; 32],
    ///     public_key: [2u8; 32],
    ///     jwks: Jwks { keys: vec![] },
    /// });
    /// let _ = store.verification_keys(&TenantId::new("t1"));
    /// ```
    fn verification_keys(&self, tenant_id: &TenantId) -> AuthzResult<Vec<TenantVerificationKey>>;
    /// Return the JWKS payload for a tenant.
    ///
    /// # What it does
    /// Provides the JWKS JSON structure containing public key data.
    ///
    /// # Inputs/outputs
    /// - Input: `tenant_id`.
    /// - Output: [`Jwks`] payload.
    ///
    /// # Errors
    /// - [`AuthzError::MissingJwks`] when JWKS is absent.
    ///
    /// # Security notes
    /// - JWKS must never contain private key material.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::{TenantId, TenantKeyStore, TenantKeyMaterial, Jwks};
    /// use jsonwebtoken::Algorithm;
    /// use std::collections::HashMap;
    ///
    /// let mut store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    /// store.insert("t1".to_string(), TenantKeyMaterial {
    ///     kid: "k1".to_string(),
    ///     alg: Algorithm::EdDSA,
    ///     private_key: [1u8; 32],
    ///     public_key: [2u8; 32],
    ///     jwks: Jwks { keys: vec![] },
    /// });
    /// let _ = store.jwks(&TenantId::new("t1"));
    /// ```
    fn jwks(&self, tenant_id: &TenantId) -> AuthzResult<Jwks>;
}

/// In-memory tenant key material with JWKS payload.
///
/// # What it does
/// Bundles tenant key data and the JWKS representation for convenience.
///
/// # Inputs/outputs
/// - Inputs: typically constructed in tests or in-memory stores.
/// - Outputs: returned via [`TenantKeyStore`] methods.
///
/// # Errors
/// - Not applicable (data container).
///
/// # Security notes
/// - `private_key` must never be logged.
///
/// # Performance
/// - Copying this struct is cheap (fixed-size arrays).
///
/// # Example
/// ```rust
/// use felix_authz::{TenantKeyMaterial, Jwks};
/// use jsonwebtoken::Algorithm;
///
/// let material = TenantKeyMaterial {
///     kid: "k1".to_string(),
///     alg: Algorithm::EdDSA,
///     private_key: [1u8; 32],
///     public_key: [2u8; 32],
///     jwks: Jwks { keys: vec![] },
/// };
/// assert_eq!(material.kid, "k1");
/// ```
#[derive(Clone)]
pub struct TenantKeyMaterial {
    pub kid: String,
    pub alg: Algorithm,
    pub private_key: [u8; ED25519_KEY_LEN],
    pub public_key: [u8; ED25519_KEY_LEN],
    pub jwks: Jwks,
}

impl TenantKeyStore for HashMap<String, TenantKeyMaterial> {
    fn current_signing_key(&self, tenant_id: &TenantId) -> AuthzResult<TenantSigningKey> {
        // We validate the algorithm to enforce EdDSA-only signing.
        let entry = self
            .get(tenant_id.as_str())
            .ok_or_else(|| AuthzError::MissingSigningKey(tenant_id.to_string()))?;
        validate_alg(entry.alg)?;
        Ok(TenantSigningKey {
            kid: entry.kid.clone(),
            alg: entry.alg,
            private_key: entry.private_key,
            public_key: entry.public_key,
        })
    }

    fn verification_keys(&self, tenant_id: &TenantId) -> AuthzResult<Vec<TenantVerificationKey>> {
        // We validate the algorithm to prevent RSA/HS downgrade.
        let entry = self
            .get(tenant_id.as_str())
            .ok_or_else(|| AuthzError::MissingVerificationKeys(tenant_id.to_string()))?;
        validate_alg(entry.alg)?;
        Ok(vec![TenantVerificationKey {
            kid: entry.kid.clone(),
            alg: entry.alg,
            public_key: entry.public_key,
        }])
    }

    fn jwks(&self, tenant_id: &TenantId) -> AuthzResult<Jwks> {
        // JWKS is public data but still scoped by tenant.
        let entry = self
            .get(tenant_id.as_str())
            .ok_or_else(|| AuthzError::MissingJwks(tenant_id.to_string()))?;
        Ok(entry.jwks.clone())
    }
}

/// Felix token issuer using Ed25519 signing keys.
///
/// # What it does
/// Mints Felix JWTs with configured issuer/audience, TTL, and key store.
///
/// # Inputs/outputs
/// - Inputs: issuer, audience, TTL, and a [`TenantKeyStore`].
/// - Output: signed JWT strings.
///
/// # Errors
/// - Errors from key lookup or JWT encoding.
///
/// # Security notes
/// - Always uses EdDSA and validates key material before signing.
///
/// # Concurrency
/// - Cloning the issuer shares the underlying cache and key store.
///
/// # Example
/// ```rust
/// use felix_authz::{FelixTokenIssuer, TenantId, TenantKeyMaterial, Jwks};
/// use jsonwebtoken::Algorithm;
/// use std::collections::HashMap;
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// let mut store = HashMap::new();
/// store.insert("t1".to_string(), TenantKeyMaterial {
///     kid: "k1".to_string(),
///     alg: Algorithm::EdDSA,
///     private_key: [1u8; 32],
///     public_key: [2u8; 32],
///     jwks: Jwks { keys: vec![] },
/// });
/// let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", Duration::from_secs(60), Arc::new(store));
/// let _ = issuer.mint(&TenantId::new("t1"), "principal", vec![]);
/// ```
pub struct FelixTokenIssuer {
    issuer: String,
    audience: String,
    ttl: Duration,
    key_store: Arc<dyn TenantKeyStore>,
    cache: Arc<TenantKeyCache>,
}

impl FelixTokenIssuer {
    /// Create a new Felix token issuer.
    ///
    /// # What it does
    /// Stores issuer/audience/TTL and key store used to sign tokens.
    ///
    /// # Inputs/outputs
    /// - Inputs: issuer, audience, TTL, key store.
    /// - Output: [`FelixTokenIssuer`].
    ///
    /// # Errors
    /// - Does not return errors.
    ///
    /// # Security notes
    /// - `issuer` and `audience` must match the verifier configuration.
    ///
    /// # Performance
    /// - Construction is O(1); key parsing happens on first mint.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::{FelixTokenIssuer, TenantKeyMaterial, Jwks};
    /// use jsonwebtoken::Algorithm;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// let mut store = HashMap::new();
    /// store.insert("t1".to_string(), TenantKeyMaterial {
    ///     kid: "k1".to_string(),
    ///     alg: Algorithm::EdDSA,
    ///     private_key: [1u8; 32],
    ///     public_key: [2u8; 32],
    ///     jwks: Jwks { keys: vec![] },
    /// });
    /// let _issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", Duration::from_secs(60), Arc::new(store));
    /// ```
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
            cache: Arc::new(TenantKeyCache::default()),
        }
    }

    /// Attach a shared tenant key cache.
    ///
    /// # What it does
    /// Reuses an existing cache to share encoding/decoding keys across components.
    ///
    /// # Inputs/outputs
    /// - Input: `cache`.
    /// - Output: `self` with updated cache.
    ///
    /// # Errors
    /// - Does not return errors.
    ///
    /// # Security notes
    /// - Shared caches must be invalidated on key rotation.
    ///
    /// # Concurrency
    /// - The cache uses `RwLock` internally for concurrent reads.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::{FelixTokenIssuer, TenantKeyCache, TenantKeyMaterial, Jwks};
    /// use jsonwebtoken::Algorithm;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// let mut store = HashMap::new();
    /// store.insert("t1".to_string(), TenantKeyMaterial {
    ///     kid: "k1".to_string(),
    ///     alg: Algorithm::EdDSA,
    ///     private_key: [1u8; 32],
    ///     public_key: [2u8; 32],
    ///     jwks: Jwks { keys: vec![] },
    /// });
    /// let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", Duration::from_secs(60), Arc::new(store))
    ///     .with_cache(Arc::new(TenantKeyCache::default()));
    /// let _ = issuer;
    /// ```
    pub fn with_cache(mut self, cache: Arc<TenantKeyCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Mint a Felix token for a tenant/principal with permissions.
    ///
    /// # What it does
    /// Builds Felix claims and signs them using the tenant's current Ed25519 key.
    ///
    /// # Inputs/outputs
    /// - Inputs: `tenant_id`, `principal_id`, `perms`.
    /// - Output: signed JWT string.
    ///
    /// # Errors
    /// - Missing signing key or invalid key algorithm.
    /// - JWT encoding errors.
    ///
    /// # Security notes
    /// - Always uses EdDSA; never emits RSA/HS tokens.
    /// - Tokens should be treated as secrets and not logged.
    ///
    /// # Performance
    /// - Uses a cached encoding key to avoid repeated PKCS8 conversion.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::{FelixTokenIssuer, TenantId, TenantKeyMaterial, Jwks};
    /// use jsonwebtoken::Algorithm;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// let mut store = HashMap::new();
    /// store.insert("t1".to_string(), TenantKeyMaterial {
    ///     kid: "k1".to_string(),
    ///     alg: Algorithm::EdDSA,
    ///     private_key: [1u8; 32],
    ///     public_key: [2u8; 32],
    ///     jwks: Jwks { keys: vec![] },
    /// });
    /// let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", Duration::from_secs(60), Arc::new(store));
    /// let _ = issuer.mint(&TenantId::new("t1"), "principal", vec![]);
    /// ```
    pub fn mint(
        &self,
        tenant_id: &TenantId,
        principal_id: &str,
        perms: Vec<String>,
    ) -> AuthzResult<String> {
        // Step 1: Build deterministic claims with issuer/audience/tenant scope.
        // These values must align with the verifier configuration.
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
        // Step 2: Load the current signing key for the tenant.
        // This enforces EdDSA-only signing via `validate_alg`.
        let signing_key = self.key_store.current_signing_key(tenant_id)?;
        // Step 3: Build or reuse encoding keys from the cache.
        // We convert to PKCS8 DER in-memory only; raw seeds remain in storage.
        let encoding_key = self.cache.encoding_key(tenant_id, &signing_key)?;
        let mut header = Header::new(signing_key.alg);
        header.kid = Some(signing_key.kid);
        let token = jsonwebtoken::encode(&header, &claims, &encoding_key)?;
        Ok(token)
    }
}

/// Felix token verifier using Ed25519 public keys.
///
/// # What it does
/// Verifies Felix JWTs against a tenant's verification keys and claim rules.
///
/// # Inputs/outputs
/// - Inputs: issuer, audience, leeway, and a [`TenantKeyStore`].
/// - Output: decoded [`FelixClaims`] on success.
///
/// # Errors
/// - Missing verification keys or JWT validation failures.
///
/// # Security notes
/// - Enforces `iss`, `aud`, and `tid` checks.
/// - Algorithm is pinned to EdDSA.
///
/// # Concurrency
/// - Cloning the verifier shares the underlying cache and key store.
///
/// # Example
/// ```rust
/// use felix_authz::{FelixTokenVerifier, TenantKeyMaterial, Jwks, TenantId};
/// use jsonwebtoken::Algorithm;
/// use std::collections::HashMap;
/// use std::sync::Arc;
///
/// let mut store = HashMap::new();
/// store.insert("t1".to_string(), TenantKeyMaterial {
///     kid: "k1".to_string(),
///     alg: Algorithm::EdDSA,
///     private_key: [1u8; 32],
///     public_key: [2u8; 32],
///     jwks: Jwks { keys: vec![] },
/// });
/// let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 60, Arc::new(store));
/// let _ = verifier.verify(&TenantId::new("t1"), "token");
/// ```
pub struct FelixTokenVerifier {
    issuer: String,
    audience: String,
    leeway: u64,
    key_store: Arc<dyn TenantKeyStore>,
    cache: Arc<TenantKeyCache>,
}

impl FelixTokenVerifier {
    /// Create a new Felix token verifier.
    ///
    /// # What it does
    /// Stores issuer/audience/leeway and key store used for verification.
    ///
    /// # Inputs/outputs
    /// - Inputs: issuer, audience, leeway, key store.
    /// - Output: [`FelixTokenVerifier`].
    ///
    /// # Errors
    /// - Does not return errors.
    ///
    /// # Security notes
    /// - Issuer and audience must match the token issuer configuration.
    ///
    /// # Performance
    /// - Construction is O(1); decoding keys are cached on first verify.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::{FelixTokenVerifier, TenantKeyMaterial, Jwks};
    /// use jsonwebtoken::Algorithm;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    ///
    /// let mut store = HashMap::new();
    /// store.insert("t1".to_string(), TenantKeyMaterial {
    ///     kid: "k1".to_string(),
    ///     alg: Algorithm::EdDSA,
    ///     private_key: [1u8; 32],
    ///     public_key: [2u8; 32],
    ///     jwks: Jwks { keys: vec![] },
    /// });
    /// let _verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 60, Arc::new(store));
    /// ```
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
            cache: Arc::new(TenantKeyCache::default()),
        }
    }

    /// Attach a shared tenant key cache.
    ///
    /// # What it does
    /// Reuses an existing cache to avoid rebuilding decoding keys.
    ///
    /// # Inputs/outputs
    /// - Input: `cache`.
    /// - Output: `self` with updated cache.
    ///
    /// # Errors
    /// - Does not return errors.
    ///
    /// # Security notes
    /// - Cache must be invalidated on key rotation.
    ///
    /// # Concurrency
    /// - The cache uses `RwLock` internally for concurrent reads.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::{FelixTokenVerifier, TenantKeyCache, TenantKeyMaterial, Jwks};
    /// use jsonwebtoken::Algorithm;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    ///
    /// let mut store = HashMap::new();
    /// store.insert("t1".to_string(), TenantKeyMaterial {
    ///     kid: "k1".to_string(),
    ///     alg: Algorithm::EdDSA,
    ///     private_key: [1u8; 32],
    ///     public_key: [2u8; 32],
    ///     jwks: Jwks { keys: vec![] },
    /// });
    /// let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 60, Arc::new(store))
    ///     .with_cache(Arc::new(TenantKeyCache::default()));
    /// let _ = verifier;
    /// ```
    pub fn with_cache(mut self, cache: Arc<TenantKeyCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Verify a Felix token for a tenant.
    ///
    /// # What it does
    /// Validates JWT signature, issuer/audience claims, and tenant ID using the
    /// tenant's verification keys (with `kid`-aware ordering).
    ///
    /// # Inputs/outputs
    /// - Inputs: `tenant_id`, `token`.
    /// - Output: decoded [`FelixClaims`].
    ///
    /// # Errors
    /// - Missing verification keys or JWT validation failures.
    ///
    /// # Security notes
    /// - Enforces EdDSA-only verification and `tid` match.
    ///
    /// # Performance
    /// - Attempts verification in `kid`-preferred order to reduce retries.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::{FelixTokenVerifier, TenantId, TenantKeyMaterial, Jwks};
    /// use jsonwebtoken::Algorithm;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    ///
    /// let mut store = HashMap::new();
    /// store.insert("t1".to_string(), TenantKeyMaterial {
    ///     kid: "k1".to_string(),
    ///     alg: Algorithm::EdDSA,
    ///     private_key: [1u8; 32],
    ///     public_key: [2u8; 32],
    ///     jwks: Jwks { keys: vec![] },
    /// });
    /// let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 60, Arc::new(store));
    /// let _ = verifier.verify(&TenantId::new("t1"), "token");
    /// ```
    pub fn verify(&self, tenant_id: &TenantId, token: &str) -> AuthzResult<FelixClaims> {
        // Step 1: Decode header to guide key ordering by `kid`.
        // This reduces failed signature attempts during key rotation.
        let header = jsonwebtoken::decode_header(token)?;
        let keys = self.key_store.verification_keys(tenant_id)?;
        let mut ordered_keys = Vec::with_capacity(keys.len());
        if let Some(kid) = header.kid.as_deref() {
            if let Some(pos) = keys.iter().position(|entry| entry.kid == kid) {
                ordered_keys.push(keys[pos].clone());
                for (idx, entry) in keys.into_iter().enumerate() {
                    if idx != pos {
                        ordered_keys.push(entry);
                    }
                }
            } else {
                ordered_keys.extend(keys);
            }
        } else {
            ordered_keys.extend(keys);
        }

        // Step 2: Configure strict validation for EdDSA tokens.
        // We enforce issuer/audience to prevent token substitution.
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.leeway = self.leeway;

        let mut last_err = None;
        for key in ordered_keys {
            // Step 3: Enforce EdDSA-only keys before attempting verification.
            validate_alg(key.alg)?;
            // Step 4: Build decoding key (cached) and verify signature.
            let decoding_key = self.cache.decoding_key(tenant_id, &key)?;
            match jsonwebtoken::decode::<FelixClaims>(token, &decoding_key, &validation) {
                Ok(data) => {
                    // Step 5: Enforce tenant ID match after signature verification.
                    if data.claims.tid != tenant_id.as_str() {
                        return Err(AuthzError::TenantMismatch {
                            expected: tenant_id.to_string(),
                            actual: data.claims.tid.clone(),
                        });
                    }
                    return Ok(data.claims);
                }
                Err(err) => last_err = Some(err),
            }
        }

        // IMPORTANT:
        // Preserve the last JWT error for diagnostics instead of discarding it.
        Err(last_err.map(AuthzError::Jwt).unwrap_or_else(|| {
            AuthzError::Jwt(jsonwebtoken::errors::Error::from(
                jsonwebtoken::errors::ErrorKind::InvalidToken,
            ))
        }))
    }
}

fn now_epoch_seconds() -> i64 {
    // We clamp to zero on clock errors to avoid panics in edge cases.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}

/// Cache for derived encoding/decoding keys.
///
/// # What it does
/// Stores `jsonwebtoken` encoding/decoding keys by tenant and `kid`.
///
/// # Inputs/outputs
/// - Inputs: tenant identifiers and key material.
/// - Outputs: cached `EncodingKey`/`DecodingKey` instances.
///
/// # Errors
/// - Not applicable (cache container).
///
/// # Security notes
/// - Cache entries must be invalidated on key rotation to avoid stale keys.
///
/// # Concurrency
/// - Uses `RwLock` to allow concurrent reads with exclusive writes on misses.
///
/// # Example
/// ```rust
/// use felix_authz::{TenantKeyCache, TenantId};
///
/// let cache = TenantKeyCache::default();
/// cache.invalidate_tenant(&TenantId::new("t1"));
/// ```
#[derive(Default)]
pub struct TenantKeyCache {
    encoding: RwLock<HashMap<String, EncodingKey>>,
    decoding: RwLock<HashMap<String, DecodingKey>>,
}

impl TenantKeyCache {
    /// Invalidate cached keys for a tenant.
    ///
    /// # What it does
    /// Removes all cached encoding/decoding keys for the given tenant.
    ///
    /// # Inputs/outputs
    /// - Input: `tenant_id`.
    /// - Output: none.
    ///
    /// # Errors
    /// - Does not return errors.
    ///
    /// # Security notes
    /// - Must be called on key rotation to prevent stale verification keys.
    ///
    /// # Performance
    /// - Eviction is O(number of cached keys for the tenant).
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::{TenantId, TenantKeyCache};
    ///
    /// let cache = TenantKeyCache::default();
    /// cache.invalidate_tenant(&TenantId::new("t1"));
    /// ```
    pub fn invalidate_tenant(&self, tenant_id: &TenantId) {
        // Step 1: Build a stable prefix so we can evict all tenant entries.
        let mut prefix = String::with_capacity(tenant_id.as_str().len() + 1);
        prefix.push_str(tenant_id.as_str());
        prefix.push(':');
        // Step 2: Evict both encoding and decoding entries.
        if let Ok(mut map) = self.encoding.write() {
            map.retain(|key, _| !key.starts_with(&prefix));
        }
        if let Ok(mut map) = self.decoding.write() {
            map.retain(|key, _| !key.starts_with(&prefix));
        }
    }

    fn encoding_key(
        &self,
        tenant_id: &TenantId,
        key: &TenantSigningKey,
    ) -> AuthzResult<EncodingKey> {
        // Step 1: Enforce EdDSA-only signing keys.
        validate_alg(key.alg)?;
        let cache_key = cache_key(tenant_id, &key.kid);
        if let Ok(map) = self.encoding.read()
            && let Some(found) = map.get(&cache_key)
        {
            return Ok(found.clone());
        }
        // Step 2: Convert raw Ed25519 seed to PKCS8 DER for jsonwebtoken.
        let signing_key = SigningKey::from_bytes(&key.private_key);
        let der = signing_key
            .to_pkcs8_der()
            .map_err(|err| AuthzError::Key(format!("encode Ed25519 key: {err}")))?;
        let encoding_key = EncodingKey::from_ed_der(der.as_bytes());
        // Step 3: Cache the derived key for reuse.
        if let Ok(mut map) = self.encoding.write() {
            map.insert(cache_key, encoding_key.clone());
        }
        Ok(encoding_key)
    }

    fn decoding_key(
        &self,
        tenant_id: &TenantId,
        key: &TenantVerificationKey,
    ) -> AuthzResult<DecodingKey> {
        // Step 1: Enforce EdDSA-only verification keys.
        validate_alg(key.alg)?;
        let cache_key = cache_key(tenant_id, &key.kid);
        if let Ok(map) = self.decoding.read()
            && let Some(found) = map.get(&cache_key)
        {
            return Ok(found.clone());
        }
        // Step 2: Convert raw public key into base64url `x` as expected by jsonwebtoken.
        let x = URL_SAFE_NO_PAD.encode(key.public_key);
        let decoding_key = DecodingKey::from_ed_components(&x).map_err(AuthzError::Jwt)?;
        // Step 3: Cache the derived key for reuse.
        if let Ok(mut map) = self.decoding.write() {
            map.insert(cache_key, decoding_key.clone());
        }
        Ok(decoding_key)
    }
}

fn cache_key(tenant_id: &TenantId, kid: &str) -> String {
    // We use tenant:kid to allow prefix invalidation by tenant.
    let tenant = tenant_id.as_str();
    let mut key = String::with_capacity(tenant.len() + 1 + kid.len());
    key.push_str(tenant);
    key.push(':');
    key.push_str(kid);
    key
}

fn validate_alg(alg: Algorithm) -> AuthzResult<()> {
    // Enforce EdDSA-only Felix tokens to avoid RSA/HS downgrades.
    if alg == Algorithm::EdDSA {
        Ok(())
    } else {
        Err(AuthzError::Key(format!(
            "invalid Felix signing algorithm: {alg:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jwks::Jwk;

    const TEST_PRIVATE_KEY: [u8; 32] = [7u8; 32];

    fn test_key_material() -> TenantKeyMaterial {
        // Deterministic test key material prevents flaky signatures.
        let signing_key = SigningKey::from_bytes(&TEST_PRIVATE_KEY);
        let public_key = signing_key.verifying_key().to_bytes();
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TEST_PRIVATE_KEY,
            public_key,
            jwks: Jwks {
                keys: vec![Jwk {
                    kty: "OKP".to_string(),
                    kid: "k1".to_string(),
                    alg: "EdDSA".to_string(),
                    use_field: crate::jwks::KeyUse::Sig,
                    crv: Some("Ed25519".to_string()),
                    x: Some("test".to_string()),
                }],
            },
        }
    }

    fn test_key_store() -> Arc<dyn TenantKeyStore> {
        // In-memory key store avoids external dependencies in unit tests.
        let mut keys = HashMap::new();
        keys.insert("tenant-a".to_string(), test_key_material());
        Arc::new(keys)
    }

    fn encoding_key_from_seed(seed: &[u8; 32]) -> EncodingKey {
        // This helper mirrors production PKCS8 conversion to keep tests aligned.
        let signing_key = SigningKey::from_bytes(seed);
        let der = signing_key.to_pkcs8_der().expect("pkcs8 der");
        EncodingKey::from_ed_der(der.as_bytes())
    }

    #[test]
    fn mint_and_verify_roundtrip() {
        // This test prevents regressions where minted EdDSA tokens fail verification.
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
                vec!["stream.publish:stream:tenant-a/payments/orders.*".to_string()],
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
        // This test ensures missing signing keys result in explicit errors.
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
        // This test prevents accepting tokens without verification keys present.
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
        // This test ensures cross-tenant tokens are rejected even if signed.
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
        // This test ensures the verifier is resilient when `kid` is absent.
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
        let encoding_key = encoding_key_from_seed(&signing.private_key);
        let token = jsonwebtoken::encode(&header, &claims, &encoding_key).expect("encode token");

        let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store);
        let verified = verifier
            .verify(&TenantId::new("tenant-a"), &token)
            .expect("verify token");
        assert_eq!(verified.sub, "principal");
    }

    #[test]
    fn verify_succeeds_when_kid_unknown() {
        // This test ensures `kid` mismatches don't prevent verification when keys rotate.
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
        let encoding_key = encoding_key_from_seed(&signing.private_key);
        let token = jsonwebtoken::encode(&header, &claims, &encoding_key).expect("encode token");

        let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store);
        let verified = verifier
            .verify(&TenantId::new("tenant-a"), &token)
            .expect("verify token");
        assert_eq!(verified.sub, "principal");
    }

    #[test]
    fn key_store_jwks() {
        // This test ensures JWKS is available from the key store.
        let key_store = test_key_store();
        let jwks = key_store.jwks(&TenantId::new("tenant-a")).expect("jwks");
        assert_eq!(jwks.keys.len(), 1);
    }

    #[test]
    fn key_store_missing_jwks() {
        // This test ensures missing JWKS is surfaced as an explicit error.
        let key_store: Arc<dyn TenantKeyStore> = Arc::new(HashMap::new());
        let err = key_store
            .jwks(&TenantId::new("tenant-a"))
            .expect_err("missing jwks");
        assert!(matches!(err, AuthzError::MissingJwks(_)));
    }

    #[test]
    fn invalid_key_algorithm_rejected() {
        // This test prevents accidental RSA usage for Felix tokens.
        let mut keys = HashMap::new();
        keys.insert(
            "tenant-a".to_string(),
            TenantKeyMaterial {
                kid: "k1".to_string(),
                alg: Algorithm::RS256,
                private_key: TEST_PRIVATE_KEY,
                public_key: TEST_PRIVATE_KEY,
                jwks: Jwks { keys: vec![] },
            },
        );
        let key_store: Arc<dyn TenantKeyStore> = Arc::new(keys);
        let signing = key_store.current_signing_key(&TenantId::new("tenant-a"));
        assert!(matches!(signing, Err(AuthzError::Key(_))));

        let verification = key_store.verification_keys(&TenantId::new("tenant-a"));
        assert!(matches!(verification, Err(AuthzError::Key(_))));
    }

    #[test]
    fn tenant_key_cache_invalidate_tenant_evicts_cached_entries() {
        let key_store = test_key_store();
        let cache = TenantKeyCache::default();
        let tenant = TenantId::new("tenant-a");
        let signing = key_store.current_signing_key(&tenant).expect("signing key");
        let verify = key_store
            .verification_keys(&tenant)
            .expect("verification keys")
            .into_iter()
            .next()
            .expect("verification key");

        let _first_encoding = cache.encoding_key(&tenant, &signing).expect("encoding key");
        let _first_decoding = cache.decoding_key(&tenant, &verify).expect("decoding key");
        let _second_encoding = cache.encoding_key(&tenant, &signing).expect("encoding key");
        let _second_decoding = cache.decoding_key(&tenant, &verify).expect("decoding key");

        cache.invalidate_tenant(&tenant);
        let _refreshed_encoding = cache.encoding_key(&tenant, &signing).expect("encoding key");
        let _refreshed_decoding = cache.decoding_key(&tenant, &verify).expect("decoding key");
    }

    #[test]
    fn cache_key_and_validate_alg_helpers() {
        let tenant = TenantId::new("tenant-a");
        assert_eq!(cache_key(&tenant, "k9"), "tenant-a:k9");
        assert!(validate_alg(Algorithm::EdDSA).is_ok());
        let err = validate_alg(Algorithm::HS256).expect_err("unsupported alg");
        assert!(matches!(err, AuthzError::Key(_)));
    }
}
