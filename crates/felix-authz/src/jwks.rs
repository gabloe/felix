//! JWKS data types used for Felix key distribution.
//!
//! # Purpose
//! Define minimal JSON Web Key Set structures for publishing Ed25519 public keys.
//!
//! # Key invariants
//! - JWKS must contain only public key material.
//! - Felix uses OKP/Ed25519 with `alg = EdDSA`.
//!
//! # Security model / threat assumptions
//! - JWKS is public and should be treated as untrusted input by consumers.
//! - Private keys must never be serialized into these structs.
//!
//! # Concurrency + ordering guarantees
//! - These are plain data types with no concurrency concerns.
//!
//! # How to use
//! Populate [`Jwks`] with one or more [`Jwk`] entries and serialize with `serde`.
//!
//! # Common pitfalls
//! - Publishing private key material in `x` or adding extra fields by mistake.
//! - Using base64 instead of base64url encoding for `x`.
//!
//! # Future work
//! - Add validation helpers to enforce OKP/Ed25519 invariants on construction.
use serde::{Deserialize, Serialize};

/// Intended usage for a JWK.
///
/// # What it does
/// Indicates whether a key is used for signatures.
///
/// # Parameters / returns
/// - Serialized/deserialized as a lowercase string value.
///
/// # Errors
/// - Not applicable (enum).
///
/// # Security notes
/// - Felix uses only signing keys (`sig`).
///
/// # Invariants
/// - `KeyUse::Sig` is the only variant supported by Felix.
///
/// # Example
/// ```rust
/// use felix_authz::KeyUse;
///
/// let usage = KeyUse::Sig;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyUse {
    Sig,
}

/// A single JSON Web Key entry.
///
/// # What it does
/// Represents an Ed25519 public key in JWKS format.
///
/// # Parameters / returns
/// - Fields are populated from key storage or JWKS responses.
/// - Serialized as JSON for JWKS endpoints.
///
/// # Errors
/// - Not applicable (data container).
///
/// # Security notes
/// - Only public key data (`x`) should be populated; never include private keys.
///
/// # Invariants
/// - `kty` must be `"OKP"`, `alg` must be `"EdDSA"`, `crv` should be `"Ed25519"`.
///
/// # Performance
/// - This is a plain data container; cloning is O(fields).
///
/// # Example
/// ```rust
/// use felix_authz::{Jwk, KeyUse};
///
/// let jwk = Jwk {
///     kty: "OKP".to_string(),
///     kid: "k1".to_string(),
///     alg: "EdDSA".to_string(),
///     use_field: KeyUse::Sig,
///     crv: Some("Ed25519".to_string()),
///     x: Some("base64url".to_string()),
/// };
/// assert_eq!(jwk.kty, "OKP");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Jwk {
    pub kty: String,
    pub kid: String,
    pub alg: String,
    #[serde(rename = "use")]
    pub use_field: KeyUse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crv: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x: Option<String>,
}

/// A JSON Web Key Set container.
///
/// # What it does
/// Wraps a list of JWK entries for serialization.
///
/// # Parameters / returns
/// - `keys` contains the public JWK entries.
///
/// # Errors
/// - Not applicable (data container).
///
/// # Security notes
/// - Ensure only public keys are included.
///
/// # Performance
/// - Cloning scales with the number of keys.
///
/// # Example
/// ```rust
/// use felix_authz::{Jwk, Jwks, KeyUse};
///
/// let jwks = Jwks {
///     keys: vec![Jwk {
///         kty: "OKP".to_string(),
///         kid: "k1".to_string(),
///         alg: "EdDSA".to_string(),
///         use_field: KeyUse::Sig,
///         crv: Some("Ed25519".to_string()),
///         x: Some("base64url".to_string()),
///     }],
/// };
/// assert_eq!(jwks.keys.len(), 1);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Jwks {
    pub keys: Vec<Jwk>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwks_roundtrip() {
        // This test ensures JWKS serialization stays stable for clients.
        let jwks = Jwks {
            keys: vec![Jwk {
                kty: "OKP".to_string(),
                kid: "k1".to_string(),
                alg: "EdDSA".to_string(),
                use_field: KeyUse::Sig,
                crv: Some("Ed25519".to_string()),
                x: Some("pubkey".to_string()),
            }],
        };

        let serialized = serde_json::to_string(&jwks).expect("serialize");
        let decoded: Jwks = serde_json::from_str(&serialized).expect("deserialize");
        assert_eq!(decoded.keys.len(), 1);
        assert_eq!(decoded.keys[0].kid, "k1");
    }
}
