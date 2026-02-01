//! Principal identity model and helpers.
//!
//! # Purpose and responsibility
//! Creates stable principal identifiers and constructs principal records from
//! validated OIDC claims.
//!
//! # Where it fits in Felix
//! The OIDC validation flow uses these helpers to normalize identities before
//! policy evaluation and token exchange.
//!
//! # Key invariants and assumptions
//! - Principal IDs are deterministic for a given issuer+subject pair.
//! - The ID is derived from normalized strings (caller must normalize).
//!
//! # Security considerations
//! - Principal IDs are derived via hashing and do not expose the raw subject.
//! - Callers must validate issuer/subject before constructing principals.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Normalized principal identity derived from an OIDC token.
///
/// # What it does
/// Stores the stable principal ID plus the original issuer/subject and groups.
///
/// # Why it exists
/// Provides a consistent identity representation for RBAC and auditing.
///
/// # Invariants
/// - `principal_id` must be derived from `issuer` + `subject`.
/// - `groups` contains raw group strings from the IdP.
///
/// # Example
/// ```rust
/// use controlplane::auth::principal::{from_claims, Principal};
///
/// let principal = from_claims("https://issuer", "user-1", vec!["admins".to_string()]);
/// assert_eq!(principal.issuer, "https://issuer");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    pub principal_id: String,
    pub issuer: String,
    pub subject: String,
    pub groups: Vec<String>,
}

/// Derive a stable principal ID from issuer and subject.
///
/// # What it does
/// Hashes `issuer|subject` using SHA-256 and returns the hex digest.
///
/// # Why it exists
/// Keeps a stable identifier without persisting raw subject values.
///
/// # Invariants
/// - The same inputs must yield the same output.
///
/// # Errors
/// - Does not return errors.
///
/// # Example
/// ```rust
/// use controlplane::auth::principal::principal_id;
///
/// let id = principal_id("https://issuer", "user-1");
/// assert!(!id.is_empty());
/// ```
pub fn principal_id(issuer: &str, subject: &str) -> String {
    // Combine issuer and subject with a delimiter to avoid ambiguity.
    let mut hasher = Sha256::new();
    hasher.update(issuer.as_bytes());
    hasher.update(b"|");
    hasher.update(subject.as_bytes());
    hex::encode(hasher.finalize())
}

/// Build a [`Principal`] from validated OIDC claims.
///
/// # What it does
/// Constructs a principal with a deterministic ID and the provided group list.
///
/// # Why it exists
/// Centralizes how principals are derived so policy evaluation is consistent.
///
/// # Invariants
/// - `issuer` and `subject` must already be validated and normalized.
///
/// # Errors
/// - Does not return errors.
///
/// # Example
/// ```rust
/// use controlplane::auth::principal::from_claims;
///
/// let principal = from_claims("https://issuer", "user-1", vec![]);
/// assert_eq!(principal.subject, "user-1");
/// ```
pub fn from_claims(issuer: &str, subject: &str, groups: Vec<String>) -> Principal {
    // Derive the stable principal ID from issuer and subject.
    Principal {
        principal_id: principal_id(issuer, subject),
        issuer: issuer.to_string(),
        subject: subject.to_string(),
        groups,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_id_is_stable() {
        let a = principal_id("https://issuer", "sub");
        let b = principal_id("https://issuer", "sub");
        assert_eq!(a, b);
    }

    #[test]
    fn principal_id_changes_with_inputs() {
        let a = principal_id("https://issuer", "sub");
        let b = principal_id("https://issuer", "sub2");
        assert_ne!(a, b);
    }
}
