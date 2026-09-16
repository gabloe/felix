//! Principal identity: stable IDs derived from validated OIDC claims.
//!
//! The ID is a hash of issuer + subject, so it is deterministic without
//! persisting the raw subject. Callers validate and normalize the claims
//! first; nothing here checks them.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Normalized identity used for RBAC and auditing: the stable `principal_id`
/// plus the original issuer/subject and the IdP's raw group strings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    pub principal_id: String,
    pub issuer: String,
    pub subject: String,
    pub groups: Vec<String>,
}

/// Stable principal ID: hex SHA-256 of `issuer|subject`. The delimiter keeps
/// `("ab", "c")` and `("a", "bc")` from colliding.
pub fn principal_id(issuer: &str, subject: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(issuer.as_bytes());
    hasher.update(b"|");
    hasher.update(subject.as_bytes());
    hex::encode(hasher.finalize())
}

/// Build a [`Principal`] from already-validated OIDC claims.
pub fn from_claims(issuer: &str, subject: &str, groups: Vec<String>) -> Principal {
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
