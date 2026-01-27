//! Principal identity model and helpers.
//!
//! # Purpose
//! Creates stable principal IDs and structures derived from validated OIDC claims.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    pub principal_id: String,
    pub issuer: String,
    pub subject: String,
    pub groups: Vec<String>,
}

pub fn principal_id(issuer: &str, subject: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(issuer.as_bytes());
    hasher.update(b"|");
    hasher.update(subject.as_bytes());
    hex::encode(hasher.finalize())
}

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
