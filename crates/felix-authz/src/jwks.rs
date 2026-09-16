//! Minimal JWKS types for publishing Ed25519 public keys.
//!
//! Only public key material belongs here. Felix signs with OKP/Ed25519
//! (`alg = EdDSA`) and nothing else. Note `x` is base64url, not plain base64.

use serde::{Deserialize, Serialize};

/// Intended use for a JWK. Felix only signs, so `Sig` is the only variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyUse {
    Sig,
}

/// One Ed25519 public key in JWK form: `kty = "OKP"`, `alg = "EdDSA"`,
/// `crv = "Ed25519"`, with the base64url public key in `x`.
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

/// The key set served to verifiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Jwks {
    pub keys: Vec<Jwk>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwks_roundtrip() {
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
