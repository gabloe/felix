use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyUse {
    Sig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Jwk {
    pub kty: String,
    pub kid: String,
    pub alg: String,
    #[serde(rename = "use")]
    pub use_field: KeyUse,
    pub n: String,
    pub e: String,
}

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
                kty: "RSA".to_string(),
                kid: "k1".to_string(),
                alg: "RS256".to_string(),
                use_field: KeyUse::Sig,
                n: "modulus".to_string(),
                e: "AQAB".to_string(),
            }],
        };

        let serialized = serde_json::to_string(&jwks).expect("serialize");
        let decoded: Jwks = serde_json::from_str(&serialized).expect("deserialize");
        assert_eq!(decoded.keys.len(), 1);
        assert_eq!(decoded.keys[0].kid, "k1");
    }
}
