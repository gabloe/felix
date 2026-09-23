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
