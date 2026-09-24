use super::*;

#[test]
fn alg_to_string_maps_all_algorithms() {
    let cases = vec![
        (jsonwebtoken::Algorithm::RS256, "RS256"),
        (jsonwebtoken::Algorithm::RS384, "RS384"),
        (jsonwebtoken::Algorithm::RS512, "RS512"),
        (jsonwebtoken::Algorithm::ES256, "ES256"),
        (jsonwebtoken::Algorithm::ES384, "ES384"),
        (jsonwebtoken::Algorithm::PS256, "PS256"),
        (jsonwebtoken::Algorithm::PS384, "PS384"),
        (jsonwebtoken::Algorithm::PS512, "PS512"),
        (jsonwebtoken::Algorithm::HS256, "HS256"),
        (jsonwebtoken::Algorithm::HS384, "HS384"),
        (jsonwebtoken::Algorithm::HS512, "HS512"),
        (jsonwebtoken::Algorithm::EdDSA, "EdDSA"),
    ];
    for (alg, expected) in cases {
        assert_eq!(alg_to_string(alg), expected);
    }
}
