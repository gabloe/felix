use jsonwebtoken::Algorithm;

use super::*;
use crate::token::tests::test_key_store;

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
