use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::AuthzError;

#[test]
fn mint_fails_without_signing_key() {
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
