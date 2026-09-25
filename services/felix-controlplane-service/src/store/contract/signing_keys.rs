//! Signing-key behaviour every backend must satisfy.
use std::sync::Arc;

use crate::model::Tenant;
use crate::store::ControlPlaneAuthStore;

const TENANT: &str = "signing-t";
/// Several rounds, because the race needs the callers to overlap between
/// the "no keys yet" read and the write, and one round may not.
const ROUNDS: usize = 5;
const CALLERS: usize = 24;

pub(crate) async fn run_signing_key_contract(store: Arc<dyn ControlPlaneAuthStore>) {
    concurrent_ensures_agree_on_one_stored_key(store).await;
}

/// Callers racing to ensure a tenant has keys must all get the key set that
/// ends up stored. A caller handed keys that a later write replaced would
/// sign tokens nothing can verify.
async fn concurrent_ensures_agree_on_one_stored_key(store: Arc<dyn ControlPlaneAuthStore>) {
    for round in 0..ROUNDS {
        let tenant_id = format!("{TENANT}-{round}");
        store
            .create_tenant(Tenant {
                tenant_id: tenant_id.clone(),
                display_name: "Signing".to_string(),
            })
            .await
            .expect("create tenant");

        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS));
        let callers: Vec<_> = (0..CALLERS)
            .map(|_| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                let tenant_id = tenant_id.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    store
                        .ensure_signing_key_current(&tenant_id)
                        .await
                        .expect("ensure signing keys")
                })
            })
            .collect();
        let mut kids = Vec::with_capacity(CALLERS);
        for caller in callers {
            kids.push(caller.await.expect("caller").current.kid);
        }

        let stored = store
            .get_tenant_signing_keys(&tenant_id)
            .await
            .expect("stored keys")
            .current
            .kid;
        let stale = kids.iter().filter(|kid| **kid != stored).count();
        assert_eq!(
            stale, 0,
            "round {round}: {stale} of {CALLERS} callers got keys that are not stored",
        );
    }
}
