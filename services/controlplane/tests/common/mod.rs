//! Helpers shared by the integration tests.
//!
//! Each test binary compiles its own copy of this module, and no single
//! binary uses every helper — so per-target dead-code analysis is noise here.
#![allow(dead_code)]

use axum::body::Body;
use axum::http::Request;
pub(crate) fn json_request(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

pub(crate) async fn read_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}

/// A schema name no other caller will produce.
///
/// `pid + timestamp` is not enough. Tests in one binary run on parallel
/// threads, so the pid is identical, and two threads starting together can read
/// the same timestamp whenever the clock's granularity is coarser than the gap
/// between them — which is how two tests came to ask for the same schema and
/// the second was refused:
///
/// ```text
/// duplicate key value violates unique constraint "pg_namespace_nspname_index"
/// Key (nspname)=(felix_migrate_31654_1789668951690256729) already exists
/// ```
///
/// `CREATE SCHEMA IF NOT EXISTS` does not save it: two concurrent creates of
/// the same name race in Postgres and one gets exactly that error, so the
/// uniqueness has to be real rather than papered over at the call site.
///
/// The counter is what makes it real. The pid separates concurrent test
/// binaries, the timestamp keeps a name readable and tells runs apart, and the
/// counter guarantees that two calls in one process differ however close
/// together they are.
pub(crate) fn unique_schema(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "{prefix}_{}_{}_{}",
        std::process::id(),
        nanos,
        NEXT.fetch_add(1, Ordering::Relaxed),
    )
}

// No `#[cfg(test)]`: this module is compiled into integration test binaries,
// which are already test crates, so the gate would remove these entirely.
mod tests {
    use super::*;

    /// The condition that broke it: many names asked for at once, from one
    /// process. A timestamp alone repeats here whenever the clock is coarser
    /// than the gap between two calls.
    #[test]
    fn names_are_unique_within_one_process() {
        let names: std::collections::HashSet<String> =
            (0..2_000).map(|_| unique_schema("felix_test")).collect();
        assert_eq!(
            names.len(),
            2_000,
            "two calls produced the same schema name, which Postgres refuses \
             with a duplicate-key error rather than reusing",
        );
    }

    /// And across threads, which is how tests in one binary actually run.
    #[test]
    fn names_are_unique_across_threads() {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    (0..500)
                        .map(|_| unique_schema("felix_test"))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let names: std::collections::HashSet<String> = handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("thread"))
            .collect();
        assert_eq!(names.len(), 4_000);
    }

    #[test]
    fn a_name_is_a_usable_identifier() {
        let name = unique_schema("felix_test");
        assert!(name.starts_with("felix_test_"));
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "a schema name has to be quotable without escaping: {name}",
        );
    }
}

/// The tenant whose keys sign every cluster-scoped credential minted here.
///
/// A cluster-scoped permission lives in some tenant's token — the `tid` only
/// picks the verification keys — so an operator needs a tenant to be minted
/// from. Tests use this one; a deployment bootstraps its own.
pub(crate) const OPERATOR_TENANT: &str = "ops";

/// Signing keys every test credential is minted with.
///
/// One key set for every tenant a test touches, so a token minted before a
/// tenant exists verifies once [`Credentials::adopt`] binds the keys to it.
pub(crate) struct Credentials {
    pub(crate) keys: controlplane::auth::felix_token::TenantSigningKeys,
}

impl Credentials {
    pub(crate) fn token(&self, tenant_id: &str, perms: &[&str]) -> String {
        controlplane::auth::felix_token::mint_token(
            &self.keys,
            tenant_id,
            "p:test",
            perms.iter().map(|perm| perm.to_string()).collect(),
            std::time::Duration::from_secs(900),
        )
        .expect("mint token")
    }

    /// An operator: manages the tenant catalog and reads cluster metadata.
    pub(crate) fn operator(&self) -> String {
        self.token(
            OPERATOR_TENANT,
            &["tenant.manage:cluster:*", "node.view:cluster:*"],
        )
    }

    /// A broker: reads the metadata feeds, nothing else.
    pub(crate) fn broker(&self) -> String {
        self.token(OPERATOR_TENANT, &["node.view:cluster:*"])
    }

    /// A tenant admin, with the same expansion token exchange would apply to
    /// `tenant.manage:tenant:{id}`.
    pub(crate) fn tenant_admin(&self, tenant_id: &str) -> String {
        self.token(
            tenant_id,
            &[
                &format!("tenant.manage:tenant:{tenant_id}"),
                &format!("ns.manage:namespace:{tenant_id}/*"),
                &format!("stream.manage:stream:{tenant_id}/*/*"),
                &format!("cache.manage:cache:{tenant_id}/*/*"),
            ],
        )
    }

    /// Bind the shared keys to `tenant_id`.
    ///
    /// Needed after a tenant is created through the API, which generates its
    /// own keys; tokens minted here would not verify against those.
    pub(crate) async fn adopt(
        &self,
        store: &(dyn controlplane::store::AuthStore + Send + Sync),
        tenant_id: &str,
    ) {
        store
            .set_tenant_signing_keys(tenant_id, self.keys.clone())
            .await
            .expect("adopt tenant keys");
    }
}

/// Seed the operator tenant and its keys, so cluster-scoped tokens verify.
pub(crate) async fn seed_credentials(
    store: &(dyn controlplane::store::ControlPlaneAuthStore + Send + Sync),
) -> Credentials {
    let keys = controlplane::auth::keys::generate_signing_keys().expect("generate keys");
    // Create-then-set, in that order: creation resets a tenant's keys.
    match store
        .create_tenant(controlplane::model::Tenant {
            tenant_id: OPERATOR_TENANT.to_string(),
            display_name: "Operators".to_string(),
        })
        .await
    {
        Ok(_) | Err(controlplane::store::StoreError::Conflict(_)) => {}
        Err(err) => panic!("seed operator tenant: {err}"),
    }
    let credentials = Credentials { keys };
    credentials.adopt(store, OPERATOR_TENANT).await;
    credentials
}

pub(crate) fn json_request_as(
    method: &str,
    uri: &str,
    token: &str,
    body: serde_json::Value,
) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .expect("request")
}

pub(crate) fn request_as(method: &str, uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .expect("request")
}
