use super::*;
use crate::readiness::HealthProbe;
use crate::store::StoreConfig;
use crate::store::memory::InMemoryStore;
use std::sync::Arc;

/// A store that is up but holds nothing the group holds -- a member whose
/// volume was replaced and has not caught up.
struct NeverReady;

#[async_trait::async_trait]
impl HealthProbe for NeverReady {
    async fn health_check(&self) -> crate::store::StoreResult<()> {
        Err(crate::store::StoreError::Unexpected(anyhow::anyhow!(
            "joined an established group; nothing replicated into this member yet"
        )))
    }
}

fn state_with(probe: Arc<dyn HealthProbe>) -> AppState {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: crate::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(crate::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    });
    crate::test_support::app_state(Arc::new(store), probe)
}

/// **A member that cannot verify says so, rather than blaming the token.**
///
/// The tenant's signing keys are missing because this instance holds none
/// of the group's state yet. The credential may be perfectly good, and
/// answering 401 sends whoever reads it after a fault that is not there.
#[tokio::test]
async fn a_member_that_has_not_caught_up_cannot_verify_rather_than_refusing() {
    let state = state_with(Arc::new(NeverReady));
    let err = verify_against(&state, "acme", "not-a-real-token")
        .await
        .expect_err("no keys for this tenant");
    let response = axum::response::IntoResponse::into_response(err);
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "a member that cannot verify must not answer 401",
    );
}

/// **A ready member still refuses an unknown tenant.** The fix must not
/// turn a genuine authentication failure into a retryable one, or a bad
/// credential becomes an infinite retry loop.
#[tokio::test]
async fn a_ready_member_still_refuses_a_token_it_cannot_verify() {
    let state = state_with(Arc::new(crate::readiness::AlwaysReady));
    let err = verify_against(&state, "acme", "not-a-real-token")
        .await
        .expect_err("no keys for this tenant");
    let response = axum::response::IntoResponse::into_response(err);
    assert_eq!(
        response.status(),
        axum::http::StatusCode::UNAUTHORIZED,
        "an instance holding the group's state knows this token is bad",
    );
}
