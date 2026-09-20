//! Shared `AppState` construction for tests across this crate.
//!
//! Three near-identical builders existed before this, one per file that
//! needed a fake `AppState`, differing only in the one or two fields each
//! test actually varied. One builder means a future field addition updates
//! once instead of three times.
#![cfg(test)]

use std::sync::Arc;

use crate::api::types::{FeatureFlags, Region};
use crate::app::AppState;
use crate::readiness::{AlwaysReady, HealthProbe, Readiness};
use crate::store::ControlPlaneAuthStore;

/// An `AppState` over `store`, ready unless `probe` says otherwise.
pub(crate) fn app_state(
    store: Arc<dyn ControlPlaneAuthStore + Send + Sync>,
    probe: Arc<dyn HealthProbe>,
) -> AppState {
    AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store,
        oidc_validator: crate::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: Arc::new(Readiness::new(probe)),
        in_flight: Default::default(),
    }
}

/// [`app_state`] with a store that always answers ready -- what every
/// existing caller wanted before one of them needed to say otherwise.
pub(crate) fn app_state_ready(store: Arc<dyn ControlPlaneAuthStore + Send + Sync>) -> AppState {
    app_state(store, Arc::new(AlwaysReady))
}
