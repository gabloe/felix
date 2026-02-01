//! Casbin model definition for Felix authorization policies.
//!
//! # Purpose
//! Provides the embedded Casbin model string and helper to build a
//! `DefaultModel` for policy evaluation.
//!
//! # How it fits
//! The authorization layer loads this model before applying policy rules.
//! Request handlers and tests call into this module to ensure model parity.
//!
//! # Key invariants
//! - The matcher must include domain and action checks.
//! - `keyMatch2` is used for resource pattern matching.
//!
//! # Important configuration
//! - None; the model is embedded and versioned with the crate.
//!
//! # Examples
//! ```rust
//! use felix_authz::casbin_model_string;
//!
//! assert!(casbin_model_string().contains("[matchers]"));
//! ```
//!
//! # Common pitfalls
//! - Changing the model without updating policies/tests causes authorization drift.
//! - Removing `keyMatch2` breaks wildcard resource matching.
//!
//! # Future work
//! - Load the model from a shared config to enable hot-reload in services.
use casbin::prelude::DefaultModel;

const MODEL: &str = r#"
[request_definition]
r = sub, dom, obj, act

[policy_definition]
p = sub, dom, obj, act

[role_definition]
g = _, _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub, r.dom) && r.dom == p.dom && keyMatch2(r.obj, p.obj) && r.act == p.act
"#;

/// Return the embedded Casbin model as a static string.
///
/// # Parameters
/// - None.
///
/// # Returns
/// - The model string, suitable for logging or tests.
///
/// # Invariants
/// - The returned string must match the model used by policy evaluation.
///
/// # Example
/// ```rust
/// use felix_authz::casbin_model_string;
///
/// assert!(casbin_model_string().contains("keyMatch2"));
/// ```
pub fn casbin_model_string() -> &'static str {
    MODEL
}

/// Build a Casbin `DefaultModel` from the embedded string.
///
/// # Parameters
/// - None.
///
/// # Returns
/// - A parsed `DefaultModel` ready for policy enforcement.
///
/// # Errors
/// - Panics if the embedded model is invalid (should never happen in CI).
///
/// # Performance
/// - Parsing is O(model size) and should be cached by callers.
///
/// # Example
/// ```rust
/// use felix_authz::casbin_model;
///
/// # async fn load() {
/// let _model = casbin_model().await;
/// # }
/// ```
pub async fn casbin_model() -> DefaultModel {
    // Parse the static model definition once per call.
    DefaultModel::from_str(MODEL)
        .await
        .expect("casbin model must be valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use casbin::Model;

    #[test]
    fn model_string_contains_matcher() {
        let model = casbin_model_string();
        assert!(model.contains("keyMatch2"));
        assert!(model.contains("request_definition"));
    }

    #[tokio::test]
    async fn model_builds() {
        let model = casbin_model().await;
        let data = model.get_model();
        assert!(data.contains_key("r"));
        assert!(data.contains_key("p"));
        assert!(data.contains_key("g"));
    }
}
