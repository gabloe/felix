//! Casbin enforcer builder for tenant-scoped RBAC.
//!
//! # Purpose and responsibility
//! Constructs an in-memory Casbin enforcer populated with a tenant's policies
//! and groupings for authorization checks.
//!
//! # Where it fits in Felix
//! Used by token exchange and admin flows to evaluate effective permissions
//! within a tenant domain.
//!
//! # Key invariants and assumptions
//! - The Casbin model configuration is embedded in `MODEL_CONF`.
//! - Policies and groupings are scoped to the provided domain.
//!
//! # Security considerations
//! - Policy inputs must be trusted or validated before building the enforcer.
//! - Domain scoping is mandatory to prevent cross-tenant access.
use crate::auth::rbac::MODEL_CONF;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use casbin::{CoreApi, DefaultModel, Enforcer, MemoryAdapter, MgmtApi, Result};

/// Build an in-memory Casbin enforcer for a tenant domain.
///
/// # What it does
/// Loads the embedded model, inserts policy and grouping rules, and compiles
/// role links for efficient enforcement.
///
/// # Why it exists
/// Centralizes enforcer setup so all authorization decisions use consistent
/// policy semantics.
///
/// # Invariants
/// - All policies and groupings are applied with the provided `domain`.
///
/// # Errors
/// - Returns Casbin errors for invalid model or policy insertion failures.
///
/// # Example
/// ```rust
/// use controlplane::auth::rbac::enforcer::build_enforcer;
/// use controlplane::auth::rbac::policy_store::{GroupingRule, PolicyRule};
///
/// # async fn build() -> casbin::Result<()> {
/// let policies: Vec<PolicyRule> = Vec::new();
/// let groupings: Vec<GroupingRule> = Vec::new();
/// let _ = build_enforcer(&policies, &groupings, "tenant-a").await?;
/// # Ok(())
/// # }
/// ```
pub async fn build_enforcer(
    policies: &[PolicyRule],
    groupings: &[GroupingRule],
    domain: &str,
) -> Result<Enforcer> {
    // Step 1: Load the embedded Casbin model.
    let model = DefaultModel::from_str(MODEL_CONF).await?;
    // Step 2: Use an in-memory adapter for fast, per-request enforcement.
    let adapter = MemoryAdapter::default();
    let mut enforcer = Enforcer::new(model, adapter).await?;

    for policy in policies {
        // Step 3: Insert policy rules with explicit domain scoping.
        enforcer
            .add_policy(vec![
                policy.subject.clone(),
                domain.to_string(),
                policy.object.clone(),
                policy.action.clone(),
            ])
            .await?;
    }

    for grouping in groupings {
        // Step 4: Insert grouping rules with domain scoping.
        enforcer
            .add_grouping_policy(vec![
                grouping.user.clone(),
                grouping.role.clone(),
                domain.to_string(),
            ])
            .await?;
    }

    // Step 5: Build role links after all rules are loaded.
    enforcer.build_role_links()?;
    Ok(enforcer)
}
