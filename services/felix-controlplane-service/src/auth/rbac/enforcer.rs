//! Builds the in-memory Casbin enforcer used by token exchange and admin
//! flows. Every rule is inserted with the tenant domain, so cross-tenant
//! matches are impossible by construction.
use crate::auth::rbac::MODEL_CONF;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use casbin::{CoreApi, DefaultModel, Enforcer, MemoryAdapter, MgmtApi, Result};

/// Build a per-tenant enforcer from the embedded model plus the tenant's
/// policy and grouping rules.
///
/// # Errors
/// Casbin model or policy-insertion failures.
pub async fn build_enforcer(
    policies: &[PolicyRule],
    groupings: &[GroupingRule],
    domain: &str,
) -> Result<Enforcer> {
    let model = DefaultModel::from_str(MODEL_CONF).await?;
    let adapter = MemoryAdapter::default();
    let mut enforcer = Enforcer::new(model, adapter).await?;

    for policy in policies {
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
        enforcer
            .add_grouping_policy(vec![
                grouping.user.clone(),
                grouping.role.clone(),
                domain.to_string(),
            ])
            .await?;
    }

    // Role links must be rebuilt after the rules are in, or role inheritance
    // silently doesn't apply.
    enforcer.build_role_links()?;
    Ok(enforcer)
}
