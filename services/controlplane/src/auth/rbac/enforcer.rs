//! Casbin enforcer builder for tenant-scoped RBAC.
//!
//! # Purpose
//! Constructs an in-memory Casbin enforcer loaded with policies and groupings
//! for a specific tenant domain.
use crate::auth::rbac::MODEL_CONF;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use casbin::{CoreApi, DefaultModel, Enforcer, MemoryAdapter, MgmtApi, Result};

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

    enforcer.build_role_links()?;
    Ok(enforcer)
}
