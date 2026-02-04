//! RBAC permission expansion helpers.
//!
//! # Purpose and responsibility
//! Computes effective permissions from Casbin and expands implied actions
//! (e.g., tenant admin implies namespace/stream/cache rights).
//!
//! # Where it fits in Felix
//! Used during token exchange to derive the permission set embedded into Felix
//! tokens and by admin flows that need to reason about effective access.
//!
//! # Key invariants and assumptions
//! - Input permissions are `action:object` strings derived from Casbin rules.
//! - Expansion rules must not widen scope beyond the implied hierarchy.
//!
//! # Security considerations
//! - Expansion must be conservative; never grant more than implied by policy.
//! - Wildcard propagation must be carefully constrained by object prefixes.
use crate::auth::rbac::authorize::{
    ACTION_CACHE_READ, ACTION_CACHE_WRITE, ACTION_NS_MANAGE, ACTION_TENANT_MANAGE, canonical_action,
};
use casbin::{Enforcer, RbacApi};
use std::collections::HashSet;

/// Compute effective permissions for a principal within a domain.
///
/// # What it does
/// Loads implicit permissions from Casbin, normalizes them into
/// `action:object` strings, and applies inheritance expansion.
///
/// # Why it exists
/// Allows the token exchange flow to embed a flattened permission list that is
/// fast to evaluate by brokers.
///
/// # Invariants
/// - Returned permissions are de-duplicated.
/// - All permissions are scoped to the provided domain.
///
/// # Errors
/// - Does not return errors; relies on Casbin APIs that return empty results.
///
/// # Example
/// ```rust,no_run
/// use controlplane::auth::rbac::permissions::effective_permissions;
/// use casbin::Enforcer;
///
/// # async fn demo(enforcer: &Enforcer) {
/// let perms = effective_permissions(enforcer, "p:user", "tenant-a");
/// let _ = perms;
/// # }
/// ```
pub fn effective_permissions(enforcer: &Enforcer, principal: &str, domain: &str) -> Vec<String> {
    // Step 1: Ask Casbin for implicit permissions (roles + groupings).
    let rules: Vec<Vec<String>> =
        enforcer.get_implicit_permissions_for_user(principal, Some(domain));

    // Step 2: Normalize to `action:object` strings and de-duplicate.
    let mut perms: HashSet<String> = HashSet::new();
    for rule in rules {
        if rule.len() < 4 {
            continue;
        }
        let obj = &rule[2];
        let Some(act) = canonical_action(&rule[3]) else {
            continue;
        };
        perms.insert(format!("{act}:{obj}"));
    }

    // Step 3: Expand implied permissions (admin -> namespace/stream/cache).
    expand_inheritance(&mut perms);
    perms.into_iter().collect()
}

fn expand_inheritance(perms: &mut HashSet<String>) {
    // Collect implied permissions to avoid mutating the set while iterating.
    let mut implied = Vec::new();
    for perm in perms.iter() {
        let Some((action, object)) = perm.split_once(':') else {
            continue;
        };
        match action {
            ACTION_TENANT_MANAGE => {
                if let Some(tenant_id) = tenant_id_from_object(object) {
                    implied.push(format!("{ACTION_NS_MANAGE}:namespace:{tenant_id}/*"));
                    implied.push(format!("stream.manage:stream:{tenant_id}/*/*"));
                    implied.push(format!("stream.publish:stream:{tenant_id}/*/*"));
                    implied.push(format!("stream.subscribe:stream:{tenant_id}/*/*"));
                    implied.push(format!("cache.manage:cache:{tenant_id}/*/*"));
                    implied.push(format!("{ACTION_CACHE_READ}:cache:{tenant_id}/*/*"));
                    implied.push(format!("{ACTION_CACHE_WRITE}:cache:{tenant_id}/*/*"));
                }
            }
            ACTION_NS_MANAGE => {
                if let Some((tenant_id, namespace)) = namespace_from_object(object) {
                    if namespace == "*" {
                        implied.push(format!("stream.manage:stream:{tenant_id}/*/*"));
                        implied.push(format!("stream.publish:stream:{tenant_id}/*/*"));
                        implied.push(format!("stream.subscribe:stream:{tenant_id}/*/*"));
                        implied.push(format!("cache.manage:cache:{tenant_id}/*/*"));
                        implied.push(format!("{ACTION_CACHE_READ}:cache:{tenant_id}/*/*"));
                        implied.push(format!("{ACTION_CACHE_WRITE}:cache:{tenant_id}/*/*"));
                    } else {
                        implied.push(format!("stream.manage:stream:{tenant_id}/{namespace}/*"));
                        implied.push(format!("stream.publish:stream:{tenant_id}/{namespace}/*"));
                        implied.push(format!("stream.subscribe:stream:{tenant_id}/{namespace}/*"));
                        implied.push(format!("cache.manage:cache:{tenant_id}/{namespace}/*"));
                        implied.push(format!(
                            "{ACTION_CACHE_READ}:cache:{tenant_id}/{namespace}/*"
                        ));
                        implied.push(format!(
                            "{ACTION_CACHE_WRITE}:cache:{tenant_id}/{namespace}/*"
                        ));
                    }
                }
            }
            _ => {}
        }
    }

    for perm in implied {
        perms.insert(perm);
    }
}

fn tenant_id_from_object(object: &str) -> Option<&str> {
    object
        .strip_prefix("tenant:")
        .filter(|value| !value.is_empty())
        .or_else(|| {
            object
                .strip_prefix("namespace:")
                .and_then(|rest| rest.split_once('/').map(|(tid, _)| tid))
        })
        .or_else(|| {
            object
                .strip_prefix("stream:")
                .and_then(|rest| rest.split_once('/').map(|(tid, _)| tid))
        })
        .or_else(|| {
            object
                .strip_prefix("cache:")
                .and_then(|rest| rest.split_once('/').map(|(tid, _)| tid))
        })
}

fn namespace_from_object(object: &str) -> Option<(&str, &str)> {
    if let Some(rest) = object.strip_prefix("namespace:") {
        return rest.split_once('/');
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::rbac::enforcer::build_enforcer;
    use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};

    #[tokio::test]
    async fn ns_manage_implies_stream_and_cache() {
        let policies = vec![PolicyRule {
            subject: "role:ns-admin".to_string(),
            object: "namespace:tenant-a/payments".to_string(),
            action: "ns.manage".to_string(),
        }];
        let groupings = vec![GroupingRule {
            user: "p:user".to_string(),
            role: "role:ns-admin".to_string(),
        }];
        let enforcer = build_enforcer(&policies, &groupings, "tenant-a")
            .await
            .expect("enforcer");

        let perms = effective_permissions(&enforcer, "p:user", "tenant-a");
        assert!(perms.contains(&"ns.manage:namespace:tenant-a/payments".to_string()));
        assert!(perms.contains(&"stream.publish:stream:tenant-a/payments/*".to_string()));
        assert!(perms.contains(&"cache.read:cache:tenant-a/payments/*".to_string()));
    }

    #[tokio::test]
    async fn tenant_admin_implies_wildcards() {
        let policies = vec![PolicyRule {
            subject: "role:admin".to_string(),
            object: "tenant:tenant-a".to_string(),
            action: "tenant.manage".to_string(),
        }];
        let groupings = vec![GroupingRule {
            user: "p:admin".to_string(),
            role: "role:admin".to_string(),
        }];
        let enforcer = build_enforcer(&policies, &groupings, "tenant-a")
            .await
            .expect("enforcer");
        let perms = effective_permissions(&enforcer, "p:admin", "tenant-a");
        assert!(perms.contains(&"ns.manage:namespace:tenant-a/*".to_string()));
        assert!(perms.contains(&"stream.publish:stream:tenant-a/*/*".to_string()));
        assert!(perms.contains(&"cache.write:cache:tenant-a/*/*".to_string()));
    }
}
