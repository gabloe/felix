//! RBAC permission expansion helpers.
//!
//! # Purpose
//! Computes effective permissions for a principal and expands implied actions
//! (e.g., tenant admin implies namespace/stream/cache rights).
use casbin::{Enforcer, RbacApi};
use std::collections::HashSet;

pub fn effective_permissions(enforcer: &Enforcer, principal: &str, domain: &str) -> Vec<String> {
    let rules: Vec<Vec<String>> =
        enforcer.get_implicit_permissions_for_user(principal, Some(domain));

    let mut perms: HashSet<String> = HashSet::new();
    for rule in rules {
        if rule.len() < 4 {
            continue;
        }
        let obj = &rule[2];
        let act = &rule[3];
        perms.insert(format!("{act}:{obj}"));
    }

    expand_inheritance(&mut perms);
    perms.into_iter().collect()
}

fn expand_inheritance(perms: &mut HashSet<String>) {
    let mut implied = Vec::new();
    for perm in perms.iter() {
        let Some((action, object)) = perm.split_once(':') else {
            continue;
        };
        match action {
            "tenant.admin" => {
                implied.push("ns.manage:ns:*".to_string());
                implied.push("stream.publish:stream:*/*".to_string());
                implied.push("stream.subscribe:stream:*/*".to_string());
                implied.push("cache.read:cache:*/*".to_string());
                implied.push("cache.write:cache:*/*".to_string());
                if object != "tenant:*" {
                    implied.push("tenant.admin:tenant:*".to_string());
                }
            }
            "ns.manage" => {
                if let Some(ns) = object.strip_prefix("ns:") {
                    let ns = ns.trim();
                    if ns == "*" {
                        implied.push("stream.publish:stream:*/*".to_string());
                        implied.push("stream.subscribe:stream:*/*".to_string());
                        implied.push("cache.read:cache:*/*".to_string());
                        implied.push("cache.write:cache:*/*".to_string());
                    } else {
                        implied.push(format!("stream.publish:stream:{ns}/*"));
                        implied.push(format!("stream.subscribe:stream:{ns}/*"));
                        implied.push(format!("cache.read:cache:{ns}/*"));
                        implied.push(format!("cache.write:cache:{ns}/*"));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::rbac::enforcer::build_enforcer;
    use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};

    #[tokio::test]
    async fn ns_manage_implies_stream_and_cache() {
        let policies = vec![PolicyRule {
            subject: "role:ns-admin".to_string(),
            object: "ns:payments".to_string(),
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
        assert!(perms.contains(&"ns.manage:ns:payments".to_string()));
        assert!(perms.contains(&"stream.publish:stream:payments/*".to_string()));
        assert!(perms.contains(&"cache.read:cache:payments/*".to_string()));
    }

    #[tokio::test]
    async fn tenant_admin_implies_wildcards() {
        let policies = vec![PolicyRule {
            subject: "role:admin".to_string(),
            object: "tenant:*".to_string(),
            action: "tenant.admin".to_string(),
        }];
        let groupings = vec![GroupingRule {
            user: "p:admin".to_string(),
            role: "role:admin".to_string(),
        }];
        let enforcer = build_enforcer(&policies, &groupings, "tenant-a")
            .await
            .expect("enforcer");
        let perms = effective_permissions(&enforcer, "p:admin", "tenant-a");
        assert!(perms.contains(&"ns.manage:ns:*".to_string()));
        assert!(perms.contains(&"stream.publish:stream:*/*".to_string()));
        assert!(perms.contains(&"cache.write:cache:*/*".to_string()));
    }
}
