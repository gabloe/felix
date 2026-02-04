//! RBAC authorization helpers for strict object parsing and delegated admin checks.
//!
//! This module centralizes the policy grammar and scope math used by admin APIs.
//! Keeping these checks in one place avoids privilege-escalation drift across
//! endpoints and makes future deny/simulation work additive.
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};

pub const ACTION_RBAC_VIEW: &str = "rbac.view";
pub const ACTION_RBAC_POLICY_MANAGE: &str = "rbac.policy.manage";
pub const ACTION_RBAC_ASSIGNMENT_MANAGE: &str = "rbac.assignment.manage";
pub const ACTION_TENANT_MANAGE: &str = "tenant.manage";
pub const ACTION_NS_MANAGE: &str = "ns.manage";
pub const ACTION_STREAM_MANAGE: &str = "stream.manage";
pub const ACTION_CACHE_MANAGE: &str = "cache.manage";
pub const ACTION_STREAM_PUBLISH: &str = "stream.publish";
pub const ACTION_STREAM_SUBSCRIBE: &str = "stream.subscribe";
pub const ACTION_CACHE_READ: &str = "cache.read";
pub const ACTION_CACHE_WRITE: &str = "cache.write";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    Exact(String),
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedObject {
    Tenant {
        tenant_id: String,
    },
    Namespace {
        tenant_id: String,
        namespace: Segment,
    },
    Stream {
        tenant_id: String,
        namespace: Segment,
        stream: Segment,
    },
    Cache {
        tenant_id: String,
        namespace: Segment,
        cache: Segment,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPermission {
    pub action: String,
    pub object: ParsedObject,
}

/// Validate and normalize RBAC action names.
pub fn canonical_action(action: &str) -> Option<&'static str> {
    match action {
        ACTION_RBAC_VIEW => Some(ACTION_RBAC_VIEW),
        ACTION_RBAC_POLICY_MANAGE => Some(ACTION_RBAC_POLICY_MANAGE),
        ACTION_RBAC_ASSIGNMENT_MANAGE => Some(ACTION_RBAC_ASSIGNMENT_MANAGE),
        ACTION_TENANT_MANAGE => Some(ACTION_TENANT_MANAGE),
        ACTION_NS_MANAGE => Some(ACTION_NS_MANAGE),
        ACTION_STREAM_MANAGE => Some(ACTION_STREAM_MANAGE),
        ACTION_CACHE_MANAGE => Some(ACTION_CACHE_MANAGE),
        ACTION_STREAM_PUBLISH => Some(ACTION_STREAM_PUBLISH),
        ACTION_STREAM_SUBSCRIBE => Some(ACTION_STREAM_SUBSCRIBE),
        ACTION_CACHE_READ => Some(ACTION_CACHE_READ),
        ACTION_CACHE_WRITE => Some(ACTION_CACHE_WRITE),
        _ => None,
    }
}

pub fn parse_permission(raw: &str, tenant_id: &str) -> Result<ParsedPermission, String> {
    let (action, object) = raw
        .split_once(':')
        .ok_or_else(|| "invalid permission format".to_string())?;
    let action = canonical_action(action)
        .ok_or_else(|| format!("unknown action: {action}"))?
        .to_string();
    let object = parse_object(object, tenant_id)?;
    Ok(ParsedPermission { action, object })
}

/// Parse an RBAC object string into a typed structure.
///
/// Canonical grammar:
/// - `tenant:{tenant_id}`
/// - `namespace:{tenant_id}/{namespace}`
/// - `stream:{tenant_id}/{namespace}/{stream}`
/// - `cache:{tenant_id}/{namespace}/{cache}`
pub fn parse_object(raw: &str, tenant_id: &str) -> Result<ParsedObject, String> {
    if raw == "tenant:*" {
        return Err("tenant:* is not allowed".to_string());
    }

    if let Some(rest) = raw.strip_prefix("tenant:") {
        if rest != tenant_id {
            return Err("tenant object must match request tenant".to_string());
        }
        return Ok(ParsedObject::Tenant {
            tenant_id: tenant_id.to_string(),
        });
    }
    if let Some(rest) = raw.strip_prefix("namespace:") {
        let (tid, ns) = split2(rest)?;
        if tid != tenant_id {
            return Err("namespace object tenant mismatch".to_string());
        }
        return Ok(ParsedObject::Namespace {
            tenant_id: tid.to_string(),
            namespace: parse_segment(ns, true)?,
        });
    }
    if let Some(rest) = raw.strip_prefix("stream:") {
        let (tid, ns, stream) = split3(rest)?;
        if tid != tenant_id {
            return Err("stream object tenant mismatch".to_string());
        }
        let namespace = parse_segment(ns, false)?;
        let stream = parse_segment(stream, true)?;
        return Ok(ParsedObject::Stream {
            tenant_id: tid.to_string(),
            namespace,
            stream,
        });
    }
    if let Some(rest) = raw.strip_prefix("cache:") {
        let (tid, ns, cache) = split3(rest)?;
        if tid != tenant_id {
            return Err("cache object tenant mismatch".to_string());
        }
        let namespace = parse_segment(ns, false)?;
        let cache = parse_segment(cache, true)?;
        return Ok(ParsedObject::Cache {
            tenant_id: tid.to_string(),
            namespace,
            cache,
        });
    }

    Err("object does not match RBAC grammar".to_string())
}

pub fn object_within_scope(scope: &ParsedObject, target: &ParsedObject) -> bool {
    match (scope, target) {
        (ParsedObject::Tenant { tenant_id: s }, ParsedObject::Tenant { tenant_id: t }) => s == t,
        (ParsedObject::Tenant { tenant_id: s }, ParsedObject::Namespace { tenant_id: t, .. }) => {
            s == t
        }
        (ParsedObject::Tenant { tenant_id: s }, ParsedObject::Stream { tenant_id: t, .. }) => {
            s == t
        }
        (ParsedObject::Tenant { tenant_id: s }, ParsedObject::Cache { tenant_id: t, .. }) => s == t,
        (
            ParsedObject::Namespace {
                tenant_id: st,
                namespace: sns,
            },
            ParsedObject::Namespace {
                tenant_id: tt,
                namespace: tns,
            },
        ) => st == tt && segment_contains(sns, tns),
        (
            ParsedObject::Namespace {
                tenant_id: st,
                namespace: sns,
            },
            ParsedObject::Stream {
                tenant_id: tt,
                namespace: tns,
                ..
            },
        ) => st == tt && segment_contains(sns, tns),
        (
            ParsedObject::Namespace {
                tenant_id: st,
                namespace: sns,
            },
            ParsedObject::Cache {
                tenant_id: tt,
                namespace: tns,
                ..
            },
        ) => st == tt && segment_contains(sns, tns),
        (
            ParsedObject::Stream {
                tenant_id: st,
                namespace: sns,
                stream: sstream,
            },
            ParsedObject::Stream {
                tenant_id: tt,
                namespace: tns,
                stream: tstream,
            },
        ) => st == tt && segment_contains(sns, tns) && segment_contains(sstream, tstream),
        (
            ParsedObject::Cache {
                tenant_id: st,
                namespace: sns,
                cache: scache,
            },
            ParsedObject::Cache {
                tenant_id: tt,
                namespace: tns,
                cache: tcache,
            },
        ) => st == tt && segment_contains(sns, tns) && segment_contains(scache, tcache),
        _ => false,
    }
}

/// Validate that a new/updated policy rule stays inside caller delegation scope.
pub fn validate_new_rule_allowed(
    caller_scopes: &[ParsedObject],
    tenant_id: &str,
    rule: &PolicyRule,
) -> Result<ParsedObject, String> {
    canonical_action(&rule.action).ok_or_else(|| "unknown action".to_string())?;
    let parsed = parse_object(&rule.object, tenant_id)?;
    if caller_scopes
        .iter()
        .any(|scope| object_within_scope(scope, &parsed))
    {
        Ok(parsed)
    } else {
        Err("scope does not allow policy object".to_string())
    }
}

/// Validate that role assignment cannot grant privileges beyond caller scope.
pub fn validate_assignment_allowed(
    caller_scopes: &[ParsedObject],
    tenant_id: &str,
    assignment: &GroupingRule,
    role_policies: &[PolicyRule],
) -> Result<(), String> {
    if assignment.user.trim().is_empty() || assignment.role.trim().is_empty() {
        return Err("invalid grouping payload".to_string());
    }
    if role_policies.is_empty() {
        return Err("role has no policies".to_string());
    }
    for policy in role_policies {
        let parsed = parse_object(&policy.object, tenant_id)?;
        if !caller_scopes
            .iter()
            .any(|scope| object_within_scope(scope, &parsed))
        {
            return Err("role policy exceeds assignment scope".to_string());
        }
    }
    Ok(())
}

fn parse_segment(raw: &str, allow_star: bool) -> Result<Segment, String> {
    if raw == "*" {
        if allow_star {
            return Ok(Segment::Any);
        }
        return Err("wildcard not allowed in this object position".to_string());
    }
    if raw.is_empty() {
        return Err("empty object segment".to_string());
    }
    if raw.contains(':') {
        return Err("invalid object segment".to_string());
    }
    Ok(Segment::Exact(raw.to_string()))
}

fn segment_contains(scope: &Segment, target: &Segment) -> bool {
    match (scope, target) {
        (Segment::Any, _) => true,
        (Segment::Exact(left), Segment::Exact(right)) => left == right,
        (Segment::Exact(_), Segment::Any) => false,
    }
}

fn split2(input: &str) -> Result<(&str, &str), String> {
    let (a, b) = input
        .split_once('/')
        .ok_or_else(|| "invalid object shape".to_string())?;
    if b.contains('/') {
        return Err("invalid object shape".to_string());
    }
    Ok((a, b))
}

fn split3(input: &str) -> Result<(&str, &str, &str), String> {
    let mut parts = input.split('/');
    let a = parts
        .next()
        .ok_or_else(|| "invalid object shape".to_string())?;
    let b = parts
        .next()
        .ok_or_else(|| "invalid object shape".to_string())?;
    let c = parts
        .next()
        .ok_or_else(|| "invalid object shape".to_string())?;
    if parts.next().is_some() {
        return Err("invalid object shape".to_string());
    }
    Ok((a, b, c))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_object_validation_accepts_canonical_forms() {
        let tenant = "t1";
        assert!(parse_object("tenant:t1", tenant).is_ok());
        assert!(parse_object("namespace:t1/payments", tenant).is_ok());
        assert!(parse_object("namespace:t1/*", tenant).is_ok());
        assert!(parse_object("stream:t1/payments/orders", tenant).is_ok());
        assert!(parse_object("stream:t1/payments/*", tenant).is_ok());
        assert!(parse_object("cache:t1/payments/sessions", tenant).is_ok());
        assert!(parse_object("cache:t1/payments/*", tenant).is_ok());
    }

    #[test]
    fn strict_object_validation_rejects_broad_wildcards() {
        let tenant = "t1";
        assert!(parse_object("tenant:*", tenant).is_err());
        assert!(parse_object("stream:*/*", tenant).is_err());
        assert!(parse_object("cache:*/*", tenant).is_err());
        assert!(parse_object("stream:t1/*/*", tenant).is_err());
        assert!(parse_object("cache:t1/*/*", tenant).is_err());
    }

    #[test]
    fn scope_contains_expected_targets() {
        let scope = parse_object("namespace:t1/payments", "t1").expect("scope");
        let in_scope = parse_object("stream:t1/payments/orders", "t1").expect("target");
        let out_of_scope = parse_object("stream:t1/orders/events", "t1").expect("target");
        assert!(object_within_scope(&scope, &in_scope));
        assert!(!object_within_scope(&scope, &out_of_scope));
    }
}
