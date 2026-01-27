use crate::{Action, AuthzResult, PermissionPattern};

pub fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let (mut p_idx, mut v_idx) = (0usize, 0usize);
    let (mut star_idx, mut match_idx) = (None, 0usize);
    let pattern_bytes = pattern.as_bytes();
    let value_bytes = value.as_bytes();

    while v_idx < value_bytes.len() {
        if p_idx < pattern_bytes.len() && pattern_bytes[p_idx] == b'*' {
            star_idx = Some(p_idx);
            match_idx = v_idx;
            p_idx += 1;
            continue;
        }

        if p_idx < pattern_bytes.len() && pattern_bytes[p_idx] == value_bytes[v_idx] {
            p_idx += 1;
            v_idx += 1;
            continue;
        }

        if let Some(star) = star_idx {
            p_idx = star + 1;
            match_idx += 1;
            v_idx = match_idx;
            continue;
        }

        return false;
    }

    while p_idx < pattern_bytes.len() && pattern_bytes[p_idx] == b'*' {
        p_idx += 1;
    }

    p_idx == pattern_bytes.len()
}

#[derive(Debug, Clone)]
pub struct PermissionMatcher {
    patterns: Vec<PermissionPattern>,
}

impl PermissionMatcher {
    pub fn new(patterns: Vec<PermissionPattern>) -> Self {
        Self { patterns }
    }

    pub fn from_strings(patterns: &[String]) -> AuthzResult<Self> {
        let mut parsed = Vec::with_capacity(patterns.len());
        for pattern in patterns {
            parsed.push(pattern.parse()?);
        }
        Ok(Self::new(parsed))
    }

    pub fn allows(&self, action: Action, resource: &str) -> bool {
        self.patterns.iter().any(|pattern| {
            pattern.action == action && wildcard_match(&pattern.resource_pattern, resource)
        })
    }

    pub fn patterns(&self) -> &[PermissionPattern] {
        &self.patterns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Action;

    #[test]
    fn wildcard_match_exact() {
        assert!(wildcard_match(
            "stream:payments/orders",
            "stream:payments/orders"
        ));
        assert!(!wildcard_match(
            "stream:payments/orders",
            "stream:payments/orders.v2"
        ));
    }

    #[test]
    fn wildcard_match_suffix() {
        assert!(wildcard_match(
            "stream:payments/*",
            "stream:payments/orders"
        ));
        assert!(wildcard_match(
            "stream:payments/*",
            "stream:payments/orders.v2"
        ));
        assert!(!wildcard_match(
            "stream:payments/*",
            "stream:accounts/orders"
        ));
    }

    #[test]
    fn wildcard_match_any() {
        assert!(wildcard_match("*", "anything"));
    }

    #[test]
    fn wildcard_match_backtrack() {
        assert!(wildcard_match("cache:*:read", "cache:payments:read"));
        assert!(!wildcard_match("cache:*:read", "cache:payments:write"));
    }

    #[test]
    fn wildcard_match_trailing_star() {
        assert!(wildcard_match("stream:payments/*", "stream:payments/"));
    }

    #[test]
    fn matcher_allows() {
        let matcher = PermissionMatcher::new(vec![PermissionPattern::new(
            Action::StreamPublish,
            "stream:payments/orders.*",
        )]);
        assert!(matcher.allows(Action::StreamPublish, "stream:payments/orders.v2"));
        assert!(!matcher.allows(Action::StreamSubscribe, "stream:payments/orders.v2"));
    }

    #[test]
    fn matcher_from_strings_and_patterns() {
        let patterns = vec![
            "stream.publish:stream:payments/orders.*".to_string(),
            "cache.read:cache:payments/session/*".to_string(),
        ];
        let matcher = PermissionMatcher::from_strings(&patterns).expect("parse patterns");
        assert_eq!(matcher.patterns().len(), 2);
        assert!(matcher.allows(Action::StreamPublish, "stream:payments/orders.v1"));
        assert!(matcher.allows(Action::CacheRead, "cache:payments/session/abc"));
    }
}
