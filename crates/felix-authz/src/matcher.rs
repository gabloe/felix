//! Permission matching helpers for Felix authorization.
//!
//! # Purpose
//! Implements a lightweight wildcard matcher and a permission matcher that
//! evaluates action/resource pairs.
//!
//! # How it fits
//! Brokers and control-plane components use this matcher to validate whether
//! a request's action/resource is allowed by a set of permission patterns.
//!
//! # Key invariants
//! - `*` matches zero or more characters.
//! - Matching is byte-based and case-sensitive.
//!
//! # Important configuration
//! - None; matching behavior is fixed and shared across services.
//!
//! # Examples
//! ```rust
//! use felix_authz::{Action, PermissionMatcher, PermissionPattern};
//!
//! let matcher = PermissionMatcher::new(vec![
//!     PermissionPattern::new(Action::StreamPublish, "stream:tenant-a/payments/*"),
//! ]);
//! assert!(matcher.allows(Action::StreamPublish, "stream:tenant-a/payments/orders"));
//! ```
//!
//! # Common pitfalls
//! - Assuming glob syntax beyond `*` (no `?`, no character classes).
//! - Passing unvalidated pattern strings; use `PermissionPattern::parse`.
//!
//! # Future work
//! - Consider a compiled matcher for large policy sets.
use crate::{Action, AuthzResult, PermissionPattern};

/// Match a value against a wildcard pattern using `*` as a glob.
///
/// # Parameters
/// - `pattern`: the wildcard pattern to apply.
/// - `value`: the candidate string to test.
///
/// # Returns
/// - `true` if the value matches the pattern.
///
/// # Invariants
/// - Matching is case-sensitive and byte-based.
///
/// # Performance
/// - O(pattern length × value length) worst case due to backtracking.
///
/// # Example
/// ```rust
/// use felix_authz::wildcard_match;
///
/// assert!(wildcard_match("stream:*", "stream:tenant-a/payments"));
/// ```
pub fn wildcard_match(pattern: &str, value: &str) -> bool {
    // Fast-path for the common "allow everything" pattern.
    if pattern == "*" {
        return true;
    }

    let (mut p_idx, mut v_idx) = (0usize, 0usize);
    let (mut star_idx, mut match_idx) = (None, 0usize);
    let pattern_bytes = pattern.as_bytes();
    let value_bytes = value.as_bytes();

    // Greedy scan with backtracking when a `*` is encountered.
    while v_idx < value_bytes.len() {
        if p_idx < pattern_bytes.len() && pattern_bytes[p_idx] == b'*' {
            // Record the position of the star to allow backtracking.
            star_idx = Some(p_idx);
            match_idx = v_idx;
            p_idx += 1;
            continue;
        }

        if p_idx < pattern_bytes.len() && pattern_bytes[p_idx] == value_bytes[v_idx] {
            // Literal match; advance both pointers.
            p_idx += 1;
            v_idx += 1;
            continue;
        }

        if let Some(star) = star_idx {
            // Backtrack to the last `*` and consume one more byte.
            p_idx = star + 1;
            match_idx += 1;
            v_idx = match_idx;
            continue;
        }

        // No wildcard to backtrack to and mismatch found.
        return false;
    }

    // Consume any trailing `*` in the pattern.
    while p_idx < pattern_bytes.len() && pattern_bytes[p_idx] == b'*' {
        p_idx += 1;
    }

    p_idx == pattern_bytes.len()
}

/// Evaluates permissions for action/resource pairs using wildcard patterns.
///
/// # Summary
/// Holds parsed permission patterns and checks them against requests.
///
/// # Invariants
/// - Patterns must be validated to ensure `action:resource` structure.
///
/// # Example
/// ```rust
/// use felix_authz::{Action, PermissionMatcher, PermissionPattern};
///
/// let matcher = PermissionMatcher::new(vec![
///     PermissionPattern::new(Action::CacheRead, "cache:tenant-a/payments/*"),
/// ]);
/// assert!(matcher.allows(Action::CacheRead, "cache:tenant-a/payments/session/1"));
/// ```
#[derive(Debug, Clone)]
pub struct PermissionMatcher {
    patterns: Vec<PermissionPattern>,
}

impl PermissionMatcher {
    /// Construct a matcher from validated permission patterns.
    ///
    /// # Parameters
    /// - `patterns`: parsed permission patterns.
    ///
    /// # Returns
    /// - A new [`PermissionMatcher`].
    ///
    /// # Performance
    /// - Clones the vector; complexity is O(patterns).
    pub fn new(patterns: Vec<PermissionPattern>) -> Self {
        Self { patterns }
    }

    /// Parse a list of permission strings into a matcher.
    ///
    /// # Parameters
    /// - `patterns`: raw permission strings like `"action:resource"`.
    ///
    /// # Returns
    /// - A matcher containing parsed patterns.
    ///
    /// # Errors
    /// - Returns the underlying parse error if a pattern is invalid.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::PermissionMatcher;
    ///
    /// let patterns = vec!["stream.publish:stream:tenant-a/payments/*".to_string()];
    /// let matcher = PermissionMatcher::from_strings(&patterns).expect("parse");
    /// assert!(matcher.allows(felix_authz::Action::StreamPublish, "stream:tenant-a/payments/1"));
    /// ```
    pub fn from_strings(patterns: &[String]) -> AuthzResult<Self> {
        let mut parsed = Vec::with_capacity(patterns.len());
        for pattern in patterns {
            // Each pattern is parsed into a typed representation for fast matching.
            parsed.push(pattern.parse()?);
        }
        Ok(Self::new(parsed))
    }

    /// Check whether any pattern allows the action on the resource.
    ///
    /// # Parameters
    /// - `action`: requested action.
    /// - `resource`: requested resource identifier.
    ///
    /// # Returns
    /// - `true` if any pattern matches.
    ///
    /// # Performance
    /// - O(number of patterns).
    pub fn allows(&self, action: Action, resource: &str) -> bool {
        // Any matching pattern grants access; no denies are modeled here.
        self.patterns.iter().any(|pattern| {
            pattern.action == action && wildcard_match(&pattern.resource_pattern, resource)
        })
    }

    /// Return the parsed permission patterns.
    ///
    /// # Returns
    /// - Slice of parsed patterns, useful for inspection and tests.
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
            "stream:tenant-a/payments/orders",
            "stream:tenant-a/payments/orders"
        ));
        assert!(!wildcard_match(
            "stream:tenant-a/payments/orders",
            "stream:tenant-a/payments/orders.v2"
        ));
    }

    #[test]
    fn wildcard_match_suffix() {
        assert!(wildcard_match(
            "stream:tenant-a/payments/*",
            "stream:tenant-a/payments/orders"
        ));
        assert!(wildcard_match(
            "stream:tenant-a/payments/*",
            "stream:tenant-a/payments/orders.v2"
        ));
        assert!(!wildcard_match(
            "stream:tenant-a/payments/*",
            "stream:accounts/orders"
        ));
    }

    #[test]
    fn wildcard_match_any() {
        assert!(wildcard_match("*", "anything"));
    }

    #[test]
    fn wildcard_match_backtrack() {
        assert!(wildcard_match(
            "cache:*:read",
            "cache:tenant-a/payments:read"
        ));
        assert!(!wildcard_match(
            "cache:*:read",
            "cache:tenant-a/payments:write"
        ));
    }

    #[test]
    fn wildcard_match_trailing_star() {
        assert!(wildcard_match(
            "stream:tenant-a/payments/*",
            "stream:tenant-a/payments/"
        ));
    }

    #[test]
    fn matcher_allows() {
        let matcher = PermissionMatcher::new(vec![PermissionPattern::new(
            Action::StreamPublish,
            "stream:tenant-a/payments/orders.*",
        )]);
        assert!(matcher.allows(Action::StreamPublish, "stream:tenant-a/payments/orders.v2"));
        assert!(!matcher.allows(
            Action::StreamSubscribe,
            "stream:tenant-a/payments/orders.v2"
        ));
    }

    #[test]
    fn matcher_from_strings_and_patterns() {
        let patterns = vec![
            "stream.publish:stream:tenant-a/payments/orders.*".to_string(),
            "cache.read:cache:tenant-a/payments/session/*".to_string(),
        ];
        let matcher = PermissionMatcher::from_strings(&patterns).expect("parse patterns");
        assert_eq!(matcher.patterns().len(), 2);
        assert!(matcher.allows(Action::StreamPublish, "stream:tenant-a/payments/orders.v1"));
        assert!(matcher.allows(Action::CacheRead, "cache:tenant-a/payments/session/abc"));
    }
}
