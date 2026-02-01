//! Strongly typed identifiers for authz-related resources.
//!
//! # Purpose
//! Wraps string identifiers to reduce accidental mix-ups between tenant,
//! namespace, stream, and cache identifiers.
//!
//! # How it fits
//! These types are used throughout authz, resource building, and policy
//! evaluation to enforce consistent formatting.
//!
//! # Key invariants
//! - Each wrapper contains a non-empty string (not validated here).
//! - Display and `as_str` must return the original value.
//!
//! # Important configuration
//! - None; validation is the responsibility of callers.
//!
//! # Examples
//! ```rust
//! use felix_authz::{Namespace, StreamName};
//!
//! let ns = Namespace::new("payments");
//! let stream = StreamName::new("orders.v1");
//! assert_eq!(format!("{}/{}", ns, stream), "payments/orders.v1");
//! ```
//!
//! # Common pitfalls
//! - Constructing these types with empty strings; validate at the API boundary.
//! - Treating `Display` as sanitized output; it is a raw passthrough.
//!
//! # Future work
//! - Add validation helpers for allowed character sets and length limits.
use serde::{Deserialize, Serialize};

/// Tenant identifier wrapper.
///
/// # Summary
/// Newtype around a tenant string ID.
///
/// # Invariants
/// - The inner string is preserved exactly.
///
/// # Example
/// ```rust
/// use felix_authz::TenantId;
///
/// let tenant = TenantId::new("tenant-a");
/// assert_eq!(tenant.as_str(), "tenant-a");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(String);

impl TenantId {
    /// Construct a new tenant ID wrapper.
    ///
    /// # Parameters
    /// - `value`: raw tenant identifier string.
    ///
    /// # Returns
    /// - A new [`TenantId`].
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Access the inner tenant string.
    ///
    /// # Returns
    /// - The raw tenant identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Namespace identifier wrapper.
///
/// # Summary
/// Newtype around a namespace string.
///
/// # Example
/// ```rust
/// use felix_authz::Namespace;
///
/// let ns = Namespace::new("payments");
/// assert_eq!(ns.to_string(), "payments");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Namespace(String);

impl Namespace {
    /// Construct a new namespace wrapper.
    ///
    /// # Parameters
    /// - `value`: raw namespace string.
    ///
    /// # Returns
    /// - A new [`Namespace`].
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Access the inner namespace string.
    ///
    /// # Returns
    /// - The raw namespace value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Stream name wrapper.
///
/// # Summary
/// Newtype around a stream name string.
///
/// # Example
/// ```rust
/// use felix_authz::StreamName;
///
/// let stream = StreamName::new("orders.v1");
/// assert_eq!(stream.as_str(), "orders.v1");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamName(String);

impl StreamName {
    /// Construct a new stream name wrapper.
    ///
    /// # Parameters
    /// - `value`: raw stream name string.
    ///
    /// # Returns
    /// - A new [`StreamName`].
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Access the inner stream name.
    ///
    /// # Returns
    /// - The raw stream name value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StreamName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Cache scope identifier wrapper.
///
/// # Summary
/// Newtype around a cache scope string.
///
/// # Example
/// ```rust
/// use felix_authz::CacheScope;
///
/// let cache = CacheScope::new("session");
/// assert_eq!(cache.to_string(), "session");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheScope(String);

impl CacheScope {
    /// Construct a new cache scope wrapper.
    ///
    /// # Parameters
    /// - `value`: raw cache scope string.
    ///
    /// # Returns
    /// - A new [`CacheScope`].
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Access the inner cache scope string.
    ///
    /// # Returns
    /// - The raw cache scope value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CacheScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{CacheScope, Namespace, StreamName, TenantId};

    #[test]
    fn type_constructors_and_display() {
        let tenant = TenantId::new("tenant-a");
        let namespace = Namespace::new("payments");
        let stream = StreamName::new("orders");
        let cache = CacheScope::new("session");

        assert_eq!(tenant.as_str(), "tenant-a");
        assert_eq!(namespace.to_string(), "payments");
        assert_eq!(stream.as_str(), "orders");
        assert_eq!(stream.to_string(), "orders");
        assert_eq!(cache.to_string(), "session");
    }
}
