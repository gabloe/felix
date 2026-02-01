//! Resource string builders for Felix permissions.
//!
//! # Purpose
//! Provides helpers to build canonical resource identifiers used in policies.
//!
//! # How it fits
//! Action handlers and policy writers use these helpers to keep resource strings
//! consistent across broker and control-plane code.
//!
//! # Key invariants
//! - Resource strings are prefixed by a stable kind (`tenant`, `ns`, `stream`, `cache`).
//! - `stream` and `cache` resources are `kind:{namespace}/{name}`.
//!
//! # Important configuration
//! - None.
//!
//! # Examples
//! ```rust
//! use felix_authz::{Namespace, StreamName, stream_resource};
//!
//! let ns = Namespace::new("payments");
//! let stream = StreamName::new("orders.v1");
//! assert_eq!(stream_resource(&ns, &stream), "stream:payments/orders.v1");
//! ```
//!
//! # Common pitfalls
//! - Using raw strings instead of these helpers can drift from policy format.
//! - Forgetting the namespace separator (`/`) breaks wildcard matching.
//!
//! # Future work
//! - Introduce typed resource enums to avoid stringly-typed identifiers.
use crate::{CacheScope, Namespace, StreamName, TenantId};

/// Build the canonical tenant resource string.
///
/// # Parameters
/// - `tenant_id`: tenant identifier.
///
/// # Returns
/// - Resource string in the form `tenant:{id}`.
pub fn tenant_resource(tenant_id: &TenantId) -> String {
    format!("tenant:{}", tenant_id.as_str())
}

/// Return the wildcard resource string for all tenants.
///
/// # Returns
/// - The string `tenant:*`.
pub fn tenant_wildcard() -> String {
    "tenant:*".to_string()
}

/// Build the canonical namespace resource string.
///
/// # Parameters
/// - `namespace`: namespace identifier.
///
/// # Returns
/// - Resource string in the form `ns:{namespace}`.
pub fn namespace_resource(namespace: &Namespace) -> String {
    format!("ns:{}", namespace.as_str())
}

/// Build the canonical stream resource string.
///
/// # Parameters
/// - `namespace`: namespace identifier.
/// - `stream`: stream name.
///
/// # Returns
/// - Resource string in the form `stream:{namespace}/{stream}`.
pub fn stream_resource(namespace: &Namespace, stream: &StreamName) -> String {
    format!("stream:{}/{}", namespace.as_str(), stream.as_str())
}

/// Build the canonical cache resource string.
///
/// # Parameters
/// - `namespace`: namespace identifier.
/// - `cache`: cache scope identifier.
///
/// # Returns
/// - Resource string in the form `cache:{namespace}/{cache}`.
pub fn cache_resource(namespace: &Namespace, cache: &CacheScope) -> String {
    format!("cache:{}/{}", namespace.as_str(), cache.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_builders() {
        let tenant = TenantId::new("tenant-a");
        let namespace = Namespace::new("payments");
        let stream = StreamName::new("orders.v1");
        let cache = CacheScope::new("session");

        assert_eq!(tenant_resource(&tenant), "tenant:tenant-a");
        assert_eq!(tenant_wildcard(), "tenant:*");
        assert_eq!(namespace_resource(&namespace), "ns:payments");
        assert_eq!(
            stream_resource(&namespace, &stream),
            "stream:payments/orders.v1"
        );
        assert_eq!(cache_resource(&namespace, &cache), "cache:payments/session");
    }
}
