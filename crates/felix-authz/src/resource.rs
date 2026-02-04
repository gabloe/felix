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
//! - Resource strings are prefixed by a stable kind (`tenant`, `namespace`, `stream`, `cache`).
//! - Namespace, stream, and cache resources are always tenant-qualified.
//!
//! # Important configuration
//! - None.
//!
//! # Examples
//! ```rust
//! use felix_authz::{Namespace, StreamName, TenantId, stream_resource};
//!
//! let tenant = TenantId::new("tenant-a");
//! let ns = Namespace::new("payments");
//! let stream = StreamName::new("orders.v1");
//! assert_eq!(
//!     stream_resource(&tenant, &ns, &stream),
//!     "stream:tenant-a/payments/orders.v1"
//! );
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

/// Build the canonical namespace resource string.
///
/// # Returns
/// - `namespace:{tenant_id}/{namespace}`.
pub fn namespace_resource(tenant_id: &TenantId, namespace: &Namespace) -> String {
    format!("namespace:{}/{}", tenant_id.as_str(), namespace.as_str())
}

/// Build the canonical stream resource string.
///
/// # Parameters
/// - `tenant_id`: tenant identifier.
/// - `namespace`: namespace identifier.
/// - `stream`: stream name.
///
/// # Returns
/// - `stream:{tenant_id}/{namespace}/{stream}`.
pub fn stream_resource(tenant_id: &TenantId, namespace: &Namespace, stream: &StreamName) -> String {
    format!(
        "stream:{}/{}/{}",
        tenant_id.as_str(),
        namespace.as_str(),
        stream.as_str()
    )
}

/// Build the canonical cache resource string.
///
/// # Parameters
/// - `tenant_id`: tenant identifier.
/// - `namespace`: namespace identifier.
/// - `cache`: cache scope identifier.
///
/// # Returns
/// - `cache:{tenant_id}/{namespace}/{cache}`.
pub fn cache_resource(tenant_id: &TenantId, namespace: &Namespace, cache: &CacheScope) -> String {
    format!(
        "cache:{}/{}/{}",
        tenant_id.as_str(),
        namespace.as_str(),
        cache.as_str()
    )
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
        assert_eq!(
            namespace_resource(&tenant, &namespace),
            "namespace:tenant-a/payments"
        );
        assert_eq!(
            stream_resource(&tenant, &namespace, &stream),
            "stream:tenant-a/payments/orders.v1"
        );
        assert_eq!(
            cache_resource(&tenant, &namespace, &cache),
            "cache:tenant-a/payments/session"
        );
    }
}
