//! Canonical resource strings for policies. Always build them through these
//! helpers — a hand-rolled string that drops the `/` separator or the kind
//! prefix silently stops matching wildcards.
use crate::{CacheScope, Namespace, StreamName, TenantId};

/// `tenant:{id}`
pub fn tenant_resource(tenant_id: &TenantId) -> String {
    format!("tenant:{}", tenant_id.as_str())
}

/// `namespace:{tenant}/{namespace}`
pub fn namespace_resource(tenant_id: &TenantId, namespace: &Namespace) -> String {
    format!("namespace:{}/{}", tenant_id.as_str(), namespace.as_str())
}

/// `stream:{tenant}/{namespace}/{stream}`
pub fn stream_resource(tenant_id: &TenantId, namespace: &Namespace, stream: &StreamName) -> String {
    format!(
        "stream:{}/{}/{}",
        tenant_id.as_str(),
        namespace.as_str(),
        stream.as_str()
    )
}

/// `cache:{tenant}/{namespace}/{cache}`
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
