use crate::{CacheScope, Namespace, StreamName, TenantId};

pub fn tenant_resource(tenant_id: &TenantId) -> String {
    format!("tenant:{}", tenant_id.as_str())
}

pub fn tenant_wildcard() -> String {
    "tenant:*".to_string()
}

pub fn namespace_resource(namespace: &Namespace) -> String {
    format!("ns:{}", namespace.as_str())
}

pub fn stream_resource(namespace: &Namespace, stream: &StreamName) -> String {
    format!("stream:{}/{}", namespace.as_str(), stream.as_str())
}

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
