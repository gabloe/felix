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
