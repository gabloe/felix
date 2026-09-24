use super::*;

#[test]
fn local_route_allowed() {
    // Same-region traffic should pass.
    let region = RegionId::new();
    let router = RegionRouter::new(region);
    assert!(router.can_route(&region, &region));
}

#[test]
fn cross_region_denied_by_default() {
    // No bridges means cross-region traffic is denied.
    let source = RegionId::new();
    let dest = RegionId::new();
    let router = RegionRouter::new(source);
    assert!(!router.can_route(&source, &dest));
}

#[test]
fn cross_region_allowed_with_bridge() {
    // Explicit bridge should enable routing.
    let source = RegionId::new();
    let dest = RegionId::new();
    let mut router = RegionRouter::new(source);
    router.allow_bridge(source, dest);
    assert!(router.can_route(&source, &dest));
}

#[test]
fn bridge_is_directional() {
    let source = RegionId::new();
    let dest = RegionId::new();
    let mut router = RegionRouter::new(source);
    router.allow_bridge(source, dest);
    assert!(!router.can_route(&dest, &source));
}

#[test]
fn local_region_accessor_returns_seed() {
    let region = RegionId::new();
    let router = RegionRouter::new(region);
    assert_eq!(*router.local_region(), region);
}
