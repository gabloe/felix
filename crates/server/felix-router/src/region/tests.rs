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

#[test]
fn with_bridges_allows_each_listed_direction() {
    let router =
        RegionRouter::with_bridges("eu".to_string(), [("eu".to_string(), "us".to_string())]);
    assert!(router.can_route(&"eu".to_string(), &"us".to_string()));
    assert!(!router.can_route(&"us".to_string(), &"eu".to_string()));
}

#[test]
fn parse_bridges_reads_directional_pairs() {
    assert_eq!(
        parse_bridges(" eu>us , us > eu,").unwrap(),
        vec![
            ("eu".to_string(), "us".to_string()),
            ("us".to_string(), "eu".to_string()),
        ]
    );
}

#[test]
fn parse_bridges_blank_is_no_bridges() {
    assert!(parse_bridges("").unwrap().is_empty());
    assert!(parse_bridges(" , ").unwrap().is_empty());
}

#[test]
fn parse_bridges_refuses_malformed_pairs() {
    for bad in ["eu", "eu>", ">us", "eu>us>ap", "eu-us"] {
        assert!(parse_bridges(bad).is_err(), "{bad} should be refused");
    }
}
