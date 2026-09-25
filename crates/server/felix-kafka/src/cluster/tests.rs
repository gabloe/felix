use felix_authz::PermissionMatcher;

use super::{Endpoint, Principal, kafka_node_id};

#[test]
fn endpoint_parses_hostnames_ips_and_bracketed_ipv6() {
    let parsed = Endpoint::parse("n1", "kafka.example:9092").expect("hostname");
    assert_eq!((parsed.host.as_str(), parsed.port), ("kafka.example", 9092));
    let parsed = Endpoint::parse("n1", "10.0.0.7:19092").expect("ip");
    assert_eq!((parsed.host.as_str(), parsed.port), ("10.0.0.7", 19092));
    let parsed = Endpoint::parse("n1", "[::1]:9092").expect("ipv6");
    assert_eq!((parsed.host.as_str(), parsed.port), ("::1", 9092));

    assert!(Endpoint::parse("n1", "no-port").is_none());
    assert!(Endpoint::parse("n1", "host:0").is_none());
    assert!(Endpoint::parse("n1", ":9092").is_none());
    assert!(Endpoint::parse("n1", "host:99999").is_none());
}

#[test]
fn node_ids_map_to_stable_non_negative_broker_ids() {
    let ids: Vec<i32> = ["node-a", "node-b", "node-c", ""]
        .iter()
        .map(|id| kafka_node_id(id))
        .collect();
    assert!(ids.iter().all(|id| *id >= 0));
    assert_eq!(kafka_node_id("node-a"), ids[0]);
    assert_ne!(ids[0], ids[1]);
    assert_ne!(ids[1], ids[2]);
}

#[test]
fn a_token_principal_reads_only_what_its_permissions_name() {
    let matcher =
        PermissionMatcher::from_strings(&["stream.subscribe:stream:t1/orders/*".to_string()])
            .expect("matcher");
    let principal = Principal::with_permissions("t1", matcher);
    assert!(principal.may_read("orders", "created"));
    assert!(!principal.may_read("billing", "invoices"));

    // Publish rights do not grant a read.
    let matcher =
        PermissionMatcher::from_strings(&["stream.publish:stream:t1/orders/*".to_string()])
            .expect("matcher");
    assert!(!Principal::with_permissions("t1", matcher).may_read("orders", "created"));
}

#[test]
fn the_anonymous_principal_reads_its_whole_tenant() {
    let principal = Principal::anonymous("dev");
    assert_eq!(principal.tenant_id(), "dev");
    assert!(principal.may_read("any", "stream"));
}
