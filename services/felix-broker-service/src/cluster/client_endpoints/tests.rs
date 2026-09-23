use super::*;

fn endpoint(node_id: &str, addr: &str) -> BrokerEndpoint {
    BrokerEndpoint {
        node_id: node_id.to_string(),
        addr: addr.to_string(),
    }
}

/// Before the first refresh a broker has not been told anything, and says
/// so. Reporting itself would be a guess about an address it has not been
/// configured to advertise.
#[test]
fn a_broker_that_has_not_refreshed_reports_nothing() {
    assert!(ClientEndpoints::new().snapshot().is_empty());
}

/// **A refresh replaces, it does not merge.** A broker that has left the
/// cluster or stopped advertising a client address has to leave the answer;
/// merging would keep handing clients an address nothing stands behind.
#[test]
fn a_refresh_drops_what_it_no_longer_reports() {
    let endpoints = ClientEndpoints::new();
    endpoints.publish(vec![
        endpoint("broker-a", "10.0.0.4:5000"),
        endpoint("broker-b", "10.0.0.5:5000"),
    ]);
    endpoints.publish(vec![endpoint("broker-a", "10.0.0.4:5000")]);

    let snapshot = endpoints.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].node_id, "broker-a");
}

/// A snapshot taken before a refresh is unaffected by it, so a request
/// being answered cannot see the list change underneath it.
#[test]
fn a_snapshot_is_not_disturbed_by_a_later_refresh() {
    let endpoints = ClientEndpoints::new();
    endpoints.publish(vec![endpoint("broker-a", "10.0.0.4:5000")]);
    let held = endpoints.snapshot();

    endpoints.publish(Vec::new());

    assert_eq!(held.len(), 1, "a held snapshot changed under a refresh");
    assert!(endpoints.snapshot().is_empty());
}
