use super::*;

#[test]
fn io_runtime_assignment_keeps_roles_disjoint() {
    for sequence in 0..32 {
        assert_eq!(io_runtime_index(EndpointRole::Server, sequence, 2), 0);
        assert_eq!(io_runtime_index(EndpointRole::Client, sequence, 2), 1);
    }

    let server_indices: Vec<_> = (0..6)
        .map(|sequence| io_runtime_index(EndpointRole::Server, sequence, 4))
        .collect();
    assert_eq!(server_indices, vec![0, 1, 2, 0, 1, 2]);
    assert_eq!(io_runtime_index(EndpointRole::Client, 99, 4), 3);
    assert_eq!(io_runtime_index(EndpointRole::Server, 99, 1), 0);
    assert_eq!(io_runtime_index(EndpointRole::Client, 99, 1), 0);
}

/// The pool must give every server endpoint its own runtime, or several
/// listeners' drivers land on one thread -- the single feeder that binding
/// several listeners exists to escape.
#[test]
fn a_derived_pool_gives_every_server_endpoint_its_own_runtime() {
    for endpoints in 1..=8 {
        let pool = required_io_runtime_threads(endpoints);
        let assigned: std::collections::HashSet<_> = (0..endpoints)
            .map(|sequence| io_runtime_index(EndpointRole::Server, sequence, pool))
            .collect();
        assert_eq!(
            assigned.len(),
            endpoints,
            "{endpoints} endpoints shared runtimes in a pool of {pool}: {assigned:?}",
        );
        // And never the one reserved for clients.
        let client = io_runtime_index(EndpointRole::Client, 0, pool);
        assert!(
            !assigned.contains(&client),
            "a server took the client runtime"
        );
    }
}

/// The historical default was right for the broker it was written for: one
/// server endpoint needs two runtimes. Deriving must not change that.
#[test]
fn one_server_endpoint_still_wants_the_historical_pool_of_two() {
    assert_eq!(required_io_runtime_threads(1), 2);
}

/// The defect this replaced: a pool of 2 gives every server `% 1`.
#[test]
fn a_pool_of_two_collapses_every_listener_onto_one_runtime() {
    let assigned: std::collections::HashSet<_> = (0..4)
        .map(|sequence| io_runtime_index(EndpointRole::Server, sequence, 2))
        .collect();
    assert_eq!(assigned.len(), 1, "expected the documented collapse");
}
