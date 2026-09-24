//! Followers: how many, which nodes, and what they cost.
use super::*;

/// A replicated stream gets followers, and they are not the leader.
#[test]
fn a_replicated_shard_is_given_followers() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);

    let (_, leader, replicas) = plan.to_place().next().expect("placed");
    assert_eq!(replicas.len(), 2, "three copies means the leader plus two");
    assert!(
        !replicas.contains(&leader.to_string()),
        "the leader must not also be listed as its own follower",
    );
    let unique: BTreeSet<_> = replicas.iter().collect();
    assert_eq!(
        unique.len(),
        replicas.len(),
        "a node cannot hold two copies"
    );
}

/// Fewer nodes than copies is not an error. Placement records what it achieved,
/// because an assignment naming a node that holds nothing is the lie failover
/// would act on.
#[test]
fn asking_for_more_copies_than_nodes_records_what_exists() {
    let streams = vec![replicated_stream("orders", 1, 5)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
    ];
    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);

    let (_, leader, replicas) = plan.to_place().next().expect("placed");
    assert_eq!(replicas.len(), 1, "two nodes can hold two copies, not five");
    assert!(!replicas.contains(&leader.to_string()));
}

/// The default is leader-only, so a stream that never asked for replication
/// behaves exactly as it did before replication existed.
#[test]
fn an_unreplicated_stream_gets_no_followers() {
    let streams = vec![stream("orders", 1)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);

    let (_, _, replicas) = plan.to_place().next().expect("placed");
    assert!(replicas.is_empty(), "replication_factor 1 is leader-only");
}

/// The whole replica set is a deterministic function of the shard key and the
/// cluster, so two control-plane instances planning the same cluster agree.
#[test]
fn replica_selection_is_deterministic() {
    let streams = vec![replicated_stream("orders", 4, 3)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];

    let first: Vec<_> = plan(&streams, &[], &nodes, &[], &NothingCaughtUp)
        .to_place()
        .map(|(k, l, r)| (k.clone(), l.to_string(), r.to_vec()))
        .collect();

    // Same cluster, nodes presented in a different order.
    let mut shuffled = nodes.clone();
    shuffled.reverse();
    let second: Vec<_> = plan(&streams, &[], &shuffled, &[], &NothingCaughtUp)
        .to_place()
        .map(|(k, l, r)| (k.clone(), l.to_string(), r.to_vec()))
        .collect();

    assert_eq!(first, second, "placement must not depend on input order");
}

/// A replica holds a copy, so it consumes capacity exactly as leadership does.
#[test]
fn replicas_count_against_node_capacity() {
    let streams = vec![replicated_stream("orders", 2, 2)];
    // Room for one copy each, of any kind.
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, Some(1)),
        node("broker-b", NodeLifecycle::Live, Some(1)),
    ];

    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);
    let placed: Vec<_> = plan.to_place().collect();

    // Two nodes with room for one copy each can hold one shard's leader and one
    // follower, and nothing more.
    let total_copies: usize = placed.iter().map(|(_, _, r)| 1 + r.len()).sum();
    assert!(
        total_copies <= 2,
        "capacity was exceeded: {total_copies} copies across two single-slot nodes",
    );
}
