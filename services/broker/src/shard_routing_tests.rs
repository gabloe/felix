//! Ingress dispatch: what a broker does with a request for each ownership state.
use super::*;
use crate::shard_lifecycle::ShardLifecycle;
use crate::shard_watch::ShardAssignment;
use felix_router::{NodeRef, RegionRouter};
use std::collections::HashMap;

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
    }
}

fn assignment(shard: u32, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: key(shard),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
    }
}

fn node(id: &str, port: u16, live: bool) -> NodeRef {
    NodeRef {
        node_id: id.to_string(),
        advertise_addr: SocketAddr::from(([10, 0, 0, 4], port)),
        region: "us-west-2".to_string(),
        live,
    }
}

fn catalog() -> HashMap<String, NodeRef> {
    [node("broker-a", 7001, true), node("broker-b", 7002, true)]
        .into_iter()
        .map(|n| (n.node_id.clone(), n))
        .collect()
}

/// Build an ingress router whose view is `assignments`, with `opened` shards
/// already through their opening phase locally.
async fn ingress(assignments: &[ShardAssignment], opened: &[u32]) -> IngressRouter {
    let nodes = catalog();
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let owned: HashMap<ShardKey, ShardAssignment> = assignments
        .iter()
        .map(|a| (a.key.clone(), a.clone()))
        .collect();
    router.publish(routing_table_from(&owned, &nodes), &nodes);

    let mut lifecycle = ShardLifecycle::new("broker-a");
    for assignment in assignments {
        lifecycle.observe(&assignment.key, Some(assignment));
        if opened.contains(&assignment.key.shard) {
            lifecycle.opened(&assignment.key, assignment.generation);
        }
    }
    IngressRouter::new(router, Arc::new(Mutex::new(lifecycle)))
}

/// The property that must not regress: a broker with no cluster identity
/// behaves exactly as it did before clustering existed.
#[tokio::test]
async fn a_single_node_broker_always_serves_locally() {
    assert_eq!(dispatch(None, &key(0)).await, Dispatch::Local);
    assert_eq!(dispatch(None, &key(41)).await, Dispatch::Local);
}

#[tokio::test]
async fn an_owned_and_open_shard_is_served_locally() {
    let ingress = ingress(&[assignment(0, "broker-a", 3)], &[0]).await;
    assert_eq!(dispatch(Some(&ingress), &key(0)).await, Dispatch::Local);
}

/// The reason ownership and readiness are two separate sources: the cluster
/// says this shard is ours, but the log is not recovered, and acknowledging a
/// write now would promise durability that is not set up.
#[tokio::test]
async fn an_owned_shard_still_opening_is_not_served() {
    let ingress = ingress(&[assignment(0, "broker-a", 3)], &[]).await;
    assert_eq!(
        dispatch(Some(&ingress), &key(0)).await,
        Dispatch::Unavailable(Reason::NotReady),
    );
}

/// Remote must be its own outcome, not an error, or M4 has nothing to act on.
#[tokio::test]
async fn a_shard_owned_elsewhere_is_forwardable() {
    let ingress = ingress(&[assignment(0, "broker-b", 2)], &[]).await;
    assert_eq!(
        dispatch(Some(&ingress), &key(0)).await,
        Dispatch::Forward {
            node_id: "broker-b".to_string(),
            advertise_addr: SocketAddr::from(([10, 0, 0, 4], 7002)),
        },
    );
}

/// The acceptance criterion: an unassigned shard must never be accepted
/// locally, or a broker writes data nobody asked it to hold.
#[tokio::test]
async fn an_unassigned_shard_is_refused() {
    let ingress = ingress(&[], &[]).await;
    assert_eq!(
        dispatch(Some(&ingress), &key(0)).await,
        Dispatch::Unavailable(Reason::NotAssigned),
    );
}

#[tokio::test]
async fn an_unavailable_owner_is_reported_with_its_reason() {
    let nodes: HashMap<String, NodeRef> = [
        node("broker-a", 7001, true),
        node("broker-dead", 7009, false),
    ]
    .into_iter()
    .map(|n| (n.node_id.clone(), n))
    .collect();
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let owned: HashMap<ShardKey, ShardAssignment> = [(key(0), assignment(0, "broker-dead", 1))]
        .into_iter()
        .collect();
    router.publish(routing_table_from(&owned, &nodes), &nodes);

    let ingress = IngressRouter::new(
        router,
        Arc::new(Mutex::new(ShardLifecycle::new("broker-a"))),
    );
    match dispatch(Some(&ingress), &key(0)).await {
        Dispatch::Unavailable(Reason::OwnerUnavailable(detail)) => {
            assert!(detail.contains("broker-dead"), "{detail}");
        }
        other => panic!("expected an unavailable owner, got {other:?}"),
    }
}

/// A shard moving away must stop being served here immediately, not on the next
/// restart.
#[tokio::test]
async fn losing_a_shard_stops_local_service() {
    let nodes = catalog();
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let mut lifecycle = ShardLifecycle::new("broker-a");
    let mine = assignment(0, "broker-a", 1);
    lifecycle.observe(&mine.key, Some(&mine));
    lifecycle.opened(&mine.key, 1);

    let owned: HashMap<ShardKey, ShardAssignment> = [(key(0), mine)].into_iter().collect();
    router.publish(routing_table_from(&owned, &nodes), &nodes);
    let lifecycle = Arc::new(Mutex::new(lifecycle));
    let ingress = IngressRouter::new(Arc::clone(&router), Arc::clone(&lifecycle));
    assert_eq!(dispatch(Some(&ingress), &key(0)).await, Dispatch::Local);

    // Reassigned to broker-b.
    let moved: HashMap<ShardKey, ShardAssignment> = [(key(0), assignment(0, "broker-b", 2))]
        .into_iter()
        .collect();
    router.publish(routing_table_from(&moved, &nodes), &nodes);

    assert!(
        matches!(
            dispatch(Some(&ingress), &key(0)).await,
            Dispatch::Forward { .. }
        ),
        "a reassigned shard must stop being served here",
    );
}

/// The router moved on before local state did. Serving at the old generation
/// would be two brokers believing they lead the same shard.
#[tokio::test]
async fn a_local_route_at_a_newer_generation_is_not_served_until_reopened() {
    let nodes = catalog();
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let mut lifecycle = ShardLifecycle::new("broker-a");
    let first = assignment(0, "broker-a", 1);
    lifecycle.observe(&first.key, Some(&first));
    lifecycle.opened(&first.key, 1);

    // The table now says generation 4; local state is still at 1.
    let newer: HashMap<ShardKey, ShardAssignment> = [(key(0), assignment(0, "broker-a", 4))]
        .into_iter()
        .collect();
    router.publish(routing_table_from(&newer, &nodes), &nodes);

    let ingress = IngressRouter::new(router, Arc::new(Mutex::new(lifecycle)));
    assert_eq!(
        dispatch(Some(&ingress), &key(0)).await,
        Dispatch::Unavailable(Reason::NotReady),
        "local state must catch up before serving the new generation",
    );
}

#[tokio::test]
async fn a_multi_shard_stream_dispatches_per_shard() {
    let ingress = ingress(
        &[
            assignment(0, "broker-a", 1),
            assignment(1, "broker-b", 1),
            assignment(2, "broker-a", 1),
        ],
        &[0, 2],
    )
    .await;

    assert_eq!(dispatch(Some(&ingress), &key(0)).await, Dispatch::Local);
    assert!(matches!(
        dispatch(Some(&ingress), &key(1)).await,
        Dispatch::Forward { .. }
    ));
    assert_eq!(dispatch(Some(&ingress), &key(2)).await, Dispatch::Local);
}

/// Without a routing key on the wire there is only one shard to choose, and
/// choosing it must be stable.
#[test]
fn shard_selection_is_deterministic() {
    assert_eq!(shard_for(1, None), 0);
    assert_eq!(shard_for(4, None), 0, "no key on the wire yet");

    // The mapping a negotiated key would use, pinned so it cannot drift.
    for shards in [2u32, 4, 8, 16] {
        let first = shard_for(shards, Some(b"customer-42"));
        assert!(first < shards);
        for _ in 0..10 {
            assert_eq!(shard_for(shards, Some(b"customer-42")), first);
        }
    }
}

#[test]
fn keys_spread_across_shards() {
    let mut seen = std::collections::HashSet::new();
    for i in 0..200u32 {
        seen.insert(shard_for(8, Some(format!("key-{i}").as_bytes())));
    }
    assert_eq!(seen.len(), 8, "every shard should be reachable: {seen:?}");
}

#[test]
fn a_single_shard_stream_always_maps_to_zero() {
    assert_eq!(shard_for(0, Some(b"anything")), 0);
    assert_eq!(shard_for(1, Some(b"anything")), 0);
}
