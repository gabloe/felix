//! Which broker a cache key resolves to, and what happens when it is not this one.
use super::*;
use crate::shard_lifecycle::ShardLifecycle;
use crate::shard_routing::routing_table_from;
use crate::shard_watch::ShardAssignment;
use felix_router::{NodeRef, RegionRouter, ShardRouter};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
/// Deliberately shares a name with the stream in `shard_routing_tests`: a cache
/// and a stream of one name are different things, and every case here would
/// pass by accident if they were not.
const CACHE: &str = "orders";

fn cache_key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: CACHE.to_string(),
        shard,
        kind: ShardKind::Cache,
    }
}

fn stream_key(shard: u32) -> ShardKey {
    ShardKey {
        kind: ShardKind::Stream,
        ..cache_key(shard)
    }
}

fn assignment(key: ShardKey, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key,
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
    }
}

fn node(id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: id.to_string(),
        advertise_addr: SocketAddr::from(([10, 0, 0, 4], port)),
        region: "us-west-2".to_string(),
        live: true,
    }
}

fn catalog() -> HashMap<String, NodeRef> {
    [node("broker-a", 7001), node("broker-b", 7002)]
        .into_iter()
        .map(|n| (n.node_id.clone(), n))
        .collect()
}

/// An ingress router for `broker-a` holding `assignments`, every one of them
/// already through its opening phase.
fn ingress(assignments: &[ShardAssignment]) -> IngressRouter {
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
        lifecycle.opened(&assignment.key, assignment.generation);
    }
    let ingress = IngressRouter::new(router);
    ingress.publish_servable(lifecycle.servable());
    ingress
}

/// The property that must not regress: a broker with no cluster identity keeps
/// serving every key itself, exactly as an unsharded cache always did.
#[test]
fn a_single_node_broker_serves_every_key_locally() {
    for key in ["a", "b", "anything at all"] {
        match resolve_cache_route(None, TENANT, NAMESPACE, CACHE, key) {
            CacheRoute::Local { shard: 0 } => {}
            other => panic!("expected shard 0 locally, got {other:?}"),
        }
    }
}

/// The whole point. Two brokers resolving the same key must agree on who owns
/// it, or both will serve it and their answers will diverge.
#[test]
fn every_key_resolves_to_exactly_one_owner() {
    let assignments: Vec<ShardAssignment> = (0..4)
        .map(|shard| {
            let leader = if shard % 2 == 0 {
                "broker-a"
            } else {
                "broker-b"
            };
            assignment(cache_key(shard), leader, 1)
        })
        .collect();
    let ours = ingress(&assignments);

    let mut local = 0;
    let mut forwarded = 0;
    for n in 0..200 {
        let key = format!("session:{n}");
        match resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, &key) {
            CacheRoute::Local { shard } => {
                assert!(shard < 4);
                // A key served here must belong to a shard this broker leads.
                assert_eq!(shard % 2, 0, "{key} is shard {shard}, which broker-b leads");
                local += 1;
            }
            CacheRoute::Forward { key: fk, target } => {
                assert_eq!(target.node_id, "broker-b");
                assert_eq!(fk.shard % 2, 1);
                assert_eq!(fk.stream, CACHE);
                forwarded += 1;
            }
            CacheRoute::Refused(reason) => panic!("{key} was refused: {reason}"),
        }
    }

    assert!(
        local > 0 && forwarded > 0,
        "the split was {local}/{forwarded}"
    );
}

/// The collision the kind exists to stop. `orders` the stream is led here and
/// `orders` the cache is led elsewhere; a table that cannot tell them apart
/// keeps one assignment and answers both questions with it.
///
/// Both directions are asserted deliberately. Checking only the cache would
/// pass whenever the surviving entry happened to be the cache's — an accident
/// of insertion order, not a working table. With both, a collapse fails
/// whichever of the two it kept.
#[test]
fn a_cache_and_a_stream_of_the_same_name_resolve_to_their_own_owners() {
    let ours = ingress(&[
        assignment(stream_key(0), "broker-a", 1),
        assignment(cache_key(0), "broker-b", 1),
    ]);

    match resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, "any-key") {
        CacheRoute::Forward { target, .. } => assert_eq!(target.node_id, "broker-b"),
        other => panic!("the cache should forward to broker-b, got {other:?}"),
    }

    assert_eq!(
        crate::shard_routing::dispatch(Some(&ours), &stream_key(0)),
        crate::shard_routing::Dispatch::Local,
        "the stream is led here, and the cache must not have taken its row",
    );
}

/// Widths are per-kind too. The cache has four shards and the stream one, so a
/// cache key resolved against the stream's width would collapse to shard 0.
#[test]
fn a_cache_uses_its_own_shard_count() {
    let mut assignments = vec![assignment(stream_key(0), "broker-a", 1)];
    assignments.extend((0..4).map(|shard| assignment(cache_key(shard), "broker-a", 1)));
    let ours = ingress(&assignments);

    let shards: std::collections::BTreeSet<u32> = (0..200)
        .filter_map(|n| {
            match resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, &format!("k{n}")) {
                CacheRoute::Local { shard } => Some(shard),
                _ => None,
            }
        })
        .collect();

    assert!(
        shards.len() > 1,
        "every key landed in {shards:?}; the cache's width was not used",
    );
}

/// An unplaced cache is refused rather than served. Serving it would be the
/// old behaviour: a local write nothing else can see and nothing reconciles.
#[test]
fn an_unassigned_cache_shard_is_refused() {
    let ours = ingress(&[assignment(stream_key(0), "broker-a", 1)]);

    match resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, "any-key") {
        CacheRoute::Refused(_) => {}
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Resolution is a pure function of the key and the view, so two brokers with
/// the same view send the same key to the same place.
#[test]
fn resolution_is_deterministic() {
    let assignments: Vec<ShardAssignment> = (0..4)
        .map(|shard| assignment(cache_key(shard), "broker-a", 1))
        .collect();
    let ours = ingress(&assignments);

    for n in 0..50 {
        let key = format!("session:{n}");
        let first = resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, &key);
        for _ in 0..5 {
            let again = resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, &key);
            assert_eq!(format!("{first:?}"), format!("{again:?}"));
        }
    }
}
