//! Placement properties: determinism, coverage, skew, and order independence.
use super::*;
use crate::model::{
    ConsistencyLevel, DeliveryGuarantee, NodeCapacity, NodeSpec, NodeStatus, RetentionPolicy,
    StreamKind,
};
use std::collections::{BTreeMap, BTreeSet};

fn stream(name: &str, shards: u32) -> Stream {
    replicated_stream(name, shards, 1)
}

/// A stream that keeps `replication_factor` copies of each shard.
fn replicated_stream(name: &str, shards: u32, replication_factor: u32) -> Stream {
    Stream {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: name.to_string(),
        kind: StreamKind::Stream,
        shards,
        replication_factor,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtMostOnce,
        durable: true,
    }
}

fn node(id: &str, lifecycle: NodeLifecycle, max_shards: Option<u32>) -> Node {
    Node {
        node_id: id.to_string(),
        spec: NodeSpec {
            advertise_addr: format!("10.0.0.4:{}", 7000 + id.len() as u16),
            client_addr: None,
            region: "us-west-2".to_string(),
            labels: Default::default(),
            capacity: NodeCapacity {
                max_shards,
                weight: 1,
            },
        },
        status: NodeStatus {
            lifecycle,
            last_heartbeat_at_millis: 1,
            registered_at_millis: 1,
            incarnation: 0,
        },
    }
}

fn live(ids: &[&str]) -> Vec<Node> {
    ids.iter()
        .map(|id| node(id, NodeLifecycle::Live, None))
        .collect()
}

fn placements(plan: &Plan) -> BTreeMap<(String, u32), String> {
    plan.shards
        .iter()
        .filter_map(|p| match &p.decision {
            Decision::Place(leader, _) => {
                Some(((p.key.stream.clone(), p.key.shard), leader.clone()))
            }
            _ => None,
        })
        .collect()
}

/// The headline property: the same snapshot always yields the same result. Two
/// control-plane instances must not disagree about who owns a shard.
#[test]
fn placement_is_deterministic() {
    let streams = vec![stream("orders", 8), stream("payments", 5)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);

    let first = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);
    for _ in 0..20 {
        assert_eq!(plan(&streams, &[], &nodes, &[], &NothingCaughtUp), first);
    }
}

/// Same set, different order in. Two instances reading the same rows through
/// different query plans must still agree.
#[test]
fn placement_does_not_depend_on_input_order() {
    let streams = vec![stream("orders", 8), stream("payments", 5)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let expected = placements(&plan(&streams, &[], &nodes, &[], &NothingCaughtUp));

    let mut reversed_streams = streams.clone();
    reversed_streams.reverse();
    let mut reversed_nodes = nodes.clone();
    reversed_nodes.reverse();

    assert_eq!(
        placements(&plan(
            &reversed_streams,
            &[],
            &reversed_nodes,
            &[],
            &NothingCaughtUp
        )),
        expected,
    );
    assert_eq!(
        placements(&plan(&streams, &[], &reversed_nodes, &[], &NothingCaughtUp)),
        expected
    );
    assert_eq!(
        placements(&plan(&reversed_streams, &[], &nodes, &[], &NothingCaughtUp)),
        expected
    );
}

#[test]
fn every_shard_gets_exactly_one_leader() {
    let streams = vec![stream("orders", 8), stream("payments", 5)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);

    assert_eq!(plan.shards.len(), 13);
    let keys: BTreeSet<(String, u32)> = plan
        .shards
        .iter()
        .map(|p| (p.key.stream.clone(), p.key.shard))
        .collect();
    assert_eq!(keys.len(), 13, "no shard planned twice");
    assert!(
        plan.shards
            .iter()
            .all(|p| matches!(p.decision, Decision::Place(..))),
        "every shard should be placeable with three live nodes",
    );
}

/// Rendezvous hashing has no balancing step, so this is a distribution claim,
/// not an exact one. The bar is loose on purpose: what it catches is a hash
/// that funnels everything at one node, which is the realistic failure.
#[test]
fn placement_skew_stays_bounded() {
    let streams = vec![stream("orders", 300)];
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);

    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for (_, leader, _) in plan.to_place() {
        *counts.entry(leader.to_string()).or_default() += 1;
    }

    assert_eq!(counts.len(), 4, "every node should get shards: {counts:?}");
    // Deterministic inputs, so this bar can be tight: the measured spread is
    // within 8% of even, and a regression to the single-pass hash this replaced
    // would blow straight through it at 43%.
    let ideal = 300.0 / 4.0;
    for (node, count) in &counts {
        let ratio = *count as f64 / ideal;
        assert!(
            (0.75..=1.25).contains(&ratio),
            "{node} took {count} of 300, ratio {ratio:.2}",
        );
    }
}

/// The realistic failure the loose 300-shard test cannot catch: at the
/// handful-of-shards scale a cluster actually starts at, pure highest-score
/// rendezvous skewed hard — a real cluster put 24 shards over three brokers at
/// 11/5/8, and 48 over two at 48/0 (single-node saturation). Bounded-load
/// rendezvous holds every node to its exact share when the share divides evenly.
#[test]
fn small_shard_counts_are_evenly_balanced() {
    let cases = [
        (24u32, &["broker-a", "broker-b", "broker-c"][..], 8usize),
        (48, &["broker-a", "broker-b"][..], 24),
    ];
    for (shards, ids, each) in cases {
        let streams = vec![stream("orders", shards)];
        let nodes = live(ids);
        let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for (_, leader, _) in plan.to_place() {
            *counts.entry(leader.to_string()).or_default() += 1;
        }
        assert_eq!(
            counts.len(),
            ids.len(),
            "every node should lead shards: {counts:?}"
        );
        assert!(
            counts.values().all(|c| *c == each),
            "{shards} shards over {} nodes should be {each} each, got {counts:?}",
            ids.len(),
        );
    }
}

/// Reconciliation must be idempotent: applying a plan and planning again keeps
/// everything, so a periodic pass does not churn the persisted rows.
#[test]
fn reconciliation_is_idempotent() {
    let streams = vec![stream("orders", 8)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);

    let first = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);
    let applied: Vec<ShardAssignment> = first
        .to_place()
        .map(|(key, leader, replicas)| assignment_for(key, leader, replicas.to_vec()))
        .collect();

    let second = plan(&streams, &[], &nodes, &applied, &NothingCaughtUp);
    assert_eq!(second.kept(), 8, "a second pass should keep everything");
    assert_eq!(second.to_place().count(), 0, "and write nothing");
}

/// An assignment whose leader is still live is kept even when the hash would
/// now prefer someone else. A balanced cluster is not reshuffled for locality.
#[test]
fn a_valid_assignment_is_kept_even_if_the_hash_disagrees() {
    let streams = vec![stream("orders", 4)];
    let nodes = live(&["broker-a", "broker-b"]);

    // Two each, which is balanced, but on the opposite nodes from the ones
    // the hash chooses.
    let by_hash = placements(&plan(&streams, &[], &nodes, &[], &NothingCaughtUp));
    let swapped: Vec<ShardAssignment> = (0..4)
        .map(|shard| {
            let hashed = &by_hash[&("orders".to_string(), shard)];
            let other = if hashed == "broker-a" {
                "broker-b"
            } else {
                "broker-a"
            };
            pinned("orders", shard, other)
        })
        .collect();
    let leaders_on_a = swapped.iter().filter(|a| a.leader == "broker-a").count();
    assert_eq!(
        leaders_on_a, 2,
        "the fixture must be balanced for this to mean anything"
    );

    let plan = plan(&streams, &[], &nodes, &swapped, &NothingCaughtUp);
    assert_eq!(plan.kept(), 4);
    assert_eq!(plan.to_place().count(), 0, "no reshuffling");
    assert_eq!(plan.moves().count(), 0, "no reshuffling");
}

fn pinned(stream: &str, shard: u32, leader: &str) -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: stream.to_string(),
            shard,
            kind: ShardKind::Stream,
        },
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation: 3,
        state: ShardState::Active,
        successor: None,
    }
}

/// Losing a node must move that node's shards and nothing else.
#[test]
fn losing_a_node_moves_only_its_shards() {
    let streams = vec![stream("orders", 30)];
    let all = live(&["broker-a", "broker-b", "broker-c"]);

    let initial = plan(&streams, &[], &all, &[], &NothingCaughtUp);
    let applied: Vec<ShardAssignment> = initial
        .to_place()
        .map(|(key, leader, replicas)| assignment_for(key, leader, replicas.to_vec()))
        .collect();

    let mut degraded = all.clone();
    degraded[0].status.lifecycle = NodeLifecycle::Down;
    let after = plan(&streams, &[], &degraded, &applied, &NothingCaughtUp);

    let lost = applied.iter().filter(|a| a.leader == "broker-a").count();
    assert!(lost > 0, "the test needs broker-a to have held something");
    assert_eq!(
        after.to_place().count(),
        lost,
        "exactly the down node's shards move",
    );
    assert_eq!(after.kept(), 30 - lost);
    assert!(
        after.to_place().all(|(_, leader, _)| leader != "broker-a"),
        "nothing may be placed on a node that is down",
    );
}

/// Only `live` leads. A draining node is finishing what it has, and down or
/// departed nodes are not there at all.
#[test]
fn only_live_nodes_are_eligible() {
    let streams = vec![stream("orders", 4)];
    for lifecycle in [
        NodeLifecycle::Draining,
        NodeLifecycle::Down,
        NodeLifecycle::Left,
    ] {
        let nodes = vec![
            node("broker-a", lifecycle, None),
            node("broker-b", NodeLifecycle::Live, None),
        ];
        let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);
        assert!(
            plan.to_place().all(|(_, leader, _)| leader == "broker-b"),
            "{lifecycle:?} must not receive placement",
        );
    }
}

/// An assignment on a node that is gone is re-placed, not kept. A draining
/// node is not gone -- its shards are moved, which `moves` covers.
#[test]
fn an_assignment_on_an_ineligible_node_is_replaced() {
    let streams = vec![stream("orders", 2)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Down, None),
        node("broker-b", NodeLifecycle::Live, None),
    ];
    let stale: Vec<ShardAssignment> = (0..2)
        .map(|shard| {
            assignment_for(
                &ShardKey {
                    tenant_id: "t1".to_string(),
                    namespace: "ns".to_string(),
                    stream: "orders".to_string(),
                    shard,
                    kind: ShardKind::Stream,
                },
                "broker-a",
                Vec::new(),
            )
        })
        .collect();

    let plan = plan(&streams, &[], &nodes, &stale, &NothingCaughtUp);
    assert_eq!(plan.kept(), 0);
    assert_eq!(plan.to_place().count(), 2);
    assert!(plan.to_place().all(|(_, leader, _)| leader == "broker-b"));
}

#[test]
fn with_no_live_nodes_every_shard_says_why() {
    let streams = vec![stream("orders", 3)];
    let nodes = vec![node("broker-a", NodeLifecycle::Down, None)];

    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);
    assert_eq!(plan.unplaceable().count(), 3);
    assert!(
        plan.unplaceable()
            .all(|(_, reason)| *reason == Unplaceable::NoEligibleNode),
    );
}

/// A cap is a hard limit, and running out of capacity is a different problem
/// from having no nodes -- an operator needs to be told which.
#[test]
fn capacity_is_respected_and_exhaustion_is_distinguishable() {
    let streams = vec![stream("orders", 5)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, Some(1)),
        node("broker-b", NodeLifecycle::Live, Some(1)),
    ];

    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);
    assert_eq!(plan.to_place().count(), 2, "two nodes, one shard each");

    let unplaceable: Vec<&Unplaceable> = plan.unplaceable().map(|(_, r)| r).collect();
    assert_eq!(unplaceable.len(), 3);
    assert!(
        unplaceable
            .iter()
            .all(|r| **r == Unplaceable::AllNodesAtCapacity),
        "capacity exhaustion must not read as an empty cluster",
    );
}

/// Existing assignments count against the cap, or a reconciliation would place
/// a node past a limit it is already at.
#[test]
fn existing_assignments_count_towards_capacity() {
    let streams = vec![stream("orders", 3)];
    let nodes = vec![node("broker-a", NodeLifecycle::Live, Some(2))];
    let existing = vec![assignment_for(
        &ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            kind: ShardKind::Stream,
        },
        "broker-a",
        Vec::new(),
    )];

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);
    assert_eq!(plan.kept(), 1);
    assert_eq!(
        plan.to_place().count(),
        1,
        "one slot left under the cap of 2"
    );
    assert_eq!(plan.unplaceable().count(), 1);
}

/// Field separation: two different shard keys must not hash to the same stream
/// of bytes just because their fields concatenate the same way.
#[test]
fn adjacent_field_values_do_not_collide() {
    let a = ShardKey {
        tenant_id: "ab".to_string(),
        namespace: "c".to_string(),
        stream: "d".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    let b = ShardKey {
        tenant_id: "a".to_string(),
        namespace: "bc".to_string(),
        stream: "d".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    assert_ne!(score(&a, "broker-a"), score(&b, "broker-a"));
}

/// The scores are a stable function of their inputs, not of the process. A
/// change here reshuffles every shard in every cluster, so it should require
/// deliberately updating this test.
#[test]
fn scores_are_stable_across_runs() {
    let key = ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 7,
        kind: ShardKind::Stream,
    };
    let first = score(&key, "broker-a");
    assert_eq!(score(&key, "broker-a"), first);
    assert_ne!(score(&key, "broker-b"), first);
}

#[test]
fn a_stream_with_no_shards_plans_nothing() {
    let plan = plan(
        &[stream("orders", 0)],
        &[],
        &live(&["broker-a"]),
        &[],
        &NothingCaughtUp,
    );
    assert!(plan.shards.is_empty());
}

/// End-to-end against a real store: the milestone's own completion signal, a
/// three-node cluster with a three-shard stream.
mod reconcile {
    use super::*;
    use crate::model::{Namespace, NodeCapacity as Cap, Tenant};
    use crate::store::memory::InMemoryStore;
    use crate::store::{ControlPlaneStore, StoreConfig};

    async fn cluster(node_ids: &[&str]) -> InMemoryStore {
        let store = InMemoryStore::new(StoreConfig {
            changes_limit: 1000,
            change_retention_max_rows: Some(1000),
        });
        store
            .create_tenant(Tenant {
                tenant_id: "t1".to_string(),
                display_name: "T".to_string(),
            })
            .await
            .expect("tenant");
        store
            .create_namespace(Namespace {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                display_name: "NS".to_string(),
            })
            .await
            .expect("namespace");
        store
            .create_stream(stream("orders", 3))
            .await
            .expect("stream");

        for (i, id) in node_ids.iter().enumerate() {
            let mut n = node(id, NodeLifecycle::Live, None);
            n.spec.advertise_addr = format!("10.0.0.4:{}", 7600 + i);
            n.spec.capacity = Cap::default();
            store.register_node(n).await.expect("node");
        }
        store
    }

    #[tokio::test]
    async fn three_nodes_and_three_shards_each_get_one_owner() {
        let store = cluster(&["broker-a", "broker-b", "broker-c"]).await;

        let outcome = reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
        assert_eq!(outcome.placed, 3);
        assert_eq!(outcome.unplaceable, 0);
        assert_eq!(outcome.failed, 0);

        let assignments = store.list_shard_assignments().await.expect("list");
        assert_eq!(assignments.len(), 3);
        assert!(assignments.iter().all(|a| a.state == ShardState::Assigning));
        assert!(assignments.iter().all(|a| a.generation == 0));
    }

    /// A pass over an already-placed cluster must write nothing, or a timer
    /// would churn the rows and flood the changefeed.
    #[tokio::test]
    async fn a_second_pass_writes_nothing() {
        let store = cluster(&["broker-a", "broker-b", "broker-c"]).await;
        reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
        let after_first = store
            .shard_assignment_snapshot()
            .await
            .expect("snap")
            .next_seq;

        for _ in 0..5 {
            let outcome = reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
            assert_eq!(outcome.placed, 0);
            assert_eq!(outcome.kept, 3);
        }

        assert_eq!(
            store
                .shard_assignment_snapshot()
                .await
                .expect("snap")
                .next_seq,
            after_first,
            "no change should have been published",
        );
    }

    /// Losing a broker re-places only its shards, and the generation moves so a
    /// stale report from the old leader can be rejected.
    #[tokio::test]
    async fn a_lost_node_has_its_shards_replaced() {
        let store = cluster(&["broker-a", "broker-b", "broker-c"]).await;
        reconcile_once(&store, &Default::default(), MovePolicy::default()).await;

        let before = store.list_shard_assignments().await.expect("list");
        let victim = before[0].leader.clone();
        let lost = before.iter().filter(|a| a.leader == victim).count();
        store
            .set_node_lifecycle(&victim, NodeLifecycle::Down)
            .await
            .expect("down");

        let outcome = reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
        assert_eq!(outcome.placed, lost);
        assert_eq!(outcome.kept, 3 - lost);

        let after = store.list_shard_assignments().await.expect("list");
        assert!(
            after.iter().all(|a| a.leader != victim),
            "nothing may remain on a node that is down",
        );
        for moved in after
            .iter()
            .filter(|a| before.iter().any(|b| b.key == a.key && b.leader == victim))
        {
            assert_eq!(moved.generation, 1, "a re-placement moves the generation");
        }
    }

    #[tokio::test]
    async fn an_empty_cluster_places_nothing_and_says_so() {
        let store = cluster(&[]).await;
        let outcome = reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
        assert_eq!(outcome.placed, 0);
        assert_eq!(outcome.unplaceable, 3);
        assert!(
            store
                .list_shard_assignments()
                .await
                .expect("list")
                .is_empty()
        );
    }
}

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

/// A follower that holds the log, for the promotion tests.
struct CaughtUpNodes(BTreeSet<String>);

impl CaughtUp for CaughtUpNodes {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.0.contains(node_id)
    }
}

fn assigned(stream: &str, leader: &str, replicas: &[&str]) -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: stream.to_string(),
            shard: 0,
            kind: ShardKind::Stream,
        },
        leader: leader.to_string(),
        replicas: replicas.iter().map(|r| r.to_string()).collect(),
        generation: 3,
        state: ShardState::Active,
        successor: None,
    }
}

/// A lost leader is replaced by a follower that holds the log.
#[test]
fn a_caught_up_follower_is_promoted() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    // broker-a is gone.
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let caught_up = CaughtUpNodes(["broker-c".to_string()].into_iter().collect());

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);
    let (_, leader, _) = plan.to_place().next().expect("placed");
    assert_eq!(
        leader, "broker-c",
        "the caught-up follower must be promoted, not the other one",
    );
}

/// **The gate.** A follower that holds nothing is not promoted, however
/// eligible it looks: promoting it would serve an empty shard, which is the
/// data loss a failover is supposed to prevent.
///
/// And nothing else is promoted in its place. A node that has never seen the
/// shard is just as empty as an uncaught-up replica, so falling back to
/// ordinary scoring would defeat the gate rather than respect it — the shard
/// stays unplaced until something that holds the log can take it.
#[test]
fn a_follower_that_holds_nothing_is_not_promoted() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];

    for replica in ["broker-b", "broker-c"] {
        let existing = vec![assigned("orders", "broker-a", &[replica])];

        let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);

        assert_eq!(
            plan.to_place().count(),
            0,
            "with {replica} holding nothing, the shard was placed anyway",
        );
    }
}

/// A node that was never a replica is never promoted, even if it reports being
/// caught up. Only the recorded replica set is promotable.
#[test]
fn only_a_recorded_replica_is_promotable() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-z", NodeLifecycle::Live, None),
    ];
    // broker-z is not in the replica set, but claims to be caught up.
    let existing = vec![assigned("orders", "broker-a", &["broker-b"])];
    let caught_up = CaughtUpNodes(
        ["broker-b".to_string(), "broker-z".to_string()]
            .into_iter()
            .collect(),
    );

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);
    let (_, leader, _) = plan.to_place().next().expect("placed");
    assert_eq!(
        leader, "broker-b",
        "promotion must come from the recorded replica set",
    );
}

/// A promoted follower is not left listed as its own follower.
#[test]
fn promotion_rebuilds_the_replica_set_without_the_new_leader() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let caught_up = CaughtUpNodes(["broker-b".to_string(), "broker-c".to_string()].into());

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);
    let (_, leader, replicas) = plan.to_place().next().expect("placed");
    assert!(
        !replicas.contains(&leader.to_string()),
        "{leader} was promoted and is still listed as a follower of itself",
    );
}

/// **A replicated shard is never handed to a node that does not hold it.**
///
/// The leader is gone and no replica is caught up. Placing the shard on a node
/// that has never seen it would serve an empty log at a new generation while
/// the records sat on the replicas — the failover would *be* the data loss, and
/// nothing downstream would report it as one.
#[test]
fn a_replicated_shard_with_no_caught_up_replica_is_left_unplaceable() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    // broker-a led it; b and c hold copies. broker-a is gone, and broker-z has
    // never seen this shard.
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-z", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);

    assert_eq!(
        plan.to_place().count(),
        0,
        "a shard was placed on a node that does not hold its log",
    );
    assert!(
        plan.unplaceable()
            .any(|(_, why)| matches!(why, Unplaceable::NoCaughtUpReplica)),
        "the shard was dropped without saying why",
    );
}

/// And it is placed the moment a replica can take over, so the state above is
/// a pause rather than a dead end.
#[test]
fn the_shard_is_placed_as_soon_as_a_replica_is_caught_up() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-z", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let caught_up = CaughtUpNodes(["broker-b".to_string()].into_iter().collect());

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);

    let (_, leader, _) = plan.to_place().next().expect("placed");
    assert_eq!(leader, "broker-b");
}

/// **A stream that never asked for replication is untouched.** It has no
/// replicas, so there was never a copy to prefer, and a fresh placement stays
/// the only thing available — refusing there would turn a recoverable single-copy
/// outage into a permanent one.
#[test]
fn an_unreplicated_shard_is_still_placed_after_its_node_is_lost() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let nodes = vec![node("broker-b", NodeLifecycle::Live, None)];
    let existing = vec![assigned("orders", "broker-a", &[])];

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);

    let (_, leader, _) = plan
        .to_place()
        .next()
        .expect("an unreplicated shard should still be placed");
    assert_eq!(leader, "broker-b");
}

/// A shard that has never been assigned is a first placement, not a failover,
/// so it is placed normally.
#[test]
fn a_shard_with_no_previous_assignment_is_placed_normally() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-z", NodeLifecycle::Live, None),
    ];

    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);

    assert_eq!(plan.to_place().count(), 1);
}

/// A leader that is still live keeps the shard, caught-up replicas or not.
/// Nothing about this changes the ordinary path.
#[test]
fn a_live_leader_keeps_its_shard() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);

    assert_eq!(plan.kept(), 1);
    assert_eq!(plan.to_place().count(), 0);
}

/// A follower that holds the log, with a position, for the promotion tests.
struct ReplicasAt(std::collections::HashMap<String, u64>);

impl CaughtUp for ReplicasAt {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.0.contains_key(node_id)
    }

    fn reported_offset(&self, _key: &ShardKey, node_id: &str) -> Option<u64> {
        self.0.get(node_id).copied()
    }
}

/// **The replica holding the most is promoted**, not the one that scores best.
///
/// "Caught up" is only ever true of the tail it was measured against, so a
/// report made before the leader's last writes can call two replicas level when
/// one holds more. Preferring the higher offset picks the replica a
/// quorum-acknowledged record is guaranteed to be on — choosing by score would
/// discard the difference, and with it the record.
#[test]
fn the_furthest_ahead_replica_is_promoted() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];

    // Whichever scoring would have picked, the one at the higher offset wins.
    for (ahead, behind) in [("broker-b", "broker-c"), ("broker-c", "broker-b")] {
        let caught_up = ReplicasAt(
            [(ahead.to_string(), 100u64), (behind.to_string(), 50u64)]
                .into_iter()
                .collect(),
        );

        let plan = plan(&streams, &[], &nodes, &existing, &caught_up);

        let (_, leader, _) = plan.to_place().next().expect("placed");
        assert_eq!(
            leader, ahead,
            "{behind} was promoted over {ahead}, which held more",
        );
    }
}

/// Replicas level with each other fall back to the deterministic score, so the
/// choice stays a function of the shard and the cluster rather than of report
/// arrival order.
#[test]
fn replicas_at_the_same_offset_break_the_tie_deterministically() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let level: std::collections::HashMap<String, u64> = [
        ("broker-b".to_string(), 100u64),
        ("broker-c".to_string(), 100u64),
    ]
    .into_iter()
    .collect();

    let first = {
        let plan = plan(&streams, &[], &nodes, &existing, &ReplicasAt(level.clone()));
        plan.to_place().next().expect("placed").1.to_string()
    };
    let again = {
        let plan = plan(&streams, &[], &nodes, &existing, &ReplicasAt(level));
        plan.to_place().next().expect("placed").1.to_string()
    };

    assert_eq!(first, again);
}

/// A replica with no reported position is not preferred over one with a
/// position, and is not promoted at all unless it is also reported caught up.
#[test]
fn a_replica_with_no_reported_position_loses_to_one_with_a_position() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    // Both are "caught up"; only broker-c has a position.
    struct OnlyOneKnown;
    impl CaughtUp for OnlyOneKnown {
        fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
            true
        }
        fn reported_offset(&self, _key: &ShardKey, node_id: &str) -> Option<u64> {
            (node_id == "broker-c").then_some(7)
        }
    }

    let plan = plan(&streams, &[], &nodes, &existing, &OnlyOneKnown);

    assert_eq!(plan.to_place().next().expect("placed").1, "broker-c");
}

// --- Caches are placed by the same pass, and are not streams -----------------

fn cache(name: &str, shards: u32) -> Cache {
    replicated_cache(name, shards, 1)
}

fn replicated_cache(name: &str, shards: u32, replication_factor: u32) -> Cache {
    Cache {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: name.to_string(),
        display_name: name.to_string(),
        shards,
        replication_factor,
    }
}

/// Placements keyed by the whole identity, so a cache and a stream sharing a
/// name are two entries rather than one overwriting the other.
fn placements_by_kind(plan: &Plan) -> BTreeMap<(ShardKind, String, u32), String> {
    plan.shards
        .iter()
        .filter_map(|p| match &p.decision {
            Decision::Place(leader, _) => Some((
                (p.key.kind, p.key.stream.clone(), p.key.shard),
                leader.clone(),
            )),
            _ => None,
        })
        .collect()
}

#[test]
fn a_cache_gets_one_assignment_per_shard() {
    let plan = plan(
        &[],
        &[cache("sessions", 4)],
        &live(&["broker-a", "broker-b"]),
        &[],
        &NothingCaughtUp,
    );

    assert_eq!(plan.shards.len(), 4);
    assert!(
        plan.shards
            .iter()
            .all(|p| p.key.kind == ShardKind::Cache && p.key.stream == "sessions"),
        "every planned shard belongs to the cache",
    );
    assert_eq!(
        plan.shards
            .iter()
            .map(|p| p.key.shard)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1, 2, 3]),
    );
}

/// The collision this whole change exists to prevent. A cache and a stream may
/// share a name in one namespace; if the key did not carry the kind, one would
/// take the other's ownership row and a client would be routed to a broker
/// holding entirely the wrong log.
#[test]
fn a_cache_and_a_stream_of_the_same_name_are_placed_separately() {
    let plan = plan(
        &[stream("orders", 2)],
        &[cache("orders", 2)],
        &live(&["broker-a", "broker-b", "broker-c"]),
        &[],
        &NothingCaughtUp,
    );

    let placed = placements_by_kind(&plan);
    assert_eq!(placed.len(), 4, "two stream shards and two cache shards");
    for shard in 0..2 {
        assert!(placed.contains_key(&(ShardKind::Stream, "orders".to_string(), shard)));
        assert!(placed.contains_key(&(ShardKind::Cache, "orders".to_string(), shard)));
    }
}

/// Rendezvous scoring must actually separate them. Two shards that score
/// identically would track each other onto the same node forever, which turns
/// one node's loss into the loss of both the stream and the cache.
#[test]
fn a_cache_does_not_score_identically_to_the_stream_it_shares_a_name_with() {
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d", "broker-e"]);
    let plan = plan(
        &[stream("orders", 16)],
        &[cache("orders", 16)],
        &nodes,
        &[],
        &NothingCaughtUp,
    );

    let placed = placements_by_kind(&plan);
    let differing = (0..16)
        .filter(|shard| {
            placed.get(&(ShardKind::Stream, "orders".to_string(), *shard))
                != placed.get(&(ShardKind::Cache, "orders".to_string(), *shard))
        })
        .count();

    // Five nodes, so a shard pair lands together about a fifth of the time by
    // chance. Anything near zero means the two are correlated, not independent.
    assert!(
        differing >= 8,
        "cache and stream shards landed together {} times out of 16",
        16 - differing,
    );
}

/// Stream placement is pinned to what it was before caches were placed at all.
///
/// These leaders were produced by the algorithm as it stood before this change
/// and are asserted verbatim: the whole reason `score` leaves a stream's hash
/// input alone is that changing it silently reshuffles every stream in every
/// fresh cluster. A comparison against a freshly computed plan cannot catch
/// that -- both sides would move together -- so the expected values are
/// written down. Hash the kind unconditionally in `score` and this fails.
#[test]
fn stream_placement_is_unchanged_by_cache_placement() {
    const GOLDEN: &[(&str, u32, &str)] = &[
        ("orders", 0, "broker-a"),
        ("orders", 1, "broker-b"),
        ("orders", 2, "broker-c"),
        ("orders", 3, "broker-a"),
        ("orders", 4, "broker-a"),
        ("orders", 5, "broker-a"),
        ("orders", 6, "broker-c"),
        ("orders", 7, "broker-a"),
        ("payments", 0, "broker-c"),
        ("payments", 1, "broker-b"),
        ("payments", 2, "broker-b"),
        ("payments", 3, "broker-c"),
        ("payments", 4, "broker-b"),
    ];

    let streams = vec![stream("orders", 8), stream("payments", 5)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let expected: BTreeMap<(String, u32), String> = GOLDEN
        .iter()
        .map(|(name, shard, leader)| ((name.to_string(), *shard), leader.to_string()))
        .collect();

    assert_eq!(
        placements(&plan(&streams, &[], &nodes, &[], &NothingCaughtUp)),
        expected,
        "stream placement moved with no caches in play",
    );

    // And placing caches alongside them moves nothing either.
    let alongside = placements_by_kind(&plan(
        &streams,
        &[cache("sessions", 6), cache("orders", 3)],
        &nodes,
        &[],
        &NothingCaughtUp,
    ));
    let stream_placements: BTreeMap<(String, u32), String> = alongside
        .into_iter()
        .filter(|((kind, _, _), _)| *kind == ShardKind::Stream)
        .map(|((_, name, shard), leader)| ((name, shard), leader))
        .collect();

    assert_eq!(stream_placements, expected);
}

/// One pass over both, so a node's `max_shards` is a budget for everything it
/// holds. Two passes would each believe it had the whole cap to itself and
/// together overshoot it.
#[test]
fn a_nodes_capacity_is_shared_between_its_streams_and_its_caches() {
    let nodes = vec![node("broker-a", NodeLifecycle::Live, Some(3))];
    let plan = plan(
        &[stream("orders", 2)],
        &[cache("sessions", 4)],
        &nodes,
        &[],
        &NothingCaughtUp,
    );

    let placed = placements_by_kind(&plan);
    assert_eq!(placed.len(), 3, "the cap is three shards, of either kind");
    assert_eq!(
        plan.shards
            .iter()
            .filter(|p| matches!(
                p.decision,
                Decision::Unplaceable(Unplaceable::AllNodesAtCapacity)
            ))
            .count(),
        3,
    );
}

#[test]
fn a_replicated_cache_gets_followers() {
    let plan = plan(
        &[],
        &[replicated_cache("sessions", 2, 3)],
        &live(&["broker-a", "broker-b", "broker-c", "broker-d"]),
        &[],
        &NothingCaughtUp,
    );

    for shard in &plan.shards {
        let Decision::Place(leader, replicas) = &shard.decision else {
            panic!("expected a placement, got {:?}", shard.decision);
        };
        assert_eq!(replicas.len(), 2, "leader plus two followers");
        assert!(!replicas.contains(leader));
    }
}

/// A cache created before it had a shard count reads back as one shard, and one
/// shard is a single owner for the whole keyspace — exactly the behaviour it
/// had when nothing placed it at all.
#[test]
fn a_cache_defaults_to_a_single_shard() {
    let json = r#"{
        "tenant_id": "t1",
        "namespace": "ns",
        "cache": "sessions",
        "display_name": "Sessions"
    }"#;
    let cache: Cache = serde_json::from_str(json).expect("cache without a shard count");

    assert_eq!(cache.shards, 1);
    assert_eq!(cache.replication_factor, 1);
}

/// Planned moves: a shard whose leader is alive is handed off, never
/// reassigned. Each step is checked from the store state that precedes it,
/// which is how a pass on any instance resumes a move.
mod moves {
    use super::*;

    /// What the leaders last reported: who is caught up, at which
    /// generation, and whether the leader has drained.
    struct Reported {
        caught_up: BTreeSet<String>,
        generation: u64,
        drained: bool,
    }

    impl Reported {
        /// At the generation `assigned` uses.
        fn caught_up(nodes: &[&str]) -> Self {
            Self {
                caught_up: nodes.iter().map(|n| n.to_string()).collect(),
                generation: 3,
                drained: false,
            }
        }

        fn at(mut self, generation: u64) -> Self {
            self.generation = generation;
            self
        }

        fn drained_at(mut self, generation: u64) -> Self {
            self.generation = generation;
            self.drained = true;
            self
        }
    }

    impl CaughtUp for Reported {
        fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
            self.caught_up.contains(node_id)
        }

        fn is_drained(&self, _key: &ShardKey, generation: u64) -> bool {
            self.drained && self.generation == generation
        }

        fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
            Some(self.generation)
        }
    }

    fn one_shard(leader: &str, replicas: &[&str]) -> ShardAssignment {
        assigned("orders", leader, replicas)
    }

    fn only_decision(plan: &Plan) -> &Decision {
        assert_eq!(plan.shards.len(), 1);
        &plan.shards[0].decision
    }

    fn draining_cluster() -> Vec<Node> {
        vec![
            node("broker-a", NodeLifecycle::Draining, None),
            node("broker-b", NodeLifecycle::Live, None),
        ]
    }

    /// Step one: the destination joins the replica set as the successor. The
    /// old leader keeps leading -- nothing is reassigned to a node holding
    /// none of the log.
    #[test]
    fn a_draining_leader_stages_its_successor_as_a_replica() {
        let streams = vec![stream("orders", 1)];
        let existing = vec![one_shard("broker-a", &[])];

        let plan = plan(
            &streams,
            &[],
            &draining_cluster(),
            &existing,
            &NothingCaughtUp,
        );

        match only_decision(&plan) {
            Decision::Move(MoveStep::Stage { successor }, next) => {
                assert_eq!(successor, "broker-b");
                assert_eq!(next.leader, "broker-a", "the leader does not change yet");
                assert_eq!(next.replicas, vec!["broker-b".to_string()]);
                assert_eq!(next.successor.as_deref(), Some("broker-b"));
                assert_eq!(next.state, ShardState::Active);
            }
            other => panic!("expected a stage, got {other:?}"),
        }
        assert_eq!(plan.to_place().count(), 0, "nothing is reassigned outright");
    }

    /// Step two waits: a staged successor that has not caught up is not fenced.
    #[test]
    fn a_successor_that_is_not_caught_up_is_waited_for() {
        let streams = vec![stream("orders", 1)];
        let mut staged = one_shard("broker-a", &["broker-b"]);
        staged.successor = Some("broker-b".to_string());

        let plan = plan(
            &streams,
            &[],
            &draining_cluster(),
            &[staged],
            &NothingCaughtUp,
        );

        assert_eq!(
            only_decision(&plan),
            &Decision::Waiting(Blocked::DestinationCatchingUp {
                successor: "broker-b".to_string()
            })
        );
    }

    /// Step two: a caught-up successor fences the leader. The assignment goes
    /// `Draining` with everything else unchanged.
    #[test]
    fn a_caught_up_successor_fences_the_leader() {
        let streams = vec![stream("orders", 1)];
        let mut staged = one_shard("broker-a", &["broker-b"]);
        staged.successor = Some("broker-b".to_string());

        let plan = plan(
            &streams,
            &[],
            &draining_cluster(),
            &[staged],
            &Reported::caught_up(&["broker-b"]),
        );

        match only_decision(&plan) {
            Decision::Move(MoveStep::Fence, next) => {
                assert_eq!(next.leader, "broker-a");
                assert_eq!(next.state, ShardState::Draining);
                assert_eq!(next.successor.as_deref(), Some("broker-b"));
            }
            other => panic!("expected a fence, got {other:?}"),
        }
    }

    /// Step three waits: a fenced leader that has not reported drained is
    /// still writing as far as the control plane knows.
    #[test]
    fn a_fenced_shard_waits_for_the_leader_to_report_drained() {
        let streams = vec![stream("orders", 1)];
        let mut fenced = one_shard("broker-a", &["broker-b"]);
        fenced.successor = Some("broker-b".to_string());
        fenced.state = ShardState::Draining;

        let plan = plan(
            &streams,
            &[],
            &draining_cluster(),
            std::slice::from_ref(&fenced),
            &Reported::caught_up(&["broker-b"]),
        );
        assert_eq!(
            only_decision(&plan),
            &Decision::Waiting(Blocked::LeaderStopping)
        );

        // A drained report from before the fence does not count.
        let plan = super::plan(
            &streams,
            &[],
            &draining_cluster(),
            &[fenced],
            &Reported::caught_up(&["broker-b"]).drained_at(2),
        );
        assert_eq!(
            only_decision(&plan),
            &Decision::Waiting(Blocked::LeaderStopping)
        );
    }

    /// Step three: the drained report at the fenced generation cuts over. The
    /// old leader is not kept as a follower -- it is draining.
    #[test]
    fn a_drained_report_cuts_over_to_the_successor() {
        let streams = vec![stream("orders", 1)];
        let mut fenced = one_shard("broker-a", &["broker-b"]);
        fenced.successor = Some("broker-b".to_string());
        fenced.state = ShardState::Draining;

        let plan = plan(
            &streams,
            &[],
            &draining_cluster(),
            &[fenced.clone()],
            &Reported::caught_up(&["broker-b"]).drained_at(fenced.generation),
        );

        match only_decision(&plan) {
            Decision::Move(MoveStep::CutOver { from, to }, next) => {
                assert_eq!((from.as_str(), to.as_str()), ("broker-a", "broker-b"));
                assert_eq!(next.leader, "broker-b");
                assert!(next.replicas.is_empty(), "replication factor one");
                assert_eq!(next.state, ShardState::Assigning);
                assert_eq!(next.successor, None);
            }
            other => panic!("expected a cut-over, got {other:?}"),
        }
    }

    /// The whole move, driven to the end, writes exactly three steps and then
    /// nothing: staged, fenced, cut over.
    #[test]
    fn a_drain_converges_in_three_writes() {
        let streams = vec![stream("orders", 1)];
        let nodes = draining_cluster();
        let mut existing = vec![one_shard("broker-a", &[])];
        let mut steps = Vec::new();
        for _ in 0..6 {
            let reported = if existing[0].state == ShardState::Draining {
                Reported::caught_up(&["broker-b"]).drained_at(existing[0].generation)
            } else {
                Reported::caught_up(&["broker-b"]).at(existing[0].generation)
            };
            let plan = plan(&streams, &[], &nodes, &existing, &reported);
            match only_decision(&plan) {
                Decision::Move(step, next) => {
                    steps.push(step.label());
                    let mut next = next.clone();
                    next.generation = existing[0].generation + 1;
                    existing = vec![next];
                }
                Decision::Kept => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(steps, vec!["stage", "fence", "cut_over"]);
        assert_eq!(existing[0].leader, "broker-b");
    }

    /// A destination that already holds a copy is fenced straight away: the
    /// stage step exists only to make the copy.
    #[test]
    fn a_caught_up_replica_needs_no_staging() {
        let streams = vec![replicated_stream("orders", 1, 2)];
        let existing = vec![one_shard("broker-a", &["broker-b"])];

        let plan = plan(
            &streams,
            &[],
            &draining_cluster(),
            &existing,
            &Reported::caught_up(&["broker-b"]),
        );

        match only_decision(&plan) {
            Decision::Move(MoveStep::Fence, next) => {
                assert_eq!(next.successor.as_deref(), Some("broker-b"));
                assert_eq!(next.state, ShardState::Draining);
            }
            other => panic!("expected a fence, got {other:?}"),
        }
    }

    /// A report from before the staging write says nothing about the
    /// successor: the leader may have written since. The move waits for a
    /// report at its own generation.
    #[test]
    fn a_report_from_an_older_generation_does_not_fence() {
        let streams = vec![stream("orders", 1)];
        let mut staged = one_shard("broker-a", &["broker-b"]);
        staged.successor = Some("broker-b".to_string());
        staged.generation = 4;

        let plan = plan(
            &streams,
            &[],
            &draining_cluster(),
            &[staged.clone()],
            &Reported::caught_up(&["broker-b"]).at(3),
        );
        assert_eq!(
            only_decision(&plan),
            &Decision::Waiting(Blocked::DestinationCatchingUp {
                successor: "broker-b".to_string()
            })
        );

        // Nor does it let a caught-up replica skip the staging.
        let existing = vec![one_shard("broker-a", &["broker-b"])];
        let plan = super::plan(
            &[replicated_stream("orders", 1, 2)],
            &[],
            &draining_cluster(),
            &existing,
            &Reported::caught_up(&["broker-b"]).at(2),
        );
        assert!(
            matches!(
                only_decision(&plan),
                Decision::Move(MoveStep::Stage { .. }, _)
            ),
            "got {:?}",
            only_decision(&plan)
        );
    }

    /// A staged destination that stops being live is dropped from the move,
    /// not waited on.
    #[test]
    fn a_lost_destination_abandons_the_move() {
        let streams = vec![stream("orders", 1)];
        let nodes = vec![
            node("broker-a", NodeLifecycle::Live, None),
            node("broker-b", NodeLifecycle::Down, None),
        ];
        let mut staged = one_shard("broker-a", &["broker-b"]);
        staged.successor = Some("broker-b".to_string());

        let plan = plan(&streams, &[], &nodes, &[staged], &NothingCaughtUp);

        match only_decision(&plan) {
            Decision::Move(MoveStep::Abandon { successor }, next) => {
                assert_eq!(successor, "broker-b");
                assert_eq!(next.leader, "broker-a");
                assert!(next.replicas.is_empty());
                assert_eq!(next.successor, None);
            }
            other => panic!("expected the move to be abandoned, got {other:?}"),
        }
    }

    /// A destination that dies between the fence and the cut-over does not
    /// take the shard. Nothing else holds the log, so the old leader takes
    /// it back at a new generation and the move is chosen again.
    #[test]
    fn a_destination_lost_after_the_fence_does_not_lead() {
        let streams = vec![stream("orders", 1)];
        let nodes = vec![
            node("broker-a", NodeLifecycle::Live, None),
            node("broker-b", NodeLifecycle::Down, None),
        ];
        let mut fenced = one_shard("broker-a", &["broker-b"]);
        fenced.successor = Some("broker-b".to_string());
        fenced.state = ShardState::Draining;

        let plan = plan(
            &streams,
            &[],
            &nodes,
            &[fenced.clone()],
            &Reported::caught_up(&["broker-b"]).drained_at(fenced.generation),
        );

        match only_decision(&plan) {
            Decision::Move(MoveStep::CutOver { to, .. }, next) => {
                assert_eq!(to, "broker-a");
                assert_eq!(next.leader, "broker-a");
                assert_eq!(next.state, ShardState::Assigning);
                assert_eq!(next.successor, None);
            }
            other => panic!("expected the leader to take the shard back, got {other:?}"),
        }
    }

    /// The leader dying mid-move is a failover, and the successor -- a replica
    /// like any other -- is promoted if it holds the log.
    #[test]
    fn a_leader_lost_mid_move_fails_over_to_the_successor() {
        let streams = vec![stream("orders", 1)];
        let nodes = vec![
            node("broker-a", NodeLifecycle::Down, None),
            node("broker-b", NodeLifecycle::Live, None),
        ];
        let mut fenced = one_shard("broker-a", &["broker-b"]);
        fenced.successor = Some("broker-b".to_string());
        fenced.state = ShardState::Draining;

        let plan = plan(
            &streams,
            &[],
            &nodes,
            &[fenced],
            &Reported::caught_up(&["broker-b"]),
        );
        assert_eq!(
            only_decision(&plan),
            &Decision::Place("broker-b".to_string(), Vec::new())
        );

        // And not if it does not: the shard stays unavailable rather than
        // served empty.
        let mut fenced = one_shard("broker-a", &["broker-b"]);
        fenced.successor = Some("broker-b".to_string());
        let plan = super::plan(&streams, &[], &nodes, &[fenced], &NothingCaughtUp);
        assert_eq!(
            only_decision(&plan),
            &Decision::Unplaceable(Unplaceable::NoCaughtUpReplica)
        );
    }

    /// The old leader stays as a follower after a rebalance, so the stream
    /// keeps its copies without a fresh catch-up.
    #[test]
    fn a_cut_over_keeps_the_old_leader_as_a_follower_when_it_is_staying() {
        let streams = vec![replicated_stream("orders", 1, 2)];
        let nodes = live(&["broker-a", "broker-b"]);
        let mut fenced = one_shard("broker-a", &["broker-b"]);
        fenced.successor = Some("broker-b".to_string());
        fenced.state = ShardState::Draining;

        let plan = plan(
            &streams,
            &[],
            &nodes,
            &[fenced.clone()],
            &Reported::caught_up(&["broker-b"]).drained_at(fenced.generation),
        );

        match only_decision(&plan) {
            Decision::Move(MoveStep::CutOver { .. }, next) => {
                assert_eq!(next.leader, "broker-b");
                assert_eq!(next.replicas, vec!["broker-a".to_string()]);
            }
            other => panic!("expected a cut-over, got {other:?}"),
        }
    }

    /// Only as many moves as the policy allows are in flight at once; the
    /// rest wait, visibly.
    #[test]
    fn moves_are_bounded_by_the_policy() {
        let streams = vec![stream("orders", 4)];
        let existing: Vec<ShardAssignment> = (0..4)
            .map(|shard| pinned("orders", shard, "broker-a"))
            .collect();

        let plan = plan_with(
            &streams,
            &[],
            &draining_cluster(),
            &existing,
            &NothingCaughtUp,
            MovePolicy { max_concurrent: 2 },
        );
        assert_eq!(plan.moves().count(), 2);
        assert_eq!(
            plan.waiting()
                .filter(|(_, why)| **why == Blocked::MoveLimit)
                .count(),
            2
        );

        // A move already in flight holds its slot.
        let mut existing = existing;
        existing[0].successor = Some("broker-b".to_string());
        existing[0].replicas = vec!["broker-b".to_string()];
        let plan = plan_with(
            &streams,
            &[],
            &draining_cluster(),
            &existing,
            &NothingCaughtUp,
            MovePolicy { max_concurrent: 1 },
        );
        assert_eq!(plan.moves().count(), 0);
        assert_eq!(plan.waiting().count(), 4);

        // Zero holds everything.
        let plan = plan_with(
            &streams,
            &[],
            &draining_cluster(),
            &existing[1..],
            &NothingCaughtUp,
            MovePolicy { max_concurrent: 0 },
        );
        assert_eq!(plan.moves().count(), 0);
    }

    /// A fresh cluster is placed with leaders spread, so it never needs a move
    /// to get there -- however the hash falls, and however many roles each node
    /// holds. Replication factor equal to the node count is the case that used
    /// to slip through: every node held a role for every shard, so the role
    /// bound was met with every leader on one node.
    #[test]
    fn fresh_placement_needs_no_rebalance() {
        for (shards, nodes, rf) in [(4, 3, 3), (6, 2, 2), (5, 3, 1), (12, 4, 3), (7, 3, 2)] {
            let streams = vec![replicated_stream("orders", shards, rf)];
            let ids: Vec<String> = (0..nodes).map(|i| format!("broker-{i}")).collect();
            let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
            let nodes = live(&ids);
            let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);
            let placed: Vec<ShardAssignment> = plan
                .to_place()
                .map(|(key, leader, replicas)| assignment_for(key, leader, replicas.to_vec()))
                .collect();
            assert_eq!(placed.len() as u32, shards);

            let again = super::plan(&streams, &[], &nodes, &placed, &NothingCaughtUp);
            assert_eq!(
                again.moves().count(),
                0,
                "{shards} shards over {} nodes at rf {rf} were placed unevenly: {:?}",
                nodes.len(),
                placed.iter().map(|a| a.leader.as_str()).collect::<Vec<_>>()
            );
            assert_eq!(again.kept() as u32, shards);
        }
    }

    /// The motivating case: every shard landed on one broker while the other
    /// was registering. Rebalancing moves shards from the node over its share
    /// to the one under it, and stops when neither holds.
    #[test]
    fn an_overloaded_node_gives_shards_to_an_idle_one_until_balanced() {
        let streams = vec![stream("orders", 6)];
        let nodes = live(&["broker-a", "broker-b"]);
        let mut existing: Vec<ShardAssignment> = (0..6)
            .map(|shard| pinned("orders", shard, "broker-a"))
            .collect();

        let mut writes = 0;
        for _ in 0..40 {
            // One report per shard in the store; here one fixture answers for
            // all of them at the generation of whichever shard is moving.
            let moving = existing
                .iter()
                .find(|a| a.state == ShardState::Draining || a.successor.is_some());
            let reported = Reported {
                caught_up: ["broker-a", "broker-b"]
                    .iter()
                    .map(|n| n.to_string())
                    .collect(),
                generation: moving.map_or(3, |a| a.generation),
                drained: moving.is_some_and(|a| a.state == ShardState::Draining),
            };
            let plan = plan_with(
                &streams,
                &[],
                &nodes,
                &existing,
                &reported,
                MovePolicy { max_concurrent: 1 },
            );
            let mut wrote = false;
            for (key, _, next) in plan.moves() {
                let slot = existing.iter_mut().find(|a| &a.key == key).expect("known");
                let mut next = next.clone();
                next.generation = slot.generation + 1;
                *slot = next;
                wrote = true;
                writes += 1;
            }
            if !wrote && plan.waiting().count() == 0 {
                break;
            }
        }

        let on_a = existing.iter().filter(|a| a.leader == "broker-a").count();
        let on_b = existing.iter().filter(|a| a.leader == "broker-b").count();
        assert_eq!((on_a, on_b), (3, 3), "balanced");
        assert_eq!(
            writes, 9,
            "three moves of three writes each, and no churn after"
        );
        assert!(existing.iter().all(|a| a.successor.is_none()));
    }

    /// One shard over is not an imbalance worth a move: the share is a ceiling,
    /// and five shards on two nodes is three and two.
    #[test]
    fn a_cluster_within_one_of_balanced_is_left_alone() {
        let streams = vec![stream("orders", 5)];
        let nodes = live(&["broker-a", "broker-b"]);
        let existing: Vec<ShardAssignment> = (0..5)
            .map(|shard| {
                pinned(
                    "orders",
                    shard,
                    if shard < 3 { "broker-a" } else { "broker-b" },
                )
            })
            .collect();

        let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);
        assert_eq!(plan.kept(), 5);
        assert_eq!(plan.moves().count(), 0);
    }

    /// An ephemeral stream has no log to hand off, so a draining node's shard
    /// of one is simply reassigned, as it always was.
    #[test]
    fn an_ephemeral_stream_is_reassigned_rather_than_moved() {
        let mut ephemeral = stream("orders", 1);
        ephemeral.durable = false;
        let existing = vec![one_shard("broker-a", &[])];

        let plan = plan(
            &[ephemeral],
            &[],
            &draining_cluster(),
            &existing,
            &NothingCaughtUp,
        );
        assert_eq!(
            only_decision(&plan),
            &Decision::Place("broker-b".to_string(), Vec::new())
        );
    }

    /// A follower on a draining node is replaced by one that is staying, so
    /// the node ends up holding nothing and can leave.
    #[test]
    fn a_follower_on_a_draining_node_is_reseated() {
        let streams = vec![replicated_stream("orders", 1, 2)];
        let nodes = vec![
            node("broker-a", NodeLifecycle::Live, None),
            node("broker-b", NodeLifecycle::Draining, None),
            node("broker-c", NodeLifecycle::Live, None),
        ];
        let existing = vec![one_shard("broker-a", &["broker-b"])];

        let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);
        match only_decision(&plan) {
            Decision::Move(MoveStep::Reseat { from, to }, next) => {
                assert_eq!((from.as_str(), to.as_str()), ("broker-b", "broker-c"));
                assert_eq!(next.leader, "broker-a");
                assert_eq!(next.replicas, vec!["broker-c".to_string()]);
            }
            other => panic!("expected a reseat, got {other:?}"),
        }

        // A follower that is merely down is left where it is.
        let nodes = vec![
            node("broker-a", NodeLifecycle::Live, None),
            node("broker-b", NodeLifecycle::Down, None),
            node("broker-c", NodeLifecycle::Live, None),
        ];
        let plan = super::plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);
        assert_eq!(only_decision(&plan), &Decision::Kept);
    }

    /// A draining node with nowhere to send its shards waits, and says so.
    #[test]
    fn a_drain_with_no_destination_waits() {
        let streams = vec![stream("orders", 1)];
        let nodes = vec![node("broker-a", NodeLifecycle::Draining, None)];
        let existing = vec![one_shard("broker-a", &[])];

        let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);
        assert_eq!(
            only_decision(&plan),
            &Decision::Waiting(Blocked::NoDestination)
        );
    }
}
