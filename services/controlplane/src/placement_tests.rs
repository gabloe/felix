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

    let first = plan(&streams, &nodes, &[], &NothingCaughtUp);
    for _ in 0..20 {
        assert_eq!(plan(&streams, &nodes, &[], &NothingCaughtUp), first);
    }
}

/// Same set, different order in. Two instances reading the same rows through
/// different query plans must still agree.
#[test]
fn placement_does_not_depend_on_input_order() {
    let streams = vec![stream("orders", 8), stream("payments", 5)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let expected = placements(&plan(&streams, &nodes, &[], &NothingCaughtUp));

    let mut reversed_streams = streams.clone();
    reversed_streams.reverse();
    let mut reversed_nodes = nodes.clone();
    reversed_nodes.reverse();

    assert_eq!(
        placements(&plan(
            &reversed_streams,
            &reversed_nodes,
            &[],
            &NothingCaughtUp
        )),
        expected,
    );
    assert_eq!(
        placements(&plan(&streams, &reversed_nodes, &[], &NothingCaughtUp)),
        expected
    );
    assert_eq!(
        placements(&plan(&reversed_streams, &nodes, &[], &NothingCaughtUp)),
        expected
    );
}

#[test]
fn every_shard_gets_exactly_one_leader() {
    let streams = vec![stream("orders", 8), stream("payments", 5)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let plan = plan(&streams, &nodes, &[], &NothingCaughtUp);

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
    let plan = plan(&streams, &nodes, &[], &NothingCaughtUp);

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

/// Reconciliation must be idempotent: applying a plan and planning again keeps
/// everything, so a periodic pass does not churn the persisted rows.
#[test]
fn reconciliation_is_idempotent() {
    let streams = vec![stream("orders", 8)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);

    let first = plan(&streams, &nodes, &[], &NothingCaughtUp);
    let applied: Vec<ShardAssignment> = first
        .to_place()
        .map(|(key, leader, replicas)| assignment_for(key, leader, replicas.to_vec()))
        .collect();

    let second = plan(&streams, &nodes, &applied, &NothingCaughtUp);
    assert_eq!(second.kept(), 8, "a second pass should keep everything");
    assert_eq!(second.to_place().count(), 0, "and write nothing");
}

/// An assignment whose leader is still live is kept even when the hash would now
/// prefer someone else. Moving it costs a log handoff v1 does not have.
#[test]
fn a_valid_assignment_is_kept_even_if_the_hash_disagrees() {
    let streams = vec![stream("orders", 4)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);

    // Pin every shard to one node, which rendezvous hashing would not choose.
    let pinned: Vec<ShardAssignment> = (0..4)
        .map(|shard| ShardAssignment {
            key: ShardKey {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "orders".to_string(),
                shard,
            },
            leader: "broker-a".to_string(),
            replicas: Vec::new(),
            generation: 3,
            state: ShardState::Active,
        })
        .collect();

    let plan = plan(&streams, &nodes, &pinned, &NothingCaughtUp);
    assert_eq!(plan.kept(), 4);
    assert_eq!(plan.to_place().count(), 0, "no reshuffling");
}

/// Losing a node must move that node's shards and nothing else.
#[test]
fn losing_a_node_moves_only_its_shards() {
    let streams = vec![stream("orders", 30)];
    let all = live(&["broker-a", "broker-b", "broker-c"]);

    let initial = plan(&streams, &all, &[], &NothingCaughtUp);
    let applied: Vec<ShardAssignment> = initial
        .to_place()
        .map(|(key, leader, replicas)| assignment_for(key, leader, replicas.to_vec()))
        .collect();

    let mut degraded = all.clone();
    degraded[0].status.lifecycle = NodeLifecycle::Down;
    let after = plan(&streams, &degraded, &applied, &NothingCaughtUp);

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
        let plan = plan(&streams, &nodes, &[], &NothingCaughtUp);
        assert!(
            plan.to_place().all(|(_, leader, _)| leader == "broker-b"),
            "{lifecycle:?} must not receive placement",
        );
    }
}

/// An assignment on a node that stopped being eligible is re-placed, not kept.
#[test]
fn an_assignment_on_an_ineligible_node_is_replaced() {
    let streams = vec![stream("orders", 2)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Draining, None),
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
                },
                "broker-a",
                Vec::new(),
            )
        })
        .collect();

    let plan = plan(&streams, &nodes, &stale, &NothingCaughtUp);
    assert_eq!(plan.kept(), 0);
    assert_eq!(plan.to_place().count(), 2);
    assert!(plan.to_place().all(|(_, leader, _)| leader == "broker-b"));
}

#[test]
fn with_no_live_nodes_every_shard_says_why() {
    let streams = vec![stream("orders", 3)];
    let nodes = vec![node("broker-a", NodeLifecycle::Down, None)];

    let plan = plan(&streams, &nodes, &[], &NothingCaughtUp);
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

    let plan = plan(&streams, &nodes, &[], &NothingCaughtUp);
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
        },
        "broker-a",
        Vec::new(),
    )];

    let plan = plan(&streams, &nodes, &existing, &NothingCaughtUp);
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
    };
    let b = ShardKey {
        tenant_id: "a".to_string(),
        namespace: "bc".to_string(),
        stream: "d".to_string(),
        shard: 0,
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
    };
    let first = score(&key, "broker-a");
    assert_eq!(score(&key, "broker-a"), first);
    assert_ne!(score(&key, "broker-b"), first);
}

#[test]
fn a_stream_with_no_shards_plans_nothing() {
    let plan = plan(
        &[stream("orders", 0)],
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

        let outcome = reconcile_once(
            &store,
            &crate::replica_positions::ReplicaPositions::new(&Default::default()),
        )
        .await;
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
        reconcile_once(
            &store,
            &crate::replica_positions::ReplicaPositions::new(&Default::default()),
        )
        .await;
        let after_first = store
            .shard_assignment_snapshot()
            .await
            .expect("snap")
            .next_seq;

        for _ in 0..5 {
            let outcome = reconcile_once(
                &store,
                &crate::replica_positions::ReplicaPositions::new(&Default::default()),
            )
            .await;
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
        reconcile_once(
            &store,
            &crate::replica_positions::ReplicaPositions::new(&Default::default()),
        )
        .await;

        let before = store.list_shard_assignments().await.expect("list");
        let victim = before[0].leader.clone();
        let lost = before.iter().filter(|a| a.leader == victim).count();
        store
            .set_node_lifecycle(&victim, NodeLifecycle::Down)
            .await
            .expect("down");

        let outcome = reconcile_once(
            &store,
            &crate::replica_positions::ReplicaPositions::new(&Default::default()),
        )
        .await;
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
        let outcome = reconcile_once(
            &store,
            &crate::replica_positions::ReplicaPositions::new(&Default::default()),
        )
        .await;
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
    let plan = plan(&streams, &nodes, &[], &NothingCaughtUp);

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
    let plan = plan(&streams, &nodes, &[], &NothingCaughtUp);

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
    let plan = plan(&streams, &nodes, &[], &NothingCaughtUp);

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

    let first: Vec<_> = plan(&streams, &nodes, &[], &NothingCaughtUp)
        .to_place()
        .map(|(k, l, r)| (k.clone(), l.to_string(), r.to_vec()))
        .collect();

    // Same cluster, nodes presented in a different order.
    let mut shuffled = nodes.clone();
    shuffled.reverse();
    let second: Vec<_> = plan(&streams, &shuffled, &[], &NothingCaughtUp)
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

    let plan = plan(&streams, &nodes, &[], &NothingCaughtUp);
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
        },
        leader: leader.to_string(),
        replicas: replicas.iter().map(|r| r.to_string()).collect(),
        generation: 3,
        state: ShardState::Active,
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

    let plan = plan(&streams, &nodes, &existing, &caught_up);
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

        let plan = plan(&streams, &nodes, &existing, &NothingCaughtUp);

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

    let plan = plan(&streams, &nodes, &existing, &caught_up);
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

    let plan = plan(&streams, &nodes, &existing, &caught_up);
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

    let plan = plan(&streams, &nodes, &existing, &NothingCaughtUp);

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

    let plan = plan(&streams, &nodes, &existing, &caught_up);

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

    let plan = plan(&streams, &nodes, &existing, &NothingCaughtUp);

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

    let plan = plan(&streams, &nodes, &[], &NothingCaughtUp);

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

    let plan = plan(&streams, &nodes, &existing, &NothingCaughtUp);

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

        let plan = plan(&streams, &nodes, &existing, &caught_up);

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
        let plan = plan(&streams, &nodes, &existing, &ReplicasAt(level.clone()));
        plan.to_place().next().expect("placed").1.to_string()
    };
    let again = {
        let plan = plan(&streams, &nodes, &existing, &ReplicasAt(level));
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

    let plan = plan(&streams, &nodes, &existing, &OnlyOneKnown);

    assert_eq!(plan.to_place().next().expect("placed").1, "broker-c");
}
