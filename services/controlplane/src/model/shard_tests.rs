//! Shard assignment validation, transitions, and serialization.
use super::*;

fn assignment() -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "payments".to_string(),
            stream: "orders".to_string(),
            shard: 2,
            kind: ShardKind::Stream,
        },
        leader: "broker-a".to_string(),
        replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
        generation: 7,
        state: ShardState::Active,
    }
}

#[test]
fn a_well_formed_assignment_validates() {
    assert_eq!(assignment().validate(), Ok(()));
    assert_eq!(assignment().validate_within(4), Ok(()));
}

/// The bound is the stream's shard count, so shard N is valid only for a stream
/// with more than N shards.
#[test]
fn a_shard_outside_the_stream_is_rejected() {
    let a = assignment();
    assert_eq!(
        a.validate_within(2),
        Err(ShardValidationError::ShardOutOfBounds {
            shard: 2,
            shards: 2
        }),
    );
    assert_eq!(a.validate_within(3), Ok(()), "shard 2 fits in 3 shards");
}

#[test]
fn a_leader_that_is_also_a_replica_is_rejected() {
    let mut a = assignment();
    a.replicas.push("broker-a".to_string());
    assert_eq!(
        a.validate(),
        Err(ShardValidationError::LeaderIsAlsoReplica(
            "broker-a".to_string()
        )),
    );
}

/// A repeated replica would make a quorum look larger than it is.
#[test]
fn a_repeated_replica_is_rejected() {
    let mut a = assignment();
    a.replicas = vec!["broker-b".to_string(), "broker-b".to_string()];
    assert_eq!(
        a.validate(),
        Err(ShardValidationError::DuplicateReplica(
            "broker-b".to_string()
        )),
    );
}

#[test]
fn malformed_node_identities_are_rejected() {
    let mut a = assignment();
    a.leader = "Broker A".to_string();
    assert!(matches!(
        a.validate(),
        Err(ShardValidationError::InvalidLeader(_))
    ));

    let mut a = assignment();
    a.replicas = vec![String::new()];
    assert!(matches!(
        a.validate(),
        Err(ShardValidationError::InvalidReplica(_))
    ));
}

#[test]
fn an_assignment_with_no_replicas_is_valid() {
    let mut a = assignment();
    a.replicas.clear();
    assert_eq!(a.validate(), Ok(()), "replication does not exist until M5");
}

/// Draining is only meaningful for a shard someone is serving, and a drained
/// shard does not return to the same leader: placement writes a new assignment
/// at a new generation instead.
#[test]
fn only_a_serving_shard_can_drain() {
    assert!(ShardState::Active.can_transition_to(ShardState::Draining));
    assert!(!ShardState::Assigning.can_transition_to(ShardState::Draining));
    assert!(!ShardState::Draining.can_transition_to(ShardState::Active));
    assert!(!ShardState::Draining.can_transition_to(ShardState::Assigning));
    assert!(!ShardState::Active.can_transition_to(ShardState::Assigning));
}

#[test]
fn every_state_can_stay_where_it_is() {
    for state in [
        ShardState::Assigning,
        ShardState::Active,
        ShardState::Draining,
    ] {
        assert!(state.can_transition_to(state), "{state:?} should be stable");
    }
}

#[test]
fn nodes_lists_the_leader_first_then_replicas() {
    let a = assignment();
    let nodes: Vec<&str> = a.nodes().map(String::as_str).collect();
    assert_eq!(nodes, vec!["broker-a", "broker-b", "broker-c"]);
}

#[test]
fn an_assignment_round_trips_through_json() {
    let before = assignment();
    let encoded = serde_json::to_string(&before).expect("serialize");
    let after: ShardAssignment = serde_json::from_str(&encoded).expect("deserialize");
    assert_eq!(after, before);
}

/// The key is flattened, so an assignment reads as one object rather than
/// nesting the identity a caller already has from the path.
#[test]
fn the_key_is_flattened_on_the_wire() {
    let encoded = serde_json::to_value(assignment()).expect("serialize");
    assert_eq!(encoded["tenant_id"], "t1");
    assert_eq!(encoded["shard"], 2);
    assert!(encoded.get("key").is_none());
}

#[test]
fn replicas_default_to_empty_when_absent() {
    let json = serde_json::json!({
        "tenant_id": "t1",
        "namespace": "payments",
        "stream": "orders",
        "shard": 0,
        "leader": "broker-a",
        "generation": 1,
        "state": "active",
    });
    let decoded: ShardAssignment = serde_json::from_value(json).expect("deserialize");
    assert!(decoded.replicas.is_empty());
    assert_eq!(decoded.state, ShardState::Active);
}

#[test]
fn states_and_ops_serialize_as_camel_case() {
    for (state, expected) in [
        (ShardState::Assigning, "\"assigning\""),
        (ShardState::Active, "\"active\""),
        (ShardState::Draining, "\"draining\""),
    ] {
        assert_eq!(serde_json::to_string(&state).expect("serialize"), expected);
    }
    for (op, expected) in [
        (ShardAssignmentChangeOp::Assigned, "\"assigned\""),
        (ShardAssignmentChangeOp::Updated, "\"updated\""),
        (ShardAssignmentChangeOp::Unassigned, "\"unassigned\""),
    ] {
        assert_eq!(serde_json::to_string(&op).expect("serialize"), expected);
    }
}

#[test]
fn a_change_round_trips_with_and_without_a_body() {
    for change in [
        ShardAssignmentChange {
            seq: 3,
            op: ShardAssignmentChangeOp::Assigned,
            key: assignment().key,
            assignment: Some(assignment()),
        },
        ShardAssignmentChange {
            seq: 4,
            op: ShardAssignmentChangeOp::Unassigned,
            key: assignment().key,
            assignment: None,
        },
    ] {
        let encoded = serde_json::to_string(&change).expect("serialize");
        let decoded: ShardAssignmentChange = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded, change);
    }
}
