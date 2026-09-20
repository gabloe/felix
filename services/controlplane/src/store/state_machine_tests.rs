//! The determinism harness, and the behavioural guarantees the snapshot
//! format makes.
//!
//! The harness is the cheap test that catches the expensive bug: one command
//! script covering every command — including the multi-entity operations
//! (expiry over many nodes, a tenant cascade) where a HashMap iteration
//! order or a clock read would leak — applied to two independent state
//! machines, which must serialize **byte-identical** snapshots. Any
//! nondeterminism in apply shows up here as a failed byte comparison, on
//! every run, rather than as two Raft replicas quietly disagreeing in
//! production.
use super::*;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::{
    Cache, ConsistencyLevel, DeliveryGuarantee, Namespace, NamespaceKey, Node, NodeCapacity,
    NodeLifecycle, NodeSpec, NodeStatus, RetentionPolicy, Stream, StreamKey, StreamKind, Tenant,
};
use crate::store::command::{
    COMMAND_VERSION, MetaCommand, MetaError, MetaResponse, decode_command, decode_result,
    encode_command,
};
use crate::store::memory::InMemoryStore;
use crate::store::{ControlPlaneStore, StoreConfig};

fn machine() -> MetadataStateMachine {
    machine_with(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    })
}

fn machine_with(config: StoreConfig) -> MetadataStateMachine {
    MetadataStateMachine::new(Arc::new(InMemoryStore::new(config)))
}

fn tenant(id: &str) -> Tenant {
    Tenant {
        tenant_id: id.to_string(),
        display_name: format!("Tenant {id}"),
    }
}

fn namespace(tenant_id: &str, namespace: &str) -> Namespace {
    Namespace {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        display_name: namespace.to_string(),
    }
}

fn stream(tenant_id: &str, namespace: &str, name: &str) -> Stream {
    Stream {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: name.to_string(),
        kind: StreamKind::Stream,
        shards: 1,
        replication_factor: 1,
        retention: RetentionPolicy {
            max_age_seconds: Some(3_600),
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtLeastOnce,
        durable: false,
    }
}

fn cache(tenant_id: &str, namespace: &str, name: &str) -> Cache {
    Cache {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        cache: name.to_string(),
        display_name: name.to_string(),
        shards: 1,
        replication_factor: 1,
    }
}

fn node(id: &str, port: u16) -> Node {
    Node {
        node_id: id.to_string(),
        spec: NodeSpec {
            advertise_addr: format!("10.0.0.9:{port}"),
            client_addr: None,
            region: "local".to_string(),
            labels: BTreeMap::new(),
            capacity: NodeCapacity {
                max_shards: Some(64),
                weight: 1,
            },
        },
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: 1_000,
            registered_at_millis: 1_000,
            incarnation: 0,
        },
    }
}

/// Deterministic key material: the fixture stands where propose-time
/// generation stands in production, which is the point — randomness enters
/// through the command, never inside apply.
fn fixed_keys(seed: u8) -> crate::auth::felix_token::TenantSigningKeys {
    let private = [seed; 32];
    let signing = ed25519_dalek::SigningKey::from_bytes(&private);
    crate::auth::felix_token::TenantSigningKeys {
        current: crate::auth::felix_token::SigningKey {
            kid: format!("kid-{seed}"),
            alg: jsonwebtoken::Algorithm::EdDSA,
            private_key: private,
            public_key: signing.verifying_key().to_bytes(),
        },
        previous: Vec::new(),
    }
}

/// Every command variant at least once, with the order-sensitive spots
/// stressed: many nodes registered before one expiry sweep, and a tenant
/// with several namespaces, streams, and caches deleted in one cascade.
fn script() -> Vec<MetaCommand> {
    let mut commands = Vec::new();
    for t in ["t-a", "t-b", "t-c"] {
        commands.push(MetaCommand::CreateTenant { tenant: tenant(t) });
        for ns in ["ns-1", "ns-2", "ns-3"] {
            commands.push(MetaCommand::CreateNamespace {
                namespace: namespace(t, ns),
            });
            commands.push(MetaCommand::CreateStream {
                stream: stream(t, ns, "orders"),
            });
            commands.push(MetaCommand::CreateStream {
                stream: stream(t, ns, "events"),
            });
            commands.push(MetaCommand::CreateCache {
                cache: cache(t, ns, "primary"),
            });
        }
    }
    for i in 0..8u16 {
        commands.push(MetaCommand::RegisterNode {
            node: node(&format!("broker-{i}"), 7_000 + i),
        });
        commands.push(MetaCommand::RecordNodeHeartbeat {
            node_id: format!("broker-{i}"),
            incarnation: 1,
            at_millis: 2_000 + u64::from(i % 2) * 10_000,
        });
    }
    // Half the fleet expires in one command: eight change events take their
    // seqs inside a single apply, which is where iteration order would leak.
    commands.push(MetaCommand::ExpireStaleNodes {
        expiry_before_millis: 5_000,
    });
    commands.push(MetaCommand::SetNodeLifecycle {
        node_id: "broker-1".to_string(),
        lifecycle: NodeLifecycle::Draining,
    });
    commands.push(MetaCommand::UpsertIdpIssuer {
        tenant_id: "t-a".to_string(),
        issuer: IdpIssuerConfig {
            issuer: "https://issuer.example.com".to_string(),
            audiences: vec!["felix".to_string()],
            discovery_url: None,
            jwks_url: Some("https://issuer.example.com/jwks".to_string()),
            claim_mappings: ClaimMappings {
                subject_claim: "sub".to_string(),
                groups_claim: None,
            },
        },
    });
    commands.push(MetaCommand::AddRbacPolicy {
        tenant_id: "t-a".to_string(),
        policy: PolicyRule {
            subject: "role:reader".to_string(),
            object: "tenant:t-a".to_string(),
            action: "rbac.view".to_string(),
        },
    });
    commands.push(MetaCommand::AddRbacGrouping {
        tenant_id: "t-a".to_string(),
        grouping: GroupingRule {
            user: "p:reader".to_string(),
            role: "role:reader".to_string(),
        },
    });
    commands.push(MetaCommand::SetTenantSigningKeys {
        tenant_id: "t-b".to_string(),
        keys: fixed_keys(9),
    });
    commands.push(MetaCommand::BootstrapTenantAuth {
        tenant_id: "t-a".to_string(),
        seed: crate::store::TenantAuthSeed {
            issuers: Vec::new(),
            policies: vec![PolicyRule {
                subject: "role:tenant-admin".to_string(),
                object: "tenant:t-a".to_string(),
                action: "tenant.manage".to_string(),
            }],
            groupings: vec![GroupingRule {
                user: "p:admin".to_string(),
                role: "role:tenant-admin".to_string(),
            }],
            signing_keys: fixed_keys(3),
        },
    });
    // A cascade across nine namespaces, six streams, three caches — all of
    // whose delete events take seqs inside one apply.
    commands.push(MetaCommand::DeleteTenant {
        tenant_id: "t-c".to_string(),
    });
    commands.push(MetaCommand::DeleteStream {
        key: StreamKey {
            tenant_id: "t-a".to_string(),
            namespace: "ns-1".to_string(),
            stream: "events".to_string(),
        },
    });
    commands.push(MetaCommand::DeleteNamespace {
        key: NamespaceKey {
            tenant_id: "t-b".to_string(),
            namespace: "ns-2".to_string(),
        },
    });
    commands
}

async fn run_script(machine: &MetadataStateMachine) {
    for command in script() {
        // Through the byte layer, exactly as Raft feeds it.
        let response =
            crate::raft::AppStateMachine::apply(machine, &encode_command(&command)).await;
        decode_result(&response)
            .expect("every scripted response decodes")
            .expect("every scripted command succeeds");
    }
}

/// **The determinism harness.** Two state machines, one script, byte-equal
/// snapshots. A clock read, a generated value, or a HashMap iteration order
/// anywhere in apply fails this on every run.
#[tokio::test]
async fn the_same_log_produces_byte_identical_snapshots() {
    let first = machine();
    let second = machine();
    run_script(&first).await;
    run_script(&second).await;

    let first_snapshot = crate::raft::AppStateMachine::snapshot(&first).await;
    let second_snapshot = crate::raft::AppStateMachine::snapshot(&second).await;
    assert!(!first_snapshot.is_empty());
    assert_eq!(
        first_snapshot, second_snapshot,
        "two replicas applying the same commands serialized different state",
    );
}

/// Restore is exact: a third machine restored from a snapshot serializes
/// the same bytes, and — the half that matters to brokers — answers the
/// change feeds identically, sequence numbers included.
#[tokio::test]
async fn a_restored_machine_is_indistinguishable() {
    let original = machine();
    run_script(&original).await;
    let snapshot = crate::raft::AppStateMachine::snapshot(&original).await;

    let restored = machine();
    crate::raft::AppStateMachine::restore(&restored, &snapshot).await;

    assert_eq!(
        snapshot,
        crate::raft::AppStateMachine::snapshot(&restored).await,
        "export → import → export must be a fixed point",
    );

    let a = original.store().node_changes(0).await.expect("changes");
    let b = restored.store().node_changes(0).await.expect("changes");
    assert_eq!(a.next_seq, b.next_seq);
    assert_eq!(a.items.len(), b.items.len());

    let a = original.store().stream_snapshot().await.expect("snapshot");
    let b = restored.store().stream_snapshot().await.expect("snapshot");
    assert_eq!(a.next_seq, b.next_seq, "watch checkpoints must survive");
}

/// The resnapshot signal survives a restore: a consumer whose checkpoint
/// fell out of the retained window gets told so by the restored store
/// exactly as the original would have — `first seq > since` — instead of a
/// quiet gap.
#[tokio::test]
async fn an_evicted_change_window_reads_the_same_after_restore() {
    let config = StoreConfig {
        changes_limit: 3,
        change_retention_max_rows: Some(3),
    };
    let original = machine_with(config.clone());
    for i in 0..10 {
        original
            .dispatch(MetaCommand::CreateTenant {
                tenant: tenant(&format!("t-{i}")),
            })
            .await
            .expect("create");
    }

    let before = original.store().tenant_changes(0).await.expect("changes");
    assert!(
        before.items[0].seq > 0,
        "the premise: seq 0 has been evicted"
    );

    let snapshot = crate::raft::AppStateMachine::snapshot(&original).await;
    let restored = machine_with(config);
    crate::raft::AppStateMachine::restore(&restored, &snapshot).await;
    let after = restored.store().tenant_changes(0).await.expect("changes");

    assert_eq!(before.items[0].seq, after.items[0].seq);
    assert_eq!(before.next_seq, after.next_seq);
}

/// The heartbeat rule crosses the command layer intact: liveness updates,
/// and the changefeed does not move.
#[tokio::test]
async fn a_heartbeat_command_publishes_no_change() {
    let machine = machine();
    machine
        .dispatch(MetaCommand::RegisterNode {
            node: node("broker-1", 7_001),
        })
        .await
        .expect("register");
    let before = machine.store().node_changes(0).await.expect("changes");

    let response = machine
        .dispatch(MetaCommand::RecordNodeHeartbeat {
            node_id: "broker-1".to_string(),
            incarnation: 1,
            at_millis: 9_999,
        })
        .await
        .expect("heartbeat");
    let MetaResponse::Node { node } = response else {
        panic!("heartbeat answers with the node");
    };
    assert_eq!(node.status.last_heartbeat_at_millis, 9_999);

    let after = machine.store().node_changes(0).await.expect("changes");
    assert_eq!(
        before.next_seq, after.next_seq,
        "a heartbeat that published a change would evict real membership \
         events from the retention window",
    );
}

/// A command from a newer build is refused loudly and identically on every
/// replica — never skipped, which would fork state.
#[tokio::test]
async fn a_newer_command_version_is_refused_not_skipped() {
    let machine = machine();
    let mut envelope = serde_json::to_value(serde_json::json!({
        "v": COMMAND_VERSION + 1,
        "op": "create_tenant",
        "tenant": {"tenant_id": "t1", "display_name": "One"}
    }))
    .expect("envelope");
    envelope["v"] = serde_json::json!(COMMAND_VERSION + 1);
    let bytes = serde_json::to_vec(&envelope).expect("bytes");

    let response = crate::raft::AppStateMachine::apply(&machine, &bytes).await;
    let result = decode_result(&response).expect("decodes");
    assert!(matches!(result, Err(MetaError::Unsupported(_))));
    assert!(
        machine
            .store()
            .list_tenants()
            .await
            .expect("list")
            .is_empty(),
        "a refused command must change nothing",
    );
}

/// The import command is the migration cutover: refused against a store
/// with any history unless the operator explicitly overwrites, and exact
/// when it lands — including the sequence positions broker watches resume
/// from.
#[tokio::test]
async fn an_import_replaces_everything_and_respects_the_guard() {
    // A populated source, exported the way the migration tool exports
    // Postgres: through the traits, sequence heads carried, windows empty.
    let source = machine();
    run_script(&source).await;
    let exported = crate::store::memory::export_state_from(
        source.store().as_ref() as &(dyn crate::store::ControlPlaneAuthStore + Send + Sync)
    )
    .await
    .expect("export");
    let source_head = source
        .store()
        .shard_assignment_changes(0)
        .await
        .expect("changes")
        .next_seq;

    // A fresh store accepts it without ceremony.
    let fresh = machine();
    fresh
        .dispatch(MetaCommand::ImportState {
            state: Box::new(exported.clone()),
            overwrite: false,
        })
        .await
        .expect("import into unused store");
    assert_eq!(
        serde_json::to_vec(
            &fresh
                .store()
                .stream_snapshot()
                .await
                .expect("snap")
                .items
                .len()
        )
        .expect("len"),
        serde_json::to_vec(
            &source
                .store()
                .stream_snapshot()
                .await
                .expect("snap")
                .items
                .len()
        )
        .expect("len"),
    );

    // A broker checkpointed at the head continues with no resnapshot: an
    // empty page whose next_seq equals its checkpoint is "nothing new".
    let at_head = fresh
        .store()
        .shard_assignment_changes(source_head)
        .await
        .expect("changes");
    assert!(at_head.items.is_empty());
    assert_eq!(at_head.next_seq, source_head);

    // A broker behind the head gets the ordinary eviction signal — empty
    // page, next_seq ahead — and resnapshots exactly once.
    let behind = fresh.store().tenant_changes(0).await.expect("changes");
    assert!(behind.items.is_empty());
    assert!(behind.next_seq > 0, "the head must survive the migration");

    // A store with history refuses the import without overwrite...
    let used = machine();
    used.dispatch(MetaCommand::CreateTenant {
        tenant: tenant("t-existing"),
    })
    .await
    .expect("create");
    let refused = used
        .dispatch(MetaCommand::ImportState {
            state: Box::new(exported.clone()),
            overwrite: false,
        })
        .await;
    assert!(matches!(refused, Err(MetaError::Conflict(_))));

    // ...and replaces everything when the operator says so — the restore
    // ceremony.
    used.dispatch(MetaCommand::ImportState {
        state: Box::new(exported),
        overwrite: true,
    })
    .await
    .expect("overwrite import");
    assert!(
        used.store()
            .tenant_exists("t-existing")
            .await
            .map(|exists| !exists)
            .expect("exists"),
        "an overwrite import leaves nothing of the old state"
    );
}

/// The round trip every command takes: encode → decode is identity, so the
/// leader and its followers apply the same value.
#[tokio::test]
async fn commands_round_trip_through_the_envelope() {
    for command in script() {
        let decoded = decode_command(&encode_command(&command)).expect("round trip");
        assert_eq!(
            serde_json::to_value(&decoded).expect("value"),
            serde_json::to_value(&command).expect("value"),
        );
    }
}

/// **A retried proposal is answered with what the first one returned.**
///
/// `RaftStore::write` caps each attempt and retries within a larger budget, so
/// a timed-out attempt may be re-proposing a command that committed. Without a
/// request id the state machine answers the retry from its post-commit state:
/// `409 tenant already exists`, for a tenant the caller successfully created
/// (#529). With one, the retry gets the original success.
#[tokio::test]
async fn a_retried_proposal_is_answered_from_the_first_apply() {
    let machine = machine();
    let create = encode_command(&MetaCommand::CreateTenant {
        tenant: tenant("acme"),
    });
    let proposal = crate::store::command::stamp_request_id(&create, "rid-1").expect("stamps");

    let first = machine.apply(&proposal).await;
    assert!(
        decode_result(&first).expect("decodes").is_ok(),
        "the first apply should create the tenant",
    );

    // The same bytes again, exactly as a retry re-proposes them.
    let second = machine.apply(&proposal).await;
    assert_eq!(
        second, first,
        "a retry must be answered with the original response, not the conflict \
         its own effect now produces",
    );
}

/// Without an id there is nothing to deduplicate on, which is what a proposal
/// from a peer that predates this looks like. It conflicts, as it always did.
#[tokio::test]
async fn an_unstamped_retry_still_conflicts() {
    let machine = machine();
    let create = encode_command(&MetaCommand::CreateTenant {
        tenant: tenant("acme"),
    });

    assert!(
        decode_result(&machine.apply(&create).await)
            .expect("decodes")
            .is_ok()
    );
    let second = decode_result(&machine.apply(&create).await).expect("decodes");
    assert!(
        matches!(second, Err(MetaError::Conflict(_))),
        "an unstamped re-proposal has no identity to be recognised by: {second:?}",
    );
}

/// Two *different* writes must not be confused for each other, however close
/// together they arrive.
#[tokio::test]
async fn different_request_ids_are_applied_separately() {
    let machine = machine();
    let first = crate::store::command::stamp_request_id(
        &encode_command(&MetaCommand::CreateTenant {
            tenant: tenant("acme"),
        }),
        "rid-1",
    )
    .expect("stamps");
    let second = crate::store::command::stamp_request_id(
        &encode_command(&MetaCommand::CreateTenant {
            tenant: tenant("acme"),
        }),
        "rid-2",
    )
    .expect("stamps");

    assert!(
        decode_result(&machine.apply(&first).await)
            .expect("decodes")
            .is_ok()
    );
    // A genuine second attempt to create the same tenant, by someone else.
    // That is a real conflict and must still be reported as one.
    let answer = decode_result(&machine.apply(&second).await).expect("decodes");
    assert!(
        matches!(answer, Err(MetaError::Conflict(_))),
        "a different write conflicting is not a retry: {answer:?}",
    );
}

/// The applied ids ride the snapshot, so a replica that restored from one
/// still recognises a retry -- the case a leader change would otherwise reopen.
#[tokio::test]
async fn applied_ids_survive_a_snapshot_restore() {
    let original = machine();
    let proposal = crate::store::command::stamp_request_id(
        &encode_command(&MetaCommand::CreateTenant {
            tenant: tenant("acme"),
        }),
        "rid-1",
    )
    .expect("stamps");
    let first = original.apply(&proposal).await;

    let restored = machine();
    restored.restore(&original.snapshot().await).await;

    assert_eq!(
        restored.apply(&proposal).await,
        first,
        "a restored replica must answer the retry the way the original did",
    );
}
