//! State machine tests. The helpers here build the entities and the
//! all-commands script the themes share.
mod commands;
mod determinism;
mod retries;

use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;
use crate::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::{
    Cache, ConsistencyLevel, DeliveryGuarantee, Namespace, NamespaceKey, Node, NodeCapacity,
    NodeLifecycle, NodeSpec, NodeStatus, RetentionPolicy, Stream, StreamKey, StreamKind, Tenant,
};
use crate::store::memory::InMemoryStore;
use crate::store::raft::command::{
    COMMAND_VERSION, MetaCommand, MetaError, MetaResponse, decode_command, decode_result,
    encode_command,
};
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
        consistency: crate::model::ConsistencyLevel::Leader,
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
