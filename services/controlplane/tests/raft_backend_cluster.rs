//! The Raft store backend as a group: transparent forwarding, single
//! leadership for background work, and honest failure when quorum is gone.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use controlplane::model::Tenant;
use controlplane::raft::{AppStateMachine, LeadershipGate, NodeId, RaftHandle, RaftSettings};
use controlplane::store::memory::InMemoryStore;
use controlplane::store::raft_backend::RaftStore;
use controlplane::store::state_machine::MetadataStateMachine;
use controlplane::store::{ControlPlaneStore, StoreConfig};

struct TestNode {
    handle: RaftHandle,
    store: Arc<RaftStore>,
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

async fn start_node(id: NodeId, dir: &std::path::Path) -> TestNode {
    let inner = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    }));
    let machine = Arc::new(MetadataStateMachine::new(inner));
    let mut settings = RaftSettings::new(id, dir.into());
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(200), Duration::from_millis(400));
    let handle = RaftHandle::start(settings, Arc::clone(&machine) as Arc<dyn AppStateMachine>)
        .await
        .expect("start node");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = handle.rpc_router();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    TestNode {
        store: Arc::new(RaftStore::new(handle.clone(), machine)),
        handle,
        addr,
        server,
    }
}

async fn wait_for_leader(nodes: &[TestNode]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !nodes.iter().any(|n| n.handle.status().leader.is_some()) {
        assert!(Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_follower_serves_writes_by_forwarding_and_reads_locally() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(start_node((i + 1) as NodeId, dir.path()).await);
    }
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|n| (n.handle.status().id, n.addr.to_string()))
        .collect();
    nodes[0]
        .handle
        .initialize(members)
        .await
        .expect("initialize");
    wait_for_leader(&nodes).await;

    let leader_id = nodes
        .iter()
        .find_map(|n| n.handle.status().leader)
        .expect("leader");
    let follower = nodes
        .iter()
        .find(|n| n.handle.status().id != leader_id)
        .expect("follower");

    // The write lands on a follower's *store trait* — exactly what an API
    // handler behind a load balancer would do — and must succeed without the
    // caller knowing forwarding happened.
    let created = follower
        .store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("write through a follower");
    assert_eq!(created.tenant_id, "t1");

    // Every member serves the read locally once applied.
    let deadline = Instant::now() + Duration::from_secs(5);
    for node in &nodes {
        loop {
            let tenants = node.store.list_tenants().await.expect("list");
            if tenants.len() == 1 {
                break;
            }
            assert!(Instant::now() < deadline, "replica never applied the write");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Exactly one member holds the leadership gate, and it is the leader:
    // the sweep and placement run once per cluster, not once per instance.
    let mut holders = Vec::new();
    for node in &nodes {
        if LeadershipGate::Leader(node.handle.clone()).holds().await {
            holders.push(node.handle.status().id);
        }
    }
    assert_eq!(holders, vec![leader_id]);

    for node in &nodes {
        let _ = node.handle.shutdown().await;
        node.server.abort();
    }
}

/// With quorum gone, a write fails within its bounded retry budget instead
/// of hanging — the caller gets an error it can surface, and brokers keep
/// serving on their catalogs exactly as during any control-plane outage.
#[tokio::test]
async fn losing_quorum_fails_writes_loudly_not_silently() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(start_node((i + 1) as NodeId, dir.path()).await);
    }
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|n| (n.handle.status().id, n.addr.to_string()))
        .collect();
    nodes[0]
        .handle
        .initialize(members)
        .await
        .expect("initialize");
    wait_for_leader(&nodes).await;

    let leader_id = nodes
        .iter()
        .find_map(|n| n.handle.status().leader)
        .expect("leader");
    let survivor = nodes
        .iter()
        .position(|n| n.handle.status().id == leader_id)
        .expect("leader position");

    for (i, node) in nodes.iter().enumerate() {
        if i != survivor {
            let _ = node.handle.shutdown().await;
            node.server.abort();
        }
    }

    let refused = nodes[survivor]
        .store
        .create_tenant(Tenant {
            tenant_id: "t-after".to_string(),
            display_name: "After".to_string(),
        })
        .await;
    assert!(
        refused.is_err(),
        "a write without quorum must fail, not hang or pretend"
    );

    // And readiness follows: the survivor still calls itself leader, but a
    // leader no quorum has acknowledged in the bound is a leader in name
    // only, and the probe must take it out of rotation.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if nodes[survivor].store.health_check().await.is_err() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "readiness kept passing on a quorumless leader"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let _ = nodes[survivor].handle.shutdown().await;
    nodes[survivor].server.abort();
}
