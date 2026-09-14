//! The metadata Raft group, exercised as a group: election, replication,
//! restart with state, rebuild from snapshot, and learner-first growth.
//!
//! Each node here is the real thing — a `RaftHandle` over the redb store,
//! serving its RPCs on a real HTTP listener — with a toy key/value app
//! standing where the metadata state machine (#338) will stand.
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use controlplane::raft::{AppStateMachine, NodeId, RaftHandle, RaftSettings};

/// A deterministic key/value app: `set k v` returns the previous value.
#[derive(Default)]
struct KvApp {
    state: RwLock<BTreeMap<String, String>>,
}

impl KvApp {
    fn get(&self, key: &str) -> Option<String> {
        self.state.read().expect("state lock").get(key).cloned()
    }

    fn len(&self) -> usize {
        self.state.read().expect("state lock").len()
    }
}

impl AppStateMachine for KvApp {
    fn apply(&self, command: &[u8]) -> Vec<u8> {
        let (key, value): (String, String) =
            serde_json::from_slice(command).expect("decode command");
        let previous = self.state.write().expect("state lock").insert(key, value);
        previous.map(String::into_bytes).unwrap_or_default()
    }

    fn snapshot(&self) -> Vec<u8> {
        serde_json::to_vec(&*self.state.read().expect("state lock")).expect("encode snapshot")
    }

    fn restore(&self, snapshot: &[u8]) {
        let restored: BTreeMap<String, String> =
            serde_json::from_slice(snapshot).expect("decode snapshot");
        *self.state.write().expect("state lock") = restored;
    }
}

fn set(key: &str, value: &str) -> Vec<u8> {
    serde_json::to_vec(&(key, value)).expect("encode command")
}

struct TestNode {
    handle: RaftHandle,
    app: Arc<KvApp>,
    addr: SocketAddr,
    dir: PathBuf,
    server: tokio::task::JoinHandle<()>,
}

/// Election timings shrunk so a test failure is a failure, not a wait.
fn settings(id: NodeId, dir: PathBuf) -> RaftSettings {
    let mut settings = RaftSettings::new(id, dir);
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(200), Duration::from_millis(400));
    settings
}

async fn start_node(dir: PathBuf, settings: RaftSettings, addr: Option<SocketAddr>) -> TestNode {
    let app = Arc::new(KvApp::default());
    let handle = RaftHandle::start(settings, Arc::clone(&app) as Arc<dyn AppStateMachine>)
        .await
        .expect("start raft node");
    // A node restarting on its predecessor's address can race the aborted
    // server task still holding the socket; retry briefly rather than flake.
    let bind_addr = addr.unwrap_or("127.0.0.1:0".parse().expect("addr"));
    let listener = {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match tokio::net::TcpListener::bind(bind_addr).await {
                Ok(listener) => break listener,
                Err(err) if Instant::now() < deadline => {
                    let _ = err;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => panic!("bind rpc listener: {err}"),
            }
        }
    };
    let addr = listener.local_addr().expect("local addr");
    let router = handle.rpc_router();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    TestNode {
        handle,
        app,
        addr,
        dir,
        server,
    }
}

impl TestNode {
    async fn stop(&self) {
        let _ = self.handle.shutdown().await;
        self.server.abort();
    }
}

async fn wait_until(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The current leader's handle, waiting out an election if one is running.
async fn leader_of(nodes: &[&TestNode]) -> RaftHandle {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for node in nodes {
            let status = node.handle.status();
            if let Some(leader) = status.leader
                && let Some(found) = nodes.iter().find(|n| n.handle.status().id == leader)
            {
                return found.handle.clone();
            }
        }
        assert!(Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_single_node_group_serves_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = start_node(dir.path().into(), settings(1, dir.path().into()), None).await;

    node.handle
        .initialize(BTreeMap::from([(1, node.addr.to_string())]))
        .await
        .expect("initialize");
    leader_of(&[&node]).await;

    let previous = node.handle.write(set("k", "v1")).await.expect("write");
    assert!(previous.is_empty(), "first write has no previous value");
    let previous = node.handle.write(set("k", "v2")).await.expect("write");
    assert_eq!(
        previous, b"v1",
        "the response is the state machine's answer"
    );
    assert_eq!(node.app.get("k").as_deref(), Some("v2"));

    node.stop().await;
}

/// The acceptance test for the group itself: three members elect, writes
/// reach every state machine, and a member that restarts with its disk
/// rejoins and catches up.
#[tokio::test]
async fn a_three_node_group_replicates_and_survives_restart() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let id = (i + 1) as NodeId;
        nodes.push(start_node(dir.path().into(), settings(id, dir.path().into()), None).await);
    }

    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|node| (node.handle.status().id, node.addr.to_string()))
        .collect();
    nodes[0]
        .handle
        .initialize(members)
        .await
        .expect("initialize");

    let leader = leader_of(&nodes.iter().collect::<Vec<_>>()).await;
    for i in 0..5 {
        leader
            .write(set(&format!("k{i}"), "before"))
            .await
            .expect("write");
    }
    for node in &nodes {
        let app = Arc::clone(&node.app);
        wait_until(
            "all writes on every member",
            Duration::from_secs(10),
            move || app.len() == 5,
        )
        .await;
    }

    // A follower goes away; the group keeps serving without it.
    let leader_id = leader.status().id;
    let follower_index = nodes
        .iter()
        .position(|node| node.handle.status().id != leader_id)
        .expect("a follower exists");
    let follower_addr = nodes[follower_index].addr;
    let follower_dir = nodes[follower_index].dir.clone();
    let follower_id = nodes[follower_index].handle.status().id;
    nodes[follower_index].stop().await;

    for i in 5..8 {
        leader
            .write(set(&format!("k{i}"), "while-away"))
            .await
            .expect("write");
    }

    // Back on the same identity, address, and disk: a restart is a rejoin.
    let restarted = start_node(
        follower_dir.clone(),
        settings(follower_id, follower_dir),
        Some(follower_addr),
    )
    .await;
    let app = Arc::clone(&restarted.app);
    wait_until(
        "the restarted member catches up",
        Duration::from_secs(10),
        move || app.len() == 8,
    )
    .await;
    assert_eq!(restarted.app.get("k7").as_deref(), Some("while-away"));

    restarted.stop().await;
    for node in &nodes {
        node.stop().await;
    }
}

/// A member that lost its disk entirely is rebuilt by snapshot install —
/// the log behind the snapshot is purged, so there is no other way back.
#[tokio::test]
async fn a_wiped_member_is_rebuilt_by_snapshot() {
    let mut tuned = Vec::new();
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    for (i, dir) in dirs.iter().enumerate() {
        let id = (i + 1) as NodeId;
        let mut s = settings(id, dir.path().into());
        // Snapshot early and keep nothing behind it, so catching up from
        // scratch cannot quietly use the log instead.
        s.snapshot_logs_since_last = 5;
        s.logs_kept_behind_snapshot = 0;
        tuned.push(s);
    }
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(start_node(dir.path().into(), tuned[i].clone(), None).await);
    }

    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|node| (node.handle.status().id, node.addr.to_string()))
        .collect();
    nodes[0]
        .handle
        .initialize(members)
        .await
        .expect("initialize");
    let leader = leader_of(&nodes.iter().collect::<Vec<_>>()).await;

    for i in 0..20 {
        leader
            .write(set(&format!("k{i}"), "v"))
            .await
            .expect("write");
    }
    leader.trigger_snapshot().await.expect("snapshot");

    let leader_id = leader.status().id;
    let victim_index = nodes
        .iter()
        .position(|node| node.handle.status().id != leader_id)
        .expect("a follower exists");
    let victim_addr = nodes[victim_index].addr;
    let victim_id = nodes[victim_index].handle.status().id;
    nodes[victim_index].stop().await;

    // The disk is gone: a brand-new directory, same identity and address.
    let fresh = tempfile::tempdir().expect("tempdir");
    let mut fresh_settings = settings(victim_id, fresh.path().into());
    fresh_settings.snapshot_logs_since_last = 5;
    fresh_settings.logs_kept_behind_snapshot = 0;
    let rebuilt = start_node(fresh.path().into(), fresh_settings, Some(victim_addr)).await;

    let app = Arc::clone(&rebuilt.app);
    wait_until(
        "the wiped member is rebuilt",
        Duration::from_secs(15),
        move || app.len() == 20,
    )
    .await;
    assert_eq!(rebuilt.app.get("k19").as_deref(), Some("v"));

    rebuilt.stop().await;
    for node in &nodes {
        node.stop().await;
    }
}

/// Growing the group is learner-first: the newcomer holds the data before
/// it holds a vote, so a join never costs quorum.
#[tokio::test]
async fn a_learner_catches_up_and_then_votes() {
    let dir1 = tempfile::tempdir().expect("tempdir");
    let one = start_node(dir1.path().into(), settings(1, dir1.path().into()), None).await;
    one.handle
        .initialize(BTreeMap::from([(1, one.addr.to_string())]))
        .await
        .expect("initialize");
    leader_of(&[&one]).await;
    for i in 0..3 {
        one.handle
            .write(set(&format!("k{i}"), "v"))
            .await
            .expect("write");
    }

    let dir2 = tempfile::tempdir().expect("tempdir");
    let two = start_node(dir2.path().into(), settings(2, dir2.path().into()), None).await;

    // add_learner blocks until the learner's *log* matches; the state
    // machine applies on the next commit notification, so the data is
    // observed with a short wait rather than instantly.
    one.handle
        .add_learner(2, two.addr.to_string())
        .await
        .expect("add learner");
    assert!(
        !one.handle.status().voters.contains(&2),
        "a learner is not yet a voter"
    );
    let two_app = Arc::clone(&two.app);
    wait_until(
        "the learner holds the data",
        Duration::from_secs(5),
        move || two_app.len() == 3,
    )
    .await;

    one.handle.change_membership([1, 2]).await.expect("promote");
    wait_until(
        "the learner becomes a voter",
        Duration::from_secs(5),
        || one.handle.status().voters.contains(&2),
    )
    .await;

    one.stop().await;
    two.stop().await;
}
