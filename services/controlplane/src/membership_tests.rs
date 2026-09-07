//! Expiry behaviour, driven at exact times rather than by waiting on a timer.
use super::*;
use crate::config::NodeLivenessConfig;
use crate::model::NodeLifecycle;
use crate::store::memory::InMemoryStore;
use crate::store::node_contract::node;
use crate::store::{ControlPlaneStore, StoreConfig};

const INTERVAL_MS: u64 = 1_000;
const TIMEOUT_MS: u64 = 3_000;
const T0: u64 = 1_700_000_000_000;

fn liveness() -> NodeLivenessConfig {
    NodeLivenessConfig {
        heartbeat_interval_ms: INTERVAL_MS,
        expiry_timeout_ms: TIMEOUT_MS,
        sweep_interval_ms: 500,
    }
}

async fn store_with_node() -> InMemoryStore {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    });
    let mut broker = node("broker-a", 7001);
    broker.status.last_heartbeat_at_millis = T0;
    store.register_node(broker).await.expect("register");
    store
}

#[tokio::test]
async fn a_broker_heartbeating_inside_the_window_stays_live() {
    let store = store_with_node().await;

    // Four intervals of healthy reporting, sweeping after each one.
    for beat in 1..=4 {
        let now = T0 + beat * INTERVAL_MS;
        store
            .record_node_heartbeat("broker-a", 0, now)
            .await
            .expect("beat");
        assert_eq!(expire_once(&store, &liveness(), now).await, 0);
    }

    let node = store.get_node("broker-a").await.expect("get");
    assert_eq!(node.status.lifecycle, NodeLifecycle::Live);
}

#[tokio::test]
async fn a_silent_broker_goes_down_once_the_timeout_elapses() {
    let store = store_with_node().await;

    // Exactly at the timeout is still inside the window.
    assert_eq!(expire_once(&store, &liveness(), T0 + TIMEOUT_MS).await, 0);
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Live,
    );

    assert_eq!(
        expire_once(&store, &liveness(), T0 + TIMEOUT_MS + 1).await,
        1
    );
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Down,
    );
}

/// Marking a node down is what placement watches for, so it has to reach the
/// changefeed like any other membership change.
#[tokio::test]
async fn expiry_publishes_a_change() {
    let store = store_with_node().await;
    let since = store.node_snapshot().await.expect("snapshot").next_seq;

    expire_once(&store, &liveness(), T0 + TIMEOUT_MS + 1).await;

    let changes = store.node_changes(since).await.expect("changes");
    assert_eq!(changes.items.len(), 1);
    let published = changes.items[0].node.as_ref().expect("body");
    assert_eq!(published.node_id, "broker-a");
    assert_eq!(published.status.lifecycle, NodeLifecycle::Down);
}

/// Two control-plane instances sweep the same database. Between them a node
/// must be marked down once and published once.
#[tokio::test]
async fn a_repeated_sweep_expires_a_node_once() {
    let store = store_with_node().await;
    let since = store.node_snapshot().await.expect("snapshot").next_seq;
    let now = T0 + TIMEOUT_MS + 1;

    assert_eq!(expire_once(&store, &liveness(), now).await, 1);
    for _ in 0..3 {
        assert_eq!(expire_once(&store, &liveness(), now).await, 0);
    }

    assert_eq!(
        store
            .node_changes(since)
            .await
            .expect("changes")
            .items
            .len(),
        1
    );
}

/// A draining broker is still serving, so losing it matters as much as losing a
/// live one.
#[tokio::test]
async fn a_draining_broker_also_expires() {
    let store = store_with_node().await;
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Draining)
        .await
        .expect("drain");

    assert_eq!(
        expire_once(&store, &liveness(), T0 + TIMEOUT_MS + 1).await,
        1
    );
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Down,
    );
}

/// A node already down is not swept again, so a permanently dead broker does
/// not publish a change on every tick forever.
#[tokio::test]
async fn a_departed_node_is_not_swept() {
    let store = store_with_node().await;
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Left)
        .await
        .expect("leave");

    assert_eq!(
        expire_once(&store, &liveness(), T0 + TIMEOUT_MS * 100).await,
        0
    );
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Left,
    );
}

/// Before `expiry_timeout_ms` has elapsed since the epoch, subtracting it would
/// wrap and expire every node in the cluster.
#[tokio::test]
async fn a_clock_near_the_epoch_expires_nothing() {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    });
    let mut broker = node("broker-a", 7001);
    broker.status.last_heartbeat_at_millis = 0;
    store.register_node(broker).await.expect("register");

    assert_eq!(expire_once(&store, &liveness(), 1).await, 0);
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Live,
    );
}
