//! The Raft backend against the same contracts every other backend passes.
//!
//! A single-member group needs no network, so these run as plain unit tests:
//! the node elects itself, and every write still travels the full path —
//! encode, propose, commit, apply, decode — that a three-member group uses.
use std::collections::BTreeMap;
use std::time::Duration;

use super::*;
use crate::raft::{AppStateMachine, RaftHandle, RaftSettings};
use crate::store::StoreConfig;

async fn single_node_store(dir: &std::path::Path) -> Arc<RaftStore> {
    let inner = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    }));
    let machine = Arc::new(MetadataStateMachine::new(inner));
    let mut settings = RaftSettings::new(1, dir.into());
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(150), Duration::from_millis(300));
    let handle = RaftHandle::start(settings, Arc::clone(&machine) as Arc<dyn AppStateMachine>)
        .await
        .expect("start raft");
    handle
        .initialize(BTreeMap::from([(1, "127.0.0.1:0".to_string())]))
        .await
        .expect("initialize single-member group");
    // A single member elects itself; writes block until then, so wait here
    // rather than in every contract step.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while handle.status().leader.is_none() {
        assert!(std::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Arc::new(RaftStore::new(handle, machine))
}

/// The same suite memory and Postgres run: the Raft backend is a fourth
/// implementation of the same contract, not a new contract.
#[tokio::test]
async fn satisfies_the_node_store_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::nodes::run_node_contract(store.clone()).await;
    crate::store::contract::nodes::run_node_concurrency_contract(store).await;
}

#[tokio::test]
async fn satisfies_the_shard_store_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::shards::run_shard_contract(store.clone()).await;
    crate::store::contract::shards::run_shard_concurrency_contract(store).await;
}

/// Writes travel the log; reads come from applied state — so a write
/// through the trait must be immediately visible to a read through the
/// trait on the same instance (the proposal only returns after apply).
#[tokio::test]
async fn a_write_is_readable_once_acknowledged() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;

    let created = store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("create");
    assert_eq!(created.tenant_id, "t1");

    let listed = store.list_tenants().await.expect("list");
    assert_eq!(listed.len(), 1);

    let conflict = store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Again".to_string(),
        })
        .await;
    assert!(
        matches!(conflict, Err(StoreError::Conflict(_))),
        "store errors survive the encode/decode round trip"
    );
}

/// `ensure` proposes install-if-absent, so racing it against itself — or
/// against an already-committed rotation — never clobbers keys.
#[tokio::test]
async fn ensure_signing_keys_never_overwrites() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("create");

    let first = store
        .ensure_signing_key_current("t1")
        .await
        .expect("ensure");
    let second = store
        .ensure_signing_key_current("t1")
        .await
        .expect("ensure");
    assert_eq!(
        first.current.kid, second.current.kid,
        "a second ensure returns the keys the first installed"
    );
}
