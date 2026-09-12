//! Which shards get shipped, and how long a cursor is believed.
//!
//! The engine's rules are tested next door; these are about the decisions
//! around it — leading versus following, a replica set that changes, and a
//! generation that moves.
use std::collections::HashMap as Map;
use std::net::SocketAddr;
use std::sync::Mutex;

use bytes::Bytes;
use felix_broker::{Broker, DurableStorage};
use felix_router::{NodeRef, RegionRouter, RoutingTable};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{InternalMessage, ReplicateOk, ReplicateRecords};
use tempfile::TempDir;

use super::*;
use crate::peer::PeerError;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const LOCAL: &str = "broker-a";

/// A follower that stores everything and records which shard it was for.
#[derive(Default)]
struct AcceptingFollower {
    sent: Mutex<Vec<(String, ReplicateRecords)>>,
}

impl AcceptingFollower {
    fn batches(&self) -> Vec<(String, u64, usize)> {
        self.sent
            .lock()
            .expect("lock")
            .iter()
            .map(|(node, batch)| (node.clone(), batch.first_offset, batch.payloads.len()))
            .collect()
    }
}

impl PeerRequester for AcceptingFollower {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let InternalMessage::ReplicateRecords(batch) = message else {
            panic!("the driver sent something other than a replication batch");
        };
        let durable_offset = batch.first_offset + batch.payloads.len() as u64;
        self.sent
            .lock()
            .expect("lock")
            .push((node_id.to_string(), batch));
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset,
        }))
    }
}

/// A follower that is gone: it answers nothing, and the dial gives up on its
/// own after `handshake` rather than hanging forever, as the pool's handshake
/// timeout makes it.
struct UnreachableFollowers {
    handshake: std::time::Duration,
}

impl PeerRequester for UnreachableFollowers {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        _message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        tokio::time::sleep(self.handshake).await;
        Err(PeerError::Unavailable {
            node_id: node_id.to_string(),
            detail: "no answer within the handshake timeout".to_string(),
        })
    }
}

fn node(node_id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: format!("10.0.0.1:{port}").parse().expect("addr"),
        region: "us-west-2".to_string(),
        live: true,
    }
}

fn key() -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: felix_router::ShardKind::Stream,
    }
}

fn nodes() -> Map<String, NodeRef> {
    ["broker-a", "broker-b", "broker-c"]
        .into_iter()
        .enumerate()
        .map(|(i, id)| (id.to_string(), node(id, 7001 + i as u16)))
        .collect()
}

/// A router where `leader` leads the shard with `replicas` behind it.
fn router(leader: &str, replicas: &[&str], generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    publish(&router, leader, replicas, generation);
    router
}

fn publish(router: &ShardRouter, leader: &str, replicas: &[&str], generation: u64) {
    let nodes = nodes();
    let table = RoutingTable::build(
        [(
            key(),
            leader.to_string(),
            replicas.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
            generation,
        )],
        &nodes,
    );
    router.publish(table, &nodes);
}

/// A broker leading the shard, with `count` records already on disk.
async fn leader_with(count: usize) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    for i in 0..count {
        log.append(&[Bytes::from(format!("v{i}"))])
            .await
            .expect("append");
    }
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    (Arc::new(broker), dir)
}

/// **Unreachable followers cost one handshake timeout, not one each.** A pass
/// that visited them one after another paid for every dead replica in turn,
/// with the quorum mark -- and so every `Quorum` publish waiting on this shard
/// -- behind the sum.
///
/// The clock is the assertion: visited sequentially, two unreachable followers
/// take twice as long as one, and the pass still finishes, just far too late to
/// be worth anything to a publish.
#[tokio::test(start_paused = true)]
async fn unreachable_followers_are_waited_on_at_the_same_time() {
    let handshake = std::time::Duration::from_secs(2);
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let requester = UnreachableFollowers { handshake };
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    let started = tokio::time::Instant::now();
    replicate_once(&requester, &broker, &router, &marks, None, &mut cursors).await;

    let took = started.elapsed();
    assert!(
        took < handshake * 2,
        "the followers were waited on one after another: {took:?}",
    );
}

/// The shard this broker leads is shipped to every follower in its set.
#[tokio::test]
async fn a_led_shard_is_shipped_to_each_follower() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

    let mut shipped_to: Vec<String> = follower
        .batches()
        .into_iter()
        .map(|(node, _, _)| node)
        .collect();
    shipped_to.sort();
    assert_eq!(shipped_to, vec!["broker-b", "broker-c"]);
}

/// **A shard this broker only follows is not shipped from here.** Two brokers
/// both shipping the same shard would each be writing the other's log.
#[tokio::test]
async fn a_shard_led_elsewhere_is_not_shipped() {
    let (broker, _dir) = leader_with(3).await;
    let router = router("broker-b", &[LOCAL], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

    assert!(
        follower.batches().is_empty(),
        "a follower shipped a shard it does not lead",
    );
}

/// A shard with no replicas has nowhere to ship, which is the unreplicated
/// default and must cost nothing.
#[tokio::test]
async fn a_shard_with_no_replicas_ships_nothing() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &[], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

    assert!(follower.batches().is_empty());
}

/// A pass ships until the follower is level, so catching up is not rationed to
/// one batch per tick.
#[tokio::test]
async fn one_pass_ships_until_the_follower_is_level() {
    let (broker, _dir) = leader_with(5).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

    let delivered: usize = follower.batches().iter().map(|(_, _, len)| len).sum();
    assert_eq!(delivered, 5, "the follower was left behind after a pass");
}

/// A second pass with nothing new ships nothing: the cursor is remembered.
#[tokio::test]
async fn a_second_pass_with_nothing_new_ships_nothing() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;
    let after_first = follower.batches().len();
    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

    assert_eq!(
        follower.batches().len(),
        after_first,
        "the cursor was forgotten between passes",
    );
}

/// **A new generation discards the cursors.** A cursor is a belief about where
/// a follower stood under a particular leadership; a new one invalidates the
/// belief, not the follower.
#[tokio::test]
async fn a_new_generation_starts_the_cursors_again() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;
    let after_first = follower.batches().len();

    publish(&router, LOCAL, &["broker-b"], 5);
    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

    assert!(
        follower.batches().len() > after_first,
        "a new generation reused the old generation's cursor",
    );
    let (_, batch) = &follower.sent.lock().expect("lock")[after_first];
    assert_eq!(
        batch.shard.generation, 5,
        "the old epoch was still being sent"
    );
}

/// A replica added to the set starts from zero rather than from the tail. The
/// leader does not know what it holds, and starting at the tail would declare
/// it caught up while it held nothing.
#[tokio::test]
async fn a_replica_added_later_starts_from_the_beginning() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;
    publish(&router, LOCAL, &["broker-b", "broker-c"], 4);
    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

    let from_c: Vec<(String, u64, usize)> = follower
        .batches()
        .into_iter()
        .filter(|(node, _, _)| node == "broker-c")
        .collect();
    assert_eq!(
        from_c.first().map(|(_, first, _)| *first),
        Some(0),
        "a newly added replica was assumed to be caught up",
    );
}

/// A replica dropped from the set stops being shipped to.
#[tokio::test]
async fn a_replica_removed_from_the_set_is_dropped() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;
    publish(&router, LOCAL, &["broker-b"], 4);
    // Something new to ship, so a still-registered follower would show up.
    let storage = broker.durable_storage().expect("storage");
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    log.append(&[Bytes::from_static(b"more")])
        .await
        .expect("append");

    let before = follower.batches().len();
    replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

    let after: Vec<String> = follower
        .batches()
        .into_iter()
        .skip(before)
        .map(|(node, _, _)| node)
        .collect();
    assert!(
        !after.contains(&"broker-c".to_string()),
        "a replica removed from the set was still shipped to",
    );
}

/// A broker with no durable storage leads only ephemeral streams, which have no
/// log to ship.
#[tokio::test]
async fn a_broker_without_durable_storage_ships_nothing() {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    assert_eq!(
        replicate_once(&follower, &broker, &router, &marks, None, &mut cursors)
            .await
            .worst_lag,
        None,
    );
    assert!(follower.batches().is_empty());
}

/// The lag reported is the distance the slowest follower is behind the tail.
#[tokio::test]
async fn the_reported_lag_is_the_distance_from_the_tail() {
    let (broker, _dir) = leader_with(4).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    // Caught up after a full pass.
    assert_eq!(
        replicate_once(&follower, &broker, &router, &marks, None, &mut cursors)
            .await
            .worst_lag,
        Some(0),
    );
}

/// What the leader tells the control plane about its replicas.
mod reports {
    use super::*;

    /// A pass reports the shard, its generation, and who could take it over.
    #[tokio::test]
    async fn a_pass_reports_who_could_take_the_shard_over() {
        let (broker, _dir) = leader_with(3).await;
        let router = router(LOCAL, &["broker-b"], 4);
        let follower = AcceptingFollower::default();
        let marks = QuorumMarks::new();
        let mut cursors = HashMap::new();

        let pass = replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

        assert_eq!(pass.reports.len(), 1);
        let report = &pass.reports[0];
        assert_eq!(report.key, key());
        assert_eq!(report.generation, 4);
        assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
    }

    /// **A follower that did not keep up is not reported as able to lead.**
    /// This is the whole point of the signal: the control plane promotes on it,
    /// and a follower named here while behind would be promoted into a shard it
    /// cannot serve.
    #[tokio::test]
    async fn a_follower_that_refused_is_not_reported_as_able_to_lead() {
        let (broker, _dir) = leader_with(3).await;
        let router = router(LOCAL, &["broker-b"], 4);
        let follower = RefusingFollower;
        let marks = QuorumMarks::new();
        let mut cursors = HashMap::new();

        let pass = replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

        assert_eq!(pass.reports.len(), 1);
        assert!(
            pass.reports[0].caught_up.is_empty(),
            "a follower holding nothing was reported as able to lead",
        );
    }

    /// A shard this broker only follows is not reported on. Reporting about a
    /// shard it does not lead would be an opinion it has no basis for.
    #[tokio::test]
    async fn a_shard_led_elsewhere_is_not_reported() {
        let (broker, _dir) = leader_with(3).await;
        let router = router("broker-b", &[LOCAL], 4);
        let follower = AcceptingFollower::default();
        let marks = QuorumMarks::new();
        let mut cursors = HashMap::new();

        let pass = replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

        assert!(pass.reports.is_empty());
    }

    /// A shard with no replicas reports nobody rather than an empty promise.
    #[tokio::test]
    async fn a_shard_with_no_replicas_is_not_reported() {
        let (broker, _dir) = leader_with(3).await;
        let router = router(LOCAL, &[], 4);
        let follower = AcceptingFollower::default();
        let marks = QuorumMarks::new();
        let mut cursors = HashMap::new();

        let pass = replicate_once(&follower, &broker, &router, &marks, None, &mut cursors).await;

        assert!(pass.reports.is_empty());
    }
}

/// A follower that refuses everything, so it never advances.
struct RefusingFollower;

impl PeerRequester for RefusingFollower {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        _message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        Err(PeerError::Unavailable {
            node_id: "broker-b".to_string(),
            detail: "not now".to_string(),
        })
    }
}
