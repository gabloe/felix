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

/// A router where `leader` leads `shards` shards, each with `replicas` behind
/// it.
fn router_over_shards(
    leader: &str,
    replicas: &[&str],
    generation: u64,
    shards: u32,
) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    let table = RoutingTable::build(
        (0..shards).map(|shard| {
            (
                ShardKey { shard, ..key() },
                leader.to_string(),
                replicas.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
                generation,
            )
        }),
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

/// A broker leading `shards` shards, each with `per_shard` records on disk.
async fn leader_with_shards(shards: u32, per_shard: usize) -> (Arc<Broker>, TempDir) {
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
    for shard in 0..shards {
        let log = storage
            .open_stream(TENANT, NAMESPACE, STREAM, shard)
            .expect("open");
        for i in 0..per_shard {
            log.append(&[Bytes::from(format!("s{shard}-v{i}"))])
                .await
                .expect("append");
        }
    }
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    (Arc::new(broker), dir)
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

/// A leader with `count` records whose log says generation `generation` began
/// at `began_at` — what `DurableShardStore::open` records when this broker
/// takes the shard.
async fn leader_led_from(count: usize, generation: u64, began_at: u64) -> (Arc<Broker>, TempDir) {
    let (broker, dir) = leader_with(count).await;
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .record_generation(generation, began_at)
        .expect("record");
    (broker, dir)
}

/// **A failover does not re-ship the whole log.**
///
/// Cursors used to start at zero, so every follower of every shard the failed
/// broker led byte-compared the entire log — records it already held, read off
/// the leader's disk and pushed across the network, before anything new could
/// move. On a log of any size that is the difference between a failover and an
/// outage.
///
/// The generation history is the bound: below where this leadership began, both
/// logs came from the same predecessor. One record earlier than that, so the
/// boundary is compared rather than assumed.
#[tokio::test]
async fn a_fresh_cursor_starts_at_the_generation_boundary() {
    let (broker, _dir) = leader_led_from(20, 4, 12).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert_eq!(
        follower.batches().first().map(|(_, first, _)| *first),
        Some(11),
        "the follower was re-sent records from before this leadership began",
    );
}

/// Without a history there is nothing to bound the comparison with, so it
/// starts at zero — slow, and what every shard did before generations were
/// recorded. Worth pinning: the fallback is what keeps a shard written by an
/// older build replicating at all.
#[tokio::test]
async fn a_shard_with_no_generation_history_still_starts_at_zero() {
    let (broker, _dir) = leader_with(20).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert_eq!(
        follower.batches().first().map(|(_, first, _)| *first),
        Some(0),
    );
}

/// The boundary bounds a replica added mid-generation too. It is not assumed
/// caught up — it starts below where this leadership began, and a `LogGap`
/// still rewinds the leader if it turns out to hold less than that.
#[tokio::test]
async fn a_replica_added_later_starts_at_the_boundary_too() {
    let (broker, _dir) = leader_led_from(20, 4, 12).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    publish(&router, LOCAL, &["broker-b", "broker-c"], 4);
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let from_c: Vec<(String, u64, usize)> = follower
        .batches()
        .into_iter()
        .filter(|(node, _, _)| node == "broker-c")
        .collect();
    assert_eq!(
        from_c.first().map(|(_, first, _)| *first),
        Some(11),
        "a newly added replica was sent the whole log",
    );
}

#[test]
fn the_comparison_point_is_one_below_where_the_generation_began() {
    use felix_storage::disk_log::epochs::Epoch;
    let history = [
        Epoch {
            generation: 3,
            start_offset: 40,
        },
        Epoch {
            generation: 7,
            start_offset: 90,
        },
    ];

    assert_eq!(compare_from(&history, 7), 89);
    assert_eq!(compare_from(&history, 3), 39);
    // A generation this log never recorded cannot bound anything, and neither
    // can one that began at zero.
    assert_eq!(compare_from(&history, 8), 0);
    assert_eq!(compare_from(&[], 7), 0);
    assert_eq!(
        compare_from(
            &[Epoch {
                generation: 1,
                start_offset: 0
            }],
            1
        ),
        0,
    );
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
    replicate_once(
        &requester,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

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

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

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

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

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

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

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

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

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

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    let after_first = follower.batches().len();
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

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

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    let after_first = follower.batches().len();

    publish(&router, LOCAL, &["broker-b"], 5);
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

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

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    publish(&router, LOCAL, &["broker-b", "broker-c"], 4);
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

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

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
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
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

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
        replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut HashMap::new(),
        )
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
        replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut HashMap::new(),
        )
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

        let pass = replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut HashMap::new(),
        )
        .await;

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

        let pass = replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut HashMap::new(),
        )
        .await;

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

        let pass = replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut HashMap::new(),
        )
        .await;

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

        let pass = replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut HashMap::new(),
        )
        .await;

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

/// A quorum mark is not published when the replica report did not land.
///
/// The mark is what releases a `Quorum` publish, and promotion reads the
/// report. Releasing on a report that never arrived is the same window the
/// report-then-publish ordering exists to close, reached by a different route:
/// a client is told its record is on a majority while the control plane knows
/// nothing about which replica holds it.
#[tokio::test]
async fn a_failed_replica_report_holds_the_quorum_mark_back() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    // A control plane that refuses every report.
    let app = axum::Router::new().route(
        "/v1/nodes/{node_id}/replica-status",
        axum::routing::post(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });

    let report_to = ReportTo {
        client: reqwest::Client::new(),
        base_url: format!("http://{addr}"),
        node_id: LOCAL.to_string(),
        token: None,
        incarnation: 0,
    };

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        Some(&report_to),
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    // Replication itself succeeded — the follower took every record — so this
    // is specifically about the report, not about shipping.
    assert_eq!(
        follower.batches().len(),
        1,
        "the records should still have been shipped: {:?}",
        follower.batches(),
    );
    // TimedOut, not Reached: the publish waits and the client is told a
    // timeout, which is the honest answer when this broker cannot make the
    // acknowledgement good at failover.
    assert!(
        matches!(
            marks
                .wait_for(&watch_key(&key()), 4, 1, Duration::from_millis(50))
                .await,
            crate::replication::quorum::QuorumWait::TimedOut
                | crate::replication::quorum::QuorumWait::NotLeading
        ),
        "the mark was published on a report the control plane never took, so a \
         client would be told its record is on a majority the control plane \
         cannot find at failover",
    );

    server.abort();
}

/// The report a broker sends parses as the type the control plane reads.
///
/// That is the whole point of sharing the definition rather than building the
/// body with `json!`: a field renamed on one side used to arrive at the other
/// as a missing one, with nothing failing to compile and nothing failing at
/// runtime until promotion went looking for a replica it could not find.
#[test]
fn a_report_body_is_the_shape_the_control_plane_parses() {
    let sent = ReplicaStatusRequest {
        incarnation: 2,
        shards: vec![ShardReplicaStatus {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 3,
            kind: WireShardKind::Cache,
            generation: 9,
            caught_up: vec!["broker-b".to_string()],
            replica_offsets: vec![ReplicaOffset {
                node_id: "broker-b".to_string(),
                durable_offset: 41,
            }],
        }],
    };

    let json = serde_json::to_value(&sent).expect("serialise");
    // The field names the control plane's handler reads, spelled out rather
    // than derived, so a rename has to be made deliberately here too.
    let shard = &json["shards"][0];
    assert_eq!(json["incarnation"], 2);
    assert_eq!(shard["tenant_id"], "t1");
    assert_eq!(shard["namespace"], "ns");
    assert_eq!(shard["stream"], "orders");
    assert_eq!(shard["shard"], 3);
    assert_eq!(shard["kind"], "cache");
    assert_eq!(shard["generation"], 9);
    assert_eq!(shard["caught_up"][0], "broker-b");
    assert_eq!(shard["replica_offsets"][0]["node_id"], "broker-b");
    assert_eq!(shard["replica_offsets"][0]["durable_offset"], 41);

    assert_eq!(
        serde_json::from_value::<ReplicaStatusRequest>(json).expect("parse"),
        sent,
    );
}

/// A durable append starts shipping without waiting for the tick.
///
/// The tick is seconds and the shipping is milliseconds, so under `Quorum` the
/// tick was most of a publish's latency — a record landed, and then the broker
/// waited out an interval before telling anyone about it (#411).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_append_ships_without_waiting_for_the_tick() {
    let (broker, _dir) = leader_with(0).await;
    // Registered on the broker, not just opened on disk: the point is that a
    // real publish wakes replication, and `publish_batch` needs the stream.
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            felix_broker::StreamMetadata {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = Arc::new(AcceptingFollower::default());
    let marks = Arc::new(QuorumMarks::new());

    // A tick long enough that reaching it would mean the signal did nothing.
    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::clone(&follower),
        Arc::clone(&broker),
        router,
        Published {
            marks: Arc::clone(&marks),
            halted: Arc::new(crate::replication::halted::HaltedReplicas::new()),
        },
        None,
        Duration::from_secs(300),
        shutdown.clone(),
    );

    // The first pass runs on the interval's immediate first tick; let it
    // settle so what follows is attributable to the append.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = follower.batches().len();

    broker
        .publish_batch(TENANT, NAMESPACE, STREAM, 0, &[Bytes::from_static(b"now")])
        .await
        .expect("publish");

    let shipped = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if follower.batches().len() > before {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    shutdown.cancel();
    let _ = driver.await;

    assert!(
        shipped.is_ok(),
        "nothing shipped within 5s of the append, against a 300s tick: the \
         append signal is not reaching the driver, so a Quorum publish waits \
         out the interval",
    );
}

/// A slow follower costs one delay for the whole pass, not one per shard.
///
/// A follower is slow for *every* shard it holds, so shipping shards one after
/// another multiplied its latency by the shard count — and every one of those
/// shards' quorum marks, and so every `Quorum` publish waiting on them, sat
/// behind the sum. One tenant's unlucky replica became every tenant's latency
/// (#411).
struct SlowFollower {
    delay: Duration,
    seen: Mutex<Vec<u32>>,
}

impl PeerRequester for SlowFollower {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let InternalMessage::ReplicateRecords(batch) = message else {
            panic!("the driver sent something other than a replication batch");
        };
        tokio::time::sleep(self.delay).await;
        self.seen.lock().expect("lock").push(batch.shard.shard);
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

#[tokio::test(start_paused = true)]
async fn a_slow_follower_does_not_cost_one_delay_per_shard() {
    const SHARDS: u32 = 4;
    let (broker, _dir) = leader_with_shards(SHARDS, 2).await;
    let router = router_over_shards(LOCAL, &["broker-b"], 4, SHARDS);
    let delay = Duration::from_secs(30);
    let requester = SlowFollower {
        delay,
        seen: Mutex::new(Vec::new()),
    };
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    let started = tokio::time::Instant::now();
    replicate_once(
        &requester,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    let took = started.elapsed();

    let seen = requester.seen.lock().expect("lock").clone();
    let mut distinct: Vec<u32> = seen.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        SHARDS as usize,
        "not every shard shipped: {seen:?}",
    );

    // Each shard sends two batches here, so sequentially this is 8 delays.
    // Concurrently it is 2 — the batches within a shard are still ordered.
    assert!(
        took < delay * 4,
        "the pass took {took:?} with a {delay:?} follower across {SHARDS} \
         shards; the shards were shipped one after another",
    );
}

/// A follower whose log disagrees with the leader's, which is a halt.
struct DivergingFollower;

impl PeerRequester for DivergingFollower {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        Ok(InternalMessage::ReplicateError(
            felix_wire::internal::ReplicateError {
                correlation_id: message.correlation_id(),
                code: felix_wire::internal::ErrorCode::LogConflict,
                expected_offset: 0,
                detail: "diverged".to_string(),
            },
        ))
    }
}

/// **A halt says which replica, not just how many.**
///
/// The metric is a bare count and has to stay one — a label per shard is a
/// label per stream per tenant. So until now the only way to learn which
/// replica had stopped, on which shard, and why, was to grep the broker's logs
/// for the warning that accompanied the halt. A halt does not resolve on its
/// own, so that is the information an operator needs before they can act at
/// all (#424).
#[tokio::test]
async fn a_halted_follower_is_named_with_its_shard_and_reason() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let marks = QuorumMarks::new();

    let pass = replicate_once(
        &DivergingFollower,
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert_eq!(pass.halted.len(), 1);
    let halted = &pass.halted[0];
    assert_eq!(halted.node_id, "broker-b");
    assert_eq!(halted.stream, STREAM);
    assert_eq!(halted.tenant_id, TENANT);
    assert_eq!(halted.namespace, NAMESPACE);
    assert_eq!(halted.shard, 0);
    assert_eq!(halted.kind, "stream");
    assert_eq!(halted.generation, 4);
    assert_eq!(halted.reason, "diverged");
    assert!(
        halted.remedy.contains("discarded and rebuilt"),
        "the listing named a halt without saying what to do about it",
    );
}

/// A healthy pass lists nothing, so a non-empty listing is always news.
#[tokio::test]
async fn a_shipping_follower_is_not_listed_as_halted() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let marks = QuorumMarks::new();

    let pass = replicate_once(
        &AcceptingFollower::default(),
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert!(pass.halted.is_empty());
}
