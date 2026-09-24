//! Where a follower's cursor starts, and when it starts again.

use super::*;

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
    use felix_storage::log::Epoch;
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
