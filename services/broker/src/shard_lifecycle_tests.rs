//! Ownership transitions: what this broker serves, and when it stops.
use super::*;

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: crate::shard_watch::ShardKind::Stream,
    }
}

fn assigned_to(leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: key(0),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
    }
}

fn lifecycle() -> ShardLifecycle {
    ShardLifecycle::new("broker-a")
}

/// The rule the whole type exists for: an assignment arriving does not mean the
/// shard can be served. The local log has to be open first, or a write would be
/// acknowledged against durability that is not set up.
#[test]
fn an_assignment_does_not_serve_until_the_log_is_open() {
    let mut own = lifecycle();

    let action = own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    assert_eq!(
        action,
        Action::Open {
            key: key(0),
            generation: 3
        }
    );
    assert_eq!(own.phase(&key(0)), Phase::Opening);
    assert!(!own.may_serve(&key(0)), "opening must not serve");

    assert!(own.opened(&key(0), 3));
    assert_eq!(own.phase(&key(0)), Phase::Active);
    assert!(own.may_serve(&key(0)));
}

#[test]
fn a_shard_assigned_elsewhere_is_never_opened() {
    let mut own = lifecycle();
    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-b", 1))),
        Action::None
    );
    assert_eq!(own.phase(&key(0)), Phase::Unassigned);
    assert!(!own.may_serve(&key(0)));
}

/// Every poll re-delivers the same assignment. Reopening on each would churn the
/// log and drop service.
#[test]
fn repeating_the_same_assignment_does_nothing() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    own.opened(&key(0), 3);

    for _ in 0..5 {
        assert_eq!(
            own.observe(&key(0), Some(&assigned_to("broker-a", 3))),
            Action::None
        );
    }
    assert_eq!(own.phase(&key(0)), Phase::Active);
}

/// A generation below the one already acted on is a duplicate, a reordered
/// poll, or a snapshot replay -- never a reason to change state.
#[test]
fn an_older_generation_is_ignored() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 5)));
    own.opened(&key(0), 5);

    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 4))),
        Action::None
    );
    assert_eq!(own.generation(&key(0)), Some(5));
    assert!(own.may_serve_at(&key(0), 5));
    assert!(!own.may_serve_at(&key(0), 4));
}

/// The shard was moved away and back. The local state has to be re-established,
/// not assumed, or this broker serves a generation it does not hold.
#[test]
fn a_newer_generation_reopens() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.opened(&key(0), 1);

    let action = own.observe(&key(0), Some(&assigned_to("broker-a", 4)));
    assert_eq!(
        action,
        Action::Open {
            key: key(0),
            generation: 4
        }
    );
    assert_eq!(own.phase(&key(0)), Phase::Opening);
    assert!(!own.may_serve(&key(0)), "must not serve while reopening");
}

/// Losing a shard drains before closing, so in-flight work is accounted for.
#[test]
fn losing_a_shard_drains_before_closing() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    own.opened(&key(0), 2);

    let action = own.observe(&key(0), Some(&assigned_to("broker-b", 3)));
    assert_eq!(
        action,
        Action::Release {
            key: key(0),
            generation: 2
        }
    );
    assert_eq!(own.phase(&key(0)), Phase::Draining);
    assert!(
        !own.may_serve(&key(0)),
        "a draining shard takes no new writes"
    );

    own.released(&key(0), 2);
    assert_eq!(own.phase(&key(0)), Phase::Closed);
    assert!(!own.may_serve(&key(0)));
}

/// A deleted assignment has to release the shard too, or a broker keeps serving
/// something the cluster no longer believes exists.
#[test]
fn an_unassigned_shard_is_released() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    own.opened(&key(0), 2);

    let action = own.observe(&key(0), None);
    assert_eq!(
        action,
        Action::Release {
            key: key(0),
            generation: 2
        }
    );
    assert_eq!(own.phase(&key(0)), Phase::Draining);
}

#[test]
fn releasing_twice_is_harmless() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    own.opened(&key(0), 2);
    own.observe(&key(0), None);
    own.released(&key(0), 2);

    assert_eq!(own.observe(&key(0), None), Action::None);
    assert_eq!(own.phase(&key(0)), Phase::Closed);
}

/// A shard reassigned back to us after we released it opens again.
#[test]
fn a_closed_shard_can_be_reacquired() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    own.opened(&key(0), 2);
    own.observe(&key(0), None);
    own.released(&key(0), 2);

    let action = own.observe(&key(0), Some(&assigned_to("broker-a", 6)));
    assert_eq!(
        action,
        Action::Open {
            key: key(0),
            generation: 6
        }
    );
    assert!(own.opened(&key(0), 6));
    assert!(own.may_serve(&key(0)));
}

#[test]
fn a_failed_open_does_not_serve() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.open_failed(&key(0), 1);

    assert_eq!(own.phase(&key(0)), Phase::Failed);
    assert!(!own.may_serve(&key(0)));
    // Not retried on every poll: the same assignment repeating is not new
    // information, and a failure that logs each tick buries itself.
    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 1))),
        Action::None
    );
}

/// A changed assignment is the one thing that retries a failed open.
#[test]
fn a_new_generation_retries_a_failed_open() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.open_failed(&key(0), 1);

    let action = own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    assert_eq!(
        action,
        Action::Open {
            key: key(0),
            generation: 2
        }
    );
}

/// A failed open never served, so losing the shard closes it without a drain.
#[test]
fn a_failed_shard_closes_without_draining() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.open_failed(&key(0), 1);

    assert_eq!(own.observe(&key(0), None), Action::None);
    assert_eq!(own.phase(&key(0)), Phase::Closed);
}

/// The shard was reassigned while we were still recovering it. Activating now
/// would serve a generation we no longer hold.
#[test]
fn an_open_that_finishes_after_a_reassignment_does_not_activate() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    // Reassigned mid-open.
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));

    assert!(
        !own.opened(&key(0), 1),
        "the open for generation 1 must not activate generation 2",
    );
    assert_eq!(own.phase(&key(0)), Phase::Opening);
    assert!(!own.may_serve(&key(0)));

    assert!(own.opened(&key(0), 2));
    assert!(own.may_serve_at(&key(0), 2));
}

/// A snapshot replaces the whole picture, so a shard absent from it has to be
/// released as surely as one reassigned away.
#[test]
fn shards_absent_from_a_snapshot_are_reported() {
    let mut own = lifecycle();
    for shard in 0..3 {
        let mut assignment = assigned_to("broker-a", 1);
        assignment.key = key(shard);
        own.observe(&key(shard), Some(&assignment));
        own.opened(&key(shard), 1);
    }

    let still_present = [key(0), key(2)];
    let mut missing = own.missing_from(still_present.iter());
    missing.sort_by_key(|k| k.shard);
    assert_eq!(missing, vec![key(1)]);
}

#[test]
fn a_closed_shard_is_not_reported_missing_again() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.opened(&key(0), 1);
    own.observe(&key(0), None);
    own.released(&key(0), 1);

    assert!(own.missing_from(std::iter::empty()).is_empty());
}

#[test]
fn active_lists_only_servable_shards() {
    let mut own = lifecycle();
    let mut second = assigned_to("broker-a", 1);
    second.key = key(1);

    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.opened(&key(0), 1);
    own.observe(&key(1), Some(&second)); // left Opening

    let active: Vec<u32> = own.active().map(|k| k.shard).collect();
    assert_eq!(active, vec![0]);
}

#[test]
fn phase_counts_track_the_shards_held() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.opened(&key(0), 1);
    let mut second = assigned_to("broker-a", 1);
    second.key = key(1);
    own.observe(&key(1), Some(&second));

    let counts = own.counts();
    assert_eq!(counts.get(&Phase::Active).copied(), Some(1));
    assert_eq!(counts.get(&Phase::Opening).copied(), Some(1));
}

/// Driving the state machine against a store, including the paths that only
/// exist because opening and flushing can fail.
mod driver {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingStore {
        opened: StdMutex<Vec<ShardKey>>,
        released: StdMutex<Vec<ShardKey>>,
        fail_open: AtomicBool,
        fail_release: AtomicBool,
    }

    #[async_trait::async_trait]
    impl ShardStore for RecordingStore {
        async fn open(&self, key: &ShardKey) -> anyhow::Result<()> {
            if self.fail_open.load(Ordering::Acquire) {
                return Err(anyhow::anyhow!("injected open failure"));
            }
            self.opened.lock().expect("lock").push(key.clone());
            Ok(())
        }

        async fn release(&self, key: &ShardKey) -> anyhow::Result<()> {
            self.released.lock().expect("lock").push(key.clone());
            if self.fail_release.load(Ordering::Acquire) {
                return Err(anyhow::anyhow!("injected flush failure"));
            }
            Ok(())
        }
    }

    fn assignments(pairs: &[(u32, &str, u64)]) -> HashMap<ShardKey, ShardAssignment> {
        pairs
            .iter()
            .map(|(shard, leader, generation)| {
                let mut assignment = assigned_to(leader, *generation);
                assignment.key = key(*shard);
                (key(*shard), assignment)
            })
            .collect()
    }

    #[tokio::test]
    async fn reconcile_opens_only_our_shards() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();

        reconcile(
            &own,
            &store,
            &assignments(&[(0, "broker-a", 1), (1, "broker-b", 1), (2, "broker-a", 1)]),
        )
        .await;

        let mut opened: Vec<u32> = store
            .opened
            .lock()
            .expect("lock")
            .iter()
            .map(|k| k.shard)
            .collect();
        opened.sort_unstable();
        assert_eq!(opened, vec![0, 2]);

        let own = own.lock().await;
        assert!(own.may_serve(&key(0)));
        assert!(own.may_serve(&key(2)));
        assert!(!own.may_serve(&key(1)));
    }

    /// A pass over unchanged assignments must not reopen anything.
    #[tokio::test]
    async fn reconcile_is_idempotent() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();
        let current = assignments(&[(0, "broker-a", 1)]);

        for _ in 0..5 {
            reconcile(&own, &store, &current).await;
        }
        assert_eq!(store.opened.lock().expect("lock").len(), 1);
    }

    /// A shard that vanished from the assignment set is released, not left
    /// being served.
    #[tokio::test]
    async fn a_vanished_assignment_releases_the_shard() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();

        reconcile(&own, &store, &assignments(&[(0, "broker-a", 1)])).await;
        assert!(own.lock().await.may_serve(&key(0)));

        reconcile(&own, &store, &HashMap::new()).await;

        assert_eq!(store.released.lock().expect("lock").len(), 1);
        let own = own.lock().await;
        assert_eq!(own.phase(&key(0)), Phase::Closed);
        assert!(!own.may_serve(&key(0)));
    }

    #[tokio::test]
    async fn losing_a_shard_to_another_broker_releases_it() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();

        reconcile(&own, &store, &assignments(&[(0, "broker-a", 1)])).await;
        reconcile(&own, &store, &assignments(&[(0, "broker-b", 2)])).await;

        assert_eq!(store.released.lock().expect("lock").len(), 1);
        assert!(!own.lock().await.may_serve(&key(0)));
    }

    /// An open failure must leave the shard unserved and visible, not silently
    /// treated as owned.
    #[tokio::test]
    async fn a_failed_open_leaves_the_shard_unserved() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();
        store.fail_open.store(true, Ordering::Release);

        reconcile(&own, &store, &assignments(&[(0, "broker-a", 1)])).await;

        let own = own.lock().await;
        assert_eq!(own.phase(&key(0)), Phase::Failed);
        assert!(!own.may_serve(&key(0)));
    }

    #[tokio::test]
    async fn a_failed_open_recovers_on_a_new_generation() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();
        store.fail_open.store(true, Ordering::Release);
        reconcile(&own, &store, &assignments(&[(0, "broker-a", 1)])).await;

        store.fail_open.store(false, Ordering::Release);
        reconcile(&own, &store, &assignments(&[(0, "broker-a", 2)])).await;

        assert!(own.lock().await.may_serve(&key(0)));
    }

    /// Ownership has moved regardless of whether the flush worked. Continuing
    /// to serve would be worse than an unflushed tail, so the shard still
    /// closes -- loudly.
    #[tokio::test]
    async fn a_failed_flush_still_gives_up_the_shard() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();
        reconcile(&own, &store, &assignments(&[(0, "broker-a", 1)])).await;

        store.fail_release.store(true, Ordering::Release);
        reconcile(&own, &store, &assignments(&[(0, "broker-b", 2)])).await;

        let own = own.lock().await;
        assert_eq!(own.phase(&key(0)), Phase::Closed);
        assert!(!own.may_serve(&key(0)));
    }

    /// Restart: a broker starting empty rebuilds ownership from the assignments
    /// alone, opening every shard it owns before serving any of them.
    #[tokio::test]
    async fn a_restarted_broker_rebuilds_ownership_from_assignments() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();

        reconcile(
            &own,
            &store,
            &assignments(&[(0, "broker-a", 4), (1, "broker-a", 9), (2, "broker-b", 1)]),
        )
        .await;

        let own = own.lock().await;
        assert!(own.may_serve_at(&key(0), 4));
        assert!(own.may_serve_at(&key(1), 9));
        assert!(!own.may_serve(&key(2)));
        assert_eq!(own.active().count(), 2);
    }
}
