use super::*;

#[test]
fn readiness_starts_ready_and_flips_once() {
    let readiness = Readiness::ready();
    assert!(readiness.is_ready());
    // The first call reports the previous state so a repeated signal is
    // distinguishable from the first one.
    assert!(readiness.begin_draining());
    assert!(!readiness.is_ready());
    assert!(!readiness.begin_draining());
}

#[test]
fn readiness_clones_share_state() {
    let readiness = Readiness::ready();
    let clone = readiness.clone();
    readiness.begin_draining();
    assert!(!clone.is_ready());
}

#[tokio::test(start_paused = true)]
async fn drain_records_only_the_subsystem_that_timed_out() {
    let mut budget = DrainBudget::new(Duration::from_millis(1000));
    assert!(budget.drain("fast", async {}).await);
    assert!(
        !budget
            .drain("slow", tokio::time::sleep(Duration::from_secs(60)))
            .await
    );
    assert_eq!(budget.unfinished(), ["slow"]);
}

#[tokio::test(start_paused = true)]
async fn budget_is_shared_across_subsystems_not_per_subsystem() {
    // Two subsystems that each hang must not consume a full deadline apiece.
    let mut budget = DrainBudget::new(Duration::from_millis(500));
    let started = Instant::now();
    budget
        .drain("first", tokio::time::sleep(Duration::from_secs(60)))
        .await;
    budget
        .drain("second", tokio::time::sleep(Duration::from_secs(60)))
        .await;
    assert!(started.elapsed() < Duration::from_millis(1000));
    assert_eq!(budget.unfinished(), ["first", "second"]);
}

#[tokio::test(start_paused = true)]
async fn exhausted_budget_records_without_awaiting() {
    let mut budget = DrainBudget::new(Duration::from_millis(10));
    budget
        .drain("first", tokio::time::sleep(Duration::from_secs(60)))
        .await;
    assert!(budget.remaining().is_zero());
    // Past the deadline nothing else is even polled.
    assert!(!budget.drain("second", std::future::pending()).await);
    assert_eq!(budget.unfinished(), ["first", "second"]);
}

// --- In-flight accounting ----------------------------------------------------

/// The count has to come back down, or a drain waits for requests that are long
/// finished and reports a forced termination that never happened.
#[test]
fn a_guard_releases_its_slot_when_dropped() {
    let in_flight = InFlight::new();
    assert_eq!(in_flight.current(), 0);

    {
        let _one = in_flight.enter();
        let _two = in_flight.enter();
        assert_eq!(in_flight.current(), 2);
    }

    assert_eq!(in_flight.current(), 0);
}

/// A handler that panics still releases its slot: the guard's `Drop` runs while
/// the stack unwinds. Without that, one panic leaks a slot for the life of the
/// process and every later drain waits out its full deadline.
#[test]
fn a_panicking_handler_does_not_leak_a_slot() {
    let in_flight = InFlight::new();
    let counted = in_flight.clone();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = counted.enter();
        panic!("handler blew up");
    }));

    assert!(result.is_err());
    assert_eq!(in_flight.current(), 0, "a panic leaked an in-flight slot");
}

/// Clones share one count, so a handler holding a clone of the state is counted
/// against the same total the drain reads.
#[test]
fn clones_share_one_count() {
    let in_flight = InFlight::new();
    let other = in_flight.clone();

    let _guard = other.enter();

    assert_eq!(in_flight.current(), 1);
}
