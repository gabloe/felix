//! Lease expiry, deterministically.
//!
//! `start_paused` gives tokio's clock to the test, so expiry is driven by
//! advancing time rather than by sleeping through it. That is what makes these
//! exact rather than approximate, and it is why `LeaseState` reads
//! `tokio::time::Instant` rather than `std::time::Instant`.
use super::*;

const LEASE: Duration = Duration::from_secs(4);

/// A quarter is held back, so a 4s lease is usable for 3s.
#[test]
fn a_quarter_of_the_lease_is_surrendered_as_margin() {
    let lease = LeaseState::new(LEASE);
    assert_eq!(lease.usable(), Duration::from_secs(3));
}

/// A broker has no authority until its first heartbeat is accepted.
///
/// Starting valid would let one that never successfully registered serve for a
/// full lease period.
#[tokio::test(start_paused = true)]
async fn a_lease_starts_invalid() {
    let lease = LeaseState::new(LEASE);
    assert!(!lease.looks_valid());
    assert!(!lease.is_valid_now());
}

#[tokio::test(start_paused = true)]
async fn a_renewed_lease_is_valid_until_its_usable_life_elapses() {
    let lease = LeaseState::new(LEASE);
    lease.renew();
    assert!(lease.is_valid_now());

    // Just inside.
    tokio::time::advance(Duration::from_millis(2_999)).await;
    assert!(lease.is_valid_now(), "still inside the usable window");

    // Just past.
    tokio::time::advance(Duration::from_millis(2)).await;
    assert!(!lease.is_valid_now(), "the usable window has elapsed");
}

/// The case the commit-boundary check exists for.
///
/// A broker suspended past its expiry wakes with the cached flag still saying
/// yes. Only the clock knows otherwise, and a write must not be committed on the
/// strength of the stale flag.
#[tokio::test(start_paused = true)]
async fn a_broker_suspended_past_expiry_fails_the_commit_check_while_admission_still_passes() {
    let lease = LeaseState::new(LEASE);
    lease.renew();

    // Nothing refreshes the cached flag: this is the suspension.
    tokio::time::advance(Duration::from_secs(10)).await;

    assert!(
        lease.looks_valid(),
        "the cached flag is stale by construction here -- that is the premise",
    );
    assert!(
        !lease.is_valid_now(),
        "the clock check must refuse, or a suspended broker writes after losing its lease",
    );
}

/// Renewal extends the lease, which is what a heartbeat is for.
#[tokio::test(start_paused = true)]
async fn renewing_extends_the_lease() {
    let lease = LeaseState::new(LEASE);
    lease.renew();

    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(2)).await;
        lease.renew();
        assert!(lease.is_valid_now(), "a renewed lease must not expire");
    }

    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(
        !lease.is_valid_now(),
        "renewal stopped, so the lease lapses"
    );
}

/// A late renewal must not resurrect an expired lease into the past.
#[tokio::test(start_paused = true)]
async fn renewal_never_moves_backwards() {
    let lease = LeaseState::new(LEASE);
    tokio::time::advance(Duration::from_secs(10)).await;
    lease.renew();
    let late = lease.is_valid_now();

    // A renewal recorded out of order -- two heartbeat responses racing -- must
    // not move the anchor backwards and shorten the lease.
    lease.renewed_at_millis.fetch_max(1, Ordering::Release);
    assert_eq!(
        lease.is_valid_now(),
        late,
        "an out-of-order renewal must not shorten the lease",
    );
}

/// Surrender is immediate, for a broker told it is no longer a member.
#[tokio::test(start_paused = true)]
async fn surrender_takes_effect_at_once() {
    let lease = LeaseState::new(LEASE);
    lease.renew();
    assert!(lease.is_valid_now());

    lease.surrender();
    assert!(!lease.looks_valid());
    assert!(
        !lease.is_valid_now(),
        "surrender must not wait out the margin"
    );
}

/// The refresh task moves the cached flag to false on its own, so the admission
/// path stops accepting without needing a publish to discover it.
#[tokio::test(start_paused = true)]
async fn the_refresh_task_invalidates_the_cached_flag() {
    let lease = Arc::new(LeaseState::new(LEASE));
    lease.renew();
    let shutdown = CancellationToken::new();
    let task = Arc::clone(&lease).spawn_refresh(shutdown.clone());

    tokio::time::advance(Duration::from_secs(4)).await;
    // Let the ticker run at the advanced time.
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(200)).await;
    tokio::task::yield_now().await;

    assert!(
        !lease.looks_valid(),
        "the admission path should stop accepting without waiting for a publish",
    );

    shutdown.cancel();
    let _ = task.await;
}
