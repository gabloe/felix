//! The rules a shared cursor has to keep, each one exercised on its own.
use super::*;

const VIS: Duration = Duration::from_secs(30);

fn at(base: Instant, secs: u64) -> Instant {
    base + Duration::from_secs(secs)
}

#[test]
fn a_new_group_hands_out_from_its_committed_position() {
    let now = Instant::now();
    let mut group = GroupTracker::new(5);

    assert_eq!(group.claim(9, 10, now, VIS), vec![5, 6, 7, 8]);
    assert_eq!(group.committed(), 5, "handing out settles nothing");
}

#[test]
fn nothing_at_or_above_the_tail_is_handed_out() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0);

    assert_eq!(group.claim(0, 10, now, VIS), Vec::<u64>::new());
    assert_eq!(group.claim(2, 10, now, VIS), vec![0, 1]);
}

/// **The queue property.** A record handed to one consumer is not handed to
/// another while the first still holds it.
#[test]
fn an_offset_in_flight_is_not_handed_out_again() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0);

    let first = group.claim(4, 2, now, VIS);
    let second = group.claim(4, 2, now, VIS);

    assert_eq!(first, vec![0, 1]);
    assert_eq!(second, vec![2, 3], "a second consumer got the same records");
    assert_eq!(group.claim(4, 10, now, VIS), Vec::<u64>::new());
}

/// **The other half of it.** A consumer that stops answering must not hold a
/// record for ever, or the group stops making progress at that offset.
#[test]
fn a_lapsed_claim_is_handed_out_again() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0);

    assert_eq!(group.claim(3, 10, base, VIS), vec![0, 1, 2]);
    // Still held while the claim stands.
    assert_eq!(group.claim(3, 10, at(base, 29), VIS), Vec::<u64>::new());
    // And owed again once it lapses.
    assert_eq!(group.claim(3, 10, at(base, 31), VIS), vec![0, 1, 2]);
}

#[test]
fn an_acknowledged_offset_is_never_handed_out_again() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0);

    group.claim(3, 10, base, VIS);
    group.ack(0);
    group.ack(1);
    group.ack(2);

    assert_eq!(group.claim(3, 10, at(base, 300), VIS), Vec::<u64>::new());
    assert_eq!(group.committed(), 3);
}

/// The cursor may only move over a contiguous run. Advancing past a gap would
/// mark a record finished that nobody has finished, and it would never be
/// handed out again.
#[test]
fn the_cursor_does_not_advance_over_a_gap() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0);
    group.claim(4, 10, now, VIS);

    assert_eq!(group.ack(1), None, "1 is acked but 0 is not");
    assert_eq!(group.ack(3), None);
    assert_eq!(group.committed(), 0);

    // Closing the run releases everything behind it at once.
    assert_eq!(group.ack(0), Some(2));
    assert_eq!(group.ack(2), Some(4));
}

/// A record acknowledged out of order is still finished — it must not come back
/// when the offsets below it are settled.
#[test]
fn an_out_of_order_ack_is_not_redelivered_when_the_gap_closes() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0);
    group.claim(3, 10, base, VIS);

    group.ack(2);
    // 0 and 1 lapse and come back; 2 must not.
    assert_eq!(group.claim(3, 10, at(base, 31), VIS), vec![0, 1]);
}

#[test]
fn a_nack_makes_a_record_owed_at_once() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0);
    group.claim(3, 10, now, VIS);

    group.nack(1);

    // Immediately, without waiting out the visibility timeout.
    assert_eq!(group.claim(3, 10, now, VIS), vec![1]);
}

#[test]
fn a_nack_after_an_ack_does_not_resurrect_the_record() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0);
    group.claim(2, 10, now, VIS);

    group.ack(1);
    group.nack(1);

    assert_eq!(group.claim(2, 10, now, VIS), Vec::<u64>::new());
}

/// Owed records go out before new ones. A group that preferred new work would
/// starve the redeliveries behind a fast producer — and those are exactly the
/// records a consumer already failed to finish once.
#[test]
fn owed_records_go_out_before_new_ones() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0);

    group.claim(2, 10, base, VIS);
    group.nack(0);
    group.nack(1);

    // The log has grown, but the owed records go first.
    assert_eq!(group.claim(10, 3, base, VIS), vec![0, 1, 2]);
}

/// A late acknowledgement, from a consumer whose claim lapsed and whose record
/// has since gone to someone else, is harmless. Both consumers answer; the
/// record is finished once.
#[test]
fn a_late_ack_after_redelivery_is_harmless() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0);

    group.claim(1, 10, base, VIS);
    let redelivered = group.claim(1, 10, at(base, 31), VIS);
    assert_eq!(redelivered, vec![0]);

    // The original consumer finally answers.
    assert_eq!(group.ack(0), Some(1));
    // And so does the second. Neither moves the cursor twice.
    assert_eq!(group.ack(0), None);
    assert_eq!(group.committed(), 1);
}

/// A claim can lapse before its consumer answers, leaving the record *owed*
/// rather than in flight. An acknowledgement arriving in that window still
/// finishes it: handing it out again would deliver a record someone has already
/// completed, which is the duplicate the visibility timeout is meant to bound,
/// not create.
#[test]
fn an_ack_while_a_record_is_owed_still_finishes_it() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0);

    group.claim(2, 10, base, VIS);
    // The claim lapses, so 0 and 1 are owed but not yet re-claimed.
    group.expire(at(base, 31));

    assert_eq!(group.ack(0), Some(1));
    assert_eq!(
        group.claim(2, 10, at(base, 31), VIS),
        vec![1],
        "an acknowledged record was handed out again",
    );
}

/// The same, reached by a nack rather than a lapse.
#[test]
fn an_ack_after_a_nack_finishes_the_record() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0);

    group.claim(2, 10, now, VIS);
    group.nack(0);
    assert_eq!(group.ack(0), Some(1));

    assert_eq!(group.claim(2, 10, now, VIS), Vec::<u64>::new());
}

#[test]
fn acking_below_the_cursor_is_ignored() {
    let now = Instant::now();
    let mut group = GroupTracker::new(10);

    assert_eq!(group.ack(3), None);
    assert_eq!(group.committed(), 10);
    assert_eq!(group.claim(12, 10, now, VIS), vec![10, 11]);
}

/// An acknowledgement for an offset that was never handed out.
///
/// Nothing in the broker produces one, but the tracker takes its input from a
/// client and must not be wrecked by it. If the cursor can move past offsets
/// that were never claimed, the next claim starts *below* the cursor and hands
/// out records the group has already finished.
#[test]
fn an_ack_for_an_unclaimed_offset_does_not_rewind_the_next_claim() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0);

    for offset in 0..5 {
        group.ack(offset);
    }
    assert_eq!(group.committed(), 5);

    let claimed = group.claim(8, 10, now, VIS);
    assert_eq!(
        claimed,
        vec![5, 6, 7],
        "the group handed out records below its own cursor",
    );
}

/// Nothing is lost or duplicated across a long run of claims, lapses, nacks and
/// acknowledgements: every offset ends up finished exactly once.
#[test]
fn every_offset_is_finished_exactly_once() {
    const TAIL: u64 = 200;
    let base = Instant::now();
    let mut group = GroupTracker::new(0);
    let mut finished: Vec<u64> = Vec::new();
    let mut clock = 0u64;

    while group.committed() < TAIL {
        clock += 7;
        let batch = group.claim(TAIL, 5, at(base, clock), VIS);
        assert!(!batch.is_empty(), "the group stopped making progress");
        for (i, offset) in batch.into_iter().enumerate() {
            match i % 3 {
                // Acknowledged.
                0 => {
                    group.ack(offset);
                    finished.push(offset);
                }
                // Handed back, to be redelivered.
                1 => group.nack(offset),
                // Abandoned: the claim will lapse.
                _ => {}
            }
        }
        // Let some claims lapse.
        clock += 31;
        group.expire(at(base, clock));
    }

    assert_eq!(group.committed(), TAIL);
    finished.sort_unstable();
    finished.dedup();
    assert_eq!(
        finished.len() as u64,
        TAIL,
        "some offset was never acknowledged, yet the cursor passed it",
    );
}
