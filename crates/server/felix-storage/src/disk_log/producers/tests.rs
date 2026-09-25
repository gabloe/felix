use super::*;

/// Observe a whole batch from `producer` at `first`, as an append would.
fn batch(state: &mut ProducerState, producer: u64, sequence: u64, first: Offset, len: usize) {
    for (offset, mark) in (first..).zip(RecordMark::for_batch(producer, sequence, len)) {
        state.observe(offset, mark);
    }
}

#[test]
fn a_held_batch_is_found_and_the_next_is_expected() {
    let mut state = ProducerState::default();
    batch(&mut state, 7, 0, 0, 2);
    batch(&mut state, 7, 1, 2, 1);

    assert_eq!(
        state.classify(7, 0),
        ProducerSequence::Held { first: 0, last: 1 }
    );
    assert_eq!(
        state.classify(7, 1),
        ProducerSequence::Held { first: 2, last: 2 }
    );
    assert_eq!(state.classify(7, 2), ProducerSequence::Next);
    assert_eq!(state.classify(7, 5), ProducerSequence::Gap { expected: 2 });
    assert_eq!(state.classify(8, 0), ProducerSequence::Unknown);
    assert_eq!(state.next_sequence(7), Some(2));
    assert_eq!(state.next_sequence(8), None);
}

#[test]
fn a_sequence_older_than_the_window_is_expired() {
    let mut state = ProducerState::default();
    for sequence in 0..(WINDOW as u64 + 3) {
        batch(&mut state, 1, sequence, sequence, 1);
    }
    assert_eq!(state.classify(1, 2), ProducerSequence::Expired);
    assert_eq!(
        state.classify(1, 3),
        ProducerSequence::Held { first: 3, last: 3 }
    );
}

#[test]
fn a_batch_is_held_only_once_every_record_is() {
    let mut state = ProducerState::default();
    let marks: Vec<_> = RecordMark::for_batch(3, 0, 3).collect();
    state.observe(10, marks[0]);
    state.observe(11, marks[1]);
    assert_eq!(
        state.classify(3, 0),
        ProducerSequence::Partial {
            first: 10,
            held: 2,
            len: 3
        }
    );
    assert!(state.is_open());

    state.observe(12, marks[2]);
    assert!(!state.is_open());
    assert_eq!(
        state.classify(3, 0),
        ProducerSequence::Held {
            first: 10,
            last: 12
        }
    );
}

/// A batch whose leader stopped partway is abandoned by whatever the log
/// holds next, and its sequence is owed again rather than half-held.
#[test]
fn a_record_that_does_not_continue_an_open_batch_abandons_it() {
    for next in [
        RecordMark::None,
        RecordMark::Opens(ProducerBatch {
            producer_id: 9,
            sequence: 0,
            len: 1,
        }),
    ] {
        let mut state = ProducerState::default();
        batch(&mut state, 3, 0, 0, 1);
        state.observe(1, RecordMark::for_batch(3, 1, 2).next().expect("first"));
        state.observe(2, next);
        assert!(!state.is_open());
        assert_eq!(
            state.classify(3, 1),
            ProducerSequence::Next,
            "after {next:?}"
        );
    }

    // Unmarked records are not observed while nothing is open, so the tail
    // moving past an open batch has to close it too.
    let mut state = ProducerState::default();
    state.observe(0, RecordMark::for_batch(3, 0, 2).next().expect("first"));
    state.settle(1);
    assert!(state.is_open(), "nothing has been written after it yet");
    state.settle(2);
    assert_eq!(state.classify(3, 0), ProducerSequence::Unknown);
}

#[test]
fn a_continuation_without_its_opening_record_is_ignored() {
    let mut state = ProducerState::default();
    state.observe(4, RecordMark::Continues);
    assert_eq!(state, ProducerState::default());
}

/// Retention decides how long a producer is remembered: once every batch it
/// wrote is gone, so is it, on every replica alike.
#[test]
fn a_producer_whose_batches_were_all_trimmed_is_forgotten() {
    let mut state = ProducerState::default();
    batch(&mut state, 1, 0, 0, 1);
    batch(&mut state, 2, 0, 1, 1);
    batch(&mut state, 2, 1, 2, 1);

    state.prune(2);
    assert_eq!(state.classify(1, 1), ProducerSequence::Unknown);
    assert_eq!(state.classify(2, 0), ProducerSequence::Expired);
    assert_eq!(
        state.classify(2, 1),
        ProducerSequence::Held { first: 2, last: 2 }
    );
    assert_eq!(state.classify(2, 2), ProducerSequence::Next);
}

#[test]
fn past_the_bound_the_producer_written_longest_ago_is_forgotten() {
    let mut state = ProducerState::default();
    for producer in 0..MAX_PRODUCERS as u64 {
        batch(&mut state, producer, 0, producer, 1);
    }
    // Producer 0 writes again, so producer 1 is now the coldest.
    batch(&mut state, 0, 1, MAX_PRODUCERS as u64, 1);
    batch(&mut state, u64::MAX, 0, MAX_PRODUCERS as u64 + 1, 1);

    assert_eq!(state.len(), MAX_PRODUCERS);
    assert_eq!(state.classify(1, 1), ProducerSequence::Unknown);
    assert_eq!(state.classify(0, 2), ProducerSequence::Next);
    assert_eq!(state.classify(u64::MAX, 1), ProducerSequence::Next);
}

#[test]
fn a_sequence_that_skips_restarts_the_window() {
    let mut state = ProducerState::default();
    batch(&mut state, 1, 0, 0, 1);
    batch(&mut state, 1, 5, 1, 1);
    assert_eq!(state.classify(1, 0), ProducerSequence::Expired);
    assert_eq!(
        state.classify(1, 5),
        ProducerSequence::Held { first: 1, last: 1 }
    );
}

#[test]
fn the_snapshot_round_trips_and_refuses_damage() {
    let mut state = ProducerState::default();
    batch(&mut state, 1, 0, 0, 2);
    batch(&mut state, 2, 4, 2, 1);
    state.observe(3, RecordMark::for_batch(1, 1, 3).next().expect("first"));

    let bytes = encode(&state, 4);
    assert_eq!(decode(&bytes), Some((state.clone(), 4)));

    let mut flipped = bytes.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 1;
    assert_eq!(decode(&flipped), None);
    assert_eq!(decode(&bytes[..bytes.len() - 1]), None);
    assert_eq!(decode(&[]), None);
}
