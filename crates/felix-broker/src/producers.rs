//! Producer sequences: what lets a re-sent batch land once.
//!
//! A producer numbers its batches on a shard from zero. The shard's leader
//! keeps, per producer, the next sequence it expects and the outcome of the
//! last few it appended. A batch carrying the expected sequence is appended
//! and remembered; one carrying a sequence already remembered is answered
//! from memory and not appended; anything else is refused with a reason. That
//! is the whole mechanism, and it is what turns "the acknowledgement never
//! arrived" from a duplicate-or-loss coin toss into a safe re-send.
//!
//! The state is the leader's and in memory. It survives everything but the
//! leader itself: a new leader knows no producers, answers `UnknownProducer`,
//! and the producer starts again under a new id rather than being told a
//! batch landed that nobody can vouch for. Persisting sequences through
//! replication is what would make a re-send safe across a failover too, and
//! is deliberately not attempted here.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::broker::PublishOutcome;
use crate::error::BrokerError;

/// Batches remembered per producer. A re-send arrives shortly after the
/// original, so the window covers a producer's in-flight pipeline rather
/// than its history.
pub(crate) const WINDOW: usize = 64;

/// Producers remembered per shard before the least recently used is let go.
/// Letting one go turns its next batch into `UnknownProducer`, which the
/// producer recovers from; keeping every producer that ever connected would
/// grow without bound.
pub(crate) const MAX_PRODUCERS: usize = 4096;

/// What to do with a batch, decided under the producer's turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sequenced {
    /// The next batch this producer owes: append it.
    Append,
    /// A batch already appended: answer with what happened the first time.
    Duplicate(PublishOutcome),
}

#[derive(Debug)]
struct Producer {
    next_sequence: u64,
    recent: VecDeque<(u64, PublishOutcome)>,
    last_used: u64,
    /// Serialises this producer's batches. Two re-sends of one sequence in
    /// flight at once must not both find it unappended.
    turn: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Default)]
struct Table {
    producers: HashMap<u64, Producer>,
    /// Advances on every use; the smallest value is the coldest producer.
    clock: u64,
}

/// The producers a shard's leader remembers.
#[derive(Debug, Default)]
pub(crate) struct ProducerTable {
    inner: Mutex<Table>,
}

impl ProducerTable {
    /// The producer's turn, creating the producer on its first batch.
    ///
    /// A producer this table has never seen may only start at sequence zero:
    /// anything later says batches came before, and there is nothing here to
    /// check them against.
    pub(crate) fn turn(
        &self,
        producer_id: u64,
        sequence: u64,
    ) -> Result<Arc<tokio::sync::Mutex<()>>, BrokerError> {
        let mut table = self.inner.lock();
        table.clock += 1;
        let clock = table.clock;
        if let Some(producer) = table.producers.get_mut(&producer_id) {
            producer.last_used = clock;
            return Ok(Arc::clone(&producer.turn));
        }
        if sequence != 0 {
            return Err(BrokerError::UnknownProducer { producer_id });
        }
        if table.producers.len() >= MAX_PRODUCERS
            && let Some(coldest) = table
                .producers
                .iter()
                .min_by_key(|(_, producer)| producer.last_used)
                .map(|(id, _)| *id)
        {
            table.producers.remove(&coldest);
        }
        let turn = Arc::new(tokio::sync::Mutex::new(()));
        table.producers.insert(
            producer_id,
            Producer {
                next_sequence: 0,
                recent: VecDeque::new(),
                last_used: clock,
                turn: Arc::clone(&turn),
            },
        );
        Ok(turn)
    }

    /// Classify a batch. Called holding the producer's turn, so the answer
    /// cannot change between deciding and acting on it.
    pub(crate) fn classify(
        &self,
        producer_id: u64,
        sequence: u64,
    ) -> Result<Sequenced, BrokerError> {
        let table = self.inner.lock();
        let Some(producer) = table.producers.get(&producer_id) else {
            // Evicted between taking the turn and using it. The producer was
            // cold enough to lose its place; starting it again from here
            // would accept a batch nothing can check.
            return Err(BrokerError::UnknownProducer { producer_id });
        };
        if sequence == producer.next_sequence {
            return Ok(Sequenced::Append);
        }
        if sequence > producer.next_sequence {
            return Err(BrokerError::SequenceGap {
                expected: producer.next_sequence,
            });
        }
        producer
            .recent
            .iter()
            .rev()
            .find(|(remembered, _)| *remembered == sequence)
            .map(|(_, outcome)| Sequenced::Duplicate(*outcome))
            .ok_or(BrokerError::SequenceExpired { sequence })
    }

    /// Record that `sequence` was appended with `outcome`, and expect the next.
    pub(crate) fn remember(&self, producer_id: u64, sequence: u64, outcome: PublishOutcome) {
        let mut table = self.inner.lock();
        if let Some(producer) = table.producers.get_mut(&producer_id) {
            producer.next_sequence = sequence + 1;
            if producer.recent.len() == WINDOW {
                producer.recent.pop_front();
            }
            producer.recent.push_back((sequence, outcome));
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().producers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(first: u64) -> PublishOutcome {
        PublishOutcome {
            subscribers: 0,
            offsets: Some((first, first)),
        }
    }

    #[test]
    fn a_new_producer_starts_at_zero_and_counts_up() {
        let table = ProducerTable::default();
        table.turn(7, 0).expect("first batch");
        assert_eq!(table.classify(7, 0).expect("classify"), Sequenced::Append);
        table.remember(7, 0, outcome(10));
        assert_eq!(table.classify(7, 1).expect("classify"), Sequenced::Append);
    }

    #[test]
    fn a_re_sent_batch_is_answered_from_memory() {
        let table = ProducerTable::default();
        table.turn(7, 0).expect("first batch");
        table.remember(7, 0, outcome(10));
        table.remember(7, 1, outcome(11));
        assert_eq!(
            table.classify(7, 0).expect("classify"),
            Sequenced::Duplicate(outcome(10))
        );
        assert_eq!(
            table.classify(7, 1).expect("classify"),
            Sequenced::Duplicate(outcome(11))
        );
    }

    #[test]
    fn a_gap_names_what_was_expected() {
        let table = ProducerTable::default();
        table.turn(7, 0).expect("first batch");
        table.remember(7, 0, outcome(10));
        match table.classify(7, 5) {
            Err(BrokerError::SequenceGap { expected }) => assert_eq!(expected, 1),
            other => panic!("expected a gap, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_producer_may_only_begin_at_zero() {
        let table = ProducerTable::default();
        match table.turn(9, 3) {
            Err(BrokerError::UnknownProducer { producer_id }) => assert_eq!(producer_id, 9),
            other => panic!("expected unknown producer, got {other:?}"),
        }
        assert_eq!(table.len(), 0, "a refused producer was remembered");
    }

    #[test]
    fn a_sequence_older_than_the_window_is_expired() {
        let table = ProducerTable::default();
        table.turn(7, 0).expect("first batch");
        for sequence in 0..(WINDOW as u64 + 1) {
            table.remember(7, sequence, outcome(sequence));
        }
        assert!(matches!(
            table.classify(7, 0),
            Err(BrokerError::SequenceExpired { sequence: 0 })
        ));
        assert_eq!(
            table.classify(7, 1).expect("classify"),
            Sequenced::Duplicate(outcome(1))
        );
    }

    #[test]
    fn the_coldest_producer_makes_room() {
        let table = ProducerTable::default();
        for producer in 0..MAX_PRODUCERS as u64 {
            table.turn(producer, 0).expect("turn");
        }
        // Touch producer 0 so it is no longer the coldest.
        table.turn(0, 0).expect("turn");
        table.turn(u64::MAX, 0).expect("one more");
        assert_eq!(table.len(), MAX_PRODUCERS);
        assert!(
            table.classify(0, 0).is_ok(),
            "the warm producer was evicted"
        );
        assert!(
            matches!(
                table.classify(1, 0),
                Err(BrokerError::UnknownProducer { .. })
            ),
            "the coldest producer survived"
        );
    }

    /// The turn is per producer: one producer's batches wait on each other,
    /// two producers' do not.
    #[tokio::test]
    async fn turns_are_per_producer() {
        let table = ProducerTable::default();
        let a = table.turn(1, 0).expect("turn");
        let a_again = table.turn(1, 1).expect("turn");
        let b = table.turn(2, 0).expect("turn");
        let held = a.lock().await;
        assert!(
            a_again.try_lock().is_err(),
            "the same producer got a second turn"
        );
        assert!(b.try_lock().is_ok(), "another producer waited");
        drop(held);
    }
}
