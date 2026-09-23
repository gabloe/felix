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
mod tests;
