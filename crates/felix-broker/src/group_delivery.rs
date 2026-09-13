//! What a consumer group has handed out, and what it owes.
//!
//! The durable cursor in [`crate::consumer_groups`] says where a group has
//! finished. This is everything between there and the tail: offsets handed to a
//! consumer and not yet settled, offsets whose consumer stopped answering, and
//! the bookkeeping that turns acknowledgements into cursor movement.
//!
//! **Deliberately in memory.** A leader that dies loses what was in flight, and
//! the group resumes from its durable cursor — so those records are delivered
//! again. That is at-least-once, which is the guarantee a queue offers anyway;
//! persisting the in-flight set would buy a smaller redelivery window at the
//! cost of a write per delivery, and would still not make it exactly-once.
//!
//! Pure logic with the clock passed in, so every rule here is testable without
//! waiting for one.
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

/// What one `claim` produced.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Claim {
    /// Offsets handed to the caller, to deliver and then settle.
    pub offsets: Vec<u64>,
    /// Offsets given up on, having been delivered too many times. The caller
    /// records them and then settles them.
    pub dead_lettered: Vec<DeadLettered>,
}

/// One group's position on one shard.
#[derive(Debug)]
pub struct GroupTracker {
    /// Everything below this is acknowledged and will never be handed out
    /// again. Mirrors the durable cursor.
    committed: u64,
    /// The next offset never yet handed to anyone.
    high_water: u64,
    /// Handed out and unsettled, with the instant its claim lapses.
    in_flight: BTreeMap<u64, Instant>,
    /// Acknowledged, but above an offset that is not. Held until the run below
    /// them closes, because the cursor can only move over a contiguous prefix:
    /// advancing past a gap would drop a record nobody has finished.
    acked_ahead: BTreeSet<u64>,
    /// Owed again — nacked, or claimed by a consumer that stopped answering.
    redeliver: BTreeSet<u64>,
    /// How many times each unsettled offset has been handed out.
    ///
    /// Dropped as soon as an offset settles, so this holds only what is
    /// currently in play rather than growing with the log.
    attempts: BTreeMap<u64, u32>,
    /// Most times a record is handed out before it is given up on.
    ///
    /// Without a bound a record that always fails is redelivered for ever and
    /// the group never gets past it — one poison record stops the queue.
    max_attempts: u32,
}

/// A record the group has given up on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeadLettered {
    pub offset: u64,
    /// How many times it was handed out before being given up on.
    pub attempts: u32,
}

impl GroupTracker {
    /// A group resuming at `committed`, giving up on a record after
    /// `max_attempts` deliveries.
    pub fn new(committed: u64, max_attempts: u32) -> Self {
        Self {
            committed,
            high_water: committed,
            in_flight: BTreeMap::new(),
            acked_ahead: BTreeSet::new(),
            redeliver: BTreeSet::new(),
            attempts: BTreeMap::new(),
            max_attempts: max_attempts.max(1),
        }
    }

    /// How many times `offset` has been handed out, if it is still in play.
    pub fn attempts(&self, offset: u64) -> u32 {
        self.attempts.get(&offset).copied().unwrap_or(0)
    }

    /// Everything below this is finished.
    pub fn committed(&self) -> u64 {
        self.committed
    }

    /// How many offsets are handed out and unsettled.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Take up to `max` offsets to deliver, claimed until `now + visibility`.
    ///
    /// Owed offsets come before new ones. A group that always preferred new
    /// records would starve the redeliveries behind a fast producer, and those
    /// are precisely the records a consumer already failed to finish once.
    ///
    /// `tail` is the shard's log tail: nothing at or above it exists yet.
    pub fn claim(&mut self, tail: u64, max: usize, now: Instant, visibility: Duration) -> Claim {
        self.expire(now);

        let deadline = now + visibility;
        let mut claim = Claim {
            offsets: Vec::with_capacity(max.min(16)),
            dead_lettered: Vec::new(),
        };

        while claim.offsets.len() < max
            && let Some(offset) = self.redeliver.iter().next().copied()
        {
            self.redeliver.remove(&offset);
            let attempts = self.attempts.get(&offset).copied().unwrap_or(0);
            if attempts >= self.max_attempts {
                // Given up on rather than handed out again. Reported so the
                // caller can record it, and left unsettled here -- the caller
                // settles it once that record is durable, or a crash in between
                // would lose the fact that it was ever tried.
                claim.dead_lettered.push(DeadLettered { offset, attempts });
                continue;
            }
            self.hand_out(offset, deadline);
            claim.offsets.push(offset);
        }

        while claim.offsets.len() < max && self.high_water < tail {
            let offset = self.high_water;
            self.high_water += 1;
            self.hand_out(offset, deadline);
            claim.offsets.push(offset);
        }

        claim
    }

    fn hand_out(&mut self, offset: u64, deadline: Instant) {
        self.in_flight.insert(offset, deadline);
        *self.attempts.entry(offset).or_insert(0) += 1;
    }

    /// Settle one offset. Returns the new committed position if it moved.
    ///
    /// Acknowledging something already settled is not an error: a consumer that
    /// answered after its claim lapsed cannot tell the difference, and the
    /// record has since been handed to someone else who will answer too.
    pub fn ack(&mut self, offset: u64) -> Option<u64> {
        if offset < self.committed {
            // Below the cursor, so the run has already closed over it. That is
            // an ordinary duplicate — or a redriven record being finished, which
            // settles it without the cursor moving, since the cursor was never
            // waiting on it.
            self.in_flight.remove(&offset);
            self.redeliver.remove(&offset);
            self.attempts.remove(&offset);
            return None;
        }
        self.in_flight.remove(&offset);
        // No longer owed: it has been finished by whoever answered first.
        self.redeliver.remove(&offset);
        self.attempts.remove(&offset);
        self.acked_ahead.insert(offset);

        let before = self.committed;
        while self.acked_ahead.remove(&self.committed) {
            self.committed += 1;
        }
        // `high_water` can lag when a group is created above its acks.
        self.high_water = self.high_water.max(self.committed);
        (self.committed != before).then_some(self.committed)
    }

    /// Give one offset back without finishing it. It is owed again at once,
    /// rather than after the visibility timeout: the consumer has said it
    /// cannot do the work, so waiting only delays someone else trying.
    pub fn nack(&mut self, offset: u64) {
        if offset < self.committed || self.acked_ahead.contains(&offset) {
            return;
        }
        self.in_flight.remove(&offset);
        self.redeliver.insert(offset);
    }

    /// Put a record the group gave up on back in play, its attempt count reset.
    ///
    /// The cursor is *not* moved backwards. It has already passed this offset,
    /// and rewinding it would redeliver everything the group finished since.
    /// The record is owed again instead, which reaches the same consumer
    /// without disturbing anything else — a queue's order was never a promise,
    /// and a redriven record is the clearest case of that.
    ///
    /// Returns whether it was taken. A record at or above the cursor is refused:
    /// it has not been given up on, so it is either in play or owed already, and
    /// resetting its attempts would let it evade the bound for ever.
    pub fn redrive(&mut self, offset: u64) -> bool {
        if offset >= self.committed {
            return false;
        }
        self.attempts.remove(&offset);
        self.redeliver.insert(offset);
        true
    }

    /// Move claims that have lapsed back to owed.
    ///
    /// This is what makes a consumer that stopped answering recoverable rather
    /// than a permanent hole in the group's progress.
    pub fn expire(&mut self, now: Instant) {
        let lapsed: Vec<u64> = self
            .in_flight
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(offset, _)| *offset)
            .collect();
        for offset in lapsed {
            self.in_flight.remove(&offset);
            self.redeliver.insert(offset);
        }
    }
}

#[cfg(test)]
#[path = "group_delivery_tests.rs"]
mod tests;
