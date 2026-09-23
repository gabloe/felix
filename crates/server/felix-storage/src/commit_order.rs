// One authoritative apply order per log.
//
// A durable write has two halves separated by an `await`:
//
//   1. the durable append, which assigns disk offsets under the segment lock
//      and then waits for the fsync its policy requires, and
//   2. everything the rest of the system observes — the broker's replay ring
//      and fanout, or the cache's index and watch observer.
//
// Without coordination those halves can disagree. Two concurrent writes A and
// B take disk offsets in that order, both wait on the same group-commit flush,
// and then resume in whatever order the scheduler picks. B can apply first, so
// the log on disk reads A, B while every reader sees B, A. Under
// `FsyncMode::OnCommit` that window is a whole device flush wide —
// milliseconds — so it is not a theoretical race.
//
// The sequencer closes it. Each writer waits until every lower offset has been
// applied, then applies its own and releases the next in line. Disk order
// becomes the single source of truth for what readers observe.
//
// What this deliberately does *not* do is serialise the durable append itself.
// Offsets are still assigned concurrently and flushes are still shared, so
// group commit keeps its fan-in; only the cheap post-flush half is ordered.
// The broker's publish path and the cache's write path both rely on that.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::log::Offset;

/// Orders the post-durability half of writes by their disk offset.
pub struct CommitSequencer {
    state: Mutex<SequenceState>,
}

#[derive(Debug)]
struct SequenceState {
    /// Offset whose turn it is. Publishers wait until this reaches their first
    /// offset.
    next: Offset,
    /// Ranges that have resolved out of order, keyed by their first offset.
    ///
    /// A range resolves when its guard drops — whether it applied cleanly or
    /// was abandoned. Resolving is *not* the same as being next: a range behind
    /// an unfinished predecessor has to wait its turn to be counted, or a
    /// cancelled range would hand its successors permission to overtake the
    /// range still in flight ahead of them, and the replay ring would receive
    /// offsets out of order. Entries live here only until the prefix catches
    /// up, so this holds at most one entry per publish in flight.
    resolved: BTreeMap<Offset, Offset>,
    /// Bumped by every reset. A turn acquired before a reset carries
    /// pre-reset offsets, so releasing it must not move the sequence: the
    /// reset is authoritative about where the log now ends.
    generation: u64,
    /// One sender per parked waiter, keyed by the offset it is waiting for.
    ///
    /// This is what makes a release wake *the* waiter rather than all of them.
    /// Broadcasting cost every parked publisher a wake-up, a lock acquisition
    /// and a re-park per commit — so the work per commit grew with the number
    /// of publishers in flight, and throughput fell as concurrency rose.
    ///
    /// At most one waiter becomes eligible per release, which is what lets this
    /// be a single wake rather than a scan: `next` only advances past a range
    /// once that range's own waiter has finished with it, so the waiter at
    /// `next` is the only one whose condition can have just become true.
    waiters: BTreeMap<Offset, oneshot::Sender<()>>,
}

impl SequenceState {
    /// Record a resolved range and advance through whatever contiguous prefix
    /// that completes.
    fn resolve(&mut self, first_offset: Offset, next_offset: Offset) {
        if next_offset > self.next {
            self.resolved.insert(first_offset, next_offset);
        }
        // Walk forward only while the next range in line has resolved. A gap
        // stops the walk, which is exactly the range still in flight.
        while let Some(end) = self.resolved.remove(&self.next) {
            self.next = self.next.max(end);
        }
    }

    /// The waiter whose turn it now is, removed from the registry.
    ///
    /// Returned rather than fired here so the send happens outside the lock:
    /// waking a task while holding the mutex it is about to want is how a
    /// wake-up turns into a hand-off stall.
    fn take_ready(&mut self) -> Option<oneshot::Sender<()>> {
        // Every waiter at or below `next` is eligible. Normally that is one —
        // a range's successor cannot be eligible until the range resolves — but
        // a reset can move `next` arbitrarily, and a waiter left parked behind
        // one would never be released.
        let first = *self.waiters.keys().next()?;
        if first <= self.next {
            self.waiters.remove(&first)
        } else {
            None
        }
    }
}

impl fmt::Debug for CommitSequencer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock();
        f.debug_struct("CommitSequencer")
            .field("next", &state.next)
            .field("pending_resolutions", &state.resolved.len())
            .field("generation", &state.generation)
            .finish()
    }
}

impl CommitSequencer {
    pub fn new(next: Offset) -> Self {
        Self {
            state: Mutex::new(SequenceState {
                next,
                resolved: BTreeMap::new(),
                generation: 0,
                waiters: BTreeMap::new(),
            }),
        }
    }

    /// Restart the sequence at `next`, used when a stream adopts a recovered
    /// log and its offsets resume from the durable tail.
    pub fn reset(&self, next: Offset) {
        let orphaned: Vec<oneshot::Sender<()>> = {
            let mut state = self.state.lock();
            state.next = next;
            // Pending resolutions describe a sequence that no longer exists.
            state.resolved.clear();
            state.generation += 1;
            // Wake everyone: a waiter parked on an offset the reset just
            // discarded would otherwise never be released. Its `wait` sees the
            // generation has moved and returns.
            state.waiters.split_off(&0).into_values().collect()
        };
        for waiter in orphaned {
            let _ = waiter.send(());
        }
    }

    /// The offset whose turn it is — everything below it has applied.
    ///
    /// Also how a caller that appends outside the reserve path (recovery,
    /// compaction, replication shipping) tells whether writers are in flight:
    /// with nothing reserved and unresolved, this equals the log's next offset.
    pub fn next_offset(&self) -> Offset {
        self.state.lock().next
    }

    /// Claim the offset range `[first_offset, next_offset)` in the commit order.
    ///
    /// Returns immediately and does **not** wait for a turn — call
    /// [`CommitTurn::wait`] for that. The split matters: the guard must be
    /// created the moment offsets are consumed, because from then on the range
    /// exists on disk and every later offset is queued behind it. If the caller
    /// then fails, or its future is cancelled part-way through the durability
    /// wait, the guard's `Drop` still releases the range and the stream keeps
    /// moving. Claiming the range only *after* a successful append is what
    /// stranded the stream: an abandoned range never released, and every
    /// subsequent publish waited on a turn that could not arrive.
    pub fn reserve(&self, first_offset: Offset, next_offset: Offset) -> CommitTurn<'_> {
        CommitTurn {
            sequencer: SequencerRef::Borrowed(self),
            first_offset,
            next_offset,
            generation: self.state.lock().generation,
        }
    }

    /// [`CommitSequencer::reserve`], but the turn owns its sequencer and so has
    /// no lifetime tying it to this call.
    ///
    /// Same guarantees, including the one that matters: the range is claimed
    /// the moment offsets are consumed, and `Drop` releases it however the
    /// caller exits. The difference is only that the claim can be moved into
    /// another task, which is what lets durability waits overlap.
    pub fn reserve_owned(
        self: &Arc<Self>,
        first_offset: Offset,
        next_offset: Offset,
    ) -> CommitTurn<'static> {
        CommitTurn {
            sequencer: SequencerRef::Owned(Arc::clone(self)),
            first_offset,
            next_offset,
            generation: self.state.lock().generation,
        }
    }
}

/// How a turn holds the sequencer it will release into.
///
/// A borrowed turn is the common case and costs nothing. An owned one exists so
/// a claim can outlive the stack frame that made it -- which is what lets a
/// caller claim its offsets in order and then await the device flush
/// concurrently with other publishes, instead of holding the whole stream
/// behind one flush at a time (#535).
enum SequencerRef<'a> {
    Borrowed(&'a CommitSequencer),
    Owned(Arc<CommitSequencer>),
}

impl std::ops::Deref for SequencerRef<'_> {
    type Target = CommitSequencer;

    fn deref(&self) -> &CommitSequencer {
        match self {
            SequencerRef::Borrowed(sequencer) => sequencer,
            SequencerRef::Owned(sequencer) => sequencer,
        }
    }
}

/// A claim on one offset range. Releasing it lets the next range proceed.
pub struct CommitTurn<'a> {
    sequencer: SequencerRef<'a>,
    first_offset: Offset,
    next_offset: Offset,
    /// Generation this range was claimed in.
    generation: u64,
}

impl CommitTurn<'_> {
    /// Wait until every lower offset has been released.
    ///
    /// Cancellation-safe: dropping the guard mid-wait releases this range too,
    /// so a cancelled publisher cannot block the ones behind it.
    pub async fn wait(&self) {
        // Registered and tested under one lock, so a release landing between
        // the two cannot be missed: whoever resolves next either sees this
        // waiter in the registry and wakes it, or has already moved `next` past
        // it and the test below returns.
        let receiver = {
            let mut state = self.sequencer.state.lock();
            // A reset means the log was rewritten underneath this range, so
            // there is nothing left to wait for.
            if state.next >= self.first_offset || state.generation != self.generation {
                return;
            }
            let (sender, receiver) = oneshot::channel();
            state.waiters.insert(self.first_offset, sender);
            receiver
        };

        // A closed channel means the sender was dropped rather than fired —
        // only possible if this waiter's entry was replaced, which needs two
        // live turns on one offset. Returning is right either way: the caller
        // re-tests nothing, but its own `Drop` still resolves its range, so the
        // sequence cannot strand.
        let _ = receiver.await;
    }
}

impl Drop for CommitTurn<'_> {
    fn drop(&mut self) {
        let ready = {
            let mut state = self.sequencer.state.lock();
            // Dropped mid-wait: take this range's own registration with it, or
            // the entry outlives the waiter and a later release wakes nobody.
            state.waiters.remove(&self.first_offset);
            // A reset while this range was held means the log was rewritten
            // underneath it. Its offsets describe a sequence that no longer
            // exists, so resolving on them would step the stream past records
            // that are gone.
            if state.generation == self.generation {
                state.resolve(self.first_offset, self.next_offset);
            }
            state.take_ready()
        };
        // Outside the lock: the woken task wants this mutex immediately.
        if let Some(waiter) = ready {
            let _ = waiter.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn the_first_offset_proceeds_immediately() {
        let sequencer = CommitSequencer::new(0);
        let turn = sequencer.reserve(0, 1);
        turn.wait().await;
        drop(turn);
        assert_eq!(sequencer.next_offset(), 1);
    }

    #[tokio::test]
    async fn a_later_offset_waits_for_its_predecessor() {
        let sequencer = Arc::new(CommitSequencer::new(0));

        // Offset 5 cannot proceed while the sequence is still at 0.
        let waiter = {
            let sequencer = Arc::clone(&sequencer);
            tokio::spawn(async move {
                let turn = sequencer.reserve(5, 6);
                turn.wait().await;
                drop(turn);
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "offset 5 ran out of turn");

        // Releasing the intervening range lets it through.
        {
            let turn = sequencer.reserve(0, 5);
            turn.wait().await;
        }
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter should be released")
            .expect("join");
        assert_eq!(sequencer.next_offset(), 6);
    }

    #[tokio::test]
    async fn turns_are_granted_in_offset_order_regardless_of_arrival_order() {
        let sequencer = Arc::new(CommitSequencer::new(0));
        let observed = Arc::new(parking_lot::Mutex::new(Vec::new()));

        // Spawn the highest offset first so arrival order is the reverse of
        // offset order — exactly the interleaving the fsync wait can produce.
        let mut tasks = Vec::new();
        for offset in (0..8u64).rev() {
            let sequencer = Arc::clone(&sequencer);
            let observed = Arc::clone(&observed);
            tasks.push(tokio::spawn(async move {
                let turn = sequencer.reserve(offset, offset + 1);
                turn.wait().await;
                observed.lock().push(offset);
                drop(turn);
            }));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("no deadlock")
                .expect("join");
        }

        assert_eq!(*observed.lock(), (0..8).collect::<Vec<_>>());
    }

    /// **A crowd parks, then drains one wake-up at a time, in order.**
    ///
    /// Waking one waiter instead of all of them can fail two ways: wake the
    /// wrong one and the order breaks, wake nobody and the drain stops dead.
    ///
    /// Holding offset 0 is what makes the crowd real. Left alone the tasks run
    /// in spawn order and each finds its turn already current, so nothing parks
    /// and the test passes against any wake-up at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_parked_crowd_drains_in_offset_order() {
        const WAITERS: u64 = 64;
        let sequencer = Arc::new(CommitSequencer::new(0));
        let observed = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let head = sequencer.reserve(0, 1);
        head.wait().await;

        let mut tasks = Vec::new();
        for offset in 1..=WAITERS {
            let sequencer = Arc::clone(&sequencer);
            let observed = Arc::clone(&observed);
            tasks.push(tokio::spawn(async move {
                let turn = sequencer.reserve(offset, offset + 1);
                turn.wait().await;
                observed.lock().push(offset);
                drop(turn);
            }));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            observed.lock().is_empty(),
            "a turn was granted while offset 0 was still held"
        );

        drop(head);

        for task in tasks {
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .expect("the drain stopped: a waiter was never woken")
                .expect("join");
        }
        assert_eq!(*observed.lock(), (1..=WAITERS).collect::<Vec<_>>());
        assert_eq!(sequencer.next_offset(), WAITERS + 1);
    }

    /// The one case that still needs to wake everybody: a reset moves `next`
    /// somewhere unrelated, so waiters parked on offsets it discarded are not
    /// next in any order and would otherwise wait forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_reset_releases_a_whole_crowd() {
        let sequencer = Arc::new(CommitSequencer::new(0));
        let mut tasks = Vec::new();
        // Every one of these is behind offset 0, which nobody holds, so all of
        // them park.
        for offset in 1..32u64 {
            let sequencer = Arc::clone(&sequencer);
            tasks.push(tokio::spawn(async move {
                let turn = sequencer.reserve(offset, offset + 1);
                turn.wait().await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        sequencer.reset(1000);

        for task in tasks {
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .expect("a waiter was left parked behind a reset")
                .expect("join");
        }
    }

    #[tokio::test]
    async fn a_dropped_turn_never_strands_the_stream() {
        let sequencer = Arc::new(CommitSequencer::new(0));

        // A publisher that takes its turn and then fails still releases it.
        let result: std::result::Result<(), ()> = async {
            let turn = sequencer.reserve(0, 1);
            turn.wait().await;
            Err(())
        }
        .await;
        assert!(result.is_err());

        // The next publisher is not blocked by the failure.
        let next = sequencer.reserve(1, 2);
        tokio::time::timeout(Duration::from_secs(5), next.wait())
            .await
            .expect("must not stall");
    }

    #[tokio::test]
    async fn an_abandoned_range_releases_the_ranges_behind_it() {
        let sequencer = Arc::new(CommitSequencer::new(0));

        // A publisher claims offsets 0..3 and is then cancelled before it ever
        // gets its turn — the shape of a client disconnecting during the fsync
        // wait, after its records are already on disk holding those offsets.
        {
            let _abandoned = sequencer.reserve(0, 3);
        }

        // The next range must still be reachable. Before the claim moved to
        // assignment time, an abandoned range released nothing and every later
        // publish on the stream waited forever.
        let next = sequencer.reserve(3, 4);
        tokio::time::timeout(Duration::from_secs(5), next.wait())
            .await
            .expect("an abandoned range stranded the stream");
        assert_eq!(sequencer.next_offset(), 3);
    }

    #[tokio::test]
    async fn a_range_cancelled_mid_wait_releases_too() {
        let sequencer = Arc::new(CommitSequencer::new(0));
        let blocker = sequencer.reserve(0, 5);

        // Waits for a turn that has not arrived, then is cancelled.
        let waiter = {
            let sequencer = Arc::clone(&sequencer);
            tokio::spawn(async move {
                let turn = sequencer.reserve(5, 9);
                turn.wait().await;
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        waiter.abort();
        let _ = waiter.await;
        drop(blocker);

        // The cancelled range released on drop, so the one behind it proceeds.
        let after = sequencer.reserve(9, 10);
        tokio::time::timeout(Duration::from_secs(5), after.wait())
            .await
            .expect("a cancelled waiter stranded the stream");
    }

    #[tokio::test]
    async fn a_cancelled_range_does_not_let_later_ranges_overtake_an_unfinished_one() {
        let sequencer = Arc::new(CommitSequencer::new(0));

        // A is in flight and has not applied yet.
        let a = sequencer.reserve(0, 3);
        // B reserves behind it, then is cancelled while waiting.
        {
            let _b = sequencer.reserve(3, 6);
        }

        // C must still wait: A has not applied, so nothing at or after offset 3
        // may reach the replay ring yet. If the cancelled B advanced the
        // sequence straight to 6, C would overtake A and the disk order would
        // disagree with the cursor order.
        let c = sequencer.reserve(6, 9);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), c.wait())
                .await
                .is_err(),
            "offset 6 overtook an unfinished range at offset 0"
        );

        // Once A applies, the contiguous prefix resolves and C proceeds.
        drop(a);
        tokio::time::timeout(Duration::from_secs(5), c.wait())
            .await
            .expect("C should be released once A completes");
    }

    #[tokio::test]
    async fn a_reset_releases_waiters_stuck_behind_a_vanished_offset() {
        let sequencer = Arc::new(CommitSequencer::new(10));

        // After a truncation the tail moves backwards; a publisher waiting on
        // an offset that no longer exists must be released rather than hang.
        let waiter = {
            let sequencer = Arc::clone(&sequencer);
            tokio::spawn(async move {
                let turn = sequencer.reserve(20, 21);
                turn.wait().await;
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        sequencer.reset(20);
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("reset should release the waiter")
            .expect("join");
    }

    #[tokio::test]
    async fn a_stale_release_cannot_undo_a_reset() {
        let sequencer = CommitSequencer::new(100);
        let turn = sequencer.reserve(100, 101);
        turn.wait().await;
        // A truncation rewinds the log while the turn is held.
        sequencer.reset(5);
        drop(turn);
        assert_eq!(
            sequencer.next_offset(),
            5,
            "stale release moved the sequence"
        );
    }
}
