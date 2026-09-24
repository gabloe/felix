//! Waking this instance's assignment long-polls when it writes an
//! assignment, instead of leaving them to their re-check.
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Wakes shared by this instance's API handlers and its placement loop.
///
/// A hint, never the only path: a long-poll still re-checks the store, which
/// is how it sees another instance's writes. A missed wake costs latency,
/// never a change.
#[derive(Debug)]
pub struct PlacementWakes {
    written: watch::Sender<u64>,
    closing: CancellationToken,
}

impl Default for PlacementWakes {
    fn default() -> Self {
        Self::new(CancellationToken::new())
    }
}

impl PlacementWakes {
    /// Long-polls waiting on these wakes return once `closing` fires, so a
    /// drain is not held open by requests that are only waiting.
    pub(crate) fn new(closing: CancellationToken) -> Self {
        Self {
            written: watch::Sender::new(0),
            closing,
        }
    }

    /// This instance wrote a shard assignment.
    pub(crate) fn assignment_written(&self) {
        self.written
            .send_modify(|count| *count = count.wrapping_add(1));
    }

    /// Resolves on each [`Self::assignment_written`] after this call.
    pub(crate) fn watch_assignments(&self) -> watch::Receiver<u64> {
        self.written.subscribe()
    }

    /// Fires when this instance stops serving.
    pub(crate) fn closing(&self) -> &CancellationToken {
        &self.closing
    }
}
