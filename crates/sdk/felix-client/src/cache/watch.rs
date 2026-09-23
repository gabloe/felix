//! Client half of a cache watch: the handle an application receives changes on.
//!
//! Establishment happens in [`crate::Client::watch_cache`]; this module owns
//! the read pump that turns the watch's uni stream into typed items, and the
//! types an application consumes.

use bytes::{Bytes, BytesMut};
use quinn::RecvStream;
use tokio::sync::mpsc;

use crate::frame_io::read_message_with_limit;
use felix_wire::Message;

/// A live cache watch. Dropping it ends the watch.
#[derive(Debug)]
pub struct CacheWatch {
    items: mpsc::Receiver<CacheWatchItem>,
    resume_offset: u64,
    resnapshot: bool,
    retained_count: Option<u64>,
    task: tokio::task::JoinHandle<()>,
}

impl CacheWatch {
    pub(crate) fn spawn_pump(
        recv: RecvStream,
        resume_offset: u64,
        resnapshot: bool,
        retained_count: Option<u64>,
        queue_capacity: usize,
        max_frame_bytes: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(queue_capacity.max(1));
        let task = tokio::spawn(run_watch_pump(recv, tx, max_frame_bytes));
        Self {
            items: rx,
            resume_offset,
            resnapshot,
            retained_count,
            task,
        }
    }

    /// The next item, or `None` once the watch has ended — the connection
    /// closed, or the broker ended it (a [`CacheWatchItem::Lagged`] is
    /// delivered first when it ended by falling behind).
    pub async fn recv(&mut self) -> Option<CacheWatchItem> {
        self.items.recv().await
    }

    /// The offset live delivery began at. Everything the broker delivered
    /// before it — replay or snapshot — was already reflected there.
    pub fn resume_offset(&self) -> u64 {
        self.resume_offset
    }

    /// True when the requested `from_offset` predated what compaction kept, so
    /// the watch began with each matching key's *current* value rather than
    /// the collapsed history.
    pub fn resnapshot(&self) -> bool {
        self.resnapshot
    }

    /// How many retained values precede live delivery, when this watch asked
    /// for retained delivery ([`crate::Client::watch_cache_retained`]).
    ///
    /// `Some(0)` is the defined "no retained value" answer: the key or prefix
    /// held nothing at join, which is not the same as a slow key. After this
    /// many changes have arrived, the watch holds the current state and
    /// everything further is live — equivalently, a change with
    /// `offset >= resume_offset()` is live rather than retained.
    pub fn retained_count(&self) -> Option<u64> {
        self.retained_count
    }
}

impl Drop for CacheWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Which changes a watch asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheWatchFilter {
    /// Exactly this key.
    Key(String),
    /// Every key beginning with this prefix; `""` is every key in the shard.
    Prefix(String),
}

/// One change delivered on a watch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheChange {
    pub key: String,
    /// The value the key now holds; `None` means the key was deleted.
    pub value: Option<Bytes>,
    /// The cache-log offset of the change. Checkpoint `offset + 1` to resume.
    ///
    /// Offsets are naturally sparse on a filtered watch — other keys' changes
    /// consume them — so a gap here is not a drop signal;
    /// [`CacheWatchItem::Lagged`] is.
    pub offset: u64,
    /// Absolute Unix milliseconds this value expires at; `0` means never.
    pub expires_at_millis: u64,
}

/// What a watch yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheWatchItem {
    Change(CacheChange),
    /// The watch fell behind and the broker ended it after this. Re-watching
    /// with `from_offset = resume_from` is gapless.
    Lagged {
        resume_from: u64,
    },
}

async fn run_watch_pump(
    mut recv: RecvStream,
    tx: mpsc::Sender<CacheWatchItem>,
    max_frame_bytes: usize,
) {
    let mut frame_scratch = BytesMut::with_capacity(16 * 1024);
    loop {
        let message =
            match read_message_with_limit(&mut recv, &mut frame_scratch, max_frame_bytes).await {
                Ok(Some(message)) => message,
                // EOF, or a broken stream: either way the watch is over, and
                // the closed channel is what tells the application.
                Ok(None) => break,
                Err(err) => {
                    tracing::debug!(error = %err, "cache watch stream failed");
                    break;
                }
            };
        let item = match message {
            Message::CacheEvent {
                key,
                value,
                offset,
                expires_at_millis,
            } => CacheWatchItem::Change(CacheChange {
                key,
                value,
                offset,
                expires_at_millis,
            }),
            Message::CacheWatchLagged { resume_from } => CacheWatchItem::Lagged { resume_from },
            other => {
                tracing::debug!(?other, "unexpected message on a cache watch stream");
                break;
            }
        };
        let lagged = matches!(item, CacheWatchItem::Lagged { .. });
        if tx.send(item).await.is_err() {
            break;
        }
        if lagged {
            // The broker finishes the stream after the lag signal; nothing
            // after it is worth waiting for.
            break;
        }
    }
}

/// The `key` and `prefix` fields a watch request carries for `filter`.
pub(crate) fn filter_fields(filter: &CacheWatchFilter) -> (Option<String>, Option<String>) {
    match filter {
        CacheWatchFilter::Key(key) => (Some(key.clone()), None),
        CacheWatchFilter::Prefix(prefix) => (None, Some(prefix.clone())),
    }
}
