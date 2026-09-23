//! Fanout for cache watches: bounded per-watcher queues over a cache shard's
//! write order.
//!
//! The hub is the cache's [`felix_storage::CacheObserver`]: the store reports
//! each applied write while holding the shard's write lock, and the hub fans it
//! out to every registered watcher whose filter matches — the same
//! publisher-never-blocks discipline stream fanout uses, with `try_send`
//! against bounded queues.
//!
//! What differs from stream fanout is what a drop means. Filtering makes
//! offsets sparse — other keys' writes consume them — so a watcher cannot read
//! a queue drop out of an offset jump the way a stream subscriber can. A
//! watcher whose queue is full is therefore *ended*, not quietly thinned: the
//! hub records the offset of the first change it could not deliver and closes
//! the queue, and the delivery path tells the client to re-watch from that
//! offset. Loss is loud, and the recovery is gapless.

use bytes::Bytes;
use hashbrown::HashMap;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;

/// Sentinel for "not lagged" in [`WatcherState::lagged_at`]. No real offset can
/// collide with it: a log would have to hold 2^64 records first.
const NOT_LAGGED: u64 = u64::MAX;

/// Which changes one watcher wants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheWatchFilter {
    /// Exactly this key.
    Key(String),
    /// Every key beginning with this prefix; `""` is every key in the shard.
    Prefix(String),
}

impl CacheWatchFilter {
    pub fn matches(&self, key: &str) -> bool {
        match self {
            Self::Key(exact) => key == exact,
            Self::Prefix(prefix) => key.starts_with(prefix.as_str()),
        }
    }
}

/// One cache change, as a watcher receives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheChangeEvent {
    pub key: String,
    /// The value the key now holds; `None` means the key was deleted.
    pub value: Option<Bytes>,
    /// The cache-log offset of the change.
    pub offset: u64,
    /// Absolute Unix milliseconds; zero means it never expires.
    pub expires_at_millis: u64,
}

/// Tenant, namespace, cache, shard — one fanout list per cache shard.
type WatchShardKey = (String, String, String, u32);

#[derive(Debug)]
struct Watcher {
    /// Monotonic and never reused. A recycled id here would let a reap aimed at
    /// a closed watcher remove a live one — the Slab-recycling defect class.
    id: u64,
    filter: CacheWatchFilter,
    sender: mpsc::Sender<CacheChangeEvent>,
    state: Arc<WatcherState>,
}

/// Shared between the hub (writer side) and the subscription (reader side).
#[derive(Debug)]
struct WatcherState {
    /// Offset of the first change the queue could not hold, or [`NOT_LAGGED`].
    /// Written once, by the fanout that overflowed the queue.
    lagged_at: AtomicU64,
}

/// Fanout registry for every cache watch this broker serves.
#[derive(Debug, Default)]
pub struct CacheWatchHub {
    shards: Mutex<HashMap<WatchShardKey, Vec<Watcher>>>,
    next_id: AtomicU64,
}

impl CacheWatchHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a watcher and hand back its queue.
    ///
    /// Registration is a point in the shard's write order: every change applied
    /// after it is either delivered on this queue or reported as the lag
    /// offset. The caller pins the live edge by registering *before* reading
    /// any history or snapshot — the same register-before-read discipline the
    /// stream resume path enforces — and drops duplicates by offset.
    pub fn register(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        filter: CacheWatchFilter,
        queue_capacity: usize,
    ) -> CacheWatchSubscription {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(queue_capacity.max(1));
        let state = Arc::new(WatcherState {
            lagged_at: AtomicU64::new(NOT_LAGGED),
        });
        let key = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            shard,
        );
        self.shards
            .lock()
            .entry(key.clone())
            .or_default()
            .push(Watcher {
                id,
                filter,
                sender,
                state: Arc::clone(&state),
            });
        CacheWatchSubscription {
            receiver,
            state,
            guard: CacheWatchGuard {
                hub: Arc::downgrade(self),
                key,
                id,
            },
        }
    }

    /// Watcher slots currently registered for one cache shard, for tests that
    /// must prove a closed watch left nothing behind.
    pub fn registered_watchers(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> usize {
        let key = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            shard,
        );
        self.shards.lock().get(&key).map_or(0, Vec::len)
    }

    fn remove(&self, key: &WatchShardKey, id: u64) {
        let mut shards = self.shards.lock();
        if let Some(watchers) = shards.get_mut(key) {
            watchers.retain(|watcher| watcher.id != id);
            if watchers.is_empty() {
                shards.remove(key);
            }
        }
    }
}

impl felix_storage::CacheObserver for CacheWatchHub {
    /// Called with the shard's write lock held, so it must not block: every
    /// enqueue is a `try_send`, and a full queue ends that watcher rather than
    /// stalling the writer.
    fn cache_changed(&self, change: felix_storage::CacheChange) {
        let mut shards = self.shards.lock();
        let key = (
            change.tenant_id,
            change.namespace,
            change.cache,
            change.shard,
        );
        let Some(watchers) = shards.get_mut(&key) else {
            return;
        };
        let event = CacheChangeEvent {
            key: change.key,
            value: change.value,
            offset: change.offset,
            expires_at_millis: change.expires_at_millis,
        };
        watchers.retain(|watcher| {
            if !watcher.filter.matches(&event.key) {
                return true;
            }
            match watcher.sender.try_send(event.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Ended, not thinned: record what was missed and close the
                    // queue by dropping the sender. The reader drains what was
                    // delivered, sees the close, and reports the lag offset so
                    // the client can re-watch from it without a gap.
                    watcher
                        .state
                        .lagged_at
                        .store(event.offset, Ordering::Release);
                    metrics::counter!("felix_cache_watch_lagged_total").increment(1);
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
        if watchers.is_empty() {
            shards.remove(&key);
        }
    }
}

/// RAII handle that unregisters the watcher on drop.
#[derive(Debug)]
pub struct CacheWatchGuard {
    hub: Weak<CacheWatchHub>,
    key: WatchShardKey,
    id: u64,
}

impl Drop for CacheWatchGuard {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            hub.remove(&self.key, self.id);
        }
    }
}

/// The receiving half of one cache watch.
#[derive(Debug)]
pub struct CacheWatchSubscription {
    receiver: mpsc::Receiver<CacheChangeEvent>,
    state: Arc<WatcherState>,
    #[allow(dead_code)]
    guard: CacheWatchGuard,
}

impl CacheWatchSubscription {
    /// The next change, or `None` once the watch has ended — check
    /// [`Self::lagged`] to learn whether it ended by falling behind.
    pub async fn recv(&mut self) -> Option<CacheChangeEvent> {
        self.receiver.recv().await
    }

    /// The next change if one is already queued.
    pub fn try_recv(&mut self) -> Result<CacheChangeEvent, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// The offset of the first change this watch missed, once it has fallen
    /// behind. Everything already queued is still delivered first; re-watching
    /// from this offset is gapless.
    pub fn lagged(&self) -> Option<u64> {
        let at = self.state.lagged_at.load(Ordering::Acquire);
        (at != NOT_LAGGED).then_some(at)
    }
}

#[cfg(test)]
#[path = "cache_watch_tests.rs"]
mod tests;
