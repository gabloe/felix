//! Cache watches.
//!
//! The cache itself, its store and its compaction, lives in `felix-storage`.
//! What the broker adds is fanout: each applied write, delivered to every
//! watcher whose filter matches, in the shard's write order.

mod watch;

pub use watch::{CacheChangeEvent, CacheWatchFilter, CacheWatchHub, CacheWatchSubscription};
