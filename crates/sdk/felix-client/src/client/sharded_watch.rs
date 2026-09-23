//! A prefix watch over every shard of a cache, read as one thing.
//!
//! A cache watch reads **one** shard, and keys sharing a prefix hash to
//! different shards, so covering a prefix of a multi-shard cache means one watch
//! per shard, each on its own shard's owner. This opens them and merges what
//! comes back. See [`ShardedCacheWatch`] for what is and is not promised.

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::client::cache_watch::{CacheChange, CacheWatch, CacheWatchFilter, CacheWatchItem};
use crate::client::cluster::ClusterClient;
use crate::client::sharded::ShardOffsets;

/// What arrives from a sharded cache watch.
///
/// Losing a shard is an item, not silence: the other shards keep delivering,
/// and a consumer that ignores these variants is choosing to watch part of the
/// prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardedCacheWatchItem {
    /// A change, and the shard it was read from.
    Change { shard: u32, change: CacheChange },
    /// Every shard has delivered its current state; from here the watch holds
    /// the prefix's state and everything further is live. Only on a retained
    /// watch, at most once, and never if a shard ended before its state was
    /// fully delivered.
    StateComplete,
    /// This shard fell behind and the broker ended its watch. The other shards
    /// are unaffected. [`ShardedCacheWatch::resume_offsets`] already accounts
    /// for it, so resuming from those is gapless.
    Lagged { shard: u32, resume_from: u64 },
    /// This shard's watch ended without a lag signal, usually because its
    /// owner went away. The other shards are unaffected.
    ShardClosed { shard: u32 },
}

/// A prefix watch over every shard of one cache, merged into a single handle.
///
/// **Ordering is per key.** A key lives on exactly one shard, so every change
/// to one key arrives in write order. Changes to keys on different shards
/// arrive in an arbitrary order relative to each other, and each change's
/// `offset` belongs to its own shard's log.
///
/// **Resumption is a vector.** [`ShardedCacheWatch::resume_offsets`] is one
/// offset per shard, never ahead of what has been handed out; pass it to
/// [`ClusterClient::watch_cache_sharded`] to resume.
///
/// Dropping this ends every shard's watch.
#[derive(Debug)]
pub struct ShardedCacheWatch {
    items: mpsc::Receiver<(u32, Option<CacheWatchItem>)>,
    progress: Vec<ShardProgress>,
    retained: bool,
    /// Shards still delivering their current state.
    state_pending: usize,
    state_announced: bool,
    tasks: Vec<JoinHandle<()>>,
}

impl ShardedCacheWatch {
    /// How many shards this watch covers.
    pub fn shards(&self) -> u32 {
        self.progress.len() as u32
    }

    /// How many retained values precede live delivery, summed across shards,
    /// on a retained watch.
    ///
    /// `Some(0)` means the prefix held nothing on any shard at join. The
    /// shards' state phases interleave with each other's live changes, so
    /// counting items does not tell when the state is complete:
    /// [`ShardedCacheWatchItem::StateComplete`] does.
    pub fn retained_count(&self) -> Option<u64> {
        self.retained
            .then(|| self.progress.iter().map(|shard| shard.retained_count).sum())
    }

    /// Where to resume each shard, as the `from_offset` a new watch should
    /// start at.
    ///
    /// Every shard is listed. A shard still in its state phase resumes at 0,
    /// because its retained values can carry any offset below where live
    /// delivery began and none of them may be skipped; that replays or
    /// resnapshots the shard's history rather than missing a key.
    pub fn resume_offsets(&self) -> ShardOffsets {
        self.progress
            .iter()
            .enumerate()
            .map(|(shard, progress)| (shard as u32, progress.resume_from))
            .collect()
    }

    /// The next item from any shard.
    ///
    /// `None` once every shard's watch has ended and its items have been
    /// handed out.
    pub async fn recv(&mut self) -> Option<ShardedCacheWatchItem> {
        if self.retained && self.state_pending == 0 && !self.state_announced {
            self.state_announced = true;
            return Some(ShardedCacheWatchItem::StateComplete);
        }
        let (shard, item) = self.items.recv().await?;
        let progress = &mut self.progress[shard as usize];
        Some(match item {
            Some(CacheWatchItem::Change(change)) => {
                if progress.observe_change(change.offset) {
                    self.state_pending -= 1;
                }
                ShardedCacheWatchItem::Change { shard, change }
            }
            Some(CacheWatchItem::Lagged { resume_from }) => {
                progress.observe_lag(resume_from);
                ShardedCacheWatchItem::Lagged { shard, resume_from }
            }
            None => ShardedCacheWatchItem::ShardClosed { shard },
        })
    }
}

impl Drop for ShardedCacheWatch {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// One shard's position, advanced as items are handed out.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ShardProgress {
    /// The `from_offset` that resumes this shard without a gap.
    resume_from: u64,
    /// Where this shard's live delivery began.
    live_from: u64,
    /// Whatever precedes live delivery arrives in log order, so each item can
    /// advance the position. False for retained values and resnapshots, which
    /// arrive in key order.
    in_log_order: bool,
    retained_count: u64,
    retained_left: u64,
}

impl ShardProgress {
    fn new(
        live_from: u64,
        from_offset: Option<u64>,
        retained: Option<u64>,
        resnapshot: bool,
    ) -> Self {
        let retained_count = retained.unwrap_or(0);
        let resume_from = if retained_count > 0 {
            0
        } else {
            from_offset.unwrap_or(live_from)
        };
        Self {
            resume_from,
            live_from,
            in_log_order: retained.is_none() && !resnapshot,
            retained_count,
            retained_left: retained_count,
        }
    }

    /// True when this change completed the shard's state phase.
    fn observe_change(&mut self, offset: u64) -> bool {
        if self.retained_left > 0 {
            self.retained_left -= 1;
            if self.retained_left == 0 {
                self.resume_from = self.resume_from.max(self.live_from);
                return true;
            }
            return false;
        }
        if offset >= self.live_from || self.in_log_order {
            self.resume_from = self.resume_from.max(offset.saturating_add(1));
        }
        false
    }

    /// The broker names the offset that resumes this shard gaplessly, except
    /// mid-state: retained values not yet delivered can sit below it, so such
    /// a shard stays at 0. It is not counted down either, since its state
    /// phase never completes.
    fn observe_lag(&mut self, resume_from: u64) {
        if self.retained_left == 0 {
            self.resume_from = resume_from;
        }
    }
}

/// Open one prefix watch per shard and forward all of them into one channel.
///
/// Every shard is opened before this returns, or the whole call fails naming
/// the shards that could not be.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn watch_sharded(
    cluster: &Arc<ClusterClient>,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    prefix: &str,
    shards: u32,
    resume: Option<ShardOffsets>,
    retained: bool,
) -> Result<ShardedCacheWatch> {
    anyhow::ensure!(shards > 0, "a cache with no shards cannot be watched");

    // Opened concurrently, so the cost is one round trip rather than one per
    // shard.
    let mut opening = Vec::with_capacity(shards as usize);
    for shard in 0..shards {
        let cluster = Arc::clone(cluster);
        let (tenant_id, namespace, cache) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
        );
        let filter = CacheWatchFilter::Prefix(prefix.to_string());
        let from_offset = resume.as_ref().and_then(|at| at.get(&shard).copied());
        opening.push(tokio::spawn(async move {
            let opened = cluster
                .watch_following_redirects(
                    &tenant_id,
                    &namespace,
                    &cache,
                    filter,
                    Some(shard),
                    from_offset,
                    retained,
                )
                .await;
            (shard, from_offset, opened)
        }));
    }

    let mut opened: Vec<(u32, Option<u64>, CacheWatch)> = Vec::with_capacity(shards as usize);
    let mut failures: Vec<String> = Vec::new();
    for task in opening {
        match task.await {
            Ok((shard, from_offset, Ok(watch))) => opened.push((shard, from_offset, watch)),
            Ok((shard, _, Err(err))) => failures.push(format!("shard {shard}: {err:#}")),
            Err(err) => failures.push(format!("a shard's open task failed: {err}")),
        }
    }
    if !failures.is_empty() {
        anyhow::bail!(
            "could not watch every shard of cache {cache}, so the watch would be silently \
             incomplete: {}",
            failures.join("; ")
        );
    }
    opened.sort_by_key(|(shard, ..)| *shard);

    let (tx, rx) = mpsc::channel(64 * shards as usize);
    let mut progress = Vec::with_capacity(opened.len());
    let mut tasks = Vec::with_capacity(opened.len());
    for (shard, from_offset, watch) in opened {
        progress.push(ShardProgress::new(
            watch.resume_offset(),
            from_offset,
            watch.retained_count(),
            watch.resnapshot(),
        ));
        tasks.push(tokio::spawn(forward_shard(shard, watch, tx.clone())));
    }
    let state_pending = progress
        .iter()
        .filter(|shard| shard.retained_left > 0)
        .count();

    Ok(ShardedCacheWatch {
        items: rx,
        progress,
        retained,
        state_pending,
        state_announced: false,
        tasks,
    })
}

/// Pump one shard's watch into the shared channel. `None` says it ended
/// without a lag signal.
async fn forward_shard(
    shard: u32,
    mut watch: CacheWatch,
    tx: mpsc::Sender<(u32, Option<CacheWatchItem>)>,
) {
    while let Some(item) = watch.recv().await {
        let lagged = matches!(item, CacheWatchItem::Lagged { .. });
        if tx.send((shard, Some(item))).await.is_err() || lagged {
            return;
        }
    }
    let _ = tx.send((shard, None)).await;
}

#[cfg(test)]
mod tests {
    use super::ShardProgress;

    #[test]
    fn a_live_watch_resumes_after_the_last_change_handed_out() {
        let mut shard = ShardProgress::new(10, None, None, false);
        assert_eq!(shard.resume_from, 10);
        assert!(!shard.observe_change(12));
        assert_eq!(shard.resume_from, 13);
    }

    /// Replay arrives in log order, so each replayed change moves the position,
    /// and nothing before the requested offset is skipped.
    #[test]
    fn a_replay_resumes_after_what_was_replayed_not_where_live_began() {
        let mut shard = ShardProgress::new(50, Some(20), None, false);
        assert_eq!(shard.resume_from, 20);
        shard.observe_change(25);
        assert_eq!(shard.resume_from, 26);
    }

    /// Retained values arrive in key order with any offset below where live
    /// began, so the position stays at 0 until the last one is handed out.
    #[test]
    fn retained_values_hold_the_position_until_the_state_phase_ends() {
        let mut shard = ShardProgress::new(100, None, Some(2), false);
        assert!(!shard.observe_change(90));
        assert_eq!(shard.resume_from, 0);
        assert!(
            shard.observe_change(40),
            "the last retained value ends the phase"
        );
        assert_eq!(shard.resume_from, 100);
        assert!(!shard.observe_change(105));
        assert_eq!(shard.resume_from, 106);
    }

    #[test]
    fn a_resnapshot_only_moves_the_position_once_live() {
        let mut shard = ShardProgress::new(100, Some(5), None, true);
        shard.observe_change(90);
        shard.observe_change(40);
        assert_eq!(shard.resume_from, 5);
        shard.observe_change(101);
        assert_eq!(shard.resume_from, 102);
    }

    #[test]
    fn a_lag_resumes_where_the_broker_says() {
        let mut shard = ShardProgress::new(10, None, None, false);
        shard.observe_change(11);
        shard.observe_lag(30);
        assert_eq!(shard.resume_from, 30);
    }

    #[test]
    fn a_lag_mid_state_resumes_from_the_start() {
        let mut shard = ShardProgress::new(100, None, Some(3), false);
        shard.observe_change(70);
        shard.observe_lag(100);
        assert_eq!(shard.resume_from, 0);
    }
}
