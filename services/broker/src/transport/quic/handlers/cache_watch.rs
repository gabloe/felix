//! QUIC cache-watch handling: a keyed subscription over a cache's log.
//!
//! A `CacheWatch` arrives on the bi-directional control stream; changes go back
//! on a fresh uni stream, bound by the same `EventStreamHello` a stream
//! subscription uses. The join is the point of this module, and it follows the
//! register-before-read discipline the stream resume path proved out:
//!
//! 1. Register the watcher with the hub — from here every applied change is
//!    either queued or reported as the lag offset.
//! 2. Read the shard log's tail. Registration happened first, so every change
//!    below the tail is on disk and every change at or past it is queued.
//! 3. Serve the catch-up — `[from_offset, tail)` from the log for a resume,
//!    or each matching key's current value for a retained watch and for a
//!    resume compaction has collapsed — then live changes, dropping queued
//!    duplicates below the tail by offset.
//!
//! Reading history first and registering after is the version that looks
//! natural and loses the write that lands in between.
//!
//! Loss is loud, never silent: a watcher whose queue overflowed is ended with
//! `CacheWatchLagged` naming the first missed offset, because filtering makes
//! offsets sparse and a drop would otherwise be indistinguishable from other
//! keys' traffic. Re-watching from that offset is gapless.

use anyhow::Result;
use felix_broker::{Broker, CacheWatchFilter, CacheWatchSubscription};
use felix_storage::log::{AppendOnlyLog, ReadRange};
use felix_wire::Message;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::publish::{Outgoing, PublishContext, send_outgoing_critical};
use crate::transport::quic::SUBSCRIPTION_ID;
use crate::transport::quic::codec::write_message;

/// Changes queued per watcher before the watch is ended as lagged. A watch is
/// a control-plane-sized feed — one key or prefix, not a stream's firehose —
/// so this bounds memory without a per-deployment knob.
const WATCH_QUEUE_CAPACITY: usize = 1024;

/// One page of history per read, bounding broker memory for a resume that
/// starts arbitrarily far back.
const HISTORY_PAGE_BYTES: usize = 1024 * 1024;

/// Everything `handle_cache_watch_message` needs to answer on the control
/// stream, bundled so the failure paths cannot drift apart.
pub(crate) struct WatchResponder<'a> {
    pub(crate) out_ack_tx: &'a mpsc::Sender<Outgoing>,
    pub(crate) out_ack_depth: &'a Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) ack_throttle_tx: &'a tokio::sync::watch::Sender<bool>,
    pub(crate) ack_timeout_state: &'a Arc<tokio::sync::Mutex<super::publish::AckTimeoutState>>,
    pub(crate) cancel_tx: &'a tokio::sync::watch::Sender<bool>,
}

impl WatchResponder<'_> {
    async fn send(&self, message: Message) -> Result<()> {
        super::publish::handle_ack_enqueue_result(
            send_outgoing_critical(
                self.out_ack_tx,
                self.out_ack_depth,
                "felix_broker_out_ack_depth",
                self.ack_throttle_tx,
                Outgoing::Message(message),
            )
            .await,
            self.ack_timeout_state,
            self.ack_throttle_tx,
            self.cancel_tx,
        )
        .await
    }
}

/// The request's watch parameters, decoded but not yet validated.
pub(crate) struct WatchRequest {
    pub(crate) tenant_id: String,
    pub(crate) namespace: String,
    pub(crate) cache: String,
    pub(crate) key: Option<String>,
    pub(crate) prefix: Option<String>,
    pub(crate) shard: Option<u32>,
    pub(crate) from_offset: Option<u64>,
    pub(crate) retained: bool,
    pub(crate) subscription_id: Option<u64>,
}

/// Handle a `CacheWatch` from the control-stream loop.
///
/// Always returns `Ok(true)` ("handled; keep the control stream alive") unless
/// the response path itself fails — the same convention subscribe follows.
pub(crate) async fn handle_cache_watch_message(
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: crate::config::BrokerConfig,
    publish_ctx: &PublishContext,
    responder: WatchResponder<'_>,
    request: WatchRequest,
    peer_features: u32,
) -> Result<bool> {
    // Exactly one of key/prefix. Refused rather than guessed at: "both" has
    // two defensible readings and a client relying on either would silently
    // get the other.
    let filter = match (&request.key, &request.prefix) {
        (Some(key), None) => CacheWatchFilter::Key(key.clone()),
        (None, Some(prefix)) => CacheWatchFilter::Prefix(prefix.clone()),
        _ => {
            responder
                .send(Message::Error {
                    message: "cache_watch takes exactly one of key or prefix".to_string(),
                })
                .await?;
            return Ok(true);
        }
    };

    if !broker
        .cache_exists(&request.tenant_id, &request.namespace, &request.cache)
        .await
    {
        responder
            .send(Message::Error {
                message: format!(
                    "cache scope not found: {}/{}/{}",
                    request.tenant_id, request.namespace, request.cache
                ),
            })
            .await?;
        return Ok(true);
    }

    // Which shard this watch reads. A key names its shard by hashing — the
    // same resolution a get or put uses — while a prefix watch reads the shard
    // it was addressed to, because keys sharing a prefix hash apart.
    let shards = publish_ctx
        .ingress
        .as_deref()
        .map(|ingress| {
            ingress.shards_for(
                crate::shard_watch::ShardKind::Cache,
                &request.tenant_id,
                &request.namespace,
                &request.cache,
            )
        })
        .unwrap_or(1);
    let shard = match &filter {
        CacheWatchFilter::Key(key) => crate::shard_routing::shard_for(shards, Some(key.as_bytes())),
        CacheWatchFilter::Prefix(_) => request.shard.unwrap_or(0),
    };
    // A shard the cache does not have is refused. Accepting it would register a
    // watch on a log nothing writes to — quiet forever, and indistinguishable
    // from a quiet prefix.
    if shard >= shards.max(1) {
        responder
            .send(Message::Error {
                message: format!(
                    "cache {} has {} shard(s); shard {} does not exist",
                    request.cache,
                    shards.max(1),
                    shard
                ),
            })
            .await?;
        return Ok(true);
    }

    // Watches are served by the shard's owner, never proxied: a watch served
    // off the owner would go quiet on writes it cannot see, which is
    // indistinguishable from a quiet key.
    if let Some(answer) = super::subscribe::redirect_for(
        publish_ctx.ingress.as_deref(),
        publish_ctx.client_endpoints.as_deref(),
        &request.tenant_id,
        &request.namespace,
        &request.cache,
        shard,
        crate::shard_watch::ShardKind::Cache,
        peer_features,
    ) {
        responder.send(answer).await?;
        return Ok(true);
    }

    let Some(hub) = broker.cache_watches() else {
        // Reachable only by a client that ignored negotiation: the feature bit
        // is advertised exactly when the hub exists.
        responder
            .send(Message::Error {
                message: "this broker's cache is not log-backed, so it cannot serve watches"
                    .to_string(),
            })
            .await?;
        return Ok(true);
    };

    if !publish_ctx
        .subscriptions
        .try_reserve(config.max_subscriptions_per_conn)
    {
        responder
            .send(Message::Error {
                message: "max subscriptions per connection exceeded".to_string(),
            })
            .await?;
        return Ok(true);
    }
    // Reserved; every exit below must release exactly once — failure paths do
    // it inline, the delivery task does it on the success path.
    let subscriptions = Arc::clone(&publish_ctx.subscriptions);

    // Register *before* reading the tail: this pins the live edge. Every
    // change applied from here on is queued (or reported as lag), so the
    // history below the tail is a closed range nothing can escape.
    let watch = hub.register(
        &request.tenant_id,
        &request.namespace,
        &request.cache,
        shard,
        filter.clone(),
        WATCH_QUEUE_CAPACITY,
    );

    let Some(log) = broker
        .cache()
        .shard_log(
            &request.tenant_id,
            &request.namespace,
            &request.cache,
            shard,
        )
        .await
    else {
        subscriptions.release();
        responder
            .send(Message::Error {
                message: "the cache shard's log could not be opened".to_string(),
            })
            .await?;
        return Ok(true);
    };
    let tail = match log.tail_offset().await {
        Ok(tail) => tail,
        Err(err) => {
            subscriptions.release();
            responder
                .send(Message::Error {
                    message: format!("the cache shard's log could not be read: {err}"),
                })
                .await?;
            return Ok(true);
        }
    };
    let base = log.base_offset();

    // What fills the gap between the requested position and the live edge.
    enum CatchUp {
        None,
        /// Replay `[from, tail)` from the log.
        Replay {
            from: u64,
        },
        /// Each matching key's current value, then live — either because the
        /// watch asked for retained delivery, or because the offset it asked
        /// to resume from was collapsed by compaction and current state is the
        /// defined answer, never a silent gap.
        Snapshot,
    }
    let catch_up = match (request.retained, request.from_offset) {
        (true, Some(_)) => {
            // A resume already reconstructs the state a retained start would
            // shortcut, and delivering both would hand over every value twice.
            subscriptions.release();
            responder
                .send(Message::Error {
                    message: "cache_watch takes retained or from_offset, not both".to_string(),
                })
                .await?;
            return Ok(true);
        }
        (true, None) => CatchUp::Snapshot,
        (false, None) => CatchUp::None,
        (false, Some(from)) if from > tail => {
            subscriptions.release();
            responder
                .send(Message::SubscribeCursorError {
                    reason: felix_wire::CursorErrorReason::InFuture,
                    requested: from,
                    available: tail,
                })
                .await?;
            return Ok(true);
        }
        (false, Some(from)) if from >= base => CatchUp::Replay { from },
        (false, Some(_)) => CatchUp::Snapshot,
    };
    // The compacted-resume case, distinct from asked-for retained delivery
    // because the confirmation reports them differently.
    let resnapshot = matches!(catch_up, CatchUp::Snapshot) && !request.retained;

    // Snapshots are read *before* the confirmation goes out, because a
    // retained watch's confirmation carries how many values follow — `Some(0)`
    // being the defined "joined an empty key" answer, which silence could
    // never be. Entries at or past the tail are left out: they were applied
    // after the watcher registered, so their changes arrive live, and writing
    // them here would double them.
    let snapshot = match catch_up {
        CatchUp::Snapshot => {
            match snapshot_entries(broker.cache(), &request, shard, &filter, tail).await {
                Ok(entries) => Some(entries),
                Err(err) => {
                    subscriptions.release();
                    responder
                        .send(Message::Error {
                            message: format!("cache snapshot failed: {err}"),
                        })
                        .await?;
                    return Ok(true);
                }
            }
        }
        _ => None,
    };
    let retained_count = request
        .retained
        .then(|| snapshot.as_ref().map_or(0, Vec::len) as u64);

    let subscription_id = request
        .subscription_id
        .unwrap_or_else(|| SUBSCRIPTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

    let mut event_send = match connection.open_uni().await {
        Ok(send) => send,
        Err(err) => {
            subscriptions.release();
            responder
                .send(Message::Error {
                    message: err.to_string(),
                })
                .await?;
            return Ok(true);
        }
    };
    // Hello first, so the client can bind subscription_id -> stream before any
    // change arrives.
    if let Err(err) = write_message(
        &mut event_send,
        Message::EventStreamHello { subscription_id },
    )
    .await
    {
        subscriptions.release();
        tracing::info!(error = %err, "cache watch event stream closed");
        return Ok(true);
    }

    // Acknowledge before writing any catch-up, exactly as subscribe does: the
    // client reads the event stream only after it sees the confirmation, so a
    // catch-up larger than the stream's flow-control window would deadlock if
    // it went first.
    responder
        .send(Message::CacheWatchStarted {
            subscription_id,
            resume_offset: tail,
            resnapshot,
            retained_count,
        })
        .await?;

    let catch_up_result = match (catch_up, snapshot) {
        (CatchUp::Replay { from }, _) => {
            write_replayed_changes(&mut event_send, &log, &filter, from, tail).await
        }
        (CatchUp::Snapshot, Some(entries)) => write_snapshot(&mut event_send, entries).await,
        _ => Ok(()),
    };
    if let Err(err) = catch_up_result {
        subscriptions.release();
        tracing::info!(error = %err, "cache watch catch-up failed");
        return Ok(true);
    }

    // Live delivery. Queued changes below the tail are duplicates of what the
    // catch-up already wrote — the registration race — and are dropped by
    // offset; everything else is written until the client goes away or the
    // watch laps its queue.
    tokio::spawn(run_watch_delivery(watch, event_send, tail, subscriptions));
    Ok(true)
}

/// Replay `[from, tail)` from the shard log, delivering only matching changes.
///
/// Deletes replay as tombstoned changes: the history of a key includes its
/// removals, and a resuming client folding the sequence must see them. Paged,
/// so a resume from far back costs one page of memory at a time and a slow
/// client backpressures the read instead of buffering the whole range.
async fn write_replayed_changes(
    event_send: &mut quinn::SendStream,
    log: &felix_storage::DiskLog,
    filter: &CacheWatchFilter,
    from: u64,
    until: u64,
) -> Result<()> {
    let mut at = from;
    while at < until {
        let records = log
            .read_range(ReadRange {
                start: at,
                max_bytes: HISTORY_PAGE_BYTES,
            })
            .await?;
        if records.is_empty() {
            break;
        }
        for record in records {
            if record.offset >= until {
                return Ok(());
            }
            at = record.offset + 1;
            let op = match felix_storage::log_cache::CacheOp::decode(&record.payload) {
                Ok(op) => op,
                Err(err) => return Err(anyhow::anyhow!("cache record did not decode: {err}")),
            };
            if !filter.matches(op.key()) {
                continue;
            }
            let message = match op {
                felix_storage::log_cache::CacheOp::Put {
                    key,
                    value,
                    expires_at_millis,
                } => Message::CacheEvent {
                    key,
                    value: Some(value),
                    offset: record.offset,
                    expires_at_millis,
                },
                felix_storage::log_cache::CacheOp::Delete { key } => Message::CacheEvent {
                    key,
                    value: None,
                    offset: record.offset,
                    expires_at_millis: 0,
                },
            };
            write_message(event_send, message).await?;
        }
    }
    Ok(())
}

/// Each matching key's current value below `tail`, in offset order.
///
/// Entries at or past `tail` are left out: they were applied after the
/// watcher registered, so their changes arrive live and including them here
/// would double them. Offset order, so the client's checkpoint advances
/// monotonically through the snapshot exactly as it does through a replay.
async fn snapshot_entries(
    cache: &(dyn felix_storage::StorageApi + Send),
    request: &WatchRequest,
    shard: u32,
    filter: &CacheWatchFilter,
    tail: u64,
) -> Result<Vec<felix_storage::CacheSnapshotEntry>> {
    let mut entries = cache
        .live_entries(
            &request.tenant_id,
            &request.namespace,
            &request.cache,
            shard,
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err}"))?;
    entries.retain(|entry| entry.offset < tail && filter.matches(&entry.key));
    entries.sort_by_key(|entry| entry.offset);
    Ok(entries)
}

/// Deliver a snapshot's current values on the event stream.
async fn write_snapshot(
    event_send: &mut quinn::SendStream,
    entries: Vec<felix_storage::CacheSnapshotEntry>,
) -> Result<()> {
    for entry in entries {
        write_message(
            event_send,
            Message::CacheEvent {
                key: entry.key,
                value: Some(entry.value),
                offset: entry.offset,
                expires_at_millis: entry.expires_at_millis,
            },
        )
        .await?;
    }
    Ok(())
}

/// Drain the watcher queue onto the event stream until the client goes away or
/// the watch falls behind.
async fn run_watch_delivery(
    mut watch: CacheWatchSubscription,
    mut event_send: quinn::SendStream,
    resume_offset: u64,
    subscriptions: Arc<super::publish::SubscriptionLimiter>,
) {
    loop {
        match watch.recv().await {
            Some(event) => {
                if event.offset < resume_offset {
                    // Applied before the tail was read, so the catch-up already
                    // covered it; the offset is what makes the duplicate cheap
                    // to detect.
                    continue;
                }
                let message = Message::CacheEvent {
                    key: event.key,
                    value: event.value,
                    offset: event.offset,
                    expires_at_millis: event.expires_at_millis,
                };
                if let Err(err) = write_message(&mut event_send, message).await {
                    tracing::debug!(error = %err, "cache watch client went away");
                    break;
                }
            }
            None => {
                if let Some(resume_from) = watch.lagged() {
                    // Ended loudly: everything queued was delivered above, and
                    // this names the first change that was not.
                    let _ =
                        write_message(&mut event_send, Message::CacheWatchLagged { resume_from })
                            .await;
                }
                break;
            }
        }
    }
    let _ = event_send.finish();
    subscriptions.release();
}
