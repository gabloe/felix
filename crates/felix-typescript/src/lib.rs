//! Node.js / TypeScript bindings for the Felix client.
//!
//! This is a **wrapper over `felix-client`, not a second implementation of the
//! protocol.** Reconnection, redirect-following, retry classification, offset
//! bookkeeping and the frame codec all live in the Rust client and are shared
//! by every language that binds to it. A TypeScript-native client would be a
//! second place for those to be subtly wrong, in exactly the areas — failover
//! and delivery accounting — where subtly wrong is most expensive. The Python
//! binding is built on the same reasoning and exposes the same surface.
//!
//! The surface is asynchronous: every call returns a `Promise`, because
//! blocking Node's event loop is not a thing a Node library may do. napi-rs
//! runs the future on its own Tokio runtime and settles the promise from
//! there, so the event loop stays free while a publish is in flight.
//!
//! A cache entry is named by tenant, namespace, cache and key before its value
//! and options ever appear, so several of these methods carry more arguments
//! than clippy's default. Bundling them into an object would move the argument
//! list rather than shorten it, and every caller has the parts separately
//! anyway — the same reasoning the Python binding records.
#![allow(clippy::too_many_arguments)]
use std::collections::{BTreeMap, HashMap};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use bytes::Bytes;
use errors::{classify, invalid};
use felix_client::{ClientConfig, ClusterClient, Subscription};
use felix_wire::{AckMode, StartPosition};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{Mutex, watch};

mod errors;
mod tls;

/// One delivered record.
#[napi(object)]
pub struct Event {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub payload: Buffer,
    /// The record's log offset on a durable stream, absent on an ephemeral one.
    ///
    /// A jump in these is exactly a drop: subscriber queues shed under the
    /// default policy rather than blocking the publisher, so a gap here is the
    /// signal that it happened.
    pub offset: Option<BigInt>,
}

/// One record handed out by a consumer group.
#[napi(object)]
pub struct GroupRecord {
    pub offset: BigInt,
    pub payload: Buffer,
    /// How many times this record has been handed out, this delivery included.
    /// `1` is a first attempt; anything higher is a redelivery, so a consumer
    /// can treat a retry differently. `0` means the broker did not report it.
    pub attempts: u32,
}

/// One change observed by a cache watch.
#[napi(object)]
pub struct CacheChange {
    pub key: String,
    /// Absent when the key was deleted or expired.
    pub value: Option<Buffer>,
    pub offset: BigInt,
    pub expires_at_millis: BigInt,
}

/// An item from a cache watch: a change, or notice that the watch fell behind.
#[napi(object)]
pub struct CacheWatchItem {
    pub change: Option<CacheChange>,
    /// Set when the watch lagged and the broker ended it. Re-watching with
    /// `start = resumeFrom` is gapless.
    pub lagged_resume_from: Option<BigInt>,
}

/// An item from a sharded subscription.
///
/// Exactly one of these is set. A lost shard does not affect the others: they
/// keep delivering while that one is re-established, and it resumes from its
/// own last offset so nothing is skipped.
#[napi(object)]
pub struct ShardEvent {
    pub shard: u32,
    pub event: Option<Event>,
    pub lost_error: Option<String>,
    pub recovered: Option<bool>,
}

/// How much the broker must have done before a publish resolves.
fn parse_ack(ack: &str) -> Result<AckMode> {
    match ack {
        "none" => Ok(AckMode::None),
        "per_message" => Ok(AckMode::PerMessage),
        "per_batch" => Ok(AckMode::PerBatch),
        other => Err(invalid(format!(
            r#"ack must be "none", "per_message" or "per_batch", got {other:?}"#
        ))),
    }
}

/// Where a new subscription begins.
fn parse_start(start: Option<Either<String, BigInt>>) -> Result<StartPosition> {
    match start {
        None => Ok(StartPosition::Latest),
        Some(Either::A(name)) => match name.as_str() {
            "latest" => Ok(StartPosition::Latest),
            "earliest" => Ok(StartPosition::Earliest),
            other => Err(invalid(format!(
                r#"start must be "latest", "earliest" or an offset, got {other:?}"#
            ))),
        },
        Some(Either::B(offset)) => {
            let (_, value, lossless) = offset.get_u64();
            if !lossless {
                return Err(invalid("start offset does not fit in a u64"));
            }
            Ok(StartPosition::Offset(value))
        }
    }
}

fn u64_of(value: BigInt) -> Result<u64> {
    let (_, out, lossless) = value.get_u64();
    if !lossless {
        return Err(invalid("offset does not fit in a u64"));
    }
    Ok(out)
}

/// Per-shard resume offsets, in the shape `positions()` hands back.
///
/// Keyed by shard number as a string because that is what a JavaScript object
/// key is, so a `positions()` result can be passed straight back with no
/// translation on the caller's part.
fn parse_positions(resume: Option<HashMap<String, BigInt>>) -> Result<Option<BTreeMap<u32, u64>>> {
    let Some(resume) = resume else {
        return Ok(None);
    };
    let mut out = BTreeMap::new();
    for (shard, offset) in resume {
        let parsed: u32 = shard
            .parse()
            .map_err(|_| invalid(format!("resume keys are shard numbers, got {shard:?}")))?;
        out.insert(parsed, u64_of(offset)?);
    }
    Ok(Some(out))
}

fn resolve(addrs: Vec<String>) -> Result<Vec<SocketAddr>> {
    let mut out = Vec::new();
    for addr in addrs {
        let resolved = addr
            .to_socket_addrs()
            .map_err(|err| invalid(format!("could not resolve {addr:?}: {err}")))?;
        out.extend(resolved);
    }
    Ok(out)
}

/// A live subscription. Read it with `nextEvent`, and `close` it when done.
///
/// A read in flight holds the handle, so `close` cannot wait for it: a consumer
/// shutting down is almost always parked on `nextEvent`, and waiting for the
/// read it is cancelling would hang exactly the path that needs to make
/// progress. `close` therefore cancels the read instead, and the reader drops
/// the subscription on its way out.
#[napi]
pub struct SubscriptionHandle {
    inner: Arc<Mutex<Option<Subscription>>>,
    closed: watch::Sender<bool>,
    /// Kept alive because the subscription's event stream belongs to it:
    /// dropping the client would close the stream underneath the reader.
    _client: Arc<felix_client::Client>,
}

#[napi]
impl SubscriptionHandle {
    /// The next record, or `null` once the subscription has ended.
    ///
    /// Resolves when a record arrives; there is no timeout argument because a
    /// caller that wants one can race this promise against a timer. The losing
    /// read stays in flight and resolves with the next record, so keep the
    /// promise rather than calling again.
    #[napi]
    pub async fn next_event(&self) -> Result<Option<Event>> {
        // A `watch` receiver and not a flag plus a notify: `wait_for` returns
        // at once when the value is already set, so a close that lands between
        // the check and the wait cannot be missed.
        let mut closed = self.closed.subscribe();
        if *closed.borrow_and_update() {
            return Ok(None);
        }
        let inner = Arc::clone(&self.inner);
        let mut guard = inner.lock().await;
        let Some(subscription) = guard.as_mut() else {
            return Ok(None);
        };
        let outcome = tokio::select! {
            result = subscription.next_event() => Some(result),
            _ = closed.wait_for(|closed| *closed) => None,
        };
        match outcome {
            // Closed underneath this read, so this reader is the one holding
            // the subscription and the one that has to let it go.
            None => {
                guard.take();
                Ok(None)
            }
            Some(Ok(Some(event))) => Ok(Some(Event {
                tenant_id: event.tenant_id.to_string(),
                namespace: event.namespace.to_string(),
                stream: event.stream.to_string(),
                payload: event.payload.to_vec().into(),
                offset: event.offset.map(BigInt::from),
            })),
            Some(Ok(None)) => Ok(None),
            Some(Err(err)) => Err(classify(err)),
        }
    }

    /// Release the subscription. Idempotent, and never waits on a read.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        // `send_replace` and not `send`: with nothing reading there is no
        // receiver, and `send` reports that as an error and leaves the value
        // alone — so a close with no read in flight would not take.
        self.closed.send_replace(true);
        // Only when nothing is reading. When something is, it owns the lock and
        // drops the subscription as it unwinds — waiting for that here is the
        // deadlock this whole arrangement exists to avoid.
        if let Ok(mut guard) = self.inner.try_lock() {
            guard.take();
        }
        Ok(())
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        *self.closed.borrow()
    }
}

/// A subscription across every shard of a stream.
#[napi]
pub struct ShardedSubscriptionHandle {
    shards: u32,
    inner: Arc<Mutex<Option<felix_client::ShardedSubscription>>>,
    closed: watch::Sender<bool>,
}

#[napi]
impl ShardedSubscriptionHandle {
    /// How many shards this subscription covers.
    #[napi(getter)]
    pub fn shards(&self) -> u32 {
        self.shards
    }

    /// The next item, or `null` once every shard has ended.
    #[napi]
    pub async fn next_event(&self) -> Result<Option<ShardEvent>> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow_and_update() {
            return Ok(None);
        }
        let mut guard = self.inner.lock().await;
        let Some(sub) = guard.as_mut() else {
            return Ok(None);
        };
        let item = tokio::select! {
            item = sub.next() => item,
            _ = closed.wait_for(|closed| *closed) => {
                guard.take();
                return Ok(None);
            }
        };
        Ok(item.map(|item| match item {
            felix_client::ShardEvent::Record { shard, event } => ShardEvent {
                shard,
                event: Some(Event {
                    tenant_id: event.tenant_id.to_string(),
                    namespace: event.namespace.to_string(),
                    stream: event.stream.to_string(),
                    payload: event.payload.to_vec().into(),
                    offset: event.offset.map(BigInt::from),
                }),
                lost_error: None,
                recovered: None,
            },
            felix_client::ShardEvent::ShardLost { shard, error } => ShardEvent {
                shard,
                event: None,
                lost_error: Some(error),
                recovered: None,
            },
            felix_client::ShardEvent::ShardRecovered { shard } => ShardEvent {
                shard,
                event: None,
                lost_error: None,
                recovered: Some(true),
            },
        }))
    }

    /// The last offset seen from each shard, for resuming.
    ///
    /// Only shards that have delivered something appear. Passed back as
    /// `resume`, each listed shard continues at `offset + 1` and the rest
    /// start wherever `start` says.
    #[napi]
    pub async fn positions(&self) -> Result<HashMap<String, BigInt>> {
        let guard = self.inner.lock().await;
        let Some(sub) = guard.as_ref() else {
            return Ok(HashMap::new());
        };
        Ok(sub
            .positions()
            .into_iter()
            .map(|(shard, offset)| (shard.to_string(), BigInt::from(offset)))
            .collect())
    }

    /// Release the subscription. Idempotent, and never waits on a read.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        self.closed.send_replace(true);
        if let Ok(mut guard) = self.inner.try_lock() {
            guard.take();
        }
        Ok(())
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        *self.closed.borrow()
    }
}

/// A live cache watch. Close it when done.
#[napi]
pub struct CacheWatchHandle {
    resume_offset: u64,
    resnapshot: bool,
    retained_count: Option<u64>,
    inner: Arc<Mutex<Option<felix_client::CacheWatch>>>,
    closed: watch::Sender<bool>,
}

#[napi]
impl CacheWatchHandle {
    /// The offset live delivery began at. Everything the broker sent before it
    /// — replay, or a retained snapshot — was already reflected there.
    #[napi(getter)]
    pub fn resume_offset(&self) -> BigInt {
        BigInt::from(self.resume_offset)
    }

    /// True when the requested start predated what compaction kept, so the
    /// watch began from each key's current value instead of replaying history.
    #[napi(getter)]
    pub fn resnapshot(&self) -> bool {
        self.resnapshot
    }

    /// How many retained values arrive before live delivery on a retained
    /// watch, so an application knows the exact moment its state is complete.
    ///
    /// `0` is a definite answer — the key or prefix held nothing at join — not
    /// a silence to wait through. `null` on a watch that did not ask for
    /// retained delivery.
    #[napi(getter)]
    pub fn retained_count(&self) -> Option<BigInt> {
        self.retained_count.map(BigInt::from)
    }

    /// The next item, or `null` once the watch has ended.
    #[napi]
    pub async fn recv(&self) -> Result<Option<CacheWatchItem>> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow_and_update() {
            return Ok(None);
        }
        let mut guard = self.inner.lock().await;
        let Some(handle) = guard.as_mut() else {
            return Ok(None);
        };
        let item = tokio::select! {
            item = handle.recv() => item,
            _ = closed.wait_for(|closed| *closed) => {
                guard.take();
                return Ok(None);
            }
        };
        Ok(item.map(|item| match item {
            felix_client::CacheWatchItem::Change(change) => CacheWatchItem {
                change: Some(CacheChange {
                    key: change.key,
                    value: change.value.map(|v| v.to_vec().into()),
                    offset: BigInt::from(change.offset),
                    expires_at_millis: BigInt::from(change.expires_at_millis),
                }),
                lagged_resume_from: None,
            },
            felix_client::CacheWatchItem::Lagged { resume_from } => CacheWatchItem {
                change: None,
                lagged_resume_from: Some(BigInt::from(resume_from)),
            },
        }))
    }

    /// Release the watch. Idempotent, and never waits on a read.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        self.closed.send_replace(true);
        if let Ok(mut guard) = self.inner.try_lock() {
            guard.take();
        }
        Ok(())
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        *self.closed.borrow()
    }
}

/// A connected Felix client.
#[napi]
pub struct Client {
    /// `None` once closed. A plain `std::sync::Mutex` and not the async one:
    /// the lock is only ever held long enough to clone the `Arc`, so callers
    /// sharing a client never queue behind each other's in-flight calls.
    inner: std::sync::Mutex<Option<Arc<ClusterClient>>>,
}

impl Client {
    /// The cluster client, or an error if this one has been closed.
    fn cluster(&self) -> Result<Arc<ClusterClient>> {
        self.inner
            .lock()
            .expect("the client lock is only ever held across a clone")
            .clone()
            .ok_or_else(|| invalid("this client is closed"))
    }
}

#[napi]
impl Client {
    /// Connect to a cluster.
    ///
    /// `addrs` is one `"host:port"` or several; any reachable one is enough
    /// and the client discovers the rest. `serverName` is the name the
    /// broker's certificate is expected to carry.
    ///
    /// TLS is not optional — QUIC has no unencrypted mode. Supply `caFile` to
    /// trust a specific CA (what a self-signed development broker needs); omit
    /// it to use the operating system's trust store.
    #[napi(factory)]
    pub async fn connect(
        addrs: Either<String, Vec<String>>,
        tenant_id: String,
        token: String,
        server_name: Option<String>,
        ca_file: Option<String>,
    ) -> Result<Client> {
        let addrs = match addrs {
            Either::A(one) => vec![one],
            Either::B(many) => many,
        };
        let seeds = resolve(addrs)?;
        if seeds.is_empty() {
            return Err(invalid("at least one broker address is required"));
        }
        let quinn = tls::client_config(ca_file.as_deref())?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id);
        config.auth_token = Some(token);

        let server_name = server_name.unwrap_or_else(|| "localhost".to_string());
        let inner = ClusterClient::connect(&seeds, &server_name, config)
            .await
            .map_err(classify)?;
        Ok(Client {
            inner: std::sync::Mutex::new(Some(Arc::new(inner))),
        })
    }

    /// Release the client. Idempotent.
    ///
    /// Later calls fail rather than quietly using a connection that was meant
    /// to be gone. Subscriptions already handed out hold their own connection
    /// and keep running — closing the client is not a way to stop them, and
    /// `close` on the subscription is.
    #[napi]
    pub fn close(&self) {
        self.inner
            .lock()
            .expect("the client lock is only ever held across a clone")
            .take();
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        self.inner
            .lock()
            .expect("the client lock is only ever held across a clone")
            .is_none()
    }

    /// Publish one record.
    ///
    /// `key` is the routing key, and it decides the shard. Without one every
    /// record lands on shard 0, so a multi-shard stream behaves like a
    /// single-shard one. Records sharing a key share a shard and stay ordered
    /// with respect to each other; records with different keys do not.
    ///
    /// `atLeastOnce` **may duplicate the record.** By default a publish that
    /// fails after the broker may already have written it is reported, not
    /// re-sent, because nothing downstream can tell the copies apart. With
    /// `atLeastOnce` it is re-sent to another broker instead: the record is
    /// then certain to land, and may land twice. That is a delivery guarantee
    /// the caller chooses, never one this client assumes.
    #[napi]
    pub async fn publish(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        payload: Buffer,
        key: Option<Buffer>,
        ack: Option<String>,
        at_least_once: Option<bool>,
    ) -> Result<()> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        let at_least_once = at_least_once.unwrap_or(false);
        let payload = payload.to_vec();
        let key = key.map(|k| Bytes::copy_from_slice(&k));
        if key.is_some() && at_least_once {
            return Err(invalid(
                "atLeastOnce does not carry a routing key yet; publish the \
                 keyed record without it, or drop the key",
            ));
        }
        let inner = self.cluster()?;
        match (key, at_least_once) {
            (Some(key), _) => {
                inner
                    .publish_keyed(&tenant_id, &namespace, &stream, payload, key, ack)
                    .await
            }
            (None, true) => {
                inner
                    .publish_at_least_once(&tenant_id, &namespace, &stream, payload, ack)
                    .await
            }
            (None, false) => {
                inner
                    .publish(&tenant_id, &namespace, &stream, payload, ack)
                    .await
            }
        }
        .map_err(classify)
    }

    /// Subscribe to a stream.
    ///
    /// `start` is `"latest"` (default), `"earliest"`, or an offset — the first
    /// record you have *not* seen, so a resuming client passes the offset it
    /// last handled plus one.
    ///
    /// A subscription reads **one shard**. For a multi-shard stream this is
    /// shard 0; `subscribeSharded` is what reads the whole stream.
    #[napi]
    pub async fn subscribe(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        start: Option<Either<String, BigInt>>,
    ) -> Result<SubscriptionHandle> {
        let start = parse_start(start)?;
        let inner = self.cluster()?;
        let (client, subscription) = inner
            .subscribe_from(&tenant_id, &namespace, &stream, Some(start))
            .await
            .map_err(classify)?;
        Ok(SubscriptionHandle {
            inner: Arc::new(Mutex::new(Some(subscription))),
            closed: watch::Sender::new(false),
            _client: client,
        })
    }

    /// Subscribe to **every** shard of a stream and merge them.
    ///
    /// Per-shard ordering only — that is all a sharded stream has. A lost
    /// shard does not disturb the others, and resumes from its own offset.
    ///
    /// `resume` is a `positions()` result: each listed shard continues at its
    /// own `offset + 1`, and a shard not listed starts wherever `start` says.
    /// Offsets are per shard, so resuming is a map rather than a number — one
    /// number carried across shards replays on all but one of them.
    #[napi]
    pub async fn subscribe_sharded(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        start: Option<Either<String, BigInt>>,
        resume: Option<HashMap<String, BigInt>>,
    ) -> Result<ShardedSubscriptionHandle> {
        let start = parse_start(start)?;
        let resume = parse_positions(resume)?;
        let inner = self.cluster()?;
        let sub = match resume {
            Some(offsets) => {
                inner
                    .resubscribe_sharded(&tenant_id, &namespace, &stream, offsets, Some(start))
                    .await
            }
            None => {
                inner
                    .subscribe_sharded(&tenant_id, &namespace, &stream, Some(start))
                    .await
            }
        }
        .map_err(classify)?;
        Ok(ShardedSubscriptionHandle {
            shards: sub.shards(),
            inner: Arc::new(Mutex::new(Some(sub))),
            closed: watch::Sender::new(false),
        })
    }

    /// How many shards a stream was placed with.
    ///
    /// A subscription reads one shard, so a client that wants the whole stream
    /// needs this to know how many there are — nothing else on the wire says.
    #[napi]
    pub async fn stream_shards(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
    ) -> Result<u32> {
        let client = self.cluster()?.client().await;
        client
            .stream_shards(&tenant_id, &namespace, &stream)
            .await
            .map_err(classify)
    }

    /// Every broker this client would try, seeds included.
    #[napi]
    pub async fn endpoints(&self) -> Result<Vec<String>> {
        Ok(self
            .cluster()?
            .endpoints()
            .await
            .into_iter()
            .map(|addr| addr.to_string())
            .collect())
    }

    /// Store a value, optionally with a time-to-live in seconds.
    #[napi]
    pub async fn cache_put(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        value: Buffer,
        ttl_seconds: Option<f64>,
    ) -> Result<()> {
        // The wire carries a TTL in milliseconds; seconds is what a caller
        // reaches for, so the conversion happens here rather than leaking the
        // unit into the signature.
        let ttl_ms = ttl_seconds.map(|s| (s.max(0.0) * 1000.0) as u64);
        let value = Bytes::copy_from_slice(&value);
        let client = self.cluster()?.client().await;
        client
            .cache_put(&tenant_id, &namespace, &cache, &key, value, ttl_ms)
            .await
            .map_err(classify)
    }

    /// Read a value, or `null` if the key is absent or expired.
    #[napi]
    pub async fn cache_get(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
    ) -> Result<Option<Buffer>> {
        let client = self.cluster()?.client().await;
        let value = client
            .cache_get(&tenant_id, &namespace, &cache, &key)
            .await
            .map_err(classify)?;
        Ok(value.map(|v| v.to_vec().into()))
    }

    /// Remove a key, returning what it held, or `null` if it held nothing.
    ///
    /// The previous value is the return rather than a discard: it is what lets
    /// a caller tell "I deleted something" from "it was already gone" without
    /// a second round trip.
    #[napi]
    pub async fn cache_delete(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
    ) -> Result<Option<Buffer>> {
        let client = self.cluster()?.client().await;
        let previous = client
            .cache_delete(&tenant_id, &namespace, &cache, &key)
            .await
            .map_err(classify)?;
        Ok(previous.map(|v| v.to_vec().into()))
    }

    /// Add to a counter and return its new value. `delta` may be negative.
    #[napi]
    pub async fn counter_add(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        delta: i64,
    ) -> Result<i64> {
        let client = self.cluster()?.client().await;
        client
            .counter_add(&tenant_id, &namespace, &cache, &key, delta)
            .await
            .map_err(classify)
    }

    /// Read a counter's current value, or `null` if it does not exist.
    ///
    /// Absent is not zero: a counter nobody has added to has never been
    /// written, and the distinction is the caller's to make.
    #[napi]
    pub async fn counter_get(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
    ) -> Result<Option<i64>> {
        let client = self.cluster()?.client().await;
        client
            .counter_get(&tenant_id, &namespace, &cache, &key)
            .await
            .map_err(classify)
    }

    /// Watch one cache for changes.
    ///
    /// Exactly one of `key` or `prefix` selects what to watch; a `prefix` of
    /// `""` is every key in the shard.
    ///
    /// `start` is the first cache-log offset you have *not* seen, so a
    /// resuming watcher passes the offset it last handled plus one. `null`
    /// means from now: live changes only.
    ///
    /// `retained` instead delivers each matching key's current value first and
    /// then live changes — join a room and immediately hold the roster.
    /// Mutually exclusive with `start`, whose replay already reconstructs the
    /// state that shortcuts.
    #[napi]
    pub async fn watch_cache(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: Option<String>,
        prefix: Option<String>,
        start: Option<BigInt>,
        retained: Option<bool>,
    ) -> Result<CacheWatchHandle> {
        let filter = match (key, prefix) {
            (Some(key), None) => felix_client::CacheWatchFilter::Key(key),
            (None, Some(prefix)) => felix_client::CacheWatchFilter::Prefix(prefix),
            (None, None) => {
                return Err(invalid("watchCache needs either key or prefix"));
            }
            (Some(_), Some(_)) => {
                return Err(invalid("watchCache takes key or prefix, not both"));
            }
        };
        let retained = retained.unwrap_or(false);
        if retained && start.is_some() {
            return Err(invalid(
                "retained and start are mutually exclusive: a resume already \
                 replays the state a retained start shortcuts",
            ));
        }
        let from = match start {
            Some(offset) => Some(u64_of(offset)?),
            None => None,
        };
        let inner = self.cluster()?;
        let opened = if retained {
            inner
                .watch_cache_retained(&tenant_id, &namespace, &cache, filter)
                .await
        } else {
            inner
                .watch_cache(&tenant_id, &namespace, &cache, filter, from)
                .await
        }
        .map_err(classify)?;
        Ok(CacheWatchHandle {
            resume_offset: opened.resume_offset(),
            resnapshot: opened.resnapshot(),
            retained_count: opened.retained_count(),
            inner: Arc::new(Mutex::new(Some(opened))),
            closed: watch::Sender::new(false),
        })
    }

    /// Claim up to `maxRecords` from a consumer group, waiting up to `waitMs`
    /// for one to appear.
    ///
    /// Each record stays claimed until acked or the visibility timeout lapses,
    /// at which point it is handed to someone else — which is why `attempts`
    /// is worth reading. An empty array is an answer, not a failure: the group
    /// is owed nothing right now.
    ///
    /// The wait is a long poll rather than a spin, because the caller would
    /// otherwise write the spin: a busy loop measures the loop, and a bare
    /// `setTimeout` between polls adds latency to a record that has arrived.
    #[napi]
    pub async fn group_poll(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        max_records: Option<u32>,
        wait_ms: Option<u32>,
    ) -> Result<Vec<GroupRecord>> {
        let max_records = max_records.unwrap_or(32);
        let wait_ms = wait_ms.unwrap_or(0);
        let client = self.cluster()?.client().await;
        let records = client
            .group_poll_wait(
                &tenant_id,
                &namespace,
                &stream,
                shard,
                &group,
                max_records,
                std::time::Duration::from_millis(u64::from(wait_ms)),
            )
            .await
            .map_err(classify)?;
        Ok(records
            .into_iter()
            .map(|r| GroupRecord {
                offset: BigInt::from(r.offset),
                payload: r.payload.to_vec().into(),
                attempts: r.attempts,
            })
            .collect())
    }

    /// Finish a record: it will not be handed out again.
    #[napi]
    pub async fn group_ack(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        let client = self.cluster()?.client().await;
        client
            .group_ack(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// Return a record for redelivery without waiting out its timeout.
    #[napi]
    pub async fn group_nack(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        let client = self.cluster()?.client().await;
        client
            .group_nack(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// Offsets this group gave up on after exhausting their attempts.
    #[napi]
    pub async fn group_dead_letters(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
    ) -> Result<Vec<BigInt>> {
        let client = self.cluster()?.client().await;
        let offsets = client
            .group_dead_letters(&tenant_id, &namespace, &stream, shard, &group)
            .await
            .map_err(classify)?;
        Ok(offsets.into_iter().map(BigInt::from).collect())
    }

    /// Drop a dead-lettered record permanently.
    #[napi]
    pub async fn group_discard(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        let client = self.cluster()?.client().await;
        client
            .group_discard(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// Put a dead-lettered record back into the group for another attempt.
    #[napi]
    pub async fn group_redrive(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        let client = self.cluster()?.client().await;
        client
            .group_redrive(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }
}
