---
title: "Rust Client SDK"
---

`felix-client` is the Rust SDK: publish, subscribe, cache, consumer groups,
and the cluster client, over pooled QUIC connections. This page is the
working reference — setup, configuration, and the patterns that matter in
practice.

## Installation

Add Felix client to your `Cargo.toml`:

```toml
[dependencies]
felix-client = "0.1"
felix-common = "0.1"  # For error types and shared utilities
```

Optional features:

```toml
[dependencies]
felix-client = { version = "0.1", features = ["telemetry"] }
```

**Features**:

- `telemetry`: Enable per-operation timing and frame counters (adds overhead)

## Quick Start

### Basic Publish/Subscribe

```rust
use felix_client::{Client, ClientConfig};
use std::net::SocketAddr;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to broker
    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);
    let addr: SocketAddr = "127.0.0.1:5000".parse()?;
    let client = Client::connect(addr, "localhost", config).await?;
    let publisher = client.publisher().await?;

    // Publish a message
    use felix_wire::AckMode;
    publisher
        .publish(
            "acme",           // tenant_id
            "prod",           // namespace
            "events",         // stream
            b"Hello Felix".to_vec(), // payload
            AckMode::None,
        )
        .await?;

    // Subscribe to stream
    let mut subscription = client.subscribe(
        "acme",
        "prod",
        "events"
    ).await?;

    // Receive events
    while let Some(event) = subscription.next_event().await? {
        println!("Received: {:?}", event.payload);
    }

    Ok(())
}
```

### Basic Cache Operations

```rust
use bytes::Bytes;

// Store value with 60-second TTL
client.cache_put(
    "acme",
    "prod",
    "sessions",
    "user-123",
    Bytes::from_static(b"session-data"),
    Some(60_000)  // TTL in milliseconds
).await?;

// Retrieve value
match client.cache_get("acme", "prod", "sessions", "user-123").await? {
    Some(value) => println!("Found: {:?}", value),
    None => println!("Not found or expired"),
}
```

## Client Configuration

### ClientConfig

```rust
use felix_client::{ClientConfig, PublishSharding};
use std::net::SocketAddr;

let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig {
    // Connection pools
    event_conn_pool: 8,              // Connections for pub/sub
    cache_conn_pool: 8,              // Connections for cache
    publish_conn_pool: 4,            // Connections for publishing
    
    // Streams per connection
    publish_streams_per_conn: 2,     // Publish streams per conn
    cache_streams_per_conn: 4,       // Cache streams per conn
    
    // Publish sharding
    publish_sharding: PublishSharding::HashStream,

    ..ClientConfig::optimized_defaults(quinn)
};

let addr: SocketAddr = "127.0.0.1:5000".parse()?;
let client = Client::connect(addr, "localhost", config).await?;
```

### Configuration Tuning

**Low-latency configuration**:

```rust
let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig {
    event_conn_pool: 4,
    cache_conn_pool: 4,
    publish_streams_per_conn: 1,
    cache_streams_per_conn: 2,
    publish_conn_pool: 2,
    ..ClientConfig::optimized_defaults(quinn)
};
```

**High-throughput configuration**:

```rust
let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig {
    event_conn_pool: 16,
    cache_conn_pool: 16,
    publish_streams_per_conn: 4,
    cache_streams_per_conn: 8,
    publish_conn_pool: 8,
    publish_sharding: PublishSharding::HashStream,
    ..ClientConfig::optimized_defaults(quinn)
};
```

## Publishing

### Single Message Publish

```rust
// Fire-and-forget (no ack)
use felix_wire::AckMode;
let publisher = client.publisher().await?;
publisher
    .publish("acme", "prod", "events", b"message".to_vec(), AckMode::None)
    .await?;

// With acknowledgement
publisher
    .publish("acme", "prod", "events", b"important".to_vec(), AckMode::PerMessage)
    .await?;
```

### Batch Publishing

Publish multiple messages efficiently:

```rust
let messages = vec![
    b"message 1".to_vec(),
    b"message 2".to_vec(),
    b"message 3".to_vec(),
];

publisher.publish_batch(
    "acme",
    "prod",
    "events",
    messages,
    AckMode::PerBatch,
).await?;
```

### Publisher API

For high-throughput publishing, use the `Publisher` API:

```rust
use felix_client::Publisher;
use felix_wire::AckMode;

// Create publisher (uses ClientConfig settings)
let publisher = client.publisher().await?;

// Publish messages
for i in 0..10000 {
    let payload = format!("Event {}", i);
    publisher
        .publish("acme", "prod", "events", payload.into_bytes(), AckMode::None)
        .await?;
}
```

### Publisher Sharding

Control load distribution across worker streams:

```rust
use felix_client::PublishSharding;

// Round-robin: distribute evenly across workers
PublishSharding::RoundRobin

// Hash-based: consistent hashing by stream name
PublishSharding::HashStream
```

**When to use each**:

- **RoundRobin**: Default, good for single stream, evenly distributes load
- **HashStream**: Publishing to multiple streams, keeps stream-specific ordering

### Error Handling

```rust
use felix_common::Error;

use felix_wire::AckMode;
let publisher = client.publisher().await?;
match publisher
    .publish("acme", "prod", "events", b"data".to_vec(), AckMode::PerMessage)
    .await
{
    Ok(()) => println!("Published successfully"),
    Err(Error::UnknownStream { .. }) => {
        eprintln!("Stream doesn't exist");
    }
    Err(Error::Timeout { .. }) => {
        eprintln!("Publish timed out, broker overloaded");
    }
    Err(Error::ConnectionLost) => {
        eprintln!("Connection lost, reconnecting...");
        // Implement retry logic
    }
    Err(e) => eprintln!("Other error: {:?}", e),
}
```

## Subscribing

### Creating Subscriptions

```rust
let mut subscription = client.subscribe("acme", "prod", "events").await?;

// Process events
while let Some(event) = subscription.next_event().await? {
    process_event(event).await?;
}
```

### Event Structure

```rust
use bytes::Bytes;
use std::sync::Arc;

pub struct Event {
    pub tenant_id: Arc<str>,
    pub namespace: Arc<str>,
    pub stream: Arc<str>,
    pub payload: Bytes,
}
```

### Multiple Subscriptions

Handle multiple streams concurrently:

```rust
use tokio::select;

let mut sub1 = client.subscribe("acme", "prod", "orders").await?;
let mut sub2 = client.subscribe("acme", "prod", "inventory").await?;
let mut sub3 = client.subscribe("acme", "staging", "logs").await?;

loop {
    select! {
        event = sub1.next_event() => {
            if let Some(event) = event? {
                handle_order(event).await?;
            } else {
                break;
            }
        }
        event = sub2.next_event() => {
            if let Some(event) = event? {
                handle_inventory(event).await?;
            } else {
                break;
            }
        }
        event = sub3.next_event() => {
            if let Some(event) = event? {
                handle_log(event).await?;
            } else {
                break;
            }
        }
    }
}
```

### Async Event Processing

Avoid blocking the subscription loop:

```rust
// Bad: blocks subscription loop
while let Some(event) = subscription.next_event().await? {
    expensive_processing(event).await?;  // Blocks next event
}

// Good: spawn task for processing
while let Some(event) = subscription.next_event().await? {
    tokio::spawn(async move {
        expensive_processing(event).await.ok();
    });
}

// Better: use bounded channel for backpressure
let (tx, mut rx) = mpsc::channel(100);

tokio::spawn(async move {
    while let Some(event) = rx.recv().await {
        expensive_processing(event).await.ok();
    }
});

while let Some(event) = subscription.next_event().await? {
    tx.send(event).await.ok();
}
```

### Subscription Lifecycle

```rust
// Subscribe
let mut sub = client.subscribe("acme", "prod", "events").await?;

// Process events
for _ in 0..100 {
    if let Some(event) = sub.next_event().await? {
        process(event);
    }
}

// Drop subscription to close
drop(sub);
```

## Cache Operations

### Put and Get

```rust
// Put with TTL
client.cache_put(
    "acme",               // tenant
    "prod",               // namespace
    "sessions",           // cache
    "user-abc",           // key
    session_data,         // value (Bytes)
    Some(3600_000)        // 1 hour TTL
).await?;

// Get
match client.cache_get("acme", "prod", "sessions", "user-abc").await? {
    Some(data) => {
        let session: Session = deserialize(&data)?;
        // Use session
    }
    None => {
        // Session expired or doesn't exist
        return Err("Invalid session");
    }
}
```

### Without TTL

```rust
// Store permanently (until evicted or restart)
client
    .cache_put("acme", "prod", "config", "app-settings", config_data, None)
    .await?;
```

### Delete

```rust
// Answers with the value that was removed, or `None` if the key was not there —
// so a caller can tell a delete that did something from one that did not.
match client.cache_delete("acme", "prod", "sessions", "user-abc").await? {
    Some(removed) => audit_log("session revoked", removed),
    None => { /* already gone, or never there */ }
}
```

Needs a broker advertising `FEATURE_CACHE_DELETE`; the client returns an error
rather than probing, because an unrecognised message type ends the broker's
control loop.

### Watch

Subscribe to changes for one key or key prefix. Each change carries its
cache-log offset, so a watch can be resumed exactly where it left off:

```rust
use felix_client::{CacheWatchFilter, CacheWatchItem};

let mut watch = client
    .watch_cache(
        "acme",
        "prod",
        "sessions",
        CacheWatchFilter::Prefix("user:".into()),
        None,          // from now; Some(offset) resumes gaplessly
    )
    .await?;

let mut checkpoint = watch.resume_offset();
while let Some(item) = watch.recv().await {
    match item {
        CacheWatchItem::Change(change) => {
            match &change.value {
                Some(value) => apply_update(&change.key, value),
                None => remove(&change.key),   // a delete
            }
            checkpoint = change.offset + 1;
        }
        CacheWatchItem::Lagged { resume_from } => {
            // The watch fell behind and was ended; re-watch from
            // `resume_from` to replay everything missed.
            checkpoint = resume_from;
            break;
        }
    }
}
```

A resume whose history compaction has collapsed begins with each matching
key's current value instead, and `watch.resnapshot()` says so. Needs a broker
advertising `FEATURE_CACHE_WATCH` — only brokers whose cache is log-backed do.
See [Cache Features](/felix/features/cache/#7-keyed-watch) for the full
contract.

### Retained Watch

Start from current state instead of from now: each matching key's current
value first, then live changes — the join primitive for presence and state
sync:

```rust
let mut watch = client
    .watch_cache_retained(
        "acme",
        "prod",
        "presence",
        CacheWatchFilter::Prefix("room:7:".into()),
    )
    .await?;

// The state phase is exactly this many changes; 0 means empty, definitively.
let state_size = watch.retained_count().expect("retained watches report a count");
```

Needs `FEATURE_CACHE_WATCH_RETAINED`, a separate bit so an older watch-capable
broker is never asked for state it would silently not deliver. Mutually
exclusive with `from_offset` — a resume already replays what a retained start
shortcuts.

### Counters

```rust
// Apply a delta and learn the sum including it, in one round trip.
let after = client.counter_add("acme", "prod", "limits", "user:42:reqs", 1).await?;

// Read; None means never written — distinct from a sum of zero.
let sum = client.counter_get("acme", "prod", "metrics", "page:home").await?;
```

Scoped and routed like cache keys, stored beside the cache; durable and
replicated with the shard. At-least-once: a retry after a lost ack counts
twice. Needs a broker advertising `FEATURE_COUNTERS` (durable brokers only).

### Concurrent Cache Operations

Pipeline multiple cache operations:

```rust
use futures::future::join_all;

// Issue multiple requests concurrently
let futures = (0..10).map(|i| {
    let key = format!("key-{}", i);
    client.cache_get("acme", "prod", "data", &key)
});

let results = join_all(futures).await;

for result in results {
    if let Ok(Some(value)) = result {
        process(value);
    }
}
```

### Cache Namespacing

Cache keys are scoped to prevent collisions:

```rust
// These are independent entries
client
    .cache_put("acme", "prod", "sessions", "user-123", data1, ttl)
    .await?;
client
    .cache_put("acme", "prod", "profiles", "user-123", data2, ttl)
    .await?;
client
    .cache_put("acme", "prod", "temp", "user-123", data3, ttl)
    .await?;
```

## Consumer Groups

The other way to read a stream. `subscribe` pushes every record to every
subscriber; a **consumer group** hands each record to one consumer and takes it
back if nobody says it was handled.

Records are **pulled**, because only the consumer knows when it has capacity:

```rust
loop {
    // Waits up to five seconds for work rather than spinning on empty polls.
    let records = client
        .group_poll_wait("acme", "prod", "jobs", 0, "fulfilment", 32, Duration::from_secs(5))
        .await?;

    for record in records {
        // `attempts` is 1 on a first delivery and higher on a redelivery, so a
        // consumer can treat a retry differently. Absent means the broker did
        // not report it, which is not the same as a first attempt.
        match handle(&record.payload, record.attempts) {
            Ok(()) => client.group_ack("acme", "prod", "jobs", 0, "fulfilment", record.offset).await?,
            // Hand it back for immediate redelivery rather than waiting out the
            // visibility timeout.
            Err(_) => client.group_nack("acme", "prod", "jobs", 0, "fulfilment", record.offset).await?,
        }
    }
}
```

An empty batch means nothing was available. **It is an answer, not an error.**

### Dead letters

Past `FELIX_GROUP_MAX_ATTEMPTS` a record is dead-lettered, so one poison record
cannot stall the queue behind it. These need `FEATURE_GROUP_DEAD_LETTERS`, a
separate bit from `FEATURE_CONSUMER_GROUP`:

```rust
let offsets = client.group_dead_letters("acme", "prod", "jobs", 0, "fulfilment").await?;
for offset in offsets {
    if worth_retrying(offset) {
        client.group_redrive("acme", "prod", "jobs", 0, "fulfilment", offset).await?;
    } else {
        client.group_discard("acme", "prod", "jobs", 0, "fulfilment", offset).await?;
    }
}
```

A dead letter is a **pointer, not a copy**: the record is still in the stream's
log at that offset, readable by an ordinary replay.

### What a group needs

- **Durable storage on the broker.** A group's position lives in a log, so a
  broker without `FELIX_DURABLE_STORAGE_DIR` serves no groups and does not
  advertise `FEATURE_CONSUMER_GROUP`.
- **The shard's leader.** A poll is refused rather than forwarded, because
  relaying would put the claim and the acknowledgement on different brokers.
- **Idempotent handling.** This is at-least-once: a crash after handling and
  before acknowledging is indistinguishable from a crash before handling, so the
  record comes back.

## Clusters

`Client` talks to one broker. `ClusterClient` follows the cluster — it takes
several addresses, learns the rest, reconnects when the broker it is using goes
away, and follows a redirect to whichever broker owns a shard.

```rust
let client = Arc::new(ClusterClient::connect(&seeds, "localhost", config).await?);

// Every shard of a multi-shard stream, merged into one channel.
let mut subscription = client
    .subscribe_sharded("acme", "prod", "orders", Some(StartPosition::Earliest))
    .await?;

while let Some(item) = subscription.next().await {
    match item {
        ShardEvent::Record { shard, event } => handle(shard, event),
        ShardEvent::ShardLost { shard, error } => warn!(shard, %error, "shard down"),
        ShardEvent::ShardRecovered { shard } => info!(shard, "shard back"),
    }
}
```

This needs a broker advertising `FEATURE_STREAM_SHARDS`, because the shard count
comes from asking one, and `FEATURE_REDIRECT` to follow each shard to its owner.

**Ordering is per shard and nothing more** — merging cannot restore an order
that never existed. Resumption is a vector: `positions()` returns one offset per
shard, and `resubscribe_sharded` takes it back. See
[Multi-node client](https://github.com/gabloe/felix/blob/main/docs/multi-node-client.md)
for the full contract.

## In-Process Client

For testing and embedded scenarios, use the in-process client:

```rust
use bytes::Bytes;
use felix_client::InProcessClient;
use felix_broker::Broker;

// Create embedded broker
let broker = Broker::new(broker_config).await?;

// Create in-process client (no network)
let client = InProcessClient::new(broker.clone());

// Same API as network client
client
    .publish("acme", "prod", "test", Bytes::from_static(b"data"))
    .await?;
let mut sub = client.subscribe("acme", "prod", "test").await?;
```

**Use cases**:

- Unit tests
- Integration tests
- Embedded applications
- Benchmarking without network overhead

## Connection Management

### Automatic Reconnection

Clients should implement reconnection logic:

```rust
use std::net::SocketAddr;

async fn connect_with_retry(
    addr: SocketAddr,
    server_name: &str,
    config: ClientConfig,
    max_retries: u32,
) -> Result<Client> {
    for attempt in 0..max_retries {
        match Client::connect(addr, server_name, config.clone()).await {
            Ok(client) => return Ok(client),
            Err(e) if attempt < max_retries - 1 => {
                let delay = Duration::from_millis(100 * 2u64.pow(attempt));
                eprintln!("Connection failed, retrying in {:?}: {}", delay, e);
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}
```

### Health Monitoring

Check connection health:

```rust
async fn monitor_connection(client: &Client) -> Result<()> {
    loop {
        match client.health_check().await {
            Ok(()) => {
                // Connection healthy
            }
            Err(e) => {
                eprintln!("Health check failed: {:?}", e);
                // Implement reconnection
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
```

## Telemetry

### Enabling Telemetry

Compile with telemetry feature:

```toml
[dependencies]
felix-client = { version = "0.1", features = ["telemetry"] }
```

### Collecting Metrics

```rust
use felix_client::{frame_counters_snapshot, reset_frame_counters};

// Get current frame counters
let counters = frame_counters_snapshot();
println!("Publish frames: {}", counters.publish_frames);
println!("Event frames: {}", counters.event_frames);
println!("Cache put frames: {}", counters.cache_put_frames);
println!("Cache get frames: {}", counters.cache_get_frames);

// Reset counters
reset_frame_counters();
```

### Timing Measurements

```rust
use felix_client::timings;

// Get timing snapshots
let publish_timings = timings::publish_timings_snapshot();
println!("Publish p50: {:?}", publish_timings.p50);
println!("Publish p99: {:?}", publish_timings.p99);

let subscribe_timings = timings::subscribe_timings_snapshot();
println!("Event delivery p50: {:?}", subscribe_timings.p50);
```

:::caution[Telemetry Overhead]
Telemetry adds measurable overhead (5-15% in high-throughput workloads). Use only for debugging and profiling, not in production hot paths unless necessary.
:::
## Patterns

### Connection Pooling

```rust
// Good: reuse client across application
use felix_wire::AckMode;
use std::net::SocketAddr;

lazy_static! {
    static ref FELIX_CLIENT: Client = {
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::optimized_defaults(quinn);
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(Client::connect(addr, "localhost", config))
            .unwrap()
    };
}

// Use shared client
let publisher = FELIX_CLIENT.publisher().await?;
publisher
    .publish("acme", "prod", "events", data.to_vec(), AckMode::None)
    .await?;
```

### Error Recovery

```rust
async fn publish_with_retry(
    client: &Client,
    tenant: &str,
    namespace: &str,
    stream: &str,
    data: &[u8],
    max_retries: u32
) -> Result<()> {
    use felix_wire::AckMode;
    let publisher = client.publisher().await?;
    for attempt in 0..max_retries {
        match publisher
            .publish(tenant, namespace, stream, data.to_vec(), AckMode::PerMessage)
            .await
        {
            Ok(()) => return Ok(()),
            Err(e) if is_retriable(&e) && attempt < max_retries - 1 => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

fn is_retriable(error: &felix_common::Error) -> bool {
    matches!(error,
        felix_common::Error::Timeout { .. } |
        felix_common::Error::ConnectionLost
    )
}
```

### Resource Cleanup

```rust
// Subscriptions are cleaned up on drop
{
    let mut sub = client.subscribe("acme", "prod", "events").await?;
    // Process events...
}  // Automatic close on drop
```

### Batching for Throughput

```rust
use felix_wire::AckMode;
use tokio::time::{interval, Duration};

async fn batching_publisher(client: &Client) -> Result<()> {
    let publisher = client.publisher().await?;
    let mut batch = Vec::new();
    let mut ticker = interval(Duration::from_millis(10));
    
    loop {
        select! {
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    publisher
                        .publish_batch("acme", "prod", "events", batch.clone(), AckMode::PerBatch)
                        .await?;
                    batch.clear();
                }
            }
            msg = receive_message() => {
                batch.push(msg);
                if batch.len() >= 64 {
                    publisher
                        .publish_batch("acme", "prod", "events", batch.clone(), AckMode::PerBatch)
                        .await?;
                    batch.clear();
                }
            }
        }
    }
}
```

## Testing

### Unit Tests with In-Process Client

```rust
#[tokio::test]
async fn test_publish_subscribe() {
    use bytes::Bytes;

    let broker = Broker::new(BrokerConfig::default()).await.unwrap();
    let client = InProcessClient::new(broker);
    
    // Subscribe first
    let mut sub = client.subscribe("test", "ns", "stream").await.unwrap();
    
    // Publish
    client
        .publish("test", "ns", "stream", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    
    // Receive
    let event = sub.recv().await.unwrap();
    assert_eq!(event, Bytes::from_static(b"hello"));
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_cache_ttl() {
    use std::net::SocketAddr;

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);
    let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
    let client = Client::connect(addr, "localhost", config).await.unwrap();
    
    // Store with 100ms TTL
    use bytes::Bytes;
    client
        .cache_put("test", "default", "cache", "key", Bytes::from_static(b"value"), Some(100))
        .await
        .unwrap();
    
    // Immediately readable
    assert_eq!(
        client
            .cache_get("test", "default", "cache", "key")
            .await
            .unwrap(),
        Some(b"value".to_vec())
    );
    
    // Wait for expiration
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    // Should be expired
    assert_eq!(
        client
            .cache_get("test", "default", "cache", "key")
            .await
            .unwrap(),
        None
    );
}
```

## Performance in one paragraph

Reuse one client (its pools are the expensive part), batch publishes when
latency permits, pipeline cache requests, and keep the subscription loop
non-blocking — spawn slow work instead of stalling the reader. Everything
else is a knob to turn off a measurement; see
[Benchmarks](/felix/features/benchmarks/).

## API Reference Summary

| Operation | Method | Use Case |
|-----------|--------|----------|
| Single publish | `Publisher::publish()` | Low-rate events |
| Batch publish | `Publisher::publish_batch()` | High-throughput |
| Subscribe | `subscribe()` | Event consumption |
| Cache put | `cache_put()` | Store with TTL |
| Cache get | `cache_get()` | Retrieve value |
| Publisher | `Client::publisher()` | Streaming publish |
| In-process | `InProcessClient::new()` | Testing, embedded |

For complete API documentation, see the [rustdoc](https://docs.rs/felix-client).
