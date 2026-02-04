# Rust Client SDK

The Felix Rust client SDK (`felix-client`) provides ergonomic, high-performance APIs for publishing, subscribing, and caching over QUIC. This document covers installation, configuration, and usage patterns for building applications with Felix.

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

!!! warning "Telemetry Overhead"
    Telemetry adds measurable overhead (5-15% in high-throughput workloads). Use only for debugging and profiling, not in production hot paths unless necessary.

## Best Practices

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

## Performance Tips

1. **Tune batching** for your workload (events per batch + flush delay)
2. **Batch publishes** when latency permits (10-100x improvement)
3. **Pool connections** appropriately for your workload
4. **Pipeline cache requests** to amortize round-trip latency
5. **Don't block subscription loops** with slow processing
6. **Size buffers** to match variance in processing latency
7. **Enable telemetry** only for debugging, not production
8. **Reuse clients** across requests (connection pools are expensive to create)

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
