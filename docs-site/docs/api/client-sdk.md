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
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to broker
    let client = Client::connect(
        "https://localhost:5000",
        ClientConfig::default()
    ).await?;

    // Publish a message
    client.publish(
        "acme",           // tenant_id
        "prod",           // namespace
        "events",         // stream
        b"Hello Felix"    // payload
    ).await?;

    // Subscribe to stream
    let mut subscription = client.subscribe(
        "acme",
        "prod",
        "events"
    ).await?;

    // Receive events
    while let Some(event) = subscription.next().await {
        println!("Received: {:?}", event.payload);
    }

    Ok(())
}
```

### Basic Cache Operations

```rust
// Store value with 60-second TTL
client.cache_put(
    "sessions",
    "user-123",
    b"session-data",
    Some(60_000)  // TTL in milliseconds
).await?;

// Retrieve value
match client.cache_get("sessions", "user-123").await? {
    Some(value) => println!("Found: {:?}", value),
    None => println!("Not found or expired"),
}
```

## Client Configuration

### ClientConfig

```rust
use felix_client::ClientConfig;

let config = ClientConfig {
    // Connection pools
    event_conn_pool: 8,              // Connections for pub/sub
    cache_conn_pool: 8,              // Connections for cache
    publish_conn_pool: 4,            // Connections for publishing
    
    // Streams per connection
    publish_streams_per_conn: 2,     // Publish streams per conn
    cache_streams_per_conn: 4,       // Cache streams per conn
    
    // Buffering
    event_buffer_size: 1024,         // Client-side event buffer
    publish_queue_size: 1024,        // Publish queue bound
    cache_queue_size: 1024,          // Cache request queue
    
    // Binary mode
    binary_mode_enabled: true,       // Enable binary batches
    binary_mode_threshold_bytes: 512, // Min size for binary
    
    // TLS
    tls_skip_verify: false,          // Skip cert validation (dev only!)
    tls_ca_cert_path: None,          // Custom CA cert
    
    // Timeouts
    connect_timeout_ms: 5000,
    idle_timeout_ms: 30000,
    
    ..Default::default()
};

let client = Client::connect("https://broker:5000", config).await?;
```

### Configuration Tuning

**Low-latency configuration**:

```rust
let config = ClientConfig {
    event_conn_pool: 4,
    cache_conn_pool: 4,
    publish_streams_per_conn: 1,
    cache_streams_per_conn: 2,
    event_buffer_size: 512,
    binary_mode_enabled: false,  // JSON for simplicity
    ..Default::default()
};
```

**High-throughput configuration**:

```rust
let config = ClientConfig {
    event_conn_pool: 16,
    cache_conn_pool: 16,
    publish_streams_per_conn: 4,
    cache_streams_per_conn: 8,
    event_buffer_size: 4096,
    publish_queue_size: 4096,
    binary_mode_enabled: true,
    binary_mode_threshold_bytes: 256,
    ..Default::default()
};
```

## Publishing

### Single Message Publish

```rust
// Fire-and-forget (no ack)
client.publish("acme", "prod", "events", b"message").await?;

// With acknowledgement
client.publish_with_ack("acme", "prod", "events", b"important").await?;
```

### Batch Publishing

Publish multiple messages efficiently:

```rust
let messages = vec![
    b"message 1".to_vec(),
    b"message 2".to_vec(),
    b"message 3".to_vec(),
];

client.publish_batch(
    "acme",
    "prod",
    "events",
    messages
).await?;
```

### Publisher API

For high-throughput publishing, use the `Publisher` API:

```rust
use felix_client::Publisher;

// Create publisher with custom config
let publisher = client.create_publisher(
    "acme",
    "prod",
    "events",
    PublisherConfig {
        sharding: PublishSharding::HashStream,  // or RoundRobin
        ack_mode: AckMode::None,
        binary_enabled: true,
        ..Default::default()
    }
).await?;

// Publish with automatic batching
for i in 0..10000 {
    let payload = format!("Event {}", i);
    publisher.publish(payload.as_bytes()).await?;
}

// Flush any pending batches
publisher.flush().await?;
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

match client.publish("acme", "prod", "events", b"data").await {
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
while let Some(event) = subscription.next().await {
    process_event(event).await?;
}
```

### Event Structure

```rust
pub struct Event {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub payload: Vec<u8>,
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
        Some(event) = sub1.next() => handle_order(event).await?,
        Some(event) = sub2.next() => handle_inventory(event).await?,
        Some(event) = sub3.next() => handle_log(event).await?,
        else => break,
    }
}
```

### Async Event Processing

Avoid blocking the subscription loop:

```rust
// Bad: blocks subscription loop
while let Some(event) = subscription.next().await {
    expensive_processing(event).await?;  // Blocks next event
}

// Good: spawn task for processing
while let Some(event) = subscription.next().await {
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

while let Some(event) = subscription.next().await {
    tx.send(event).await.ok();
}
```

### Subscription Lifecycle

```rust
// Subscribe
let mut sub = client.subscribe("acme", "prod", "events").await?;

// Process events
for _ in 0..100 {
    if let Some(event) = sub.next().await {
        process(event);
    }
}

// Explicitly close (or drop to auto-close)
sub.close().await?;
```

## Cache Operations

### Put and Get

```rust
// Put with TTL
client.cache_put(
    "sessions",           // cache namespace
    "user-abc",           // key
    session_data,         // value
    Some(3600_000)        // 1 hour TTL
).await?;

// Get
match client.cache_get("sessions", "user-abc").await? {
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
client.cache_put("config", "app-settings", config_data, None).await?;
```

### Concurrent Cache Operations

Pipeline multiple cache operations:

```rust
use futures::future::join_all;

// Issue multiple requests concurrently
let futures = (0..10).map(|i| {
    let key = format!("key-{}", i);
    client.cache_get("data", &key)
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
client.cache_put("sessions", "user-123", data1, ttl).await?;
client.cache_put("profiles", "user-123", data2, ttl).await?;
client.cache_put("temp", "user-123", data3, ttl).await?;
```

## In-Process Client

For testing and embedded scenarios, use the in-process client:

```rust
use felix_client::InProcessClient;
use felix_broker::Broker;

// Create embedded broker
let broker = Broker::new(broker_config).await?;

// Create in-process client (no network)
let client = InProcessClient::new(broker.clone());

// Same API as network client
client.publish("acme", "prod", "test", b"data").await?;
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
async fn connect_with_retry(
    url: &str,
    config: ClientConfig,
    max_retries: u32
) -> Result<Client> {
    for attempt in 0..max_retries {
        match Client::connect(url, config.clone()).await {
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
lazy_static! {
    static ref FELIX_CLIENT: Client = {
        let config = ClientConfig::default();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(Client::connect("https://broker:5000", config))
            .unwrap()
    };
}

// Use shared client
FELIX_CLIENT.publish("acme", "prod", "events", data).await?;
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
    for attempt in 0..max_retries {
        match client.publish(tenant, namespace, stream, data).await {
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
// Subscriptions are cleaned up on drop, but explicit close is better
{
    let mut sub = client.subscribe("acme", "prod", "events").await?;
    // Process events...
    sub.close().await?;  // Explicit close
}  // Or automatic close on drop
```

### Batching for Throughput

```rust
use tokio::time::{interval, Duration};

async fn batching_publisher(client: &Client) -> Result<()> {
    let mut batch = Vec::new();
    let mut ticker = interval(Duration::from_millis(10));
    
    loop {
        select! {
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    client.publish_batch("acme", "prod", "events", batch.clone()).await?;
                    batch.clear();
                }
            }
            msg = receive_message() => {
                batch.push(msg);
                if batch.len() >= 64 {
                    client.publish_batch("acme", "prod", "events", batch.clone()).await?;
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
    let broker = Broker::new(BrokerConfig::default()).await.unwrap();
    let client = InProcessClient::new(broker);
    
    // Subscribe first
    let mut sub = client.subscribe("test", "ns", "stream").await.unwrap();
    
    // Publish
    client.publish("test", "ns", "stream", b"hello").await.unwrap();
    
    // Receive
    let event = sub.next().await.unwrap();
    assert_eq!(event.payload, b"hello");
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_cache_ttl() {
    let client = Client::connect("https://localhost:5000", Default::default())
        .await
        .unwrap();
    
    // Store with 100ms TTL
    client.cache_put("test", "key", b"value", Some(100)).await.unwrap();
    
    // Immediately readable
    assert_eq!(
        client.cache_get("test", "key").await.unwrap(),
        Some(b"value".to_vec())
    );
    
    // Wait for expiration
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    // Should be expired
    assert_eq!(client.cache_get("test", "key").await.unwrap(), None);
}
```

## Performance Tips

1. **Use binary mode** for payloads > 512 bytes
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
| Single publish | `publish()` | Low-rate events |
| Batch publish | `publish_batch()` | High-throughput |
| Subscribe | `subscribe()` | Event consumption |
| Cache put | `cache_put()` | Store with TTL |
| Cache get | `cache_get()` | Retrieve value |
| Publisher | `create_publisher()` | Streaming publish |
| In-process | `InProcessClient::new()` | Testing, embedded |

For complete API documentation, see the [rustdoc](https://docs.rs/felix-client).
