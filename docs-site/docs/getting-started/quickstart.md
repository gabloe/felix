# Quickstart

Get Felix up and running in under 5 minutes.

## Prerequisites

- **Rust:** 1.92.0 or later ([install rustup](https://rustup.rs/))
- **Git:** For cloning the repository
- **Optional:** [Task](https://taskfile.dev/) for convenience commands

## Clone and Build

```bash
# Clone the repository
git clone https://github.com/gabloe/felix.git
cd felix

# Build the entire workspace in release mode
cargo build --workspace --release
```

The release build is recommended for performance testing. Development builds have significantly higher overhead.

## Start the Broker

Run the Felix broker in a terminal:

```bash
cargo run --release -p broker
```

You should see structured log output:

```
2026-01-25T10:00:00.000Z INFO felix_broker: Starting Felix broker
2026-01-25T10:00:00.001Z INFO felix_broker: QUIC listening on 0.0.0.0:5000
2026-01-25T10:00:00.001Z INFO felix_broker: Metrics server on 0.0.0.0:8080
```

The broker is now ready to accept connections!

**Default ports:**

- `5000`: QUIC data plane (publish, subscribe, cache)
- `8080`: Metrics/health endpoint

## Run a Demo

Felix includes several demonstration programs. Open a new terminal and try the pub/sub demo:

```bash
cargo run --release -p broker --bin pubsubdemo
```

This demo:

1. Creates a client connection to the broker
2. Subscribes to a test stream
3. Publishes messages to that stream
4. Displays received events

**Sample output:**

```
Subscriber task started
Published 100 messages
Received event: payload #0
Received event: payload #1
Received event: payload #2
...
Demo completed: 100/100 messages delivered
```

## Try the Cache

Run the cache demonstration:

```bash
cargo run --release -p broker --bin cachedemo
```

This benchmarks cache operations (put, get_hit, get_miss) across various payload sizes and measures latency/throughput.

## Using the Client SDK

Here's a minimal example of using the Felix Rust client:

### Publish and Subscribe

```rust
use felix_client::{Client, ClientConfig};
use felix_wire::AckMode;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Configure client
    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);
    let addr: SocketAddr = "127.0.0.1:5000".parse()?;

    // Connect to broker
    let client = Client::connect(addr, "localhost", config).await?;
    let publisher = client.publisher().await?;

    // Subscribe to a stream
    let mut subscription = client
        .subscribe("my-tenant", "my-namespace", "my-stream")
        .await?;

    // Spawn a task to receive events
    tokio::spawn(async move {
        while let Some(event) = subscription.next_event().await.unwrap() {
            println!("Received: {:?}", event.payload);
        }
    });

    // Publish messages
    for i in 0..10 {
        let payload = format!("Message {}", i);
        publisher
            .publish(
                "my-tenant",
                "my-namespace",
                "my-stream",
                payload.into_bytes(),
                AckMode::None,
            )
            .await?;
    }

    Ok(())
}
```

### Cache Operations

```rust
use bytes::Bytes;
use felix_client::{Client, ClientConfig};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);
    let addr: SocketAddr = "127.0.0.1:5000".parse()?;
    let client = Client::connect(addr, "localhost", config).await?;

    // Put a value with 60-second TTL
    client
        .cache_put(
            "my-tenant",
            "my-namespace",
            "users",
            "user:123",
            Bytes::from_static(b"alice"),
            Some(60_000),
        )
        .await?;

    // Get the value
    if let Some(value) = client
        .cache_get("my-tenant", "my-namespace", "users", "user:123")
        .await?
    {
        println!("Cached value: {:?}", value);
    }

    Ok(())
}
```

!!! note "API Surface"
    The exact client API is evolving. Check `crates/felix-client/src/` for the current implementation. The examples above represent the intended ergonomics.

## Performance Testing

### Latency Benchmark

Run the latency demo with various configurations:

```bash
# Basic run with defaults
cargo run --release -p broker --bin latencydemo

# Custom configuration
cargo run --release -p broker --bin latencydemo -- \
    --binary \
    --fanout 10 \
    --batch 64 \
    --payload 4096 \
    --total 10000 \
    --warmup 500
```

**Parameters:**

- `--binary`: Use binary batch format (higher throughput)
- `--fanout N`: Number of concurrent subscribers
- `--batch N`: Batch size for publishing
- `--payload N`: Payload size in bytes
- `--total N`: Total messages to send
- `--warmup N`: Warmup messages before measurement

### Cache Benchmark

```bash
cargo run --release -p broker --bin cachedemo
```

Measures cache operations at various payload sizes with configurable concurrency.

## Configuration

Felix can be configured via environment variables or a YAML config file.

### Environment Variables

Key performance tuning variables:

```bash
# Event delivery (pub/sub)
export FELIX_EVENT_CONN_POOL=8
export FELIX_EVENT_BATCH_MAX_DELAY_US=250

# Cache operations
export FELIX_CACHE_CONN_POOL=8
export FELIX_CACHE_STREAMS_PER_CONN=4

# Publishing
export FELIX_PUBLISH_CHUNK_BYTES=16384
```

### Config File

Create `/tmp/felix-config.yml`:

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
event_batch_max_events: 64
event_batch_max_delay_us: 250
cache_conn_recv_window: 268435456
```

Run with custom config:

```bash
FELIX_BROKER_CONFIG=/tmp/felix-config.yml cargo run --release -p broker
```

See [Configuration Reference](../reference/configuration.md) for all options.

## Using Task

If you have [Task](https://taskfile.dev/) installed, you can use convenience commands:

```bash
# Build
task build

# Run tests
task test

# Format code
task fmt

# Run linter
task lint

# Run demos
task demo-pubsub
task demo-cache
task demo-latency
```

See `Taskfile.yml` in the repository root for all available tasks.

## Next Steps

Now that you have Felix running:

- **Explore the Architecture:** [System Design](../architecture/system-design.md)
- **Learn the APIs:** [Broker API](../api/broker-api.md)
- **Tune Performance:** [Performance Guide](../features/performance.md)
- **Deploy Properly:** [Deployment Guides](../deployment/local.md)
- **Contribute:** [Development Guide](../development/contributing.md)

## Troubleshooting

### Port Already in Use

If port 5000 or 8080 is in use:

```bash
# Change broker port
export FELIX_QUIC_BIND="0.0.0.0:5001"
export FELIX_METRICS_BIND="0.0.0.0:8081"

cargo run --release -p broker
```

### Build Errors

Ensure you have Rust 1.92.0 or later:

```bash
rustc --version
# Should show: rustc 1.92.0 or higher
```

Update if needed:

```bash
rustup update
```

### Connection Refused

Make sure the broker is running and listening:

```bash
# Check if broker is running
lsof -i :5000

# Check broker logs for errors
```

See [Troubleshooting Guide](../reference/troubleshooting.md) for more help.
