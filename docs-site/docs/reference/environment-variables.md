# Environment Variables Reference

Complete reference for all Felix environment variables, organized by category.

## Overview

Felix uses environment variables prefixed with `FELIX_` for configuration. These variables provide quick overrides without modifying config files.

**Priority**: Environment variables override built-in defaults but are overridden by YAML config files.

## Network and Binding

### `FELIX_QUIC_BIND`

**Description**: QUIC listener bind address and port (UDP).

**Type**: `SocketAddr` format

**Default**: `0.0.0.0:5000`

**Example**:
```bash
export FELIX_QUIC_BIND="0.0.0.0:5000"
export FELIX_QUIC_BIND="127.0.0.1:5001"  # Localhost only
export FELIX_QUIC_BIND="10.0.1.5:5000"   # Specific interface
```

**Notes**:
- Must be a valid IP:Port combination
- UDP port for QUIC transport
- Use `0.0.0.0` to bind all interfaces

### `FELIX_BROKER_METRICS_BIND`

**Description**: HTTP metrics and health endpoint bind address.

**Type**: `SocketAddr` format

**Default**: `0.0.0.0:8080`

**Example**:
```bash
export FELIX_BROKER_METRICS_BIND="0.0.0.0:8080"
```

**Exposed endpoints**:
- `/healthz`: Health check
- `/metrics`: Prometheus metrics (when telemetry enabled)

## Control Plane

### `FELIX_CONTROLPLANE_URL`

**Description**: Control plane base URL for metadata synchronization.

**Type**: String (URL)

**Default**: None

**Example**:
```bash
export FELIX_CONTROLPLANE_URL="http://felix-controlplane:8443"
export FELIX_CONTROLPLANE_URL="https://cp.example.com:8443"
```

**Usage**:
- Optional for single-node deployments
- Required for multi-broker clusters
- Include scheme (`http://` or `https://`)

### `FELIX_CONTROLPLANE_SYNC_INTERVAL_MS`

**Description**: Control plane polling interval in milliseconds.

**Type**: Unsigned integer

**Default**: `2000`

**Example**:
```bash
export FELIX_CONTROLPLANE_SYNC_INTERVAL_MS="2000"
export FELIX_CONTROLPLANE_SYNC_INTERVAL_MS="500"   # Fast polling
export FELIX_CONTROLPLANE_SYNC_INTERVAL_MS="10000" # Slow polling
```

## Publishing Configuration

### `FELIX_ACK_ON_COMMIT`

**Description**: Enable publish acknowledgements after commit.

**Type**: Boolean

**Default**: `false`

**Accepted values**: `1`, `true`, `yes` (case-insensitive) = enabled

**Example**:
```bash
export FELIX_ACK_ON_COMMIT="true"
export FELIX_ACK_ON_COMMIT="1"
export FELIX_ACK_ON_COMMIT="yes"
```

**Trade-off**:
- `false`: Fire-and-forget, lower latency
- `true`: Explicit acks, higher latency guarantee

### `FELIX_MAX_FRAME_BYTES`

**Description**: Maximum frame size accepted on QUIC streams.

**Type**: Positive integer (bytes)

**Default**: `16777216` (16 MiB)

**Example**:
```bash
export FELIX_MAX_FRAME_BYTES="16777216"    # 16 MiB
export FELIX_MAX_FRAME_BYTES="33554432"    # 32 MiB
export FELIX_MAX_FRAME_BYTES="8388608"     # 8 MiB
```

**Notes**:
- Value of `0` uses default
- Affects max message size
- Must align with client configuration

### `FELIX_PUBLISH_QUEUE_WAIT_MS`

**Description**: Maximum wait time when publish queue is full.

**Type**: Positive integer (milliseconds)

**Default**: `2000`

**Example**:
```bash
export FELIX_PUBLISH_QUEUE_WAIT_MS="2000"
export FELIX_PUBLISH_QUEUE_WAIT_MS="5000"  # More patient
export FELIX_PUBLISH_QUEUE_WAIT_MS="500"   # Fail fast
```

**Behavior**:
- Publisher blocks if queue full
- Returns error after timeout
- Backpressure mechanism

### `FELIX_ACK_WAIT_TIMEOUT_MS`

**Description**: Maximum wait time for ack-on-commit completion.

**Type**: Positive integer (milliseconds)

**Default**: `2000`

**Example**:
```bash
export FELIX_ACK_WAIT_TIMEOUT_MS="2000"
```

**Notes**:
- Only relevant when `FELIX_ACK_ON_COMMIT=true`
- Publisher gets error if timeout exceeded

## Event Batching and Delivery

### `FELIX_EVENT_BATCH_MAX_EVENTS`

**Description**: Maximum events per subscription batch frame.

**Type**: Positive integer (count)

**Default**: `64`

**Example**:
```bash
export FELIX_EVENT_BATCH_MAX_EVENTS="64"
export FELIX_EVENT_BATCH_MAX_EVENTS="1"    # No batching
export FELIX_EVENT_BATCH_MAX_EVENTS="256"  # Large batches
```

**Tuning**:
- Small values (1-16): Low latency
- Medium values (32-64): Balanced
- Large values (128-256): High throughput

### `FELIX_EVENT_BATCH_MAX_BYTES`

**Description**: Maximum bytes per subscription batch frame.

**Type**: Positive integer (bytes)

**Default**: `65536` (64 KiB)

**Example**:
```bash
export FELIX_EVENT_BATCH_MAX_BYTES="65536"    # 64 KiB (default)
export FELIX_EVENT_BATCH_MAX_BYTES="524288"   # 512 KiB
export FELIX_EVENT_BATCH_MAX_BYTES="1048576"  # 1 MiB
```

**Notes**:
- Batch sent when event count OR byte limit reached
- Adjust based on typical message size

### `FELIX_EVENT_BATCH_MAX_DELAY_US`

**Description**: Maximum delay before flushing batch (microseconds).

**Type**: Unsigned integer

**Default**: `250`

**Example**:
```bash
export FELIX_EVENT_BATCH_MAX_DELAY_US="250"
export FELIX_EVENT_BATCH_MAX_DELAY_US="50"    # Ultra-low latency
export FELIX_EVENT_BATCH_MAX_DELAY_US="1000"  # Prioritize batching
export FELIX_EVENT_BATCH_MAX_DELAY_US="5000"  # Maximum batching
```

**Critical tuning parameter**:
- Lower: Reduced latency, more frequent sends
- Higher: Better batching, higher latency
- Typical range: 50-1000 microseconds

### `FELIX_FANOUT_BATCH`

**Description**: Subscribers to process in parallel during fanout.

**Type**: Positive integer (count)

**Default**: `64`

**Example**:
```bash
export FELIX_FANOUT_BATCH="64"
export FELIX_FANOUT_BATCH="128"  # High fanout
export FELIX_FANOUT_BATCH="16"   # Low fanout
```

**Recommendations**:
- Match to typical subscriber count
- Higher values for high-fanout streams
- Lower values reduce concurrency overhead

### Event Frame Encoding

Subscription event delivery uses binary `EventBatch` frames by default.

### `FELIX_SUBSCRIBER_QUEUE_CAPACITY`

**Description**: Per-subscriber queue capacity in broker core.

**Type**: Positive integer (count)

**Default**: `4096`

```bash
export FELIX_SUBSCRIBER_QUEUE_CAPACITY="4096"
# Alias (same behavior):
export FELIX_SUB_QUEUE_CAPACITY="4096"
```

### `FELIX_SUB_QUEUE_POLICY`

**Description**: Backpressure policy when broker subscriber queues are full.

**Type**: Enum (`block`, `drop_new`, `drop_old`)

**Default**: `block`

```bash
export FELIX_SUB_QUEUE_POLICY="drop_new"
```

**Policy notes**:
- `block`: await queue space (strongest delivery guarantee, may reduce publish throughput).
- `drop_new`: drop incoming item when queue is full.
- `drop_old`: currently emulated with `drop_new` semantics and tracked separately.

### `FELIX_SUB_SINGLE_WRITER_PER_CONN`

**Description**: Keep all subscribers on the same QUIC connection on one writer lane.

**Type**: Boolean (`1|true|yes` to enable)

**Default**: `true`

```bash
export FELIX_SUB_SINGLE_WRITER_PER_CONN="true"
```

### `FELIX_SUB_WRITER_LANES`

**Description**: Requested outbound subscriber writer lanes.

**Type**: Positive integer (count)

**Default**: `4`

```bash
export FELIX_SUB_WRITER_LANES="4"
```

### `FELIX_SUB_LANE_QUEUE_DEPTH`

**Description**: Queue depth per outbound writer lane.

**Type**: Positive integer (count)

**Default**: `8192`

```bash
export FELIX_SUB_LANE_QUEUE_DEPTH="8192"
```

### `FELIX_MAX_SUB_WRITER_LANES`

**Description**: Safety clamp for writer lanes.

**Type**: Positive integer (count)

**Default**: `8`

```bash
export FELIX_MAX_SUB_WRITER_LANES="8"
```

### `FELIX_SUB_LANE_SHARD`

**Description**: Outbound lane sharding policy.

**Type**: Enum (`auto`, `subscriber_id_hash`, `connection_id_hash`, `round_robin_pin`)

**Default**: `auto`

```bash
export FELIX_SUB_LANE_SHARD="auto"
```

**Policy notes**:
- `auto`: prefers connection-aware routing when connection id is known.
- `subscriber_id_hash`: stable by subscriber id.
- `connection_id_hash`: stable by connection id.
- `round_robin_pin`: pinned RR assignment per subscriber.

## Cache Configuration

### `FELIX_CACHE_CONN_POOL`

**Description**: Number of QUIC connections in cache pool (client-side).

**Type**: Positive integer (count)

**Default**: `8`

**Example**:
```bash
export FELIX_CACHE_CONN_POOL="8"
export FELIX_CACHE_CONN_POOL="16"  # High concurrency
export FELIX_CACHE_CONN_POOL="4"   # Low concurrency
```

**Notes**:
- Client-side setting
- Affects concurrent request capacity
- Each connection can have multiple streams

### `FELIX_CACHE_STREAMS_PER_CONN`

**Description**: Cache request streams per connection (client-side).

**Type**: Positive integer (count)

**Default**: `4`

**Example**:
```bash
export FELIX_CACHE_STREAMS_PER_CONN="4"
export FELIX_CACHE_STREAMS_PER_CONN="8"   # More parallelism
export FELIX_CACHE_STREAMS_PER_CONN="2"   # Less overhead
```

**Tuning**:
- Total cache parallelism = `pool × streams_per_conn`
- Higher values for high-concurrency workloads

### `FELIX_CACHE_CONN_RECV_WINDOW`

**Description**: Cache connection flow-control receive window (broker).

**Type**: Positive integer (bytes)

**Default**: `268435456` (256 MiB)

**Example**:
```bash
export FELIX_CACHE_CONN_RECV_WINDOW="268435456"    # 256 MiB
export FELIX_CACHE_CONN_RECV_WINDOW="536870912"    # 512 MiB
export FELIX_CACHE_CONN_RECV_WINDOW="134217728"    # 128 MiB
```

**Memory impact**:
- Per-connection credit
- Multiplied by connection pool size
- Affects burst tolerance

### `FELIX_CACHE_STREAM_RECV_WINDOW`

**Description**: Cache stream flow-control receive window (broker).

**Type**: Positive integer (bytes)

**Default**: `67108864` (64 MiB)

**Example**:
```bash
export FELIX_CACHE_STREAM_RECV_WINDOW="67108864"   # 64 MiB
export FELIX_CACHE_STREAM_RECV_WINDOW="134217728"  # 128 MiB
export FELIX_CACHE_STREAM_RECV_WINDOW="33554432"   # 32 MiB
```

**Notes**:
- Per-stream credit
- Total: `stream_window × streams_per_conn × conn_pool`

### `FELIX_CACHE_SEND_WINDOW`

**Description**: Cache connection send window (broker).

**Type**: Positive integer (bytes)

**Default**: `268435456` (256 MiB)

**Example**:
```bash
export FELIX_CACHE_SEND_WINDOW="268435456"
```

### `FELIX_CACHE_BENCH_CONCURRENCY`

**Description**: Concurrency level for cache benchmark (demo only).

**Type**: Positive integer

**Default**: `32`

**Example**:
```bash
export FELIX_CACHE_BENCH_CONCURRENCY="32"
export FELIX_CACHE_BENCH_CONCURRENCY="64"  # Stress test
```

### `FELIX_CACHE_BENCH_KEYS`

**Description**: Number of keys for cache benchmark (demo only).

**Type**: Positive integer

**Default**: `1024`

**Example**:
```bash
export FELIX_CACHE_BENCH_KEYS="1024"
```

## Event Connection Pool (Client)

### `FELIX_EVENT_CONN_POOL`

**Description**: Number of QUIC connections for event delivery (client).

**Type**: Positive integer (count)

**Default**: `8`

**Example**:
```bash
export FELIX_EVENT_CONN_POOL="8"
export FELIX_EVENT_CONN_POOL="4"   # Lower overhead
export FELIX_EVENT_CONN_POOL="16"  # More parallelism
# Alias used by perf scripts:
export FELIX_SUB_CONNS="8"
```

### `FELIX_EVENT_CONN_RECV_WINDOW`

**Description**: Event connection receive window (client).

**Type**: Positive integer (bytes)

**Default**: `268435456` (256 MiB)

**Example**:
```bash
export FELIX_EVENT_CONN_RECV_WINDOW="268435456"
```

### `FELIX_EVENT_STREAM_RECV_WINDOW`

**Description**: Event stream receive window (client).

**Type**: Positive integer (bytes)

**Default**: `67108864` (64 MiB)

**Example**:
```bash
export FELIX_EVENT_STREAM_RECV_WINDOW="67108864"
```

### `FELIX_EVENT_SEND_WINDOW`

**Description**: Event connection send window (client).

**Type**: Positive integer (bytes)

**Default**: `268435456` (256 MiB)

**Example**:
```bash
export FELIX_EVENT_SEND_WINDOW="268435456"
```

### `FELIX_CLIENT_SUB_QUEUE_CAPACITY`

**Description**: Bounded queue capacity between client subscription IO and dispatch stages.

**Type**: Positive integer (count)

**Default**: `4096`

```bash
export FELIX_CLIENT_SUB_QUEUE_CAPACITY="4096"
```

### `FELIX_CLIENT_SUB_QUEUE_POLICY`

**Description**: Client-side backpressure policy for subscription pipeline queues.

**Type**: Enum (`block`, `drop_new`, `drop_old`)

**Default**: `drop_new`

```bash
export FELIX_CLIENT_SUB_QUEUE_POLICY="block"
```

## Publishing Pool (Client)

### `FELIX_PUB_CONN_POOL`

**Description**: Number of publishing QUIC connections (client).

**Type**: Positive integer (count)

**Default**: `4`

**Example**:
```bash
export FELIX_PUB_CONN_POOL="4"
export FELIX_PUB_CONN_POOL="8"  # More publishers
```

### `FELIX_PUB_STREAMS_PER_CONN`

**Description**: Publishing streams per connection (client).

**Type**: Positive integer (count)

**Default**: `2`

**Example**:
```bash
export FELIX_PUB_STREAMS_PER_CONN="2"
export FELIX_PUB_STREAMS_PER_CONN="4"  # More concurrency
```

### `FELIX_PUBLISH_CHUNK_BYTES`

**Description**: Chunk size for publishing large messages (client).

**Type**: Positive integer (bytes)

**Default**: `16384` (16 KiB)

**Example**:
```bash
export FELIX_PUBLISH_CHUNK_BYTES="16384"    # 16 KiB
export FELIX_PUBLISH_CHUNK_BYTES="32768"    # 32 KiB
export FELIX_PUBLISH_CHUNK_BYTES="8192"     # 8 KiB
```

## Broker Workers and Queues

### `FELIX_BROKER_PUB_WORKERS_PER_CONN`

**Description**: Publish workers per QUIC connection (broker).

**Type**: Positive integer (count)

**Default**: `4`

**Example**:
```bash
export FELIX_BROKER_PUB_WORKERS_PER_CONN="4"
export FELIX_BROKER_PUB_WORKERS_PER_CONN="8"   # High concurrency
export FELIX_BROKER_PUB_WORKERS_PER_CONN="2"   # Lower overhead
```

### `FELIX_BROKER_PUB_QUEUE_DEPTH`

**Description**: Per-worker publish queue depth (broker).

**Type**: Positive integer (count)

**Default**: `1024`

**Example**:
```bash
export FELIX_BROKER_PUB_QUEUE_DEPTH="1024"
export FELIX_BROKER_PUB_QUEUE_DEPTH="2048"  # More buffering
export FELIX_BROKER_PUB_QUEUE_DEPTH="512"   # Less memory
```

## Performance and Monitoring

### `FELIX_DISABLE_TIMINGS`

**Description**: Disable per-stage timing collection.

**Type**: Boolean

**Default**: `false`

**Accepted values**: `1`, `true`, `yes` = disabled

**Example**:
```bash
export FELIX_DISABLE_TIMINGS="false"  # Enable timings
export FELIX_DISABLE_TIMINGS="true"   # Disable for performance
export FELIX_DISABLE_TIMINGS="1"
```

**Trade-off**:
- `false`: Detailed metrics, slight overhead
- `true`: Maximum performance, no timing data

### `FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS`

**Description**: Timeout for control stream drain (broker).

**Type**: Positive integer (milliseconds)

**Default**: `50`

**Example**:
```bash
export FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS="50"
export FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS="100"  # More graceful
export FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS="20"   # Faster shutdown
```

## Configuration File

### `FELIX_BROKER_CONFIG`

**Description**: Path to YAML configuration file.

**Type**: String (file path)

**Default**: `/usr/local/felix/config.yml` (optional)

**Example**:
```bash
export FELIX_BROKER_CONFIG="/etc/felix/broker.yml"
export FELIX_BROKER_CONFIG="/tmp/felix-dev.yml"
```

**Behavior**:
- If set and file missing: **error**
- If not set and default missing: **continue with defaults**

## Logging

### `RUST_LOG`

**Description**: Rust logging filter (not Felix-specific but commonly used).

**Type**: String (filter expression)

**Default**: Varies by build

**Example**:
```bash
export RUST_LOG="info"
export RUST_LOG="debug"
export RUST_LOG="felix_broker=debug,felix_wire=trace"
export RUST_LOG="warn"
```

**Levels**: `error`, `warn`, `info`, `debug`, `trace`

## Performance Profiles

### Balanced Profile

```bash
export FELIX_EVENT_CONN_POOL="8"
export FELIX_EVENT_CONN_RECV_WINDOW="268435456"
export FELIX_EVENT_STREAM_RECV_WINDOW="67108864"
export FELIX_EVENT_SEND_WINDOW="268435456"
export FELIX_EVENT_BATCH_MAX_DELAY_US="250"
export FELIX_CACHE_CONN_POOL="8"
export FELIX_CACHE_STREAMS_PER_CONN="4"
export FELIX_DISABLE_TIMINGS="0"
```

### High Memory Profile

```bash
export FELIX_EVENT_CONN_POOL="8"
export FELIX_EVENT_CONN_RECV_WINDOW="536870912"
export FELIX_EVENT_STREAM_RECV_WINDOW="134217728"
export FELIX_EVENT_SEND_WINDOW="536870912"
export FELIX_EVENT_BATCH_MAX_DELAY_US="250"
export FELIX_CACHE_CONN_POOL="8"
export FELIX_CACHE_STREAMS_PER_CONN="4"
export FELIX_DISABLE_TIMINGS="1"
```

### Low Latency Profile

```bash
export FELIX_EVENT_BATCH_MAX_EVENTS="1"
export FELIX_EVENT_BATCH_MAX_DELAY_US="50"
export FELIX_FANOUT_BATCH="16"
export FELIX_DISABLE_TIMINGS="1"
```

### High Throughput Profile

```bash
export FELIX_EVENT_BATCH_MAX_EVENTS="256"
export FELIX_EVENT_BATCH_MAX_BYTES="1048576"
export FELIX_EVENT_BATCH_MAX_DELAY_US="1000"
export FELIX_FANOUT_BATCH="128"
export FELIX_DISABLE_TIMINGS="1"
```

## Validation

Check current configuration:

```bash
# Print effective configuration
cargo run --release -p broker -- --dump-config

# Validate without starting
cargo run --release -p broker -- --validate-config
```

## Next Steps

- **Full configuration details**: [Configuration Reference](configuration.md)
- **Troubleshooting**: [Troubleshooting Guide](troubleshooting.md)
- **Performance tuning**: [Performance Guide](../features/performance.md)
