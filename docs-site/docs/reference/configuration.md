# Configuration Reference

Complete reference for all Felix broker configuration options.

## Configuration Methods

Felix supports three configuration methods, applied in order (later sources override earlier):

1. **Built-in defaults**: Sensible defaults for development
2. **Environment variables**: `FELIX_*` variables for quick overrides
3. **YAML config file**: Structured configuration for production

### Precedence Example

```bash
# Default: quic_bind = 0.0.0.0:5000
# Environment: FELIX_QUIC_BIND=127.0.0.1:5001
# YAML: quic_bind: "0.0.0.0:6000"
# Result: 0.0.0.0:6000 (YAML wins)
```

## Configuration Structure

### YAML Config File

**Location priority:**

1. `$FELIX_BROKER_CONFIG` (explicit path)
2. `/usr/local/felix/config.yml` (default, optional)

**Example:**

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
controlplane_url: "http://controlplane:8443"
controlplane_sync_interval_ms: 2000
ack_on_commit: false
max_frame_bytes: 16777216
publish_queue_wait_timeout_ms: 2000
ack_wait_timeout_ms: 2000
disable_timings: false
control_stream_drain_timeout_ms: 50
cache_conn_recv_window: 268435456
cache_stream_recv_window: 67108864
cache_send_window: 268435456
event_batch_max_events: 64
event_batch_max_bytes: 65536
event_batch_max_delay_us: 250
fanout_batch_size: 64
pub_workers_per_conn: 4
pub_queue_depth: 1024
subscriber_queue_capacity: 128
subscriber_writer_lanes: 4
subscriber_lane_queue_depth: 8192
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
```

## Network Configuration

### `quic_bind`

**Description**: QUIC listener bind address and port.

**Type**: `SocketAddr` (IP:Port)

**Default**: `0.0.0.0:5000`

**Environment**: `FELIX_QUIC_BIND`

**Example**:
```yaml
quic_bind: "0.0.0.0:5000"
```

**Notes**:
- UDP port for QUIC transport
- Use `0.0.0.0` to listen on all interfaces
- Use `127.0.0.1` for localhost only

### `metrics_bind`

**Description**: HTTP metrics and health endpoint bind address.

**Type**: `SocketAddr` (IP:Port)

**Default**: `0.0.0.0:8080`

**Environment**: `FELIX_BROKER_METRICS_BIND`

**Example**:
```yaml
metrics_bind: "0.0.0.0:8080"
```

**Endpoints**:
- `/healthz`: Health check
- `/metrics`: Prometheus metrics (if enabled)

## Control Plane Configuration

### `controlplane_url`

**Description**: Optional control plane base URL for metadata sync.

**Type**: `String` (URL)

**Default**: `None`

**Environment**: `FELIX_CONTROLPLANE_URL`

**Example**:
```yaml
controlplane_url: "http://felix-controlplane:8443"
```

**Notes**:
- Optional for single-node deployments
- Required for multi-node clusters
- Should include scheme (`http://` or `https://`)

### `controlplane_sync_interval_ms`

**Description**: Interval for polling control plane changes.

**Type**: `u64` (milliseconds)

**Default**: `2000`

**Environment**: `FELIX_CONTROLPLANE_SYNC_INTERVAL_MS`

**Example**:
```yaml
controlplane_sync_interval_ms: 2000
```

**Recommendations**:
- **Fast changes**: `500-1000ms`
- **Normal operation**: `2000-5000ms`
- **Stable clusters**: `5000-10000ms`

## Publishing Configuration

### `ack_on_commit`

**Description**: Send acknowledgements after message commit.

**Type**: `bool`

**Default**: `false`

**Environment**: `FELIX_ACK_ON_COMMIT` (`1`, `true`, `yes` = enabled)

**Example**:
```yaml
ack_on_commit: true
```

**Trade-offs**:
- **`false`**: Lower latency, fire-and-forget semantics
- **`true`**: Higher latency, explicit acknowledgement

### `max_frame_bytes`

**Description**: Maximum frame size accepted on QUIC streams.

**Type**: `usize` (bytes)

**Default**: `16777216` (16 MiB)

**Environment**: `FELIX_MAX_FRAME_BYTES`

**Example**:
```yaml
max_frame_bytes: 16777216
```

**Notes**:
- Limits individual message size
- Affects memory usage per stream
- Must match client expectations

### `publish_queue_wait_timeout_ms`

**Description**: Maximum time to wait when backpressuring publish enqueue.

**Type**: `u64` (milliseconds)

**Default**: `2000`

**Environment**: `FELIX_PUBLISH_QUEUE_WAIT_MS`

**Example**:
```yaml
publish_queue_wait_timeout_ms: 2000
```

**Behavior**:
- Publish blocks if queue is full
- Returns error after timeout
- Prevents unbounded memory growth

### `ack_wait_timeout_ms`

**Description**: Maximum time to wait for ack-on-commit completion.

**Type**: `u64` (milliseconds)

**Default**: `2000`

**Environment**: `FELIX_ACK_WAIT_TIMEOUT_MS`

**Example**:
```yaml
ack_wait_timeout_ms: 2000
```

**Notes**:
- Only applies when `ack_on_commit: true`
- Publisher receives error if exceeded

## Event Delivery Configuration

### `event_batch_max_events`

**Description**: Maximum events per batched subscription frame.

**Type**: `usize` (count)

**Default**: `64`

**Environment**: `FELIX_EVENT_BATCH_MAX_EVENTS`

**Example**:
```yaml
event_batch_max_events: 64
```

**Tuning**:
- **Low latency**: `1-16`
- **Balanced**: `32-64`
- **High throughput**: `128-256`

### `event_batch_max_bytes`

**Description**: Maximum bytes per batched subscription frame.

**Type**: `usize` (bytes)

**Default**: `65536` (64 KiB)

**Environment**: `FELIX_EVENT_BATCH_MAX_BYTES`

**Example**:
```yaml
event_batch_max_bytes: 65536
```

**Notes**:
- Whichever limit hits first triggers batch send
- Consider payload size when tuning

### `event_batch_max_delay_us`

**Description**: Maximum delay before flushing a subscription batch.

**Type**: `u64` (microseconds)

**Default**: `250`

**Environment**: `FELIX_EVENT_BATCH_MAX_DELAY_US`

**Example**:
```yaml
event_batch_max_delay_us: 250
```

**Tuning**:
- **Ultra-low latency**: `50-100us`
- **Balanced**: `250-500us`
- **High throughput**: `1000-5000us`

!!! warning "Latency Impact"
    Lower values reduce latency but may decrease throughput. Higher values improve batching efficiency but increase tail latency.

### `fanout_batch_size`

**Description**: Publish-side fanout batching hint and subscribe delivery batch cap.

**Type**: `usize` (count)

**Default**: `64`

**Environment**: `FELIX_FANOUT_BATCH`

**Example**:
```yaml
fanout_batch_size: 64
```

**Recommendations**:
- **Low fanout (1-10)**: `16-32`
- **Medium fanout (10-100)**: `64-128`
- **High fanout (100+)**: `128-256`

### Event Frame Encoding

Subscription event delivery uses binary `EventBatch` frames.

### Outbound Writer Lanes

Broker outbound subscribe delivery uses lane-sharded writer tasks to reduce contention under
high fanout / large payload workloads.

#### `subscriber_queue_capacity`

**Description**: Per-subscriber queue capacity in broker core (drop-on-full boundary).

**Type**: `usize` (count)

**Default**: `128`

**Environment**: `FELIX_SUBSCRIBER_QUEUE_CAPACITY`

```yaml
subscriber_queue_capacity: 128
```

#### `subscriber_writer_lanes`

**Description**: Requested number of outbound writer lanes.

**Type**: `usize` (count)

**Default**: `4`

**Environment**: `FELIX_SUB_WRITER_LANES`

```yaml
subscriber_writer_lanes: 4
```

#### `subscriber_lane_queue_depth`

**Description**: Bounded command queue depth per writer lane.

**Type**: `usize` (count)

**Default**: `8192`

**Environment**: `FELIX_SUB_LANE_QUEUE_DEPTH`

```yaml
subscriber_lane_queue_depth: 8192
```

#### `max_subscriber_writer_lanes`

**Description**: Safety clamp for `subscriber_writer_lanes` to avoid oversubscription regressions.

**Type**: `usize` (count)

**Default**: `8`

**Environment**: `FELIX_MAX_SUB_WRITER_LANES`

```yaml
max_subscriber_writer_lanes: 8
```

#### `subscriber_lane_shard`

**Description**: Lane assignment policy for subscriber outbound writes.

**Type**: `enum` (`auto`, `subscriber_id_hash`, `connection_id_hash`, `round_robin_pin`)

**Default**: `auto`

**Environment**: `FELIX_SUB_LANE_SHARD`

```yaml
subscriber_lane_shard: auto
```

Policy guidance:
- `auto`: Prefer connection-aware routing when connection id is available; fallback to subscriber id.
- `subscriber_id_hash`: Good general distribution independent of connection topology.
- `connection_id_hash`: Useful when many subscribers share connections and connection-local contention dominates.
- `round_robin_pin`: Pins lane at subscribe-time; preserves ordering, but can underperform in skewed workloads.

## Cache Configuration

### `cache_conn_recv_window`

**Description**: Cache connection flow-control receive window.

**Type**: `u64` (bytes)

**Default**: `268435456` (256 MiB)

**Environment**: `FELIX_CACHE_CONN_RECV_WINDOW`

**Example**:
```yaml
cache_conn_recv_window: 268435456
```

**Notes**:
- Per-connection receive credit
- Multiplied by connection pool size
- Affects burst tolerance

### `cache_stream_recv_window`

**Description**: Cache stream flow-control receive window.

**Type**: `u64` (bytes)

**Default**: `67108864` (64 MiB)

**Environment**: `FELIX_CACHE_STREAM_RECV_WINDOW`

**Example**:
```yaml
cache_stream_recv_window: 67108864
```

**Notes**:
- Per-stream receive credit
- Multiplied by streams per connection
- Total credit = `stream_window × streams_per_conn × conn_pool`

### `cache_send_window`

**Description**: Cache connection send window.

**Type**: `u64` (bytes)

**Default**: `268435456` (256 MiB)

**Environment**: `FELIX_CACHE_SEND_WINDOW`

**Example**:
```yaml
cache_send_window: 268435456
```

**Notes**:
- Per-connection send credit
- Affects concurrent request throughput

## Worker and Queue Configuration

### `pub_workers_per_conn`

**Description**: Publish worker count per QUIC connection.

**Type**: `usize` (count)

**Default**: `4`

**Environment**: `FELIX_BROKER_PUB_WORKERS_PER_CONN`

**Example**:
```yaml
pub_workers_per_conn: 4
```

**Recommendations**:
- **Low concurrency**: `2-4`
- **High concurrency**: `8-16`
- Match to expected concurrent publishers per connection

### `pub_queue_depth`

**Description**: Per-worker publish queue depth.

**Type**: `usize` (count)

**Default**: `1024`

**Environment**: `FELIX_BROKER_PUB_QUEUE_DEPTH`

**Example**:
```yaml
pub_queue_depth: 1024
```

**Tuning**:
- Larger values allow more buffering under burst
- Affects memory usage per worker
- Consider with `publish_queue_wait_timeout_ms`

## Performance Configuration

### `disable_timings`

**Description**: Disable per-stage timing collection for lower overhead.

**Type**: `bool`

**Default**: `false`

**Environment**: `FELIX_DISABLE_TIMINGS` (`1`, `true`, `yes` = disabled)

**Example**:
```yaml
disable_timings: true
```

**Trade-offs**:
- **`false`**: Detailed latency metrics, slight overhead
- **`true`**: Maximum performance, no per-stage timings

**Recommendations**:
- Development: `false` (debug performance)
- Production low-load: `false` (observability)
- Production high-load: `true` (reduce overhead)

### `control_stream_drain_timeout_ms`

**Description**: Maximum time to wait for control-stream writer to drain.

**Type**: `u64` (milliseconds)

**Default**: `50`

**Environment**: `FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS`

**Example**:
```yaml
control_stream_drain_timeout_ms: 50
```

**Notes**:
- Affects graceful connection shutdown
- Balance between responsiveness and reliability

## Client-Side Configuration

While this reference covers broker configuration, clients also have tunable parameters:

### Event Connection Pool

**Environment**: `FELIX_EVENT_CONN_POOL`

**Default**: `8`

**Description**: Number of QUIC connections in the event pool.

### Cache Connection Pool

**Environment**: `FELIX_CACHE_CONN_POOL`

**Default**: `8`

**Description**: Number of QUIC connections for cache operations.

### Cache Streams Per Connection

**Environment**: `FELIX_CACHE_STREAMS_PER_CONN`

**Default**: `4`

**Description**: Concurrent cache streams per connection.

### Publish Chunk Bytes

**Environment**: `FELIX_PUBLISH_CHUNK_BYTES`

**Default**: `16384` (16 KiB)

**Description**: Chunk size for publishing large messages.

## Configuration Validation

Felix validates configuration at startup:

```bash
# Test configuration
cargo run --release -p broker -- --dry-run

# Explicit config file
FELIX_BROKER_CONFIG=/path/to/config.yml cargo run --release -p broker
```

**Common validation errors**:

- Invalid socket address format
- Negative or zero values where positive required
- Conflicting settings

## Performance Profiles

### Low Latency (p50-optimized)

```yaml
event_batch_max_events: 1
event_batch_max_delay_us: 50
fanout_batch_size: 16
subscriber_writer_lanes: 2
subscriber_lane_shard: auto
subscriber_queue_capacity: 64
disable_timings: true
```

### Balanced (recommended)

```yaml
event_batch_max_events: 64
event_batch_max_bytes: 65536
event_batch_max_delay_us: 250
fanout_batch_size: 64
subscriber_writer_lanes: 4
subscriber_lane_shard: auto
subscriber_queue_capacity: 128
disable_timings: false
cache_conn_recv_window: 268435456
```

### High Throughput (batch-optimized)

```yaml
event_batch_max_events: 256
event_batch_max_bytes: 1048576
event_batch_max_delay_us: 1000
fanout_batch_size: 128
subscriber_writer_lanes: 8
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
subscriber_queue_capacity: 256
disable_timings: true
```

### High Memory (burst-tolerant)

```yaml
cache_conn_recv_window: 536870912
cache_stream_recv_window: 134217728
cache_send_window: 536870912
subscriber_queue_capacity: 2048
subscriber_lane_queue_depth: 16384
pub_queue_depth: 2048
```

## Configuration Examples

### Development

```yaml
quic_bind: "127.0.0.1:5000"
metrics_bind: "127.0.0.1:8080"
disable_timings: false
event_batch_max_events: 32
```

### Production Single-Node

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
ack_on_commit: true
disable_timings: true
event_batch_max_events: 64
event_batch_max_delay_us: 250
subscriber_writer_lanes: 4
subscriber_lane_shard: auto
cache_conn_recv_window: 268435456
```

### Production Cluster

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
controlplane_url: "http://felix-controlplane:8443"
controlplane_sync_interval_ms: 2000
ack_on_commit: true
disable_timings: true
event_batch_max_events: 128
event_batch_max_bytes: 262144
fanout_batch_size: 128
subscriber_writer_lanes: 4
subscriber_lane_shard: auto
```

## Next Steps

- **Environment variables reference**: [Environment Variables](environment-variables.md)
- **Troubleshooting issues**: [Troubleshooting Guide](troubleshooting.md)
- **Performance tuning**: [Performance Guide](../features/performance.md)
