# Performance Tuning

Felix is designed for predictable low-latency performance with tunable trade-offs between latency, throughput, and memory usage. This guide provides comprehensive performance tuning guidance based on real benchmarks and production-tested configurations.

## Understanding Felix Performance

Felix performance is determined by several interconnected factors:

1. **Network transport**: QUIC connection and stream configuration
2. **Batching**: Message aggregation at publish and delivery stages
3. **Parallelism**: Connection pools and worker threads
4. **Buffering**: Queue depths and flow control windows
5. **Encoding**: Binary wire framing efficiency
6. **Outbound lanes**: Subscriber writer lane count and lane sharding policy

## Performance Profiles

Felix provides three pre-configured profiles as starting points.

### Balanced Profile (Default)

General-purpose settings for mixed workloads:

**Broker configuration**:

```yaml
# Connection pools
pub_conn_pool: 4
pub_streams_per_conn: 2
event_conn_pool: 8
cache_conn_pool: 8
cache_streams_per_conn: 4

# QUIC flow control
event_conn_recv_window: 268435456      # 256 MiB
event_stream_recv_window: 67108864     # 64 MiB
event_send_window: 268435456           # 256 MiB

# Batching
event_batch_max_events: 64
event_batch_max_bytes: 65536
event_batch_max_delay_us: 250
fanout_batch_size: 64

# Queue depths
pub_queue_depth: 1024
subscriber_queue_capacity: 128
subscriber_writer_lanes: 4
subscriber_lane_queue_depth: 8192
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto

publish_chunk_bytes: 16384
```

**Expected performance** (fanout=10, batch=64, payload=4KB):

- **p50 latency**: 400-800 µs
- **p99 latency**: 2-4 ms  
- **Throughput**: 150-200k msg/sec per broker
- **Memory**: ~2-4 GB working set

**Best for**:
- Mixed pub/sub and cache workloads
- Moderate fanout (1-20 subscribers)
- General application development
- Starting point for tuning

### Latency-Optimized Profile

Minimize tail latency at the cost of throughput:

**Broker configuration**:

```yaml
# Smaller pools
pub_conn_pool: 2
pub_streams_per_conn: 1
event_conn_pool: 4

# Smaller windows
event_conn_recv_window: 67108864       # 64 MiB
event_stream_recv_window: 16777216     # 16 MiB
event_send_window: 67108864            # 64 MiB

# Minimal batching
event_batch_max_events: 8
event_batch_max_bytes: 32768
event_batch_max_delay_us: 100
fanout_batch_size: 8

# Fast acknowledgements
ack_on_commit: true

# Shallow queues
pub_queue_depth: 512
subscriber_queue_capacity: 64
subscriber_writer_lanes: 2
subscriber_lane_shard: auto

```

**Expected performance**:

- **p50 latency**: 150-300 µs
- **p99 latency**: 500-1000 µs
- **Throughput**: 50-80k msg/sec per broker
- **Memory**: ~1-2 GB working set

**Best for**:
- Real-time interactive applications
- Trading systems, gaming
- Sensor data with immediate processing
- Low fanout (1-5 subscribers)

### Throughput-Optimized Profile

Maximize throughput and burst tolerance:

**Broker configuration**:

```yaml
# Large pools
pub_conn_pool: 8
pub_streams_per_conn: 4
event_conn_pool: 16
cache_conn_pool: 16
cache_streams_per_conn: 8

# Large windows
event_conn_recv_window: 536870912      # 512 MiB
event_stream_recv_window: 134217728    # 128 MiB
event_send_window: 536870912           # 512 MiB

# Aggressive batching
event_batch_max_events: 256
event_batch_max_bytes: 1048576
event_batch_max_delay_us: 2000
fanout_batch_size: 256

# Async acknowledgements
ack_on_commit: false

# Deep queues
pub_queue_depth: 4096
subscriber_queue_capacity: 256
subscriber_writer_lanes: 8
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto

publish_chunk_bytes: 32768
```

**Expected performance**:

- **p50 latency**: 1-3 ms
- **p99 latency**: 5-15 ms
- **Throughput**: 300-500k msg/sec per broker
- **Memory**: ~8-16 GB working set

**Best for**:
- High-throughput data pipelines
- Log aggregation, metrics collection
- High fanout (20-100+ subscribers)
- Batch processing workflows

## Configuration Parameters

### Pub/Sub Parameters

#### Connection Pooling

```yaml
event_conn_pool: 8              # QUIC connections for events
pub_conn_pool: 4                # QUIC connections for publishing
pub_streams_per_conn: 2         # Publish streams per connection
```

**Tuning guidance**:

| Workload | event_conn_pool | pub_conn_pool | streams_per_conn |
|----------|----------------|---------------|------------------|
| Light | 2-4 | 2 | 1-2 |
| Medium | 4-8 | 2-4 | 2 |
| Heavy | 8-16 | 4-8 | 2-4 |
| Very heavy | 16-32 | 8-16 | 4-8 |

!!! warning "Worker Sizing"
    Set `pub_workers_per_conn` ≤ `pub_streams_per_conn`. Excess workers create contention without benefit.

#### Flow Control Windows

```yaml
event_conn_recv_window: 268435456      # Per-connection receive window
event_stream_recv_window: 67108864     # Per-stream receive window
event_send_window: 268435456           # Per-connection send window
```

**Memory impact calculation**:

```
Worst-case memory = (conn_window × conn_pool) + 
                    (stream_window × avg_streams × conn_pool)
```

Example:
- `conn_pool=8`, `conn_window=256MB`, `stream_window=64MB`, `avg_streams=10`
- Memory ≈ (256MB × 8) + (64MB × 10 × 8) = 2GB + 5.1GB = **7.1GB**

**Tuning guidance**:

- **Low latency, limited bursts**: Use smaller windows (64-128 MiB)
- **High throughput, bursty**: Use larger windows (256-512 MiB)
- **Memory constrained**: Reduce pool size before reducing windows

#### Batching Parameters

```yaml
event_batch_max_events: 64             # Max events per batch
event_batch_max_bytes: 262144          # Max batch size (256 KB)
event_batch_max_delay_us: 250          # Max batching delay (250 µs)
fanout_batch_size: 64                  # Fanout batch size
```

**Batch triggers**: Event batch is sent when **any** condition is met.

**Trade-off analysis**:

| Parameter | ↑ Increase Effect | ↓ Decrease Effect |
|-----------|------------------|------------------|
| `max_events` | Higher throughput, higher latency | Lower latency, lower throughput |
| `max_delay_us` | Higher throughput, higher latency | Lower latency, lower throughput |
| `max_bytes` | Fewer frames, more efficiency | More frames, less efficiency |
| `fanout_batch_size` | Better fanout efficiency | Lower fanout latency |

**Recommended settings by workload**:

```yaml
# Ultra-low latency
event_batch_max_events: 4
event_batch_max_delay_us: 50

# Low latency
event_batch_max_events: 8
event_batch_max_delay_us: 100

# Balanced (default)
event_batch_max_events: 64
event_batch_max_delay_us: 250

# High throughput
event_batch_max_events: 128
event_batch_max_delay_us: 1000

# Maximum throughput
event_batch_max_events: 256
event_batch_max_delay_us: 2000
```

#### Queue Depths

```yaml
pub_queue_depth: 1024                  # Publish pipeline queue
subscriber_queue_capacity: 128         # Per-subscriber broker-core queue
subscriber_lane_queue_depth: 8192      # Per-lane outbound writer queue
pub_workers_per_conn: 4                # Publish workers per connection
```

**Queue depth impact**:

- **Shallow queues** (256-512): Lower memory, fail-fast backpressure, lower burst tolerance
- **Medium queues** (1024-2048): Balanced, good burst tolerance
- **Deep queues** (4096-8192): High memory, high burst tolerance, slower failure detection

**Memory per queue**:

```
Queue memory ≈ queue_depth × avg_message_size

Example: 128 × 4KB = 512KB per subscriber queue
With 100 subscribers: 100 × 512KB = 50MB
```

#### Outbound Writer Lanes and Sharding

Writer lanes parallelize outbound subscriber writes while preserving per-subscriber ordering.

```yaml
subscriber_writer_lanes: 4
max_subscriber_writer_lanes: 8
subscriber_lane_queue_depth: 8192
subscriber_lane_shard: auto  # auto | subscriber_id_hash | connection_id_hash | round_robin_pin
```

Recent release-mode sweeps (`payload=4096`, `batch=64`, `pub_conns=4`, `pub_streams_per_conn=2`):

| Workload | Key Result |
|----------|------------|
| fanout=10 | lanes improved throughput from ~54.9k (1 lane) to ~68.2k (4 lanes); gains plateaued at higher lanes |
| fanout=20 | lanes improved throughput from ~38.1k (1 lane) to ~44.8k (16 lanes); p99 improved with tuned lanes |
| lanes=4 shard sweep | `auto` / `subscriber_id_hash` outperformed `connection_id_hash` / `round_robin_pin` in this workload |

Start here:
1. `subscriber_lane_shard: auto`
2. `subscriber_writer_lanes: 4`
3. Increase to `8` only if throughput is still lane-bound
4. Avoid assuming larger lane counts always help; watch p99/p999

### Cache Parameters

```yaml
cache_conn_pool: 8                     # QUIC connections for cache
cache_streams_per_conn: 4              # Streams per connection
cache_conn_recv_window: 268435456      # 256 MiB per connection
cache_stream_recv_window: 67108864     # 64 MiB per stream
```

**Concurrency calculation**:

```
Max concurrent cache ops = cache_conn_pool × cache_streams_per_conn
```

**Recommended by workload**:

| Workload | conn_pool | streams_per_conn | Max Concurrency |
|----------|-----------|------------------|-----------------|
| Low | 4 | 2 | 8 |
| Medium | 8 | 4 | 32 |
| High | 16 | 8 | 128 |
| Very high | 32 | 16 | 512 |

### Event Frame Encoding

Subscription event delivery uses binary `EventBatch` framing by default.

## Benchmark Results

### Pub/Sub Latency and Throughput (Release, Localhost)

Reference stress profile:
- `payload=4096`
- `batch=64`
- `pub_conns=4`
- `pub_streams_per_conn=2`

Observed with writer lanes:

| Workload | Result |
|----------|--------|
| fanout=10, lanes=1, shard=auto | ~54.9k msg/s, p99 ~67.4ms |
| fanout=10, lanes=4, shard=auto | ~68.2k msg/s, p99 ~51.2ms |
| fanout=20, lanes=1, shard=auto | ~38.1k msg/s, p99 ~65.4ms |
| fanout=20, lanes=16, shard=auto | ~44.8k msg/s, p99 ~54.4ms |
| fanout=10, lanes=4 shard sweep | `auto` / `subscriber_id_hash` best in this workload |

Takeaway:
- Writer lanes materially improve heavy fanout/payload workloads.
- Gains plateau; more lanes can stop helping depending on workload and host.

### Cache Performance (Localhost)

**Configuration**: 8 connections, 4 streams/conn, concurrency=32

| Operation | Payload | p50 | p99 | Throughput |
|-----------|---------|-----|-----|------------|
| put | 0 B | 158 µs | 350 µs | 184k ops/sec |
| put | 256 B | 179 µs | 380 µs | 155k ops/sec |
| put | 4 KB | 260 µs | 480 µs | 78k ops/sec |
| get (hit) | 256 B | 177 µs | 360 µs | 166k ops/sec |
| get (miss) | - | 165 µs | 340 µs | 179k ops/sec |

## Profiling and Diagnostics

### Telemetry Feature

Enable detailed performance telemetry:

```toml
[dependencies]
felix-client = { version = "0.1", features = ["telemetry"] }
felix-broker = { version = "0.1", features = ["telemetry"] }
```

```yaml
# Broker config
disable_timings: false                 # Enable timing measurements
```

**Metrics collected**:

- Per-operation latency histograms (publish, subscribe, cache)
- Frame counters (publish frames, event frames, cache frames)
- Queue depth samples
- Flow control events

**Overhead**: 5-15% throughput reduction in high-load scenarios.

!!! warning "Production Use"
    Disable telemetry in production for maximum throughput. Enable only for profiling and debugging specific issues.

### Performance Debugging

**High publish latency**:

1. Check `pub_queue_depth` - is queue filling up?
2. Check `pub_workers_per_conn` - enough workers?
3. Check broker CPU usage - saturated?
4. Enable telemetry - where is time spent?

**High subscribe latency**:

1. Check `subscriber_queue_capacity` and lane drop counters - subscribers falling behind?
2. Check `event_batch_max_delay_us` - batching too aggressive?
3. Check QUIC flow control - windows exhausted?
4. Check subscriber processing time - bottleneck in application?

**Low throughput**:

1. Increase `event_batch_max_events` - more aggressive batching
2. Increase connection pools - more parallelism
3. Confirm binary `EventBatch` decoding path in subscribers
4. Check network bandwidth - saturated?
5. Increase `pub_workers_per_conn` - more publish parallelism

**High memory usage**:

1. Reduce flow control windows
2. Reduce queue depths
3. Reduce connection pool sizes
4. Check for slow subscribers - filling buffers?

## Production Recommendations

### Sizing Guidelines

**Small deployment** (< 10k msg/sec):

```yaml
pub_conn_pool: 2
event_conn_pool: 4
cache_conn_pool: 4
event_batch_max_events: 32
pub_queue_depth: 512
subscriber_queue_capacity: 64
subscriber_writer_lanes: 2
```

**Expected resources**: 2 CPU cores, 2-4 GB RAM

**Medium deployment** (10k-100k msg/sec):

```yaml
pub_conn_pool: 4
event_conn_pool: 8
cache_conn_pool: 8
event_batch_max_events: 64
pub_queue_depth: 1024
subscriber_queue_capacity: 128
subscriber_writer_lanes: 4
```

**Expected resources**: 4-8 CPU cores, 4-8 GB RAM

**Large deployment** (100k-500k msg/sec):

```yaml
pub_conn_pool: 8
event_conn_pool: 16
cache_conn_pool: 16
event_batch_max_events: 128
pub_queue_depth: 2048
subscriber_queue_capacity: 256
subscriber_writer_lanes: 8
```

**Expected resources**: 16-32 CPU cores, 16-32 GB RAM

### Tuning Workflow

1. **Start with balanced profile**: Use defaults
2. **Measure baseline**: Run realistic workload, measure latency/throughput
3. **Identify bottleneck**: CPU? Memory? Network? Queue depths?
4. **Tune one parameter**: Change single parameter
5. **Re-measure**: Verify improvement
6. **Iterate**: Repeat until requirements met

!!! tip "Measure, Don't Guess"
    Performance tuning without measurement leads to worse performance. Always benchmark before and after changes.

### Monitoring in Production

**Key metrics to track**:

- Publish rate and latency (p50, p99, p999)
- Subscribe rate and latency
- Queue depths (publish, event)
- Lane queue pressure (per-lane enqueue/drop/highwater)
- Connection count
- CPU and memory usage
- Network bandwidth
- Dropped event count
- Slow subscriber count

**Alerting thresholds**:

- p99 latency > 2× baseline
- Queue depth > 80% of max
- Dropped events > 0.1% of published
- CPU usage > 80%
- Memory usage > 85%

## Hardware Recommendations

### CPU

- **Minimum**: 2 cores
- **Recommended**: 4-8 cores for medium workloads
- **High performance**: 16-32 cores for high throughput

Felix is CPU-bound for:
- JSON encoding/decoding
- QUIC encryption
- Message batching and fanout

### Memory

- **Minimum**: 2 GB
- **Recommended**: 4-8 GB for medium workloads
- **High performance**: 16-32 GB for high throughput with large queues

Memory usage scales with:
- Connection pool sizes × flow control windows
- Queue depths × subscriber count
- Cache size

### Network

- **Minimum**: 1 Gbps
- **Recommended**: 10 Gbps for high throughput
- **Ideal**: 25+ Gbps for very high throughput

QUIC benefits from:
- Low latency networks (< 1 ms RTT)
- High bandwidth
- Low packet loss (< 0.1%)

### Disk

- **MVP**: Not used (ephemeral only)
- **Future (durable mode)**: NVMe SSD for WAL and segments

## Best Practices Summary

1. ✓ Start with balanced profile, measure, then tune
2. ✓ Size connection pools for your parallelism needs
3. ✓ Use batching for throughput, minimize batching for latency
4. ✓ Validate binary `EventBatch` decode performance in clients
5. ✓ Monitor queue depths - they reveal backpressure
6. ✓ Disable telemetry in production for maximum throughput
7. ✓ Profile before optimizing - don't guess
8. ✓ Test with realistic workloads, not synthetic benchmarks
9. ✓ Plan for 2-3× headroom above expected load
10. ✓ Document your tuning decisions and benchmark results

!!! success "Predictable Performance"
    Felix is designed for predictable p99/p999 latency under load. Tuning trades off between latency, throughput, and memory—but tail latency remains controlled with proper configuration.
