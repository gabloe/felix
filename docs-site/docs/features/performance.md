# Performance Tuning

Felix is designed for predictable low-latency performance with tunable trade-offs between latency, throughput, and memory usage. This guide provides comprehensive performance tuning guidance based on real benchmarks and production-tested configurations.

## Understanding Felix Performance

Felix performance is determined by several interconnected factors:

1. **Network transport**: QUIC connection and stream configuration
2. **Batching**: Message aggregation at publish and delivery stages
3. **Parallelism**: Connection pools and worker threads
4. **Buffering**: Queue depths and flow control windows
5. **Encoding**: JSON vs binary wire format

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
event_batch_max_delay_us: 250
fanout_batch_size: 64

# Queue depths
pub_queue_depth: 1024
event_queue_depth: 1024

# Binary mode
event_single_binary_enabled: false
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
event_batch_max_delay_us: 100
fanout_batch_size: 8

# Fast acknowledgements
ack_on_commit: true

# Shallow queues
pub_queue_depth: 512
event_queue_depth: 512

# No binary mode (JSON is faster for small batches)
event_single_binary_enabled: false
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
event_batch_max_delay_us: 2000
fanout_batch_size: 256

# Async acknowledgements
ack_on_commit: false

# Deep queues
pub_queue_depth: 4096
event_queue_depth: 4096

# Binary mode enabled
event_single_binary_enabled: true
event_single_binary_min_bytes: 256
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
event_queue_depth: 1024                # Per-subscriber event queue
pub_workers_per_conn: 4                # Publish workers per connection
```

**Queue depth impact**:

- **Shallow queues** (256-512): Lower memory, fail-fast backpressure, lower burst tolerance
- **Medium queues** (1024-2048): Balanced, good burst tolerance
- **Deep queues** (4096-8192): High memory, high burst tolerance, slower failure detection

**Memory per queue**:

```
Queue memory ≈ queue_depth × avg_message_size

Example: 1024 × 4KB = 4MB per queue
With 100 subscribers: 100 × 4MB = 400MB
```

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

### Binary Encoding

```yaml
event_single_binary_enabled: true      # Enable binary encoding
event_single_binary_min_bytes: 512     # Min payload size for binary
```

**Binary vs JSON performance** (batch=64, fanout=10):

| Payload Size | JSON Throughput | Binary Throughput | Improvement |
|--------------|----------------|-------------------|-------------|
| 64 B | 180k msg/sec | 190k msg/sec | +5% |
| 256 B | 170k msg/sec | 205k msg/sec | +20% |
| 1 KB | 140k msg/sec | 195k msg/sec | +39% |
| 4 KB | 80k msg/sec | 115k msg/sec | +44% |

**When to enable**:

✓ Payload sizes > 512 bytes  
✓ High throughput priority  
✓ High fanout workloads  
✓ Validated binary encoding implementation  

✗ Ultra-low latency priority (JSON is faster for tiny batches)  
✗ Debugging (JSON is human-readable)  
✗ Small payloads < 256 bytes  

## Benchmark Results

### Pub/Sub Latency (Localhost)

**Configuration**: Balanced profile, binary mode, fanout=10, batch=64, payload=4KB

| Metric | p50 | p95 | p99 | p999 |
|--------|-----|-----|-----|------|
| Publish ack | 200 µs | 380 µs | 520 µs | 850 µs |
| End-to-end | 450 µs | 980 µs | 1.8 ms | 3.2 ms |
| Fanout processing | 120 µs | 240 µs | 380 µs | 680 µs |

### Pub/Sub Throughput

| Fanout | Batch | Payload | Throughput | Notes |
|--------|-------|---------|------------|-------|
| 1 | 1 | 256 B | 85k msg/sec | Single publishes |
| 1 | 64 | 256 B | 240k msg/sec | Batched |
| 1 | 64 | 4 KB | 180k msg/sec | Large payloads |
| 10 | 64 | 4 KB | 170k msg/sec | High fanout |
| 100 | 64 | 4 KB | 120k msg/sec | Very high fanout |

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

1. Check `event_queue_depth` - subscribers falling behind?
2. Check `event_batch_delay_us` - batching too aggressive?
3. Check QUIC flow control - windows exhausted?
4. Check subscriber processing time - bottleneck in application?

**Low throughput**:

1. Increase `event_batch_max_events` - more aggressive batching
2. Increase connection pools - more parallelism
3. Enable binary mode - reduce encoding overhead
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
event_queue_depth: 512
```

**Expected resources**: 2 CPU cores, 2-4 GB RAM

**Medium deployment** (10k-100k msg/sec):

```yaml
pub_conn_pool: 4
event_conn_pool: 8
cache_conn_pool: 8
event_batch_max_events: 64
pub_queue_depth: 1024
event_queue_depth: 1024
```

**Expected resources**: 4-8 CPU cores, 4-8 GB RAM

**Large deployment** (100k-500k msg/sec):

```yaml
pub_conn_pool: 8
event_conn_pool: 16
cache_conn_pool: 16
event_batch_max_events: 128
pub_queue_depth: 2048
event_queue_depth: 2048
event_single_binary_enabled: true
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
4. ✓ Enable binary mode for large payloads (> 512 bytes)
5. ✓ Monitor queue depths - they reveal backpressure
6. ✓ Disable telemetry in production for maximum throughput
7. ✓ Profile before optimizing - don't guess
8. ✓ Test with realistic workloads, not synthetic benchmarks
9. ✓ Plan for 2-3× headroom above expected load
10. ✓ Document your tuning decisions and benchmark results

!!! success "Predictable Performance"
    Felix is designed for predictable p99/p999 latency under load. Tuning trades off between latency, throughput, and memory—but tail latency remains controlled with proper configuration.
