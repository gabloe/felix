# Observability

Felix provides comprehensive observability through structured logging, optional telemetry, and metrics exposure. This document covers logging configuration, performance telemetry, monitoring integration, and operational debugging.

## Logging

Felix uses structured logging for all operational events, making it easy to parse, filter, and analyze logs in production environments.

### Log Levels

Felix supports standard log levels:

- **ERROR**: Critical failures requiring immediate attention
- **WARN**: Degraded behavior or approaching limits
- **INFO**: Normal operational events (default)
- **DEBUG**: Detailed diagnostic information
- **TRACE**: Very verbose, includes per-request details

### Configuration

**Environment variable**:

```bash
RUST_LOG=info                         # Default: info level for all modules
RUST_LOG=felix_broker=debug           # Debug level for broker only
RUST_LOG=felix_broker=trace,felix_wire=debug  # Multiple modules
```

**Structured output** (JSON):

```bash
FELIX_LOG_FORMAT=json                 # JSON structured logs
```

Example JSON log:

```json
{
  "timestamp": "2026-01-15T10:30:45.123Z",
  "level": "INFO",
  "target": "felix_broker",
  "message": "Subscription created",
  "fields": {
    "tenant_id": "acme-corp",
    "namespace": "production",
    "stream": "orders",
    "subscription_id": "sub-abc-123",
    "subscriber_addr": "10.0.1.45:52341"
  }
}
```

### Key Log Events

**Broker startup**:

```
INFO felix_broker: Broker starting
INFO felix_broker: QUIC listening on 0.0.0.0:5000
INFO felix_broker: Control plane sync disabled (MVP mode)
INFO felix_broker: Broker ready
```

**Connection events**:

```
INFO felix_broker: New connection from 10.0.1.45:52341
DEBUG felix_broker: Control stream opened stream_id=0
INFO felix_broker: Connection closed duration=45.2s
```

**Publish events**:

```
DEBUG felix_broker: Publish received tenant=acme ns=prod stream=orders batch_size=64
DEBUG felix_broker: Fanout complete stream=orders subscribers=12 duration_us=180
```

**Subscribe events**:

```
INFO felix_broker: Subscription created tenant=acme ns=prod stream=orders subscription_id=sub-123
WARN felix_broker: Subscriber falling behind subscription_id=sub-123 queue_depth=980/1024
WARN felix_broker: Events dropped for subscriber subscription_id=sub-123 dropped=15
```

**Cache events**:

```
DEBUG felix_broker: Cache put key=session:user-abc ttl_ms=3600000
DEBUG felix_broker: Cache get key=session:user-abc result=hit
DEBUG felix_broker: Cache get key=session:user-xyz result=miss
```

**Error events**:

```
ERROR felix_broker: Unknown tenant in publish request tenant=unknown-tenant
ERROR felix_broker: Publish queue timeout after 2000ms
WARN felix_broker: QUIC connection error error="connection lost"
```

### Log Aggregation

Felix logs integrate seamlessly with log aggregation systems:

**Fluentd/Fluent Bit**:

```yaml
<source>
  @type tail
  path /var/log/felix/broker.log
  pos_file /var/log/felix/broker.log.pos
  tag felix.broker
  <parse>
    @type json
    time_key timestamp
    time_format %Y-%m-%dT%H:%M:%S.%LZ
  </parse>
</source>

<match felix.**>
  @type elasticsearch
  host elasticsearch.example.com
  port 9200
  index_name felix
</match>
```

**Promtail/Loki**:

```yaml
clients:
  - url: http://loki:3100/loki/api/v1/push

scrape_configs:
  - job_name: felix
    static_configs:
      - targets:
          - localhost
        labels:
          job: felix-broker
          __path__: /var/log/felix/*.log
    pipeline_stages:
      - json:
          expressions:
            level: level
            message: message
            tenant_id: fields.tenant_id
```

## Telemetry

Felix supports optional telemetry for detailed performance profiling. Telemetry is **disabled by default** to avoid overhead.

### Enabling Telemetry

**Compile-time**:

```toml
[dependencies]
felix-broker = { version = "0.1", features = ["telemetry"] }
felix-client = { version = "0.1", features = ["telemetry"] }
```

**Runtime** (broker):

```yaml
# Broker config
disable_timings: false                 # Enable timing measurements
```

!!! warning "Performance Impact"
    Telemetry adds 5-15% overhead in high-throughput scenarios. Use for profiling and debugging only, not in production hot paths.

### Telemetry Metrics

**Client-side metrics**:

```rust
use felix_client::{frame_counters_snapshot, timings};

// Frame counters
let counters = frame_counters_snapshot();
println!("Publish frames sent: {}", counters.publish_frames);
println!("Event frames received: {}", counters.event_frames);
println!("Cache put frames: {}", counters.cache_put_frames);
println!("Cache get frames: {}", counters.cache_get_frames);

// Timing histograms
let publish_timings = timings::publish_timings_snapshot();
println!("Publish p50: {:?}", publish_timings.p50);
println!("Publish p99: {:?}", publish_timings.p99);
println!("Publish p999: {:?}", publish_timings.p999);
```

**Broker-side metrics** (future):

- Per-stream publish rate
- Per-subscription delivery rate
- Queue depth samples
- Fanout timing breakdown
- QUIC flow control events

### Telemetry Use Cases

**Profiling publish latency**:

```rust
// Enable telemetry
use felix_client::{Client, ClientConfig};
use felix_wire::AckMode;
use std::net::SocketAddr;

let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig::optimized_defaults(quinn);
let addr: SocketAddr = "127.0.0.1:5000".parse()?;
let client = Client::connect(addr, "localhost", config).await?;
let publisher = client.publisher().await?;

// Run workload
for _ in 0..10000 {
    publisher
        .publish("tenant", "ns", "stream", data.to_vec(), AckMode::None)
        .await?;
}

// Analyze results
let timings = felix_client::timings::publish_timings_snapshot();
println!("Publish latency:");
println!("  p50:  {:?}", timings.p50);
println!("  p95:  {:?}", timings.p95);
println!("  p99:  {:?}", timings.p99);
println!("  p999: {:?}", timings.p999);
```

**Identifying bottlenecks**:

```rust
// Instrument different stages
// (requires broker-side telemetry support)

// Stage 1: Wire encode
let encode_timings = telemetry::encode_timings();

// Stage 2: QUIC send
let quic_timings = telemetry::quic_send_timings();

// Stage 3: Broker enqueue
let enqueue_timings = telemetry::enqueue_timings();

// Stage 4: Fanout processing
let fanout_timings = telemetry::fanout_timings();

// Find slowest stage
```

## Metrics (Planned)

Felix will expose metrics in Prometheus format for integration with standard monitoring stacks.

### Metrics Endpoint

```yaml
# Broker config
metrics_bind: "0.0.0.0:8080"           # Prometheus metrics endpoint
```

**Scrape configuration**:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'felix-broker'
    static_configs:
      - targets: ['broker-1:8080', 'broker-2:8080', 'broker-3:8080']
```

### Metric Types

**Pub/Sub metrics**:

```prometheus
# Publish operations
felix_publish_total{tenant, namespace, stream}              # Counter
felix_publish_bytes_total{tenant, namespace, stream}        # Counter
felix_publish_latency_seconds{tenant, namespace, stream}    # Histogram
felix_publish_errors_total{tenant, namespace, stream, error_type}  # Counter

# Subscribe operations
felix_subscriptions_active{tenant, namespace, stream}       # Gauge
felix_events_delivered_total{tenant, namespace, stream}     # Counter
felix_events_dropped_total{tenant, namespace, stream}       # Counter
felix_subscriber_lag_messages{subscription_id}              # Gauge

# Queue depths
felix_publish_queue_depth                                   # Gauge
felix_event_queue_depth{subscription_id}                    # Gauge
```

**Cache metrics**:

```prometheus
# Cache operations
felix_cache_puts_total{tenant, namespace, cache}            # Counter
felix_cache_gets_total{tenant, namespace, cache, result}    # Counter (hit/miss)
felix_cache_latency_seconds{operation, tenant, namespace}   # Histogram

# Cache state
felix_cache_entries{tenant, namespace, cache}               # Gauge
felix_cache_bytes{tenant, namespace, cache}                 # Gauge
felix_cache_evictions_total{tenant, namespace, cache}       # Counter
```

**System metrics**:

```prometheus
# Connections
felix_connections_active                                    # Gauge
felix_connections_total                                     # Counter
felix_connection_errors_total{error_type}                   # Counter

# QUIC metrics
felix_quic_streams_active{stream_type}                      # Gauge
felix_quic_packets_sent_total                               # Counter
felix_quic_packets_lost_total                               # Counter
felix_quic_flow_control_blocked_total                       # Counter

# Resource usage
felix_memory_bytes{type}                                    # Gauge (heap, stack, mmap)
felix_cpu_seconds_total                                     # Counter
felix_goroutines                                            # Gauge
```

### Grafana Dashboards

**Example dashboard queries**:

```promql
# Publish rate
rate(felix_publish_total[1m])

# p99 publish latency
histogram_quantile(0.99, rate(felix_publish_latency_seconds_bucket[5m]))

# Cache hit rate
rate(felix_cache_gets_total{result="hit"}[5m]) / 
  rate(felix_cache_gets_total[5m])

# Subscriber lag
felix_subscriber_lag_messages

# Dropped events
rate(felix_events_dropped_total[5m])
```

**Pre-built dashboards** (planned):

- Felix Overview (system health, throughput, latency)
- Pub/Sub Deep Dive (per-stream metrics, fanout performance)
- Cache Performance (hit rates, latency, size)
- System Resources (CPU, memory, network, QUIC metrics)

## Health Checks

**HTTP health endpoint**:

```bash
curl http://broker:8080/health
```

**Response**:

```json
{
  "status": "healthy",
  "version": "0.1.0",
  "uptime_seconds": 3600,
  "checks": {
    "quic_listener": "ok",
    "memory_usage": "ok",
    "queue_depths": "ok"
  }
}
```

**Kubernetes liveness probe**:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30
```

**Readiness probe**:

```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

## Distributed Tracing (Future)

Felix will support OpenTelemetry for distributed tracing:

**Configuration**:

```yaml
tracing:
  enabled: true
  exporter: otlp
  endpoint: http://otel-collector:4317
  sample_rate: 0.1                     # Sample 10% of requests
```

**Example trace**:

```
Publish Request (trace_id: abc123)
├─ Wire Encode         120 µs
├─ QUIC Send           80 µs
├─ Broker Receive      50 µs
├─ Enqueue             30 µs
├─ Fanout Processing   150 µs
│  ├─ Subscriber 1     45 µs
│  ├─ Subscriber 2     48 µs
│  └─ Subscriber 3     43 µs
└─ Ack Send            40 µs
Total: 470 µs
```

## Alerting

### Recommended Alerts

**Critical alerts** (page immediately):

```yaml
- alert: BrokerDown
  expr: up{job="felix-broker"} == 0
  for: 1m
  
- alert: HighPublishErrorRate
  expr: rate(felix_publish_errors_total[5m]) > 10
  for: 2m
  
- alert: MemoryExhaustion
  expr: felix_memory_bytes{type="heap"} > 0.9 * felix_memory_limit_bytes
  for: 5m
```

**Warning alerts** (investigate soon):

```yaml
- alert: HighP99Latency
  expr: histogram_quantile(0.99, rate(felix_publish_latency_seconds_bucket[5m])) > 0.01
  for: 10m
  
- alert: HighDropRate
  expr: rate(felix_events_dropped_total[5m]) / rate(felix_publish_total[5m]) > 0.001
  for: 5m
  
- alert: HighQueueDepth
  expr: felix_publish_queue_depth > 0.8 * felix_publish_queue_capacity
  for: 5m
```

### On-Call Runbooks

**High latency**:

1. Check Grafana dashboard for bottleneck
2. Check queue depths - are they filling?
3. Check subscriber lag - slow subscribers?
4. Check CPU/memory - resource exhausted?
5. Consider scaling horizontally or tuning config

**Dropped events**:

1. Identify which subscriptions are dropping
2. Check subscriber lag for those subscriptions
3. Check subscriber application logs - slow processing?
4. Consider increasing `event_queue_depth` or fixing subscriber performance

**Memory pressure**:

1. Check cache size - growing unbounded?
2. Check queue depths - deep queues holding large payloads?
3. Check connection count - too many connections?
4. Consider reducing flow control windows or adding memory

## Operational Debugging

### Common Issues and Solutions

**Issue**: Subscribers not receiving events

**Debug steps**:

1. Check broker logs for subscription creation
2. Verify tenant/namespace/stream exist
3. Check network connectivity from subscriber
4. Check subscriber application is calling `subscription.next_event()`
5. Enable DEBUG logging to see event delivery

**Issue**: High publish latency

**Debug steps**:

1. Enable telemetry on client
2. Check where time is spent (encode, send, ack)
3. Check broker queue depth - backing up?
4. Check CPU usage on broker - saturated?
5. Consider tuning `pub_workers_per_conn` or batching

**Issue**: Cache misses unexpectedly

**Debug steps**:

1. Check TTL - has entry expired?
2. Check broker restart - cache is ephemeral
3. Check key spelling - exact match required
4. Enable DEBUG logging to see cache lookups
5. Verify tenant/namespace/cache scope is correct

### Diagnostic Tools

**Connection debugging**:

```bash
# Check QUIC connectivity
nc -uz broker-ip 5000

# Check TLS certificate
openssl s_client -connect broker-ip:5000 -showcerts
```

**Log analysis**:

```bash
# Filter by level
cat broker.log | jq 'select(.level == "ERROR")'

# Filter by tenant
cat broker.log | jq 'select(.fields.tenant_id == "acme-corp")'

# Count errors by type
cat broker.log | jq -r 'select(.level == "ERROR") | .message' | sort | uniq -c
```

**Performance profiling**:

```bash
# CPU profiling (future)
curl http://broker:8080/debug/pprof/profile?seconds=30 > cpu.prof

# Memory profiling (future)
curl http://broker:8080/debug/pprof/heap > heap.prof
```

## Best Practices

1. ✓ Use INFO level in production
2. ✓ Enable JSON logs for structured parsing
3. ✓ Centralize logs in aggregation system
4. ✓ Monitor key metrics (latency, throughput, errors)
5. ✓ Set up alerts for critical conditions
6. ✓ Enable telemetry only for debugging
7. ✓ Document baseline performance metrics
8. ✓ Create runbooks for common issues
9. ✓ Test monitoring and alerting before production
10. ✓ Review logs and metrics weekly

!!! tip "Observability is Essential"
    Felix is designed to be observable. Structured logs, metrics, and telemetry make it possible to understand system behavior, debug issues quickly, and optimize performance with confidence.
