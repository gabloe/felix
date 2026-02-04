# Frequently Asked Questions

Common questions about Felix, its design, and usage.

## General Questions

### What is Felix?

Felix is a low-latency, QUIC-based pub/sub and distributed cache system designed for high fanout, high throughput, and predictable tail latency. It unifies event streaming (publish/subscribe) and request/response caching (put/get with TTL) over a single transport protocol.

Key features:

- **QUIC transport**: Modern, multiplexed, encrypted by default
- **Unified protocol**: Single wire protocol for pub/sub and cache
- **Predictable latency**: Optimized for p99/p999, not just throughput
- **Kubernetes-native**: Designed for cloud-native deployments
- **Region-aware**: Built-in support for data sovereignty

### How is Felix different from Kafka?

Felix is **not** a Kafka replacement but serves different use cases:

| Feature | Felix | Kafka |
|---------|-------|-------|
| **Transport** | QUIC (UDP) | TCP |
| **Encryption** | Built-in (TLS 1.3) | Optional (TLS/SASL) |
| **Latency focus** | p99/p999 optimization | Throughput optimization |
| **Primitives** | Pub/sub + cache unified | Log-based streaming |
| **Persistence** | Optional per stream | Always durable |
| **Fanout** | Native high fanout | Consumer groups |
| **Use case** | Low-latency streaming, real-time cache | High-throughput log processing |

**Use Felix when**:
- You need ultra-low latency (sub-millisecond to low-millisecond p99)
- High fanout with many concurrent subscribers
- Combined pub/sub and caching requirements
- QUIC transport benefits (multiplexing, better loss recovery)

**Use Kafka when**:
- You need guaranteed durability and replay
- Processing historical data with consumer groups
- Existing Kafka ecosystem integrations
- Traditional log-based semantics

### How is Felix different from Redis?

Felix complements Redis rather than replacing it:

| Feature | Felix | Redis |
|---------|-------|-------|
| **Primary use** | Pub/sub + cache | Cache + data structures |
| **Transport** | QUIC | TCP (RESP protocol) |
| **Streaming** | First-class pub/sub | Basic pub/sub |
| **Batching** | Native batch delivery | Individual messages |
| **Persistence** | Optional, log-based | RDB/AOF snapshots |
| **Fanout** | Optimized for high fanout | Basic pub/sub fanout |

**Use Felix for**:
- High-fanout real-time event distribution
- Streaming with batching and flow control
- Combined streaming and caching workloads

**Use Redis for**:
- Rich data structures (sets, sorted sets, etc.)
- Lua scripting and transactions
- Existing Redis ecosystem tools

### Is Felix production-ready?

**No, Felix is in early active development.** Current status:

- ✅ Core protocol stabilizing
- ✅ Single-node broker working
- ✅ Rust client SDK functional
- ⏳ Multi-node clustering in progress
- ⏳ Control plane under development
- ❌ Production hardening incomplete
- ❌ No official releases yet

**Use Felix for**:
- Research and prototyping
- Performance benchmarking
- Contributing to development

**Do not use Felix for**:
- Production workloads requiring high availability
- Scenarios requiring data durability guarantees
- Mission-critical applications

## Architecture Questions

### Can I grant stream access by IdP group instead of per-user?

Yes. Configure `groups_claim` for the tenant issuer, then bind RBAC roles to
`group:<name>` subjects. During token exchange, Felix maps incoming group claims
to those group subjects and evaluates role permissions.

Example:
- grouping: `g, group:g1, role:reader, tenant-a`
- policy: `p, role:reader, tenant-a, stream:tenant-a/payments/*, stream.subscribe`

### Why QUIC instead of TCP?

QUIC provides several advantages for Felix's use case:

**Benefits**:

1. **Multiplexing without head-of-line blocking**: Multiple streams share one connection without one slow stream blocking others
2. **Built-in encryption**: TLS 1.3 integrated, no separate TLS handshake
3. **Connection migration**: Survive IP changes (mobile, load balancer updates)
4. **Fast connection establishment**: 0-RTT for resumed connections
5. **Better loss recovery**: Stream-level retransmits instead of connection-level
6. **Modern congestion control**: BBR and other advanced algorithms

**Trade-offs**:

- UDP may be blocked by some networks (though increasingly rare)
- Requires newer network infrastructure for optimal performance
- Limited debugging tools compared to TCP

### What is the wire protocol?

Felix uses `felix-wire`, a framed protocol with:

- **Binary event frames**: Efficient `EventBatch` delivery for subscriptions
- **Binary fast paths**: Zero-copy binary frames for high-throughput data
- **Versioned envelope**: Forward/backward compatibility
- **Type-specific framing**: Different frame types for pub/sub, cache, control

This hybrid approach balances observability with performance.

### How does Felix handle backpressure?

Felix implements backpressure at multiple levels:

1. **QUIC flow control**: Built-in connection and stream-level credit
2. **Publish queues**: Bounded queues with timeout-based blocking
3. **Event queues**: Per-subscription buffering with depth limits
4. **Batching delays**: Implicit batching creates natural flow smoothing
5. **Subscriber isolation**: Slow subscribers don't block fast ones

When backpressure triggers:
- Publishers receive timeout errors if queues full
- Subscribers drop behind but don't impact others (future: disconnect slow subscribers)
- Flow control credit exhaustion blocks at QUIC layer

### What is ephemeral vs durable storage?

**Ephemeral streams**:
- In-memory only
- Ultra-low latency (no disk I/O)
- Data lost on broker restart
- Suitable for real-time data, metrics, logs

**Durable streams** (planned):
- WAL + segmented log on persistent storage
- Survive restarts
- Replay capability
- Higher latency (disk writes)
- Suitable for business events, audit logs

Currently, Felix MVP supports **ephemeral only**. Durability is planned.

### How does clustering work?

**Current**: Single-node broker only.

**Planned**:
- **Sharding**: Streams divided into shards, distributed across brokers
- **Replication**: Raft-based consensus for shard replicas
- **Leader election**: One leader per shard accepts writes
- **Follower reads**: Optional read-from-follower for scalability
- **Metadata service**: Control plane tracks topology and routing

See [Control Plane documentation](../api/control-plane-api.md) for details.

## Configuration Questions

### What's the most important configuration parameter?

**For latency**: `FELIX_EVENT_BATCH_MAX_DELAY_US`

This controls the maximum time events wait in a batch before being sent. Lower values reduce latency but may decrease throughput.

```bash
# Ultra-low latency
export FELIX_EVENT_BATCH_MAX_DELAY_US="50"

# Balanced
export FELIX_EVENT_BATCH_MAX_DELAY_US="250"

# High throughput
export FELIX_EVENT_BATCH_MAX_DELAY_US="1000"
```

**For throughput**: `FELIX_EVENT_BATCH_MAX_EVENTS`

```bash
# Small batches
export FELIX_EVENT_BATCH_MAX_EVENTS="16"

# Large batches
export FELIX_EVENT_BATCH_MAX_EVENTS="256"
```

### Should I enable binary encoding?

Subscription event delivery is already binary `EventBatch` by default.

### How do I tune for high fanout?

High fanout (100+ subscribers):

```bash
# Increase fanout batch size
export FELIX_FANOUT_BATCH="128"

# Increase per-subscriber buffering
export FELIX_SUBSCRIBER_QUEUE_CAPACITY="256"
export FELIX_SUB_WRITER_LANES="4"

# Enable batching
export FELIX_EVENT_BATCH_MAX_EVENTS="128"
export FELIX_EVENT_BATCH_MAX_DELAY_US="500"

# Consider disabling timings
export FELIX_DISABLE_TIMINGS="1"
```

Monitor for:
- CPU saturation (scale horizontally)
- Memory pressure (adjust windows)
- Slow subscribers (may need to disconnect)

### How much memory does Felix need?

Memory usage depends on configuration:

**Minimal setup**: ~100-200 MB base

**Per connection memory** (approximate):
```
conn_memory = cache_conn_recv_window + (cache_stream_recv_window × streams_per_conn)
```

**Example (default config)**:
```
256 MiB + (64 MiB × 4) = 512 MiB per connection

With FELIX_CACHE_CONN_POOL=8:
8 × 512 MiB = 4 GiB potential max
```

**Recommendations**:
- **Development**: 2-4 GB
- **Production**: 4-8 GB base, adjust based on workload
- **High throughput**: 8-16 GB

Monitor actual RSS usage and adjust windows accordingly.

## Performance Questions

### What latency can I expect?

**Localhost benchmarks** (release build, timings disabled):

**Pub/sub (fanout=1, batch=1)**:
- p50: 0.5-2 ms
- p99: 2-10 ms
- p999: 10-50 ms

**Pub/sub (fanout=10, batch=64)**:
- p50: 30-50 ms
- p99: 60-100 ms
- Throughput: 100-200k msgs/s

**Cache operations** (concurrency=32):
- `get_hit` p50: 160-240 µs
- `get_miss` p50: 160-180 µs
- `put` p50: 160-260 µs

**Network latency adds**:
- Same datacenter: +0.5-2 ms
- Cross-region: +20-200 ms

These are reference points. Actual performance depends on hardware, network, configuration, and workload.

### Why is debug build so slow?

Debug builds include:
- No optimizations
- Debug assertions
- Symbol information
- Bounds checking

**Performance impact**: 10-100x slower than release builds.

**Always use release builds for benchmarking**:
```bash
cargo build --release
cargo run --release -p broker
```

### How do I reduce memory usage?

1. **Reduce flow-control windows**:
```bash
export FELIX_CACHE_CONN_RECV_WINDOW="134217728"    # 128 MiB
export FELIX_CACHE_STREAM_RECV_WINDOW="33554432"   # 32 MiB
export FELIX_EVENT_CONN_RECV_WINDOW="134217728"
```

2. **Reduce connection pools** (client-side):
```bash
export FELIX_CACHE_CONN_POOL="4"
export FELIX_EVENT_CONN_POOL="4"
```

3. **Reduce queue depths**:
```bash
export FELIX_BROKER_PUB_QUEUE_DEPTH="512"
export FELIX_SUBSCRIBER_QUEUE_CAPACITY="64"
```

4. **Limit concurrent connections**: Configure client connection limits.

### How do I maximize throughput?

1. **Increase batch sizes**:
```bash
export FELIX_EVENT_BATCH_MAX_EVENTS="256"
export FELIX_EVENT_BATCH_MAX_BYTES="1048576"
export FELIX_EVENT_BATCH_MAX_DELAY_US="1000"
```

2. **Disable timings**:
```bash
export FELIX_DISABLE_TIMINGS="1"
```

3. **Increase parallelism**:
```bash
export FELIX_EVENT_CONN_POOL="16"
export FELIX_FANOUT_BATCH="128"
```

4. **Scale horizontally**: Run multiple broker instances.

## Deployment Questions

### Can I run Felix in Docker?

Yes! See [Docker Compose guide](../deployment/docker-compose.md).

```bash
docker compose up -d felix-broker
```

Pre-built images are not yet available; build from source using the provided `docker/broker.Dockerfile`.

### Can I run Felix on Kubernetes?

Yes! Felix is designed for Kubernetes. See [Kubernetes guide](../deployment/kubernetes.md).

Use StatefulSets for stable identity and persistent storage:

```bash
kubectl apply -f deploy/kubernetes/statefulset.yaml
```

### How do I monitor Felix?

**Metrics endpoint**:
```bash
curl http://broker:8080/healthz
curl http://broker:8080/metrics  # If telemetry enabled
```

**Structured logs**:
```bash
export RUST_LOG="info"
# Or debug for verbose logging
export RUST_LOG="felix_broker=debug"
```

**Prometheus** (when telemetry enabled):
- Scrape `/metrics` endpoint
- Use provided Grafana dashboards (future)

**Observability guide**: [Observability](../features/observability.md)

### How do I secure Felix?

**Current state**: Transport security only (QUIC/TLS 1.3).

**Planned**:
- mTLS authentication
- Token-based authorization
- Tenant isolation
- Encryption at rest
- Audit logging

**Best practices** (now):
- Run in private networks
- Use network policies (Kubernetes)
- Limit exposed ports
- Monitor access logs

See [Security guide](../features/security.md) for details.

## Development Questions

### How do I contribute?

See [Contributing guide](../development/contributing.md).

Quick start:
1. Fork repository
2. Create feature branch
3. Make changes with tests
4. Run `task lint` and `task test`
5. Submit pull request

Felix welcomes contributions!

### How do I run tests?

```bash
# All tests
cargo test --workspace

# Specific crate
cargo test -p felix-broker

# With output
cargo test -- --nocapture

# Using Task
task test
```

### How do I build the documentation?

```bash
# Install mkdocs
pip install mkdocs mkdocs-material

# Serve locally
cd docs-site
mkdocs serve

# Build static site
mkdocs build
```

### What's the project structure?

```
crates/           # Rust crates
  felix-broker/   # Broker core logic
  felix-wire/     # Protocol framing
  felix-client/   # Client SDK
  ...
services/         # Runnable binaries
  broker/         # Broker service
docs/             # Design docs
docs-site/        # User documentation (mkdocs)
```

See [Project Structure](../development/project-structure.md) for details.

## Roadmap Questions

### When will Felix have durability?

Durable storage is planned for **Q2 2026** (tentative):

- WAL-based append log
- Segmented storage
- Retention policies
- Replay from offset

Track progress in GitHub issues.

### When will clustering be available?

Multi-node clustering is in active development:

- **Sharding**: Q1 2026 (tentative)
- **Replication**: Q2 2026 (tentative)
- **Control plane**: In progress

See [Control Plane docs](../api/control-plane-api.md) for design.

### Will there be clients for other languages?

Planned client SDKs:

- ✅ **Rust**: Available (in progress)
- ⏳ **Go**: Planned
- ⏳ **Python**: Planned
- ⏳ **Java**: Planned
- ⏳ **JavaScript/TypeScript**: Planned

Community contributions welcome! The wire protocol is language-agnostic.

### What about exactly-once semantics?

Exactly-once delivery is extremely difficult in distributed systems and often misleading. Felix focuses on **at-least-once** with idempotency support:

**Current**: At-least-once delivery with acks (when `ack_on_commit=true`)

**Future**:
- Deduplication based on message IDs
- Idempotent producer semantics
- Transaction support (under research)

For exactly-once processing, implement idempotent consumers.

## Troubleshooting Questions

### Why won't the broker start?

Common causes:

1. **Port in use**: Change `FELIX_QUIC_BIND`
2. **Invalid config**: Check YAML syntax
3. **Missing dependencies**: Run `cargo build`
4. **Wrong Rust version**: Update to 1.92.0+

See [Troubleshooting guide](troubleshooting.md).

### Why is latency so high?

Check:

1. **Debug vs release build**: Use `--release`
2. **Timings enabled**: Disable with `FELIX_DISABLE_TIMINGS=1`
3. **Batch delay**: Reduce `FELIX_EVENT_BATCH_MAX_DELAY_US`
4. **Network latency**: Test localhost first
5. **System load**: Check CPU/memory usage

### How do I debug connection issues?

```bash
# Enable debug logging
export RUST_LOG="felix_transport=debug,felix_broker=debug"

# Check broker is listening
lsof -i UDP:5000

# Test connectivity
nc -zvu <broker-ip> 5000

# Capture traffic
sudo tcpdump -i any -w felix.pcap udp port 5000
```

## Comparison Questions

### Felix vs NATS?

| Feature | Felix | NATS |
|---------|-------|------|
| **Transport** | QUIC | TCP |
| **Persistence** | Planned | JetStream |
| **Maturity** | Early development | Production-ready |
| **Language** | Rust | Go |
| **Focus** | Low-latency, high fanout | Lightweight messaging |

Use NATS for production workloads today. Felix is experimental.

### Felix vs Pulsar?

| Feature | Felix | Pulsar |
|---------|-------|--------|
| **Transport** | QUIC | TCP |
| **Storage** | Planned | BookKeeper |
| **Maturity** | Early development | Production-ready |
| **Complexity** | Simple | Complex (multiple components) |

Pulsar is production-ready with many features. Felix is simpler and focused on latency.

### Felix vs RabbitMQ?

| Feature | Felix | RabbitMQ |
|---------|-------|----------|
| **Protocol** | QUIC/felix-wire | AMQP |
| **Routing** | Topic-based | Exchange/queue patterns |
| **Persistence** | Planned | Built-in |
| **Maturity** | Early development | Production-ready |

RabbitMQ is mature with rich routing. Felix focuses on simple, fast pub/sub.

## Next Steps

- **Quick start**: [Quickstart Guide](../getting-started/quickstart.md)
- **Detailed configuration**: [Configuration Reference](configuration.md)
- **Troubleshooting**: [Troubleshooting Guide](troubleshooting.md)
- **Contributing**: [Contributing Guide](../development/contributing.md)
