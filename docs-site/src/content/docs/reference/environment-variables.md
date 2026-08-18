---
title: "Environment Variables Reference"
---

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

**Default**: `512`

```bash
export FELIX_SUBSCRIBER_QUEUE_CAPACITY="512"
# Alias (same behavior):
export FELIX_SUB_QUEUE_CAPACITY="512"
```

### `FELIX_MAX_SUBSCRIPTIONS_PER_CONN`

**Description**: Max concurrent subscriptions a single QUIC connection may hold. Independent of `FELIX_SUBSCRIBER_QUEUE_CAPACITY` (which bounds one subscription's buffer size) — this bounds how many subscriptions a connection can open in total, protecting broker memory from a connection that opens unbounded subscriptions.

**Type**: Positive integer (count)

**Default**: `4096`

```bash
export FELIX_MAX_SUBSCRIPTIONS_PER_CONN="4096"
```

### `FELIX_SUB_QUEUE_POLICY`

**Description**: Backpressure policy when broker subscriber queues are full.

**Type**: Enum (`block`, `drop_new`, `drop_old`)

**Default**: `drop_new`

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

**Default**: `false`

```bash
export FELIX_SUB_SINGLE_WRITER_PER_CONN="true"
```

### `FELIX_SUB_WRITER_LANES`

**Description**: Requested outbound subscriber writer lanes.

**Type**: Positive integer (count)

**Default**: `4`

```bash
export FELIX_SUB_WRITER_LANES="4"
# Alias (checked first, same behavior):
export FELIX_SUB_EGRESS_LANES="4"
```

### `FELIX_SUB_LANE_QUEUE_DEPTH`

**Description**: Queue depth per outbound writer lane.

**Type**: Positive integer (count)

**Default**: `64`

```bash
export FELIX_SUB_LANE_QUEUE_DEPTH="64"
# Alias (same behavior):
export FELIX_SUB_QUEUE_BOUND="64"
```

### `FELIX_SUB_QUEUE_MODE`

**Description**: Backpressure policy for the writer-lane command queue (downstream of `FELIX_SUB_QUEUE_POLICY`, which gates the earlier broker-core fanout enqueue).

**Type**: Enum (`block`, `drop_new`, `drop_old`)

**Default**: `drop_new`

```bash
export FELIX_SUB_QUEUE_MODE="drop_new"
# Alias (same behavior):
export FELIX_SUB_LANE_QUEUE_POLICY="drop_new"
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

### `FELIX_SUB_FLUSH_MAX_ITEMS`

**Description**: Maximum queued lane commands drained per flush before a write is issued.

**Type**: Positive integer (count)

**Default**: `16`

```bash
export FELIX_SUB_FLUSH_MAX_ITEMS="16"
```

### `FELIX_SUB_FLUSH_MAX_DELAY_US`

**Description**: Maximum time spent waiting to fill a lane flush buffer before writing what's accumulated.

**Type**: Unsigned integer (microseconds)

**Default**: `50`

```bash
export FELIX_SUB_FLUSH_MAX_DELAY_US="50"
```

### `FELIX_SUB_MAX_BYTES_PER_WRITE`

**Description**: Upper bound on coalesced bytes per QUIC write call to a subscriber stream.

**Type**: Positive integer (bytes)

**Default**: `65536` (64 KiB)

```bash
export FELIX_SUB_MAX_BYTES_PER_WRITE="65536"
```

### `FELIX_SUB_STREAMS_PER_CONN`

**Description**: Number of delivery streams per connection in hashed-pool mode.

**Type**: Positive integer (count)

**Default**: `4`

```bash
export FELIX_SUB_STREAMS_PER_CONN="4"
```

### `FELIX_SUB_STREAM_MODE`

**Description**: Strategy for mapping subscribers to event streams. `hashed_pool` is not yet enabled — the broker falls back to `per_subscriber` and logs a debug warning if requested.

**Type**: Enum (`per_subscriber`, `hashed_pool`)

**Default**: `per_subscriber`

```bash
export FELIX_SUB_STREAM_MODE="per_subscriber"
```

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

**Default**: `256`

```bash
export FELIX_CLIENT_SUB_QUEUE_CAPACITY="256"
```

### `FELIX_CLIENT_SUB_QUEUE_POLICY`

**Description**: Client-side backpressure policy for subscription pipeline queues.

**Type**: Enum (`block`, `drop_new`, `drop_old`)

**Default**: `drop_new`

```bash
export FELIX_CLIENT_SUB_QUEUE_POLICY="drop_new"
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

### `FELIX_PUBLISH_QUEUE_DEPTH`

**Description**: Bounded request queue depth per client publish worker.

**Type**: Positive integer (count)

**Default**: `64`

```bash
export FELIX_PUBLISH_QUEUE_DEPTH="64"
```

### `FELIX_PUBLISH_INFLIGHT_BYTES`

**Description**: Shared queued and in-flight publish byte budget across client workers.

**Type**: Positive integer (bytes)

**Default**: `4194304` (4 MiB)

```bash
export FELIX_PUBLISH_INFLIGHT_BYTES="4194304"
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

**Default**: `64`

**Example**:
```bash
export FELIX_BROKER_PUB_QUEUE_DEPTH="64"
export FELIX_BROKER_PUB_QUEUE_DEPTH="256"  # More buffering
export FELIX_BROKER_PUB_QUEUE_DEPTH="32"   # Less memory
```

### `FELIX_BROKER_PUBLISH_INFLIGHT_BYTES`

**Description**: Shared in-flight publish byte budget across all publish workers (process-wide). Bounds queued-or-processing bytes independent of `FELIX_BROKER_PUB_QUEUE_DEPTH`'s item count, so a handful of large payloads/batches can't blow past the intended ingress memory budget.

**Type**: Positive integer (bytes)

**Default**: `67108864` (64 MiB)

```bash
export FELIX_BROKER_PUBLISH_INFLIGHT_BYTES="67108864"
```

### `FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES`

**Description**: Per-connection share of `FELIX_BROKER_PUBLISH_INFLIGHT_BYTES`. Bounds how much of the shared, process-wide publish byte budget a single connection can occupy at once, so one connection publishing large batches can't starve every other connection's admission into the shared budget.

**Type**: Positive integer (bytes)

**Default**: `16777216` (16 MiB)

```bash
export FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES="16777216"
```

### `FELIX_PUB_INGRESS_WAIT`

**Description**: When enabled, un-acked (fire-and-forget) publishes wait — bounded by `FELIX_PUBLISH_QUEUE_WAIT_MS` — for ingress capacity instead of being shed when the publish queue or byte budget is full. Backpressure then propagates through QUIC flow control to the publisher. Leave off in production for visible shedding under overload; turn on for lossless pipelines and sustainable-throughput benchmarking.

**Type**: Boolean (`1`, `true`, `yes` = enabled)

**Default**: disabled

```bash
export FELIX_PUB_INGRESS_WAIT="1"
```

### `FELIX_CORE_SHARDS`

**Description**: Number of core-pinned shard executors owning stream work (thread-per-core, shared-nothing). Each stream is owned by one shard: its publish worker and its subscriptions' lane feeders run on that shard's dedicated single-threaded runtime, pinned to a CPU core on Linux. Benefits scale with stream count; single-stream workloads serialize on one shard by design.

**Type**: Positive integer (count; `0` = disabled)

**Default**: `0`

```bash
export FELIX_CORE_SHARDS="4"
```

## QUIC Transport Tuning

Process-wide levers read by every Felix QUIC endpoint (broker, client, demos). See [Benchmarks](/felix/features/benchmarks/) for measured impact.

### `FELIX_MTU_UPPER_BOUND`

**Description**: Upper bound for QUIC path-MTU discovery. Probes are loss-tolerant, so the bound is safe on any network; discovery converges to the real path MTU at or below it. The default matches loopback/jumbo ceilings; small-message-dominated workloads may prefer `4096` (finer ACK clocking).

**Type**: Positive integer (bytes, clamped to 1200–65527)

**Default**: `16384`

```bash
export FELIX_MTU_UPPER_BOUND="16384"
export FELIX_MTU_UPPER_BOUND="4096"   # small-message optimized
```

### `FELIX_INITIAL_MTU`

**Description**: Starting datagram size before path-MTU discovery completes. The RFC-safe default works everywhere; raising it on known-good paths (jumbo-frame LAN) skips the discovery ramp. Connections to a loopback peer automatically start at the loopback MTU and *guarantee* it, which makes the path immune to spurious black-hole collapse (see `FELIX_MTU_BLACK_HOLE_COOLDOWN_MS`) and, because the guarantee also freezes the discovery bound, removes probe traffic entirely.

The guaranteed size is **16,336 bytes on macOS and 4,096 elsewhere** (both capped by `FELIX_MTU_UPPER_BOUND`). The lower cap off macOS is not conservatism. Linux UDP GSO packs a whole `sendmsg` batch into a single IP datagram, so `MTU × segments` must stay under 65,535; quinn batches up to 10, putting the real ceiling at 6,553 bytes. Above it the kernel rejects every batch and delivery stalls outright — measured as a total stall at both 8,192 and 16,336. macOS has no GSO (one syscall per datagram) and no such limit. 4,096 also measured fastest on Linux; see the [performance case study](/felix/features/performance-case-study/).

The loopback path additionally requires the socket's *granted* UDP buffers to reach ~1 MiB. That threshold is a proxy for "this host has been tuned", not a burst-headroom calculation — hosts where Linux silently clamps `SO_RCVBUF` to a stock `net.core.rmem_max` (~208 KB) keep the RFC-safe default; raise `rmem_max`/`wmem_max` to enable it. Setting `FELIX_INITIAL_MTU` explicitly disables the loopback special case and applies to every path.

**Type**: Positive integer (bytes, clamped to 1200–65527)

**Default**: `1200` (loopback peers: `16336` on macOS, `4096` elsewhere)

```bash
export FELIX_INITIAL_MTU="1200"
```

### `FELIX_MTU_BLACK_HOLE_COOLDOWN_MS`

**Description**: How long a connection waits after an MTU black-hole verdict before re-probing for a larger MTU. Quinn's black-hole detector cannot distinguish a path that silently drops large packets from a congestive loss burst that happened to contain only full-MTU packets (which is what overflowing the peer's UDP socket buffer looks like at high rate). A false verdict collapses the path MTU to the initial value, multiplying datagram and syscall counts per byte by ~13× on a 16 KiB-MTU path; quinn's stock 60-second cooldown then pins that state. Felix shortens the cooldown so a spurious collapse re-probes at the connection's next idle gap. Note that quinn only sends recovery probes when the connection has nothing else to transmit, so a sender with a continuous backlog cannot recover until its load has a gap regardless of this setting — which is why loopback connections start at full MTU instead (see `FELIX_INITIAL_MTU`).

**Type**: Positive integer (milliseconds, minimum 100)

**Default**: `2000`

```bash
export FELIX_MTU_BLACK_HOLE_COOLDOWN_MS="2000"
```

### `FELIX_INITIAL_CWND`

**Description**: Optional initial congestion window override in bytes. By default Felix keeps Quinn's RFC 9002 behavior, including raising the minimum window to two datagrams after path-MTU discovery. Increase only on trusted low-loss paths where a larger initial burst is acceptable.

**Type**: Positive integer (bytes)

**Default**: Quinn's RFC 9002 default

```bash
export FELIX_INITIAL_CWND="1048576"
```

### `FELIX_UDP_SEND_BUFFER` / `FELIX_UDP_RECV_BUFFER`

**Description**: Requested UDP socket buffer sizes (SO_SNDBUF / SO_RCVBUF). Applied best-effort: halved until the OS accepts. Kernel-level datagram drops surface as QUIC retransmits and tail-latency spikes, so large buffers matter at high message rates.

**Type**: Positive integer (bytes)

**Default**: `8388608` (8 MiB)

```bash
export FELIX_UDP_SEND_BUFFER="8388608"
export FELIX_UDP_RECV_BUFFER="8388608"
```

### `FELIX_MAX_UDP_PAYLOAD`

**Description**: Largest UDP datagram the endpoint accepts (receive side). Must be at least the peer's discovered MTU or large datagrams are rejected.

**Type**: Positive integer (bytes, clamped to 1200–65527)

**Default**: `65527`

```bash
export FELIX_MAX_UDP_PAYLOAD="65527"
```

### `FELIX_IO_RUNTIME_THREADS`

**Description**: Size of the dedicated QUIC I/O runtime pool. Quinn's driver tasks (endpoint receive loop, per-connection transmit/ACK loops) do a bounded slice of work per poll and reschedule themselves, so their scheduler re-poll latency is the transport's throughput ceiling. Felix therefore runs them on a pool of single-threaded runtimes isolated from application tasks, assigned by role: server endpoints spread across every runtime but the last, client endpoints share the last. `0` disables the isolation and runs drivers on the application runtime (the pre-fix behavior). A larger pool cannot make a single endpoint faster — an endpoint's driver is one task on one runtime — and it splits endpoints that talk to each other onto separate threads, which measured 5–6× slower.

:::caution[A macOS optimization; off by default elsewhere]
The ceiling this pool removes is specific to macOS. On Linux the same
benchmark already sustains ~643 MB/s (628 K msg/s × 1 KiB) *without* it —
more than macOS reaches even with the pool — and isolating the drivers there
only adds a cross-thread hop per datagram. Measured on Linux: p50 latency
86 µs → 152 µs and fanout-10 throughput 1.48 M → 1.09 M msg/s, consistent
across pool sizes 1/2/4/8 and with pump colocation on or off. Non-macOS
platforms therefore default to `0`; set the variable explicitly to
experiment.
:::

**Type**: Non-negative integer

**Default**: `2` on macOS, `0` elsewhere

```bash
export FELIX_IO_RUNTIME_THREADS="2"
export FELIX_IO_RUNTIME_THREADS="0"   # disable driver isolation
```

### `FELIX_ACK_ELICITING_THRESHOLD`

**Description**: How many ack-eliciting packets a peer may receive before it must send an ACK (QUIC ACK-frequency extension; applies between quinn peers). The RFC default of every other packet costs a reverse-path datagram — plus its wakeup chain — per ~2 datagrams of data; the higher default trades a little loss-detection latency (bounded by the 2 ms `max_ack_delay` Felix also negotiates) for measurably less per-byte wakeup traffic (~+15% throughput on loopback).

**Type**: Positive integer (packets)

**Default**: `20`

```bash
export FELIX_ACK_ELICITING_THRESHOLD="20"
export FELIX_ACK_ELICITING_THRESHOLD="1"   # RFC-like cadence
```

### `FELIX_ACK_FREQ_DISABLE`

**Description**: Set to any value to skip negotiating the ACK-frequency extension entirely, restoring stock quinn ACK behavior (25 ms max ack delay, ACK every other packet).

**Type**: Presence toggle

**Default**: unset (extension negotiated)

```bash
export FELIX_ACK_FREQ_DISABLE="1"
```

### `FELIX_CONN_STATS_MS`

**Description**: Log live `quinn::ConnectionStats` (path MTU, cwnd, rtt, loss, congestion events, blocked-frame counters, UDP datagram/byte/io counts) for every connection on this interval, on both the broker and the client. The client-side log is the only place the publish path's sender-side congestion state is visible. Diagnostic; off unless set.

**Type**: Positive integer (milliseconds)

**Default**: unset (disabled)

```bash
export FELIX_CONN_STATS_MS="1000"
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

### `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS`

**Description**: Total budget for draining in-flight work after SIGTERM or SIGINT,
before remaining tasks are force-cancelled. Applies to both the broker and the
control plane. This is a single budget shared by every subsystem, not a per-subsystem
timeout, so total shutdown time stays bounded by this value.

**Type**: Positive integer (milliseconds)

**Default**: `25000`

**Example**:
```bash
export FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS="25000"
export FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS="55000"  # With terminationGracePeriodSeconds: 60
export FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS="5000"   # Fast rollouts, short-lived requests
```

**Note**: Keep this below the platform's kill deadline — Kubernetes'
`terminationGracePeriodSeconds` (default `30`) — so the drain finishes and logs its
outcome before SIGKILL. See [Graceful Shutdown](/felix/deployment/graceful-shutdown/).

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

## Durable Storage Configuration

Durable stream storage is opt-in. With `FELIX_DURABLE_STORAGE_DIR` unset the
broker is in-memory only, and any stream the control plane marks `durable: true`
is **rejected at registration** rather than silently downgraded to a guarantee
the broker cannot keep.

See [Durable Storage](/felix/architecture/durable-storage/) for what each policy
guarantees and what it costs.

### `FELIX_DURABLE_STORAGE_DIR`

**Description**: Root directory for durable stream segments. Setting it enables
durable streams; one subdirectory is created per stream shard.

**Type**: Path

**Default**: unset (durable storage disabled)

**Example**:
```bash
export FELIX_DURABLE_STORAGE_DIR="/var/lib/felix/streams"
```

### `FELIX_DURABLE_FSYNC_MODE`

**Description**: When written bytes are pushed to the storage device.

**Type**: One of `none`, `periodic`, `on_commit`

**Default**: `periodic`

| Value | Acknowledged when | Loss window |
| --- | --- | --- |
| `none` | bytes reach the page cache | unbounded — survives a process crash, not a power loss |
| `periodic` | bytes reach the page cache | one flush interval |
| `on_commit` | bytes reach the device | none |

**Example**:
```bash
export FELIX_DURABLE_FSYNC_MODE="on_commit"
```

**Trade-off**: `on_commit` costs one device flush per commit (~4ms on typical
NVMe), amortised across concurrent publishers by group commit. `periodic` adds
no measurable append latency at all.

### `FELIX_DURABLE_FSYNC_INTERVAL_MS`

**Description**: Flush interval for `FELIX_DURABLE_FSYNC_MODE=periodic`. Bounds
how much acknowledged data a machine crash can lose.

**Type**: Positive integer (milliseconds)

**Default**: `250`

**Example**:
```bash
export FELIX_DURABLE_FSYNC_INTERVAL_MS="100"
```

**Note**: Setting this without setting the mode implies `periodic`. Zero is
rejected at startup — it is a busy loop, not "always sync"; use `on_commit` for
per-commit durability.

### `FELIX_DURABLE_SEGMENT_BYTES`

**Description**: Size at which the active segment rolls over to a new file.

**Type**: Positive integer (bytes)

**Default**: `268435456` (256 MiB)

**Example**:
```bash
export FELIX_DURABLE_SEGMENT_BYTES="67108864"  # 64 MiB
```

**Trade-off**: Smaller segments bound recovery time (only the active segment is
fully scanned at startup) at the cost of more files and more rollovers.

### `FELIX_DURABLE_INDEX_SPACING_BYTES`

**Description**: Bytes of segment data between sparse index entries. A read
binary-searches the index, then scans forward at most one interval.

**Type**: Positive integer (bytes)

**Default**: `4096`

**Example**:
```bash
export FELIX_DURABLE_INDEX_SPACING_BYTES="8192"
```

**Trade-off**: Smaller spacing means faster seeks and larger index files.

### `FELIX_DURABLE_MAX_RECORDS_PER_READ`

**Description**: Ceiling on records returned by a single range read, on top of
the caller's byte budget. Payload bytes alone do not bound a response made of
empty records.

**Type**: Positive integer

**Default**: `10000`

**Example**:
```bash
export FELIX_DURABLE_MAX_RECORDS_PER_READ="5000"
```

### `FELIX_DURABLE_PREALLOCATE`

**Description**: Reserve a segment's blocks when it is created, keeping block
allocation off the append path.

**Type**: Boolean

**Default**: `true`

**Example**:
```bash
export FELIX_DURABLE_PREALLOCATE="false"
```

**Note**: Disable on filesystems where reservations are expensive or where thin
provisioning makes them counter-productive.

### `FELIX_DURABLE_VERIFY_ALL_ON_OPEN`

**Description**: Checksum every record of every segment at startup.

**Type**: Boolean

**Default**: `false`

**Example**:
```bash
export FELIX_DURABLE_VERIFY_ALL_ON_OPEN="true"
```

**Trade-off**: Off by default because startup would otherwise cost one full pass
over all data on disk. The active segment is always fully scanned regardless, and
every read verifies the records it returns — so bit rot in cold data is still
caught, just when it is read rather than at boot.

## Validation

Check current configuration:

```bash
# Print effective configuration
cargo run --release -p broker -- --dump-config

# Validate without starting
cargo run --release -p broker -- --validate-config
```

## Next Steps

- **Full configuration details**: [Configuration Reference](/felix/reference/configuration/)
- **Troubleshooting**: [Troubleshooting Guide](/felix/reference/troubleshooting/)
- **Performance tuning**: [Performance Guide](/felix/features/performance/)
