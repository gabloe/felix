# Benchmarks

Felix ships a reproducible benchmark harness (`latency-demo`) that measures
end-to-end pub/sub performance — client publish → broker fanout → client
delivery — over real QUIC connections with TLS 1.3. This page documents the
methodology, current results, the transport levers that matter, and how to
compare Felix against other pub/sub systems fairly.

## Running the benchmark

```bash
# Full matrix: latency-focused (batch=1) then throughput-focused (batch=64)
cargo run --release -p broker --bin latency-demo --all-features

# Single case
cargo run --release -p broker --bin latency-demo --all-features -- \
  --warmup 500 --total 20000 --payload 1024 --fanout 10 --batch 64
```

## Methodology

Numbers are only useful if they are honest. The harness enforces:

- **Truthful delivery windows.** Delivered throughput is computed from publish
  start to the instant the *last event actually arrived* — not to when drain
  tasks give up. Idle-timeout waits never inflate the denominator.
- **Lossless backpressure in throughput mode.** The throughput profile
  (batch > 1) runs with blocking queues end-to-end and bounded ingress waits
  (`pub_ingress_wait`), so the publisher is paced to the pipeline's sustainable
  rate and **every message is delivered** (`delivery drops 0`). A number
  measured while shedding is not a throughput number.
- **Latency mode measures per-message RTT.** The latency profile (batch = 1)
  publishes with per-message acks, measuring full round-trip behavior rather
  than fire-and-forget enqueue rates.
- **Warmup excluded.** Handshake, stream setup, path-MTU discovery, and
  congestion ramp are absorbed by warmup messages before measurement starts.
- **Fanout counted honestly.** `delivered throughput` counts every
  subscriber delivery; `per-sub throughput` divides by fanout.

## Results

Measured on an Apple Silicon macOS host, loopback, release build, TLS 1.3
enabled (QUIC always encrypts), defaults — no environment tuning.

### Latency profile (batch = 1, per-message ack)

| Payload | Fanout | p50 | p99 | p999 |
|---|---|---|---|---|
| 0 B | 1 | 121 µs | 199 µs | 311 µs |
| 256 B | 1 | 128 µs | 208 µs | 263 µs |
| 1 KiB | 1 | 124 µs | 212 µs | 329 µs |
| 0 B | 10 | 460 µs | 1.09 ms | 1.49 ms |
| 256 B | 10 | 447 µs | 1.13 ms | 2.09 ms |
| 1 KiB | 10 | 444 µs | 1.05 ms | 1.32 ms |

Sub-millisecond p999 at fanout 1; low-single-digit-ms p999 at fanout 10.

### Throughput profile (batch = 64, lossless, zero drops)

| Payload | Fanout | Delivered msg/s | Delivered bytes/s |
|---|---|---|---|
| 0 B | 1 | 650 K | — |
| 0 B | 10 | 2.46–2.58 M | — |
| 1 KiB | 1 | 244–263 K | ~250 MB/s |
| 1 KiB | 10 | 460 K | ~470 MB/s |
| 4 KiB | 1 | 62–87 K | ~255–356 MB/s |
| 4 KiB | 10 | ~107 K | ~437 MB/s |

Every row completes with `delivery drops 0`: these are sustainable rates under
end-to-end backpressure, not burst rates measured while shedding load.

### Encode-once fanout CPU efficiency

An A/B run of 200 K deliveries at 1 KiB × fanout 10 showed broker/client
user-space CPU falling from 1.43–1.61 s to 0.58–0.67 s after shared event-frame
fanout: roughly **60% less user CPU per delivered message**. Delivered
throughput remained within variance at 410–431 K msg/s because macOS loopback
was dominated by UDP kernel time; the same change improved 4 KiB × fanout 10
p99 latency from 365 ms to 221 ms. Linux GSO/GRO results should be measured
separately before extrapolating throughput gains.

## The transport levers that matter

These findings came out of profiling the QUIC path and are wired into
`felix-transport` defaults; each has an environment override:

| Lever | Default | Why it matters |
|---|---|---|
| `FELIX_MTU_UPPER_BOUND` | 16384 | Path-MTU discovery bound. quinn's stock 1452 caps loopback/jumbo paths at Ethernet size; bounding at the real path MTU (loopback lo0 = 16384) cut per-byte syscall and crypto costs ~7x for byte-heavy workloads. Small-message workloads can prefer ~4096 (finer ACK clocking, measured ~1.5x for 0-byte messages). |
| `FELIX_INITIAL_MTU` | 1200 | RFC-safe starting datagram size. Raise on known-good paths to skip discovery entirely. |
| `FELIX_INITIAL_CWND` | Quinn RFC default | Optional initial congestion-window override for trusted low-loss paths. Quinn raises its minimum window to two datagrams when path-MTU discovery finds a larger MTU; the measured default workloads did not benefit from a larger initial burst. |
| `FELIX_UDP_SEND_BUFFER` / `FELIX_UDP_RECV_BUFFER` | 8 MiB | Socket buffers absorb bursts; kernel-level drops surface as QUIC retransmits and tail-latency spikes. Applied best-effort (halved until the OS accepts). |
| `FELIX_MAX_UDP_PAYLOAD` | 65527 | Receive-side datagram cap. Must exceed the peer's discovered MTU or large datagrams are silently rejected. |

Broker-side levers (see [Configuration](../reference/configuration.md)):
`pub_inflight_bytes` (ingress byte budget), `pub_ingress_wait` (lossless
backpressure vs. shed-on-overload), subscriber queue policies
(`block` / `drop_new` / `drop_old`) and depths.

## Saturation behavior

Felix's production defaults favor **bounded latency with visible overload**:
subscriber queues default to `drop_new`, so an overloaded subscriber sheds
(counted in `felix_subscribe_dropped_total`) instead of growing unbounded
backlog. Flip to lossless pacing per deployment with
`FELIX_SUB_QUEUE_POLICY=block` + `FELIX_PUB_INGRESS_WAIT=1` when producers
should slow down rather than lose events.

## Comparing against other pub/sub systems

Cross-system numbers are only meaningful when measured side-by-side on the
same hardware, same payload sizes, same fanout, and same delivery guarantees.
Published figures for other brokers vary by an order of magnitude across
hardware and configurations, so treat any single citation with suspicion —
including ours. Structural differences to keep in mind:

- **Transport & encryption.** Felix runs QUIC with mandatory TLS 1.3 —
  per-packet crypto is included in every number above. NATS/Redis/Kafka
  benchmarks are typically plaintext TCP; enabling TLS on those systems
  materially changes their numbers.
- **Delivery model.** The Felix numbers above are *lossless, backpressured,
  fanout-counted* rates. Many published pub/sub benchmarks report publisher
  enqueue rates or allow silent slow-consumer drops (e.g. Redis client output
  buffer limits, NATS slow-consumer disconnects).
- **Batching.** Kafka-class systems trade latency for batch throughput;
  compare them against Felix's batch=64 profile, not the latency profile.

For a like-for-like harness against NATS on the same machine:

```bash
# NATS side (requires nats-server + nats CLI)
nats-server &
nats bench benchsubject --pub 1 --sub 10 --size 1024 --msgs 100000

# Felix side, matching shape
cargo run --release -p broker --bin latency-demo --all-features -- \
  --warmup 500 --total 100000 --payload 1024 --fanout 10 --batch 64
```

Compare *delivered* (per-subscriber × fanout) rates and tail latencies, and
match TLS configuration on both sides before drawing conclusions.

## Regenerating this page's data

```bash
cargo run --release -p broker --bin latency-demo --all-features 2>&1 | tee latency.log
```

Key output fields: `delivered throughput` (all subscribers), `delivered
per-sub throughput`, `p50/p99/p999` (per-message publish→delivery latency),
`delivery drops` (must be 0 in throughput profile), `publish submit
throughput` (client-side enqueue rate — an upper bound, not a delivery claim).
