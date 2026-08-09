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
- **Lossless backpressure in both profiles.** The throughput profile
  (batch > 1) runs with blocking queues end-to-end and bounded ingress waits
  (`pub_ingress_wait`), so the publisher is paced to the pipeline's sustainable
  rate. The latency profile (batch = 1) blocks the broker's core
  per-subscriber queue and the client's own subscriber channel, not just the
  writer-lane queue below them — both defaulted to `DropNew` and could drop
  warmup/measurement messages under load before that was tightened. In both
  profiles **every message is delivered** (`delivery drops 0`). A number
  measured while shedding is not a throughput or latency number.
- **Latency mode measures per-message RTT.** The latency profile (batch = 1)
  publishes with per-message acks, measuring full round-trip behavior rather
  than fire-and-forget enqueue rates.
- **Warmup excluded.** Handshake, stream setup, path-MTU discovery, and
  congestion ramp are absorbed by warmup messages before measurement starts.
- **Fanout counted honestly.** `delivered throughput` counts every
  subscriber delivery; `per-sub throughput` divides by fanout.

## Results

Measured on the same Apple Silicon host using native macOS and a Linux
devcontainer, loopback, release build, TLS 1.3 enabled (QUIC always encrypts),
defaults — no environment tuning. Linux can use UDP GSO/GRO, so the comparison
also shows how strongly kernel batching affects QUIC throughput.

### Latency profile (batch = 1, per-message ack)

| Payload | Fanout | macOS p50 / p99 / p999 | Linux p50 / p99 / p999 |
|---|---:|---|---|
| 0 B | 1 | 121 / 199 / 311 µs | 82 / 116 / 175 µs |
| 256 B | 1 | 128 / 208 / 263 µs | 91 / 172 / 241 µs |
| 1 KiB | 1 | 124 / 212 / 329 µs | 96 / 161 / 223 µs |
| 0 B | 10 | 460 µs / 1.09 / 1.49 ms | 98 / 147 / 193 µs |
| 256 B | 10 | 447 µs / 1.13 / 2.09 ms | 99 / 148 / 181 µs |
| 1 KiB | 10 | 444 µs / 1.05 / 1.32 ms | 142 / 720 µs / 4.96 ms |

Both platforms stay sub-millisecond at fanout 1. Linux substantially improves
most fanout-10 percentiles, although the 1 KiB p999 shows an isolated
low-single-digit-ms tail.

### Throughput profile (batch = 64, lossless, zero drops)

| Payload | Fanout | macOS JSON | Linux JSON | Linux binary |
|---|---:|---:|---:|---:|
| 0 B | 1 | 650 K | 1.29 M | 1.23 M |
| 0 B | 10 | 2.46–2.58 M | 1.19 M | 2.92 M |
| 1 KiB | 1 | 244–263 K | 178 K | 442 K |
| 1 KiB | 10 | 410–460 K | 1.54 M | 1.57 M |
| 4 KiB | 1 | 62–87 K | 97 K | 120 K |
| 4 KiB | 10 | ~107 K | 499–561 K | 566–590 K |

Every row completes with `delivery drops 0`: these are sustainable rates under
end-to-end backpressure, not burst rates measured while shedding load. Rates
count subscriber deliveries. The Linux 4 KiB × fanout 10 binary result delivers
about **2.3 GB/s** of payload.

The 1 KiB × fanout 10 Linux figures are averages of five interleaved runs:
JSON averaged 1.541 M msg/s and binary averaged 1.569 M msg/s. Binary won three
of five runs, but the averages are within 2%, confirming that the earlier
single-run JSON lead was variance rather than an encoding-path reversal.

### Encode-once fanout CPU efficiency

An A/B run of 200 K deliveries at 1 KiB × fanout 10 showed broker/client
user-space CPU falling from 1.43–1.61 s to 0.58–0.67 s after shared event-frame
fanout: roughly **60% less user CPU per delivered message**. Delivered
throughput remained within variance at 410–431 K msg/s because macOS loopback
was dominated by UDP kernel time; the same change improved 4 KiB × fanout 10
p99 latency from 365 ms to 221 ms.

Linux GSO closes that kernel-bound gap decisively: 4 KiB × fanout 10 reaches
499–561 K JSON and 566–590 K binary delivered msg/s, up to roughly 5x the
macOS JSON rate. For the representative 1 KiB × fanout 10 binary workload,
`/usr/bin/time -v` reported 1.23 s user time and 0.33 s system time over
0.59 s wall time. The equivalent macOS run used about 0.6 s user and 3.3 s
system time. System time therefore fell by about 90%, flipping the workload
from roughly 5.5:1 system-dominated to 3.7:1 user-dominated.

This Linux workload is no longer syscall-bound. The next optimization target is
user-space scheduling and synchronization, not an `io_uring` transport rewrite.
A CPU flamegraph is still required to distinguish lock/map contention from
channel handoff and wakeup costs before committing to a thread-per-core design;
the measured 11,002 voluntary context switches alone cannot separate them.

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
(`block` / `drop_new` / `drop_old`) and depths, and `core_shards`
(`FELIX_CORE_SHARDS`) — thread-per-core stream ownership: each stream's
publish worker and lane feeders run on a dedicated core-pinned runtime
(Linux pinning; dedicated threads elsewhere). Measured lossless (zero drops):
+27% on a 4-stream × fanout-4 workload (1.24M → 1.59M msg/s, unpinned macOS);
on Linux (devcontainer), +25% single-stream (1 KiB × fanout 10: 1.63M → 2.04M
msg/s) and parity-to-2.5× multi-stream with high environment variance — never
below baseline in any run. Opt-in until bare-metal numbers tighten the
multi-stream claim.

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

## Continuous benchmarking

Three CI workflows keep this data honest and current, rather than relying on
someone remembering to re-run the harness by hand:

- **`.github/workflows/perf-pr.yml`** — on every PR, builds and benchmarks
  both the PR's merge-base and the PR head back-to-back on the same runner
  instance (a small fast subset, not the full matrix below), then posts a
  PR comment comparing them via a Welch's t-test
  (`scripts/perf/compare_benchmarks.py`). Running both sides on the same
  runner controls for GitHub Actions' shared/virtualized runner noise far
  better than comparing against a historical stored value. Advisory only —
  it does not block merging.
- **`.github/workflows/perf-publish.yml`** — on every merge to `main`, runs
  the same fast subset once and publishes it as a historical time series
  (via `benchmark-action/github-action-benchmark`, stored on the
  `benchmark-data` branch) so trends over time are browsable.
- **`.github/workflows/perf-comprehensive.yml`** — the full matrix below,
  on a weekly schedule or manual dispatch (too slow — roughly 1,000
  individual runs — for every PR or every merge). Produces the artifacts
  used to regenerate this page; see below.

### Historical dashboards

Every successful performance publish updates the live dashboards on GitHub
Pages:

- [Latency history](../../benchmarks/latency/)
- [Throughput history](../../benchmarks/throughput/)

The dashboards are generated by `github-action-benchmark`, stored on the
`benchmark-data` branch, and copied into the documentation artifact whenever
the Pages workflow runs.

## Regenerating this page's data

```bash
# Single case, quick sanity check
cargo run --release -p broker --bin latency-demo --all-features -- \
  --warmup 500 --total 20000 --payload 1024 --fanout 10 --batch 64

# Full matrix (scripts/perf/presets.yml) + aggregation + charts + markdown.
# Same pipeline as the perf-comprehensive.yml workflow.
pip install -r scripts/perf/requirements.txt
python3 scripts/perf/run_latency_matrix.py
python3 scripts/perf/normalize_and_aggregate.py
python3 scripts/perf/make_charts.py
python3 scripts/perf/render_markdown_snippets.py
# -> charts/latency_demo/latency_demo_snippet.md
```

Key output fields: `delivered throughput` (all subscribers), `delivered
per-sub throughput`, `p50/p99/p999` (per-message publish→delivery latency),
`delivery drops` (must be 0 in both profiles — see the methodology note
above), `publish submit throughput` (client-side enqueue rate — an upper
bound, not a delivery claim).
