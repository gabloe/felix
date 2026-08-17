---
title: "Performance & Platform Notes"
---

Felix performance tuning is currently optimized for Linux-first deployments.

## Platform sensitivity

- Felix is optimized for Linux; microsecond-scale localhost QUIC measurements are OS-sensitive.
- At `batch=1`, tail latency is often dominated by scheduler wakeups and UDP/QUIC receive-path behavior, not broker business logic.
- On macOS and other desktop OSes, expect higher jitter in `p99`/`p999` for the same benchmark profile.

## Transport scheduling defaults

The performance-critical transport defaults are on out of the box — no
environment tuning is required to get sustained-throughput behavior:

- QUIC driver tasks run on dedicated single-threaded I/O runtimes
  (`FELIX_IO_RUNTIME_THREADS`; `0` disables), with transport-facing pump
  tasks colocated on the same threads.
- The ACK-frequency extension is negotiated between quinn peers (2 ms max
  ACK delay, ACK every 20th packet; `FELIX_ACK_ELICITING_THRESHOLD` /
  `FELIX_ACK_FREQ_DISABLE`).
- Path-MTU discovery probes up to 16 KiB and UDP socket buffers request
  8 MiB.

Together these raised sustained macOS loopback throughput ~7.5×: driver
isolation was the dominant term, since per-datagram scheduler wakeup latency
had been the byte-rate ceiling. Single-process setups that host many endpoints
(like `latency-demo`) perform best with a small I/O pool; the demo pins
`FELIX_IO_RUNTIME_THREADS=2` itself. A minority of in-process benchmark runs
can still start in a degraded scheduling mode — rerun rather than tune if a
single run lands far below the numbers in
[Benchmarks](/felix/features/benchmarks/).

## Recommended perf environment

- Prefer a Linux host (or a Linux VM pinned to dedicated CPU resources).
- Use release builds.
- Pin CPU governor to performance mode where possible.
- Isolate benchmark cores and avoid noisy neighbors/background load.

## Throughput semantics in `latency-demo`

- Throughput output primarily reflects pipeline throughput through publish/fanout/delivery stages.
- If your scenario needs strict end-to-end acknowledgement barriers, run with ack-enabled publish modes and compare against ack counters.
- `publish_write_shape` output helps compare write fragmentation (`writes_per_batch`, `avg_bytes_per_write`) across settings like `publish_fastpath` true/false.

## Reproducing perf runs

```bash
# Baseline Linux-friendly settings
export FELIX_SUB_QUEUE_CAPACITY=4096
export FELIX_SUB_QUEUE_POLICY=drop_new
export FELIX_SUB_SINGLE_WRITER_PER_CONN=true
export FELIX_CLIENT_SUB_QUEUE_CAPACITY=4096
export FELIX_CLIENT_SUB_QUEUE_POLICY=drop_new

# Optional flow-control tuning
export FELIX_EVENT_CONN_RECV_WINDOW=268435456
export FELIX_EVENT_STREAM_RECV_WINDOW=67108864
export FELIX_EVENT_SEND_WINDOW=268435456

cargo run --release -p broker --bin latency-demo -- \
  --all false \
  --warmup 2000 \
  --total 20000 \
  --payload 4096 \
  --fanout 10 \
  --batch 64 \
  --binary
```

For latency-focused runs (`batch=1`), keep fanout low first, then scale fanout to identify queueing and flow-control effects.
