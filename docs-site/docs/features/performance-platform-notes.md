# Performance & Platform Notes

Felix performance tuning is currently optimized for Linux-first deployments.

## Platform sensitivity

- Felix is optimized for Linux; microsecond-scale localhost QUIC measurements are OS-sensitive.
- At `batch=1`, tail latency is often dominated by scheduler wakeups and UDP/QUIC receive-path behavior, not broker business logic.
- On macOS and other desktop OSes, expect higher jitter in `p99`/`p999` for the same benchmark profile.

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
