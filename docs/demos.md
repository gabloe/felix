# Felix Demos

This document summarizes the runnable demos under `demos/`. Each demo is a
self-contained binary that starts an in-process broker and QUIC server, then
executes an example workflow. Demos prioritize clarity over production
hardening and are intended for learning, diagnostics, and performance tuning.

## How to run

From the repository root:

```bash
cargo run --release -p broker --bin <demo-binary>
```

Or, if you have [Task](https://taskfile.dev/) installed:

```bash
task <demo-task>
```

**Notes**:
- These demos do **not** require a separately running broker.
- Demo auth helpers are enabled; do not reuse them for production.
- Each demo binds to a random localhost port for the in-process broker.

---

## Demo catalog

### Pub/Sub Demo (`pubsub-demo-simple`)
**What it shows**:
- QUIC publish/subscribe round-trip
- Basic fanout and per-message acknowledgements

**Run**:
```bash
cargo run --release -p broker --bin pubsub-demo-simple
```

**What to expect**:
- Step-by-step logs (boot broker, connect client, open subscription)
- Two events printed (`hello`, `world`)
- Clean shutdown with "Demo complete"

---

### Cache Demo (`cache-demo`)
**What it shows**:
- Cache `put`, `get_hit`, and `get_miss` operations over QUIC
- Latency/throughput statistics across payload sizes
- TTL sanity check

**Run**:
```bash
cargo run --release -p broker --bin cache-demo
```

**Common tuning env vars**:
```bash
FELIX_CACHE_BENCH_WARMUP=200
FELIX_CACHE_BENCH_SAMPLES=2000
FELIX_CACHE_BENCH_PAYLOADS=0,64,256,1024,4096
FELIX_CACHE_BENCH_CONCURRENCY=1
FELIX_CACHE_BENCH_KEYS=1024
FELIX_CACHE_BENCH_OPS=put,get_hit,get_miss
```

**What to expect**:
- A config summary line
- Per-payload stats (p50/p99/p999 and throughput)
- A TTL check that confirms expiry behavior

---

### Latency Demo (`latency-demo`)
**What it shows**:
- Pub/sub latency and throughput under configurable fanout, batch sizes, and payloads
- Optional matrix or full sweep execution

**Run**:
```bash
# Single run
cargo run --release -p broker --bin latency-demo

# Customized run
cargo run --release -p broker --bin latency-demo -- \
    --binary \
    --fanout 10 \
    --batch 64 \
    --payload 4096 \
    --total 10000 \
    --warmup 500
```

**What to expect**:
- One or more result lines with p50/p99/p999 latencies
- Throughput metrics (overall and per-subscriber)

---

### Notifications Demo (`pubsub-demo-notifications`)
**What it shows**:
- Multi-tenant fanout and isolation (t1 vs t2)
- Cache snapshots of the last N alerts
- Failure injection via subscriber drop/restart

**Run**:
```bash
task demo:notifications
# or
cargo run --release -p broker --bin pubsub-demo-notifications
```

**Optional flags**:
- `--alerts=10` (default: 10)
- `--last-n=5` (default: 5)
- `--drop-subscriber` (restart one subscriber mid-run)

**What to expect**:
- Cross-tenant access denial
- Alert fanout logs per tenant
- Cache snapshot output for `last_alerts`

---

### Orders/Payments Pipeline Demo (`pubsub-demo-orders`)
**What it shows**:
- Multi-stage pipeline: `orders` -> `payments` -> `shipments`
- Idempotent workers (dedupe by event ID)
- Cache-backed last-known state
- Failure injection via worker restart

**Run**:
```bash
task demo:orders
# or
cargo run --release -p broker --bin pubsub-demo-orders
```

**Optional flags**:
- `--orders=12` (default: 12)
- `--duplicate-every=5` (default: 5)
- `--kill-worker=payments` (restart payments mid-run)

**What to expect**:
- Step-by-step pipeline logs
- Cache snapshots for each order in `order_state`
- Final summary matching expected processed count
