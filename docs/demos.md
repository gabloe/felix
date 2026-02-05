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

### Live RBAC Policy Change (`demo-rbac-live`)

Demonstrates a full end-to-end authorization flow using the real control plane,
broker, and token exchange endpoints. It starts a fake ES256 OIDC IdP, bootstraps
tenant configuration, performs publish/subscribe/cache operations that are
initially denied, then applies RBAC updates via the control plane API and shows
the same operations succeed without restarting services.

This demo uses an in-memory control-plane store (no Postgres required).

Run:

```bash
cargo run --manifest-path demos/rbac-live/Cargo.toml
```

Or via Task:

```bash
task demo:rbac-live
```

Expected output includes step-by-step PASS/FAIL markers such as:

```
STEP 9 publish denied: PASS
STEP 12 RBAC policies added: PASS
STEP 15 publish allowed: PASS
STEP 17 cache allowed: PASS
```

### Cross-Tenant Isolation (`demo-cross-tenant-isolation`)

Demonstrates that Felix enforces tenant boundaries end-to-end. The demo boots a
Postgres-backed control plane, a real broker, a fake ES256 OIDC IdP, and then
shows that a token minted for `t1` cannot access `t2` resources. It also shows
that a `t2` token without RBAC grants is denied in `t2`.

Run:

```bash
cargo run --manifest-path demos/cross_tenant_isolation/Cargo.toml
```

Or via Task:

```bash
task demo:cross-tenant-isolation
```

Expected output includes step-by-step PASS/FAIL markers such as:

```
STEP 13 t1 publish allowed: PASS
STEP 16 t1 token on t2 publish denied: PASS
STEP 19 t2 token publish denied: PASS
```

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

# Fairness A/B knobs for throughput-mode cliffs
cargo run --release -p broker --bin latency-demo --all-features -- \
    --warmup 200 --total 5000 --payload 256 --fanout 1 --batch 64 \
    --pub-conns 4 --pub-streams-per-conn 2 --pub-stream-count 1 \
    --pub-yield-every-batches 1

cargo run --release -p broker --bin latency-demo --all-features -- \
    --warmup 200 --total 5000 --payload 256 --fanout 1 --batch 64 \
    --pub-conns 4 --pub-streams-per-conn 2 --pub-stream-count 1 \
    --sub-dedicated-thread
```

**What to expect**:
- One or more result lines with p50/p99/p999 latencies
- Throughput metrics (overall and per-subscriber)
- Scheduler fairness probes:
  - `--pub-yield-every-batches N` reduces publisher burst monopolization
  - dedicated subscriber drain is on by default (set `FELIX_SUB_DEDICATED_THREAD=0` or `--sub-shared-thread` to opt out)
  - `--sub-dedicated-thread` forces isolation explicitly

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
