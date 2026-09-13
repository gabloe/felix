<p align="center">
  <img src="https://raw.githubusercontent.com/gabloe/felix/main/docs/assets/logo.PNG" alt="Felix logo" width="360" />
</p>
<p align="center">
  <a href="https://github.com/gabloe/felix/actions/workflows/ci.yml">
    <img src="https://github.com/gabloe/felix/actions/workflows/ci.yml/badge.svg" alt="CI status" />
  </a>
  <a href="https://github.com/gabloe/felix/actions/workflows/coverage.yml">
    <img src="https://raw.githubusercontent.com/gabloe/felix/badges/coverage.svg" alt="Coverage" />
  </a>
  <a href="https://github.com/gabloe/felix/blob/main/LICENSING.md">
    <img src="https://img.shields.io/badge/license-AGPL--3.0%20%2B%20Apache--2.0-blue.svg" alt="License: AGPL-3.0 + Apache-2.0 (split, see LICENSING.md)" />
  </a>
  <a href="https://www.rust-lang.org/">
    <img src="https://img.shields.io/badge/rust-1.97.1-blue" alt="Rust 1.97.1" />
  </a>
  <a href="https://gabloe.github.io/felix">
    <img src="https://img.shields.io/badge/Documentation-8A2BE2" alt="Documentation" />
  </a>
</p>

---

Felix is in **early active development**. This README is intentionally brief while the design and
implementation are still moving quickly.

## System Overview

Felix is a low-latency, QUIC-based pub/sub and cache system designed for high fanout,
high throughput, and predictable tail latency when properly tuned.

At its core, Felix uses a framed protocol (felix-wire) over QUIC streams to unify
event streaming (publish/subscribe) and request/response caching (put/get with TTL),
with explicit control over multiplexing, batching, and flow control.

Core components
- `felix-wire`: framed binary protocol for all clients and brokers.
- `felix-transport`: QUIC abstraction layer (client/server, pools, stream lifecycle).
- `felix-broker`: pub/sub logic, cache storage, stream registry, fanout.
- `felix-client`: publisher/subscriber/cache APIs over QUIC with connection/stream pooling.
- `felix-storage`: storage layer for broker.
- `services/broker`: runnable broker node.
- `services/controlplane`: runnable control plane node.

Pub/sub data flow (happy path)
- Client opens a bidirectional control stream to publish/subscribe and receive acks.
- Broker validates scope, enqueues publish jobs, and fans out to subscribers.
- Each subscription has a dedicated unidirectional event stream for delivery.
- Events are sent as single frames or binary batches with count/time-bounded batching.

Cache data flow (current architecture)
- Client maintains a cache connection pool with long-lived stream workers.
- Cache requests carry a `request_id` and are multiplexed over these streams.
- Broker processes request frames in a read loop and replies on the same stream.
- This avoids per-request stream setup costs and improves tail latency under concurrency.

Performance

Felix is tuned end-to-end: QUIC transport (path MTU discovery, congestion
window, socket buffers), a shared-frame fanout path that encodes a publish
batch once regardless of subscriber count, dense stream handles on the
publish hot path, byte-budgeted admission control at both client and broker
ingest, and an opt-in thread-per-core mode (`core_shards`) for stream
ownership. Measured, lossless, with TLS 1.3 always on: sub-millisecond
p999 latency at low fanout, millions of deliveries/sec for small payloads,
and multi-hundred-MB/s sustained for KB-sized payloads at fanout 10. See
[Benchmarks](https://gabloe.github.io/felix/features/benchmarks/) for
current numbers and methodology, and
[Environment Variables](https://gabloe.github.io/felix/reference/environment-variables/) /
[Configuration](https://gabloe.github.io/felix/reference/configuration/) for
the full set of tuning knobs (transport, queue depths/policies, batching,
admission control, core sharding).

- Instrumentation: build with `--features telemetry` to enable per-stage
  timings and frame counters. Default builds compile telemetry out
  (`cfg(feature = "telemetry")`, no runtime branches when disabled) to avoid
  instrumentation overhead on hot paths — validate overhead on your own
  workload before enabling it in production.

Use cases
- Real-time streaming with high fanout and tunable latency/throughput trade-offs.
- Event pipelines with batch publishing and batch delivery for efficient fanout.
- Low-latency caching over QUIC with predictable tail latency under load.

![One append-only log per shard, read three ways: as a stream by offset, as a cache through a key index, and as a queue through a cursor shared by a consumer group.](docs/assets/one-log.svg)

Streams, caches and queues are three readings of the same bytes, not three
subsystems. They share one durability path, one recovery path, one placement
rule and one replication path — which is the point of building it this way.

For how a cluster fits together, see
[`docs/architecture.md`](docs/architecture.md); for what each reading stores and
the test behind every claim, [`docs/projections.md`](docs/projections.md).

## Current Focus

- Fanout, backpressure, and isolation as core product behavior
- Broker/data-plane foundations
- Control-plane metadata and sync (including locality-aware routing policies)
- Protocol and conformance

## Docs

Full documentation site: **https://gabloe.github.io/felix** — architecture,
wire protocol, configuration/environment-variable reference, benchmarks,
and (for contributors) function-by-function internals walkthroughs of the
publish path, subscribe/fanout path, and backpressure/concurrency model.

In-repo design docs (`docs/`):
- `docs/architecture.md` — system architecture
- `docs/protocol.md` — wire protocol specification
- `docs/control-plane.md` — control plane; its Raft sections are design intent, not current behaviour
- `docs/semantics.md` — delivery semantics and guarantees
- `docs/design.md` — product and protocol design notes
- `docs/auth.md` — authentication and authorization
- `docs/broker-config.md`, `docs/client-config.md` — config field reference with example profiles
- `docs/demos.md` — demo binaries and what each one shows
- `docs/todos.md` — implementation checklist

The project is intentionally building depth before breadth: defining a
stable wire envelope and internal data model, and measuring
latency/backpressure behavior early to keep p99/p999 predictable.

---

## What works today

- Multi-broker clusters, with every shard of every stream and cache placed on
  one owner by rendezvous hashing
- Durable log-structured storage: segments, sparse indexes rebuilt rather than
  trusted, torn-tail repair, and a refusal to start on interior corruption
- Replication with leader leases, `Leader` or `Quorum` acknowledgement, and
  failover to a replica that actually holds the log
- A log-backed cache, routed to one owner per key and replicated
- Consumer groups: poll, acknowledge, redeliver, bound the redelivery,
  dead-letter and redrive
- A control plane over REST and Postgres, tenant-scoped tokens with RBAC, and
  capability negotiation on the wire

## What does not exist yet

- Raft for control-plane metadata, so its availability does not rest on Postgres
- Retention: a policy is recorded and nothing acts on it, so a stream grows
  until the disk does
- Rebalancing: a shard whose leader is alive is never moved, however uneven that
  leaves the cluster
- mTLS between brokers, tiered storage, cross-region bridges, and clients in any
  language but Rust

The [status table](https://gabloe.github.io/felix/getting-started/what-felix-is-for/)
is kept current per capability and is the page to trust when another disagrees.

---

## Repository Layout (High-Level)

```
crates/
  felix-common      # shared IDs, config, errors
  felix-wire        # wire framing and protocol
  felix-transport   # QUIC-based transport
  felix-storage     # ephemeral + durable storage
  felix-broker      # broker core (fanout, isolation, cache)
  felix-metadata    # metadata abstractions
  felix-router      # region-aware routing
  felix-crypto      # encryption and key handling
  felix-authz       # authentication and authorization
  felix-client      # Rust client SDK
  felix-conformance # shared wire protocol conformance runner

services/
  broker             # broker service binary
  controlplane       # control plane service
  agent              # node/infra agent (future)

demos/
  broker             # broker demo binaries
  rbac-live          # live RBAC mutation demo (control plane + broker)
  cross_tenant_isolation # cross-tenant isolation demo (Postgres + control plane + broker)

docs/
  architecture.md    # system architecture
  control-plane.md   # control plane (Raft sections are design intent)
  protocol.md        # wire protocol specification
  design.md          # product + protocol design notes
  todos.md           # implementation checklist
  assets/            # documentation images (logo, diagrams)

docs-site/           # Astro Starlight site sources
docker/              # local Docker assets
scripts/             # developer tooling and utilities
charts/              # Helm charts
data/                # sample data and artifacts
.github/             # CI workflows and repo metadata
Taskfile.yml         # task runner shortcuts
Cargo.toml           # workspace manifest
deny.toml            # cargo-deny policy
```

---

## Getting Started

Build the workspace:

```bash
cargo build --workspace
```

Run the broker service:

```bash
cargo run -p broker
```

Run the wire protocol conformance runner:

```bash
cargo run -p felix-conformance
```

The conformance runner validates that the wire framing and binary message encoding
match the shared test vectors. It exists to keep client implementations honest:
any client or server that passes the suite can interoperate without guessing at
edge cases or relying on Rust-specific behavior.

Felix runs as a cluster of brokers over a control plane, and as a single broker for development. Neither has been run in production by anyone.

---

## Design Discipline

Felix intentionally prioritizes:
- Fanout + backpressure + isolation over unified feature bundles
- Clear invariants over feature count
- Explicit boundaries over implicit behavior
- Measured performance over assumptions

If a feature cannot be enforced in code, it is considered incomplete.

---

## Roadmap (Condensed)

Done: QUIC transport with backpressure, the durable log, the control plane and
placement, intra-region clustering with replication and failover, the log-backed
cache, consumer groups, and tenant-scoped RBAC.

Next, roughly in order:

- Control-plane high availability, and Raft for its metadata
- Retention, so a stream stops growing until the disk does
- mTLS between brokers, and the rest of the security hardening
- Rebalancing and Kubernetes packaging
- Tiered storage and cold-tier reads
- Explicit cross-region bridges
- Compliance features and auditing

Detailed plans live in `docs/`, and the per-capability status table on the docs
site is the authority.

---

## License

Felix uses a split license: the wire protocol (`felix-wire`), client SDK
(`felix-client`), transport layer (`felix-transport`), shared types
(`felix-common`), and conformance suite (`felix-conformance`) are
Apache-2.0. The broker and control-plane server components are AGPL-3.0:
open source, but running a modified Felix as a network service means
publishing your changes. See [LICENSING.md](LICENSING.md) for the full
breakdown and rationale.

Copyright (c) 2026 Felix Authors
