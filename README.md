# Felix

<p align="center">
  <img src="docs/assets/logo.png" alt="Felix logo" width="220" />
</p>

<p align="center">
  <a href="https://github.com/gabloe/felix/actions/workflows/ci.yml">
    <img src="https://github.com/gabloe/felix/actions/workflows/ci.yml/badge.svg" alt="CI status" />
  </a>
  <a href="https://github.com/gabloe/felix/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0" />
  </a>
  <a href="https://www.rust-lang.org/">
    <img src="https://img.shields.io/badge/rust-1.92.0-blue" alt="Rust 1.92.0" />
  </a>
</p>

<p align="center">
  <em>Sovereign-first, Kubernetes-native data backbone for real-time systems.</em>
</p>

Felix is in **early active development**. This README is intentionally brief while the design and
implementation are still moving quickly.

## Current Focus

- Broker/data-plane foundations
- Control-plane metadata and sync
- Storage abstractions
- Protocol and conformance

## Docs

- `docs/architecture.md`
- `docs/protocol.md`
- `docs/control-plane.md`
- `docs/todos.md`
- Defining a stable wire envelope and internal data model
- Measuring latency and backpressure behavior early

The project is intentionally building depth before breadth.

---

## MVP Scope

The initial MVP targets:

- Single-node broker
- In-process pub/sub with fanout
- Ephemeral cache with TTL
- Stable wire envelope (v1)
- Basic observability (structured logs)
- Tests validating core invariants

Durability, clustering, regions, and security are layered on incrementally after the MVP.

---

## Repository Layout (High-Level)

```
crates/
  felix-common      # shared IDs, config, errors
  felix-wire        # wire framing and protocol
  felix-transport   # QUIC-based transport
  felix-storage     # ephemeral + durable storage
  felix-broker      # broker core (streams, queues, cache)
  felix-metadata    # metadata abstractions
  felix-router      # region-aware routing
  felix-crypto      # encryption and key handling
  felix-authz       # authentication and authorization
  felix-client      # Rust client SDK
  felix-consensus   # consensus/coordination placeholders
  felix-conformance # shared wire protocol conformance runner

services/
  broker             # broker service binary
  controlplane       # control plane service (future)
  agent              # node/infra agent (future)

docs/
  architecture.md    # system architecture
  control-plane.md   # control plane + RAFT plan (DRAFT)
  protocol.md        # wire protocol specification
  design.md          # product + protocol design notes
  todos.md           # implementation checklist
  assets/            # documentation images (logo, diagrams)

.github/             # CI workflows and repo metadata
Taskfile.yml         # task runner shortcuts
Cargo.toml           # workspace manifest
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

The conformance runner validates that the wire framing and JSON message encoding
match the shared test vectors. It exists to keep client implementations honest:
any client or server that passes the suite can interoperate without guessing at
edge cases or relying on Rust-specific behavior.

At this stage, Felix runs as a local, single-node process intended for development and testing.

---

## Design Discipline

Felix intentionally prioritizes:
- Clear invariants over feature count
- Explicit boundaries over implicit behavior
- Measured performance over assumptions

If a feature cannot be enforced in code, it is considered incomplete.

---

## Roadmap (Condensed)

- Single-node broker MVP
- QUIC transport + backpressure
- Durable log and retention
- Metadata and control plane
- Intra-region clustering
- Explicit cross-region bridges
- Security hardening (mTLS, RBAC, E2EE)
- Compliance features and auditing

Detailed plans live in `docs/`.

---

## License

Apache 2.0

Copyright (c) 2025 Gabriel Loewen
