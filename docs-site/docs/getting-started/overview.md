# Overview

Felix is a low-latency, QUIC-based distributed data backend that unifies three critical patterns into a single system:

- **Event Streaming (Pub/Sub):** High-fanout message delivery with isolation and backpressure
- **Message Queues:** Shared consumer groups with acknowledgements
- **Distributed Cache:** Key-value storage with TTL support

## Design Philosophy

Felix is built around core principles that differentiate it from traditional message brokers and caches:

### 1. Sovereignty by Default

Each Felix cluster represents a **single sovereign region**. Data cannot leave the region unless an explicit, configured bridge exists. This isn't just a deployment suggestion—it's enforced in routing, metadata, and encryption boundaries.

This design is critical for:

- Financial services with regional data regulations
- Healthcare systems with patient data residency requirements
- Government systems with strict data sovereignty rules
- Multi-tenant SaaS platforms with customer-specific compliance needs

### 2. Low Latency First

Felix prioritizes **predictable low latency** over maximum batch throughput:

- QUIC transport eliminates head-of-line blocking
- Optional ephemeral streams with no disk on the hot path
- Aggressive backpressure prevents cascade failures
- Bounded memory everywhere to maintain predictable behavior
- Explicit performance knobs for latency/throughput trade-offs

Real-world results (single-node localhost):

- **Pub/Sub:** p50 ~40-50μs, p99 ~300-500μs (varies by payload and fanout)
- **Cache:** p50 ~160-180μs, p99 ~350-450μs at concurrency=32

### 3. One Core Log, Many Semantics

Internally, Felix uses a single append-only log abstraction. Different external semantics are projections over this core:

- **Streams:** fanout cursors per subscription
- **Queues:** shared consumer-group cursors with acks
- **Cache:** key → latest value with TTL

This eliminates the operational complexity and consistency bugs from running multiple systems (Kafka + Redis + RabbitMQ) side-by-side.

### 4. Kubernetes-Native

Felix assumes Kubernetes for:

- Process lifecycle management
- Identity (ServiceAccounts for mTLS)
- Networking and service discovery
- Failure detection and orchestration

Felix does **not** reimplement scheduling or node membership—it leverages what Kubernetes already provides.

## Core Components

### Felix Wire Protocol (`felix-wire`)

Language-neutral framed protocol over QUIC:

- Fixed header with magic number, version, and flags
- JSON payloads for control plane (v1)
- Binary batch format for high-throughput data plane
- Forward-compatible versioning scheme

See the [Wire Protocol](../architecture/wire-protocol.md) documentation for full specification.

### Transport Layer (`felix-transport`)

QUIC abstraction layer providing:

- Client and server connection management
- Connection pooling with configurable size
- Stream lifecycle management
- Flow control window configuration
- TLS 1.3 encryption by default

### Broker (`felix-broker`)

The core data plane implementation:

- Pub/sub logic with fanout and batching
- Cache storage with TTL and lazy expiration
- Stream registry and routing
- Backpressure and isolation enforcement

### Client SDK (`felix-client`)

Rust client SDK with:

- Publisher/subscriber/cache APIs
- Connection and stream pooling
- Automatic reconnection
- Configurable batching and flow control

**Planned:** Thin adapters for Python, Go, and other languages.

### Control Plane (Planned)

Metadata and coordination layer (future):

- RAFT-based consensus for cluster metadata
- Stream definitions and placement
- Tenant/namespace management
- Quota and retention policies
- Health monitoring and metrics aggregation

## Consistency & Delivery Guarantees

Felix provides **tunable consistency** configured per stream:

### Current MVP (Single-Node)

- **Delivery:** At-most-once (best-effort)
- **Ordering:** Per-stream ordering preserved for each subscriber
- **Acknowledgements:** Broker acknowledges receipt, not delivery to subscribers

### Planned Multi-Node

- **Leader-only acks:** Low latency, no replication wait
- **Quorum acks:** Higher durability, waits for replica confirmation
- **At-least-once:** With durable storage and replay
- **Exactly-once:** (future) via idempotent producers and transactions

## Security Architecture

### Current

- TLS 1.3 for all QUIC connections
- Transport-level encryption by default
- OIDC token exchange via control plane with tenant-scoped Felix JWTs
- Broker-side RBAC enforcement using Felix token permissions

### Planned

- **mTLS:** Mutual authentication between brokers and clients
- **Envelope Encryption:** Per-region and per-tenant key isolation
- **End-to-End Encryption:** Optional client-to-client encryption
- **Audit Logging:** Complete audit trail for compliance

## Deployment Models

### Single-Node (MVP)

Current implementation for development and testing:

```
┌─────────────┐
│   Broker    │
│  (in-proc)  │
│             │
│ • Pub/Sub   │
│ • Cache     │
│ • Ephemeral │
└─────────────┘
```

### Multi-Node Cluster (Planned)

```
     Control Plane          Data Plane
    ┌──────────────┐      ┌──────────┐
    │ RAFT Quorum  │──────│ Broker A │
    │              │      │ Broker B │
    │ • Metadata   │      │ Broker C │
    │ • Placement  │      └──────────┘
    │ • Health     │
    └──────────────┘
```

See [Deployment Guides](../deployment/local.md) for detailed instructions.

## Performance Characteristics

Felix is designed for workloads where:

- **Latency matters more than maximum throughput**
- **Predictable p99/p999 is critical**
- **High fanout is common** (1:N message delivery)
- **Mixed workloads** (streams + cache) share infrastructure

### When Felix Excels

✅ Real-time event streaming with tight latency SLAs  
✅ Microservice communication with low overhead  
✅ Regional data isolation requirements  
✅ Cache + stream unification to reduce system count  

### When to Use Something Else

❌ Maximum historical batch processing throughput (use Kafka)  
❌ Complex stream processing / transformations (use Kafka Streams, Flink)  
❌ Mature ecosystem with hundreds of connectors required  
❌ Multi-petabyte data warehouse workloads  

## What's Next?

- [Quickstart Guide](quickstart.md) - Get Felix running in minutes
- [Installation](installation.md) - Build from source
- [Architecture](../architecture/system-design.md) - Deep dive into system design
- [API Documentation](../api/broker-api.md) - Learn the APIs
