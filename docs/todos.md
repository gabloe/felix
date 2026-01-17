
# Felix To‑Do List

This is a best‑guess to‑do list based on the current state of the repo (early scaffold),
the intended architecture (`docs/architecture.md`, `docs/design.md`), and the implementation
strategy we want to achieve (K8s-only, sovereignty-first, QUIC, single-node MVP first).

If something here conflicts with code reality, treat this file as the “next actions” queue
and update it as the codebase evolves.

---

## Now (make the repo runnable + measurable)

### Workspace / build health
- [X] `cargo build --workspace` succeeds cleanly (no warnings) on fresh clone
- [X] `cargo test --workspace` exists and runs (even if tests are minimal)
- [ ] Add CI workflow: fmt, clippy, test, deny (licenses/advisories)
- [X] Add `Taskfile.yml` targets: `build`, `test`, `fmt`, `lint`
- [ ] Add baseline deps management: `cargo-deny` + `deny.toml`

### Logging + metrics (day 1 observability)
- [ ] Standardize tracing init in service binaries (env-filter, json option)
- [ ] Add minimal Prometheus metrics endpoint to `services/broker` (HTTP)
- [ ] Metrics v0:
  - [ ] publish requests total
  - [ ] subscribe connections total
  - [ ] inflight messages (per connection)
  - [ ] dropped/backpressured messages total
  - [ ] p50/p99 publish→deliver latency (approx via histogram)

### K8s scaffolding (minimal “runs in cluster”)
- [ ] Create Helm chart skeleton in `deployments/k8s/charts/felix`
- [ ] Deploy broker as a Deployment (for MVP)
- [ ] Add Service exposing:
  - [ ] QUIC port
  - [ ] metrics/health HTTP port
- [ ] Add readiness/liveness probes (HTTP)
- [ ] Add resource requests/limits defaults

---

## Next (MVP: single-node broker with pub/sub + cache)

### Wire protocol v1 (`felix-wire`)
- [ ] Define a versioned frame header (magic/version/type/len)
- [ ] Define message types (minimum):
  - [ ] `Publish { stream, shard?, payload }`
  - [ ] `Subscribe { stream, from_cursor? }`
  - [ ] `Ack { stream, cursor }` (optional for MVP)
  - [ ] `Put/Get` for cache (or separate cache API)
  - [ ] `Error` and `Ok` responses
- [ ] Implement encode/decode with fuzz tests (wire never panics)
- [ ] Add compatibility notes (reserved fields for future encryption/compression)

### Transport (`felix-transport`)
- [ ] QUIC server endpoint (quinn)
- [ ] Connection lifecycle + graceful shutdown
- [ ] Stream multiplexing strategy:
  - [ ] control stream (requests)
  - [ ] data streams (subscriptions)
- [ ] Backpressure + bounded buffering:
  - [ ] per-connection inflight cap
  - [ ] per-subscription send queue cap
  - [ ] drop/slow-subscriber policy (configurable)

### Broker core (`felix-broker`)
- [X] In-memory stream registry (create-on-first-publish for MVP)
- [ ] Append-only in-memory log per stream (ring buffer)
- [ ] Subscription model:
  - [X] fanout to N subscribers
  - [ ] cursor per subscriber (initially “tail” only is fine)
- [ ] Queue semantics (optional in MVP, but plan structures now):
  - [ ] consumer group cursor
  - [ ] ack + redelivery skeleton
- [ ] Cache semantics (`felix-storage` ephemeral):
  - [X] `Put(key, value, ttl)`
  - [X] `Get(key)` with TTL expiry
  - [ ] eviction policy placeholder (size cap + LRU later)

### Service binary (`services/broker`)
- [ ] Wire `felix-transport` + `felix-broker` into a runnable server
- [ ] Config via env vars:
  - [ ] bind addresses/ports
  - [ ] max message size
  - [ ] inflight caps
  - [ ] default TTLs
- [ ] Health endpoints:
  - [ ] `/live`
  - [ ] `/ready`

### Client + examples (prove usability)
- [ ] Add a tiny Rust client in `felix-client` (or examples) supporting:
  - [X] publish
  - [X] subscribe (streaming)
  - [ ] cache get/put
- [ ] Add `examples/quickstart_pubsub` runnable in 60 seconds
- [ ] Add a simple loadgen/bench harness (even crude) to track regressions

---

## Later (cluster, sovereignty, durability, security)

### Durable storage (`felix-storage`)
- [ ] WAL + segmented log design
- [ ] Sparse index for offset→position
- [ ] Cursor persistence
- [ ] Retention enforcement (time + size)
- [ ] Corruption detection (CRC per record/segment)

### Metadata + control plane
- [ ] Decide initial metadata strategy:
  - [ ] minimal strongly-consistent store (raft/OpenRaft) OR
  - [ ] bootstrap single-leader with persistence, then raft
- [ ] Control plane API for:
  - [ ] regions/tenants/namespaces
  - [ ] streams + shard counts
  - [ ] policies (retention/quotas/encryption profiles)
- [ ] Broker watches metadata; data path never blocks on control plane

### Intra-region clustering
- [ ] Shard leadership and placement
- [ ] Replication protocol (leader→followers)
- [ ] Ack policies (leader-only vs quorum)
- [ ] Failover + leader promotion
- [ ] Rebalancing strategy

### Multi-region sovereignty + bridges
- [ ] Define “one cluster == one region” as enforced config
- [ ] Router enforcement: no cross-region routing by default
- [ ] Bridge component:
  - [ ] allowlisted stream replication
  - [ ] async first; sync optional later
  - [ ] explicit audit trail

### Security + compliance
- [ ] mTLS between brokers (cert-manager integration)
- [ ] Client authn options (mTLS, JWT/OIDC)
- [ ] RBAC enforcement (tenant/namespace/stream)
- [ ] Envelope encryption (per region, per tenant)
- [ ] Optional end-to-end encryption (broker routes ciphertext)
- [ ] FIPS build mode (crypto backend abstraction)
- [ ] GDPR capabilities:
  - [ ] retention defaults by data class
  - [ ] deletion workflows (stream purge + key destruction strategy)
  - [ ] access/audit logs export

### Quality + correctness
- [ ] Property tests for broker invariants (no cursor regression, no panic)
- [ ] Fuzz targets: wire decode, WAL parser, replication frames
- [ ] Chaos tests (kill pods, partitions) in K8s
- [ ] Performance budget + SLO doc (latency/throughput targets)

---

## MVP Definition of Done (single-node)

MVP is “done” when:
- [ ] A single broker process can:
  - [ ] accept QUIC connections
  - [ ] publish messages to a named stream
  - [ ] stream those messages to subscribers with bounded memory
  - [ ] serve cache `put/get` with TTL expiry
- [ ] There are runnable examples demonstrating pub/sub and cache
- [ ] Basic metrics exist and show latency/throughput
- [ ] Unit tests cover core encode/decode and broker fanout behavior
- [ ] A Helm chart deploys the broker to a K8s cluster with health + metrics

---

## Notes / Open Questions (park them here)
- [ ] Do we standardize on QUIC-only permanently, or add TCP later for constrained clients?
- [ ] For MVP, do we model shards explicitly, or treat each stream as one shard?
- [ ] Cache and streams: do we expose a unified API or separate endpoints?
- [ ] What is the minimal “cursor” model for MVP: tail-only, offset-based, or timestamp?
- [ ] What is the earliest point we want to bake in tenancy concepts (authz later)?
