# Felix MVP To-Do List

This list tracks only the **minimal single-node MVP**. Items beyond MVP live in
future planning docs.

## What the MVP Achieves
The MVP delivers a single-node broker that accepts QUIC connections, supports
publish/subscribe over a framed v1 protocol, and provides an in-memory TTL cache.
It focuses on correctness and operability (tests, demo, basic wiring) rather than
durability, clustering, or advanced observability.

---

## Build + repo health (Done)
- [X] `cargo build --workspace` succeeds cleanly
- [X] `cargo test --workspace` exists and runs
- [X] `Taskfile.yml` targets: `build`, `test`, `fmt`, `lint`, `demo`
- [X] Add CI workflow: fmt, clippy, test
- [X] Add baseline deps management: `cargo-deny` + `deny.toml`

## Wire protocol v1 (`felix-wire`)
- [X] Versioned frame header (magic/version/flags/len)
- [ ] Add message type in header (type field) for non-JSON parsing
- [X] Message types: `Publish`, `Subscribe`, `Event`, `CachePut`, `CacheGet`, `CacheValue`, `Ok`, `Error`
- [X] Encode/decode message payloads with frames
- [X] Define v1 wire spec in `docs/protocol.md`
- [X] Add test vectors in `crates/felix-wire/tests/vectors/`
- [X] Add conformance runner tool (felix-conformance)
- [ ] Add fuzz tests for frame + message decoding
- [ ] Add compatibility notes (reserved fields for future encryption/compression)

## QUIC transport (`felix-transport`)
- [X] QUIC server endpoint wrapper (quinn)
- [X] QUIC client endpoint wrapper
- [X] Connection info + stream helpers (bi/uni)
- [ ] Graceful shutdown hooks (drain connections on SIGTERM)
- [ ] Backpressure defaults (caps per connection/subscription)

## Broker core (`felix-broker`)
- [X] In-memory stream registry (explicit registration)
- [X] Fanout to N subscribers
- [X] Cursor model (tail-only is fine for MVP)
- [X] Append-only in-memory log per stream (ring buffer)
- [X] Cache: `Put/Get` with TTL expiry
- [X] Cache eviction policy placeholder (size cap + LRU later)

## Broker service (`services/broker`)
- [X] Wire QUIC transport to broker protocol handler
- [X] Env var for QUIC bind address (`FELIX_QUIC_BIND`)
- [X] Health endpoints: `/live`, `/ready`
- [X] Metrics endpoint (Prometheus)

## Demo + examples (Done)
- [X] QUIC broker demo using framed messages
- [X] Demo for cache `put/get` over QUIC (or document separate cache API)
- [X] Demo for testing latency with a comprehensive-ish matrix

## MVP Definition of Done
- [X] Single broker accepts QUIC connections
- [X] Client can publish to a named stream over QUIC
- [X] Subscribers receive stream events over QUIC
- [X] Cache `put/get` available over QUIC
- [ ] Latency target: p999 <= 1 ms for small payloads on localhost baseline
- [X] Basic metrics exist and show throughput/latency
- [X] Unit tests cover wire encode/decode and broker fanout behavior

## Post-MVP (explicitly out of scope for minimal MVP)
- [ ] Queue semantics (consumer groups, ack/redelivery, delivery guarantees)
- [ ] Distributed cache backed by Raft/consensus
- [ ] Multi-node clustering and replication
- [ ] Design sharding/replication/quorum to move beyond single-node; align with the RAFT control-plane draft and work through the details

## Data scalability with sharding
- [X] Streams should be defined with shard count
- [ ] Caches should be defined with shard count
- [ ] Ops should be directed to the correct shard leader
- [ ] Leader election should be managed
- [ ] Leader failover should be handled (RAFT handles it?)

## Data durability and persistence
- [ ] Add durable backend for control plane
- [ ] Add durable backend for data plane
- [ ] Handle crash/recover of data/broker and control plane nodes (read from disk first, then re-sync from new leader?)
- [ ] Define requirements for tiered storage (hot/cold path, LCU?)
- [ ] Implement tiered storage primitives (durability funneled down to K8s FS mounts?)

### Durable storage sketch
- Split durable storage into **Index** and **Data** files: Index holds metadata (key + pointer/status), data file holds payload bytes.
- Keep keys (index entries) in memory as a write-through cache; writes first grow the data file, mark the index entry “in use,” then write payload bytes so that the system never reports data as durable until bytes hit disk.
- The append-only data file with in-memory index mimics a simple log (LSM-style without levels) and gives us a predictable replay order for fanout and replication.

### Open questions
1. Do we need checksums/hashes to detect silent data corruption during reads or after reboots?
2. How should data file segmentation and garbage collection work to honor per-segment size caps?  What is a sane segment cap? 512MB?
3. What is the delete policy? Should we retain the last *N* versions per key for rollbacks, or can we drop them immediately?
4. What crash recovery guarantees do we need? We can’t mark an entry as committed until the payload bytes are actually persisted.

## Performance optimization
- [ ] Figure out how to handle backpressure
- [X] Measure P999 tail latency and throughput
- [ ] Identify optimizations and minspec clustering for optimization

## Kubernetes deployment + scale units
- [ ] Define scale unit boundaries (per-tenant vs per-namespace vs per-cache/stream)
- [ ] Create Helm chart or Kustomize manifests for control plane + broker
- [ ] Add ConfigMap/Secret wiring (QUIC bind, control plane URL, auth keys)
- [ ] Implement readiness/liveness endpoints used by probes
- [ ] Expose service types (ClusterIP + optional LoadBalancer) for broker QUIC
- [ ] Add horizontal scaling plan (replicas + shard assignment)
- [ ] Add PodDisruptionBudget and resource requests/limits
- [ ] Document deployment topology and upgrade/rollback steps
