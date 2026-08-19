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
- [ ] Add message type in header (type field) for faster dispatch
- [X] Message types: `Publish`, `Subscribe`, `Event`, `CachePut`, `CacheGet`, `CacheValue`, `Ok`, `Error`
- [X] Encode/decode message payloads with frames
- [X] Define v1 wire spec in `docs/protocol.md`
- [X] Add test vectors in `crates/felix-wire/tests/vectors/`
- [X] Add conformance runner tool (felix-conformance)
- [ ] Add fuzz tests for frame + message decoding (storage decoding is covered:
      see `crates/felix-storage/fuzz/`)
- [ ] Add compatibility notes (reserved fields for future encryption/compression)

## QUIC transport (`felix-transport`)
- [X] QUIC server endpoint wrapper (quinn)
- [X] QUIC client endpoint wrapper
- [X] Connection info + stream helpers (bi/uni)
- [X] Graceful shutdown hooks (drain connections on SIGTERM) — see
      [Graceful Shutdown](../docs-site/src/content/docs/deployment/graceful-shutdown.md).
      Readiness flip, accept-loop cancellation, and a bounded per-connection drain
      (`TaskTracker` grace window, then the connection is closed) are done. Cancelling
      each subsystem individually was deliberately *not* the design: publish and
      subscribe streams are long-lived by construction, so waiting on them would hang
      exactly as long as waiting on the connection. What is still untested is a real
      signal: `run_with_shutdown` is driven by a synthetic future in tests, and nothing
      sends an actual SIGTERM to a running process.
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
- [X] Add durable backend for data plane — segmented, checksummed, crash-safe
      log behind `StreamMetadata::durable`. See
      [Durable Storage](durable-storage.md).
- [X] Handle crash/recover of broker data-plane nodes (torn-tail repair on
      startup; loud failure on interior corruption). Control-plane recovery and
      re-sync from a new leader remain open.
- [X] Enforce retention — sealed segments are deleted on age or size, `base_offset`
      advances, and offsets below it report `Trimmed` / `CursorTooOld`. Off by
      default. See [Durable Storage](durable-storage.md#retention).
- [ ] Define requirements for tiered storage (hot/cold path, LCU?) — tracked as
      [#172](https://github.com/gabloe/felix/issues/172)
- [ ] Implement tiered storage primitives. `TieredStore` and friends are declared
      in `crates/felix-storage/src/tiered.rs` and nothing implements them; see
      [#172](https://github.com/gabloe/felix/issues/172) for what M1's sealed
      segments already set up for it.

### Durable storage sketch
- Split durable storage into **Index** and **Data** files: Index holds metadata (key + pointer/status), data file holds payload bytes.
- Keep keys (index entries) in memory as a write-through cache; writes first grow the data file, mark the index entry “in use,” then write payload bytes so that the system never reports data as durable until bytes hit disk.
- The append-only data file with in-memory index mimics a simple log (LSM-style without levels) and gives us a predictable replay order for fanout and replication.

### Open questions
1. ~~Do we need checksums/hashes to detect silent data corruption during reads or after reboots?~~
   **Answered:** yes. Every record carries a CRC-32 over its header and payload,
   verified on every read and during recovery. See
   [the format spec](storage-format.md).
2. ~~How should data file segmentation and garbage collection work to honor per-segment size caps? What is a sane segment cap? 512MB?~~
   **Answered:** segments roll at `segment_size_bytes`, default 256 MiB — chosen
   so a full scan of the active segment at startup stays under a second.
   Collection is whole-segment retention by age or size, off by default; see
   [Durable Storage](durable-storage.md#retention).
3. What is the delete policy? Should we retain the last *N* versions per key for rollbacks, or can we drop them immediately?
4. ~~What crash recovery guarantees do we need? We can’t mark an entry as committed until the payload bytes are actually persisted.~~
   **Answered:** three policies with explicit windows — `OnCommit` acknowledges
   only after a device flush, `Periodic` bounds loss by its interval, `None`
   makes no promise beyond the page cache. See
   [Durable Storage](durable-storage.md).

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
