# Felix MVP To-Do List

This list tracks only the **minimal single-node MVP**. Items beyond MVP live in
future planning docs.

## What the MVP Achieves
The MVP delivers a single-node broker that accepts QUIC connections, supports
publish/subscribe over a framed v1 protocol, and provides an in-memory TTL cache.
It focuses on correctness and operability (tests, demo, basic wiring) rather than
durability, clustering, or advanced observability.

---

## Build + repo health
- [X] `cargo build --workspace` succeeds cleanly
- [X] `cargo test --workspace` exists and runs
- [X] `Taskfile.yml` targets: `build`, `test`, `fmt`, `lint`, `demo`
- [X] Add CI workflow: fmt, clippy, test
- [X] Add baseline deps management: `cargo-deny` + `deny.toml`

## Wire protocol v1 (`felix-wire`)
- [X] Versioned frame header (magic/version/flags/len)
- [ ] Add message type in header (type field) for non-JSON parsing
- [X] Message types: `Publish`, `Subscribe`, `Event`, `Ok`, `Error`
- [X] Encode/decode message payloads with frames
- [X] Define v1 wire spec in `PROTOCOL.md`
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
- [X] In-memory stream registry (create-on-first-publish)
- [X] Fanout to N subscribers
- [ ] Cursor model (tail-only is fine for MVP)
- [ ] Append-only in-memory log per stream (ring buffer)
- [X] Cache: `Put/Get` with TTL expiry
- [ ] Cache eviction policy placeholder (size cap + LRU later)

## Broker service (`services/broker`)
- [X] Wire QUIC transport to broker protocol handler
- [X] Env var for QUIC bind address (`FELIX_QUIC_BIND`)
- [ ] Health endpoints: `/live`, `/ready`
- [ ] Metrics endpoint (Prometheus)

## Demo + examples
- [X] QUIC broker demo using framed messages
- [X] Demo for cache `put/get` over QUIC (or document separate cache API)

## MVP Definition of Done
- [X] Single broker accepts QUIC connections
- [X] Client can publish to a named stream over QUIC
- [X] Subscribers receive stream events over QUIC
- [X] Cache `put/get` available over QUIC
- [ ] Latency target: p999 <= 1 ms for small payloads on localhost baseline
- [ ] Basic metrics exist and show throughput/latency
- [X] Unit tests cover wire encode/decode and broker fanout behavior

## Post-MVP (explicitly out of scope for minimal MVP)
- [ ] Queue semantics (consumer groups, ack/redelivery, delivery guarantees)
- [ ] Distributed cache backed by Raft/consensus
- [ ] Multi-node clustering and replication
