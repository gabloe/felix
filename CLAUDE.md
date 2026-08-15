# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Rust 1.97, edition 2024. The `Taskfile.yml` shortcuts are the source of truth — CI runs the same ones.

```bash
task lint          # cargo fmt --check + clippy --workspace --all-targets --all-features -D warnings
task test          # cargo test --workspace, then cargo test -p felix-client --features in-process
task demo:check    # build/clippy/test the demo crates that are OUTSIDE the workspace
task coverage      # llvm-cov; spins up a Postgres container unless CI or FELIX_TEST_DATABASE_URL is set
task conformance   # wire-protocol conformance runner
```

Single test / narrower runs:

```bash
cargo test -p felix-storage --lib disk_log::                    # one module
cargo test -p felix-broker --test durable_streams <name>        # one integration test
cargo test -p broker quic_subscribe                             # QUIC integration tests
```

Docs and perf:

```bash
cd docs-site && npm run build          # runs check:mermaid first, then astro build
node scripts/check-mermaid.mjs         # validate every mermaid diagram in docs/ and docs-site/
task perf:latency-matrix               # run matrix -> aggregate -> charts -> markdown snippets
```

`scripts/perf/` needs `matplotlib` (see `scripts/perf/requirements.txt`); the aggregate step
degrades gracefully without `pandas`.

### Two traps worth knowing

- **`demos/slow-consumer`, `demos/state-divergence`, `demos/rbac-live`, and
  `demos/cross_tenant_isolation` are separate crates, not workspace members.** `task lint`
  and `task test` cannot see them, so a workspace-scoped "this is unused, delete it" signal
  is unreliable — deleting a public item that only a demo uses passes lint and breaks the
  build. Run `task demo:check` after changing public APIs.
- **`task test` deliberately does not use `--all-features` on the whole workspace.**
  `felix-client`'s `in-process` feature is run separately so its tests do not add concurrent
  load to timing-sensitive tests elsewhere.

## Architecture

### The publish path is where most invariants live

A publish crosses four components in a fixed order, and the order is the design:

1. **`services/broker/src/transport/quic/`** decodes the frame. `handlers/publish.rs` and
   `handlers/subscribe.rs` own the per-message work; `streams/control.rs` is the control-stream
   loop that owns auth state and dispatches every `Message` variant.
2. **`crates/felix-broker/src/broker.rs`** takes offsets from storage *before* waiting on
   durability (`begin_append` → `commit`), so the batch claims its place in the stream's order
   the instant its offsets are consumed. `commit_order.rs` (`CommitSequencer`) then makes
   later publishes wait behind earlier ones regardless of whether those succeed, fail, or are
   cancelled.
3. **`crates/felix-storage/src/disk_log/`** persists it. See below.
4. **Fanout** happens after durability, via `delivery.rs`. One `DeliveryEnvelope` is shared by
   every subscriber and caches its encoded frame, so a publish is encoded once regardless of
   fanout — with a second cached encoding when some subscribers negotiated event offsets and
   others did not.

Reordering these is almost always a bug. Several past defects were "the natural order" —
e.g. reading history before registering a subscriber loses any publish landing in between.

### Storage: a log-structured segment store, not a WAL

`crates/felix-storage/src/`:

- `segment/` — the byte format (`format.rs`), platform I/O (`io.rs`: `pread`, preallocation,
  `F_FULLFSYNC` on macOS), writer, reader, sparse index.
- `disk_log/` — `segments.rs` (rollover, offset routing, truncation), `recovery.rs` (startup
  validation and torn-tail repair), `sync.rs` (fsync policy and group commit), `mod.rs` (the
  async `AppendOnlyLog` seam).

Load-bearing properties, all documented in `docs/durable-storage.md` and
`docs/storage-format.md`:

- **Records are never rewritten**, so recovery can trust "valid bytes end at EOF". Preallocation
  reserves blocks *without* changing `st_size` for exactly this reason.
- **A torn tail is repaired; interior corruption is fatal.** Refusing to start beats silently
  losing acknowledged records.
- **Indexes are derived, never trusted** — a missing, short, or stale index is rebuilt from the
  segment it describes. This is why it is safe to skip fsyncing a freshly created index.
- **Group commit** is the single biggest throughput lever under `FsyncMode::OnCommit`; one
  blocking flush serves many waiters.

### Subscribers are isolated by construction

Per-subscriber bounded queues with an explicit overflow policy (`SubQueuePolicy`, default
`DropNew`). A publisher never blocks on a slow subscriber. `handlers/subscribe/feeder.rs`
and `event_writer.rs` do per-subscriber batching and writing behind a lane manager.

Because dropping is the default, **a subscriber can silently miss records** — which is why
delivered events carry log offsets for durable streams: a jump in offsets is exactly a drop.

### Wire protocol: capability negotiation, not versioning

`crates/felix-wire/`. Frame flags (`frame.rs`) select the *payload layout*, so an unknown flag
bit is rejected rather than masked off — masking one means confidently misparsing the body.

New features are added as negotiated flag bits, not version bumps: a client offers
`Auth.client_flags`, the broker answers `AuthOk.server_flags`. A peer that predates negotiation
sends/receives a plain `Ok`, and the only safe reading of that silence is `ORIGINAL_V1_FLAGS`.
`ORIGINAL_V1_FLAGS` is frozen — never add to it.

Optional JSON fields must default to the pre-existing behaviour so an old peer and a new peer
exchange byte-identical frames.

### Control plane and startup ordering

`services/controlplane/` serves metadata over REST; the broker seeds from it at startup.
`services/broker/src/main.rs` gates readiness and the accept loop on that seeding, so the
broker does not accept traffic for streams it does not yet know about.

## Conventions

- **Comments explain *why*, and are load-bearing.** Much of this codebase's reasoning about
  ordering, durability, and failure modes lives in comments next to the code that depends on
  it. Match that density; do not strip it.
- **Docs are treated as part of the change.** `docs/protocol.md`, `docs/durable-storage.md`,
  `docs/storage-format.md`, `docs/storage-performance.md`, and the status tables in
  `docs-site/src/content/docs/getting-started/what-felix-is-for.md` make specific claims about
  what is implemented. That page states outright that stale status markers are worse than none.
  If you ship a capability, move its row; if you find a claim the code cannot back, fix the claim.
- **A regression test that passes without the fix proves nothing.** For concurrency and
  durability fixes, revert the fix, watch the new test fail, then restore it.
- Benchmarks: `throughput` is what publishers sent, `delivered_throughput` counts subscriber
  deliveries — they differ by the fanout factor. Batched runs (`batch > 1`) measure a
  throughput profile, not request latency.
