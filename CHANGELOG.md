# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Felix is pre-1.0: the wire protocol and broker semantics may change between
minor versions. See [delivery semantics](docs-site/src/content/docs/architecture/semantics.md)
for what the current release actually guarantees.

## [Unreleased]

## [0.2.0] - 2026-09-07

Durable streams. A stream registered with `durable: true` now persists every
record before acknowledging it, replays it after a restart, and lets a
subscriber resume from an exact offset.

**No wire-protocol break.** `felix-wire` `VERSION` remains `1`. The new
capabilities are negotiated flag bits, so a 0.1.1 client and a 0.2.0 broker
still interoperate — they simply agree on the original flag set. That is the
whole point of negotiating capabilities rather than bumping a version.

**Durable storage is opt-in and off by default.** Nothing persists unless the
broker is started with `FELIX_DURABLE_STORAGE_DIR` *and* the stream is
registered durable. The on-disk format is version 2; there is no version 1 to
migrate from, because durable storage did not exist in 0.1.1.

**Clustering is not in this release.** It contains broker membership plumbing —
brokers can register, heartbeat, and appear in a control-plane listing — but
nothing routes across brokers. A client still talks to one broker and gets
exactly what it got in 0.1.1. See *Known limitations* below.

### Added

- **Durable single-node log.** A log-structured segment store behind the broker:
  records are never rewritten, a torn tail is repaired on startup while interior
  corruption refuses to start, and indexes are derived rather than trusted.
  Group commit means one device flush serves many appends. Configured with
  `FELIX_DURABLE_STORAGE_DIR`, `FELIX_DURABLE_SEGMENT_BYTES`, and the fsync
  policy (`none`, `periodic`, `on_commit`). (#173, #174)
- **Resumable subscriptions over the wire.** `Subscribe` carries an optional
  `start` — `latest` (the default, and what every older client sends),
  `earliest`, or an exact offset — and every delivered event carries its offset.
  History read from disk joins live delivery with no gap and no duplicate.
  Asking for an offset retention has discarded is a typed error, not a silent
  restart at the tail. (#197)
- **Retention on durable logs.** `FELIX_DURABLE_RETENTION_BYTES` and
  `FELIX_DURABLE_RETENTION_SECONDS` bound a stream's growth by deleting the
  oldest sealed segments. Unset means unbounded, which remains the default. (#203)
- **Capability negotiation for stream authentication.** A client offers
  `Auth.client_flags` and the broker answers `AuthOk.server_flags`. A peer that
  predates negotiation exchanges a plain `Ok`, and the only safe reading of that
  silence is the original flag set. (#170)
- **Binary publish acknowledgements**, so an acked publish no longer pays for a
  JSON round trip. (#167)
- **Broker cluster membership** (control plane only, see *Known limitations*):
  a Node resource model, membership persisted in both stores with an ordered
  changefeed, heartbeat and liveness expiry, broker-side registration and
  graceful deregistration, an operator listing at `GET /v1/nodes`, and
  Prometheus metrics for the fleet. (#218, #219, #220, #221, #222, #223)
- **Documentation site** migrated from MkDocs to Astro Starlight, with the
  durable segment format, storage performance, and delivery semantics written
  up. (#165, #166, #169, #174)
- Demos for lossy and lossless state settlement and for slow-consumer
  isolation. (#163)

### Fixed

- **Lossless publishing was not lossless end to end.** (#171)
- **The backlog-to-live handoff dropped records.** Reading history before
  registering a subscriber loses any publish landing in between; the handoff is
  now ordered so it cannot. (#191)
- **A failed rollover is now terminal.** A rollover fails on a failed fsync or a
  failed file creation, and both mean the log can no longer honour what it has
  already acknowledged. It rejects further appends rather than continuing
  quietly. (#196)
- **An inline rollover no longer parks every Tokio worker.** The segment set is
  behind a synchronous lock; held across a rollover's device flushes it stalled
  everything else on the runtime. It also stopped appends being rejected outright
  under rollover contention — six runs in twenty became zero in twenty. (#217)
- **Record header checksums (format v2)**, plus cancellation, startup, and
  hydration defects found in review. (#178)
- **MTU black-hole collapse on Linux**, where a path MTU below the probed size
  silently destroyed throughput. Acked publishes are now pipelined. (#200, #202)
- Durability, ordering, and recovery defects from successive reviews of the
  durable log. (#176, #179, #180)
- Cursor identity is pinned to disk offsets, and the broker proves its catalog
  seeded before reporting ready. (#180)

### Changed

- **An fsync was cut from the rollover path** — a freshly created and therefore
  empty index file was being flushed. Median p999 on the default inline path
  improved from 5.12ms to 3.98ms (−22%), and max from 61.8ms to 47.0ms (−24%).
  Background rollover was added alongside it and ships **disabled**, because it
  measured worse: `F_FULLFSYNC` does not overlap with concurrent writes, so
  moving a rollover's flushes off the append lock stops confining them rather
  than hiding them. (#192)
- Performance is now measured on a Linux 16 vCPU VM in addition to macOS, since
  the two platforms' fsync semantics differ enough to invert a result. (#201)

### Dependencies

- `opentelemetry` 0.31.0 → 0.32.0, `opentelemetry-otlp` 0.31.1 → 0.32.0,
  `opentelemetry_sdk` 0.31.0 → 0.32.1, `tracing-opentelemetry` 0.32.1 → 0.33.0.
  These four are version-locked to each other and have to move together.
- `sqlx` 0.8.6 → 0.9.0. Its `SqlSafeStr` bound means `sqlx::query` now takes
  only `&'static str`, so dynamic statements need `AssertSqlSafe` and a reason.
- `base64` 0.22 → 0.23, `bytes` 1.11 → 1.12, `dashmap` 6.1 → 6.2,
  `uuid` 1.21 → 1.24, `socket2` 0.6.2 → 0.6.5, `rcgen` 0.14.7 → 0.14.9,
  `metrics` 0.24.3 → 0.24.6, `serde_json` 1.0.149 → 1.0.151.
- `h2` → 0.4.16 for RUSTSEC-2026-0258.

### Known limitations

- **No clustering.** The membership endpoints added here are a registry. No
  traffic crosses a broker, brokers do not talk to each other, and there is no
  sharding, replication, or failover. Placement lands in M3, cross-broker
  forwarding in M4.
- **The membership endpoints are not authenticated.** Registration, heartbeat,
  drain, and deregister accept any caller that can reach them; only the operator
  read endpoints require a token. Safe on a trusted network, not on an open one.
  Tracked in #126.
- **At-most-once delivery.** Slow subscribers drop under the default policy.
  Durable streams make the *record* recoverable — a subscriber can resume from
  an offset — but nothing redelivers automatically.
- Single-region, single control-plane instance.

### Upgrading from 0.1.1

No code changes are required, and no configuration becomes mandatory. Every
addition above is opt-in.

- 0.1.1 clients work against a 0.2.0 broker unchanged. Capabilities are
  negotiated, so an older client simply does not offer the new flags.
- Durable storage stays off until `FELIX_DURABLE_STORAGE_DIR` is set. Setting it
  is not sufficient on its own: a stream must also be registered `durable: true`.
- If you do enable durable storage, decide the fsync policy deliberately.
  `periodic` (the default) bounds loss to one interval; `on_commit` loses
  nothing acknowledged and costs milliseconds per publish.
- `segment_size_bytes` is a latency knob as well as a recovery knob. Below a few
  MiB, tail latency is dominated by rollover flushes. Prefer the 256 MiB default
  unless restart time is genuinely the binding constraint, and measure the tail
  if you change it.

## [0.1.1] - 2026-08-09

Maintenance release. No wire-protocol changes — `felix-wire` `VERSION` remains `1`,
and 0.1.0 clients interoperate with 0.1.1 brokers.

The headline items are two correctness fixes in the subscriber path and a set of
new per-connection resource limits that are enabled by default.

### Added

- Per-connection publish and subscription limits, configurable via
  `FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES` (default 16 MiB) and
  `FELIX_MAX_SUBSCRIPTIONS_PER_CONN` (default 4096). Previously a single
  connection could occupy the entire process-wide publish budget. (#145)
- `FELIX_PUB_INGRESS_WAIT` and `FELIX_SUB_QUEUE_POLICY` to select ingress
  backpressure and subscriber-queue overflow behavior. (#145)
- SIGTERM-aware graceful shutdown for the broker, so in-flight work drains
  instead of being cut off at process exit. (#139, #153)
- Resource sampling in the soak harness — CPU, memory, and queue-depth series
  captured across a run. (#156)
- Benchmark regression gate on pull requests, plus historical benchmark
  dashboards published with the docs. (#147, #149, #152)

### Fixed

- **Subscriber queue depth accounting race.** Depth is now released by RAII on
  the queued item, so a dropped receiver, a cancelled `recv`, or a channel
  teardown can no longer leak depth and wedge a subscriber's backpressure
  signal. The process-global depth counter was replaced by per-stream deltas. (#157)
- **Subscription ID collisions and fanout delivery loss.** Subscription IDs are
  now broker-assigned, and per-connection writers are created atomically, fixing
  events being routed to the wrong subscriber or dropped entirely under
  concurrent subscribe. (#148)
- Removed two `unsafe impl Send` blocks that asserted nothing — both types were
  already `Send + Sync` by their fields. Replaced with `const _` assertions that
  fail the build at the definition if a future field breaks the property. (#140, #153)
- Bounded attacker-declared payload counts in the wire decoder before
  allocating, closing an allocation-abort denial-of-service path on binary batch
  frames. (#140, #153)
- Soak harness resource settling and reporting. (#158)
- Benchmark dashboards now load Chart.js locally rather than from a CDN, and
  benchmark data paths were corrected. (#150, #151)
- Core shards disabled in performance workflows, which were causing spurious
  regression reports. (#155)

### Changed

- Split the four largest modules into focused submodules — `felix-wire`
  (1641 lines), `felix-broker` (1986), and the QUIC `publish` (4231) and
  `subscribe` (3346) handlers. No public paths changed; every item is
  re-exported from its original location. (#159)
- Crate versions are now inherited from `[workspace.package]` via
  `version.workspace = true`, so a release bump is a single-line edit. (#160)
- Minimum supported Rust version raised to 1.97. (#146)
- Licensing clarified per path: `felix-wire`, `felix-client`, `felix-common`,
  `felix-transport`, and `felix-conformance` are Apache-2.0; the broker,
  control plane, and remaining crates are Elastic-2.0. See
  [LICENSING.md](LICENSING.md). (#145)

### Dependencies

- `tokio` 1.49 → 1.50, `rand` 0.9.2 → 0.10.0, `tempfile` 3.26 → 3.27,
  `tracing-subscriber` 0.3.22 → 0.3.23, `opentelemetry-otlp` 0.31.0 → 0.31.1.
- CI: `actions/deploy-pages` 4 → 5, `actions/upload-pages-artifact` 4 → 5.

### Upgrading from 0.1.0

No code changes are required. Two things to check before deploying:

1. The new per-connection limits are **on by default**. A deployment that
   sustains more than 16 MiB of in-flight publish bytes or more than 4096
   subscriptions on a single connection will now be throttled where it
   previously was not. Raise
   `FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES` / `FELIX_MAX_SUBSCRIPTIONS_PER_CONN`
   if that describes your workload.
2. Building from source now requires Rust 1.97.

## [0.1.0] - 2026-08-07

Initial public milestone: QUIC transport, JSON and binary wire framing,
in-process pub/sub broker with bounded subscriber queues and slow-consumer
isolation, ephemeral cache, tenant/namespace/stream registries, RBAC and Felix
token authorization, a control plane with a Postgres-backed store, a Rust client
SDK, and a protocol conformance runner.

[Unreleased]: https://github.com/gabloe/felix/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/gabloe/felix/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/gabloe/felix/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/gabloe/felix/releases/tag/v0.1.0
