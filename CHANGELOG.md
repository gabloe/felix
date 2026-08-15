# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Felix is pre-1.0: the wire protocol and broker semantics may change between
minor versions. See [delivery semantics](docs-site/src/content/docs/architecture/semantics.md)
for what the current release actually guarantees.

## [Unreleased]

### Dependencies

- `opentelemetry` 0.31.0 → 0.32.0, `opentelemetry-otlp` 0.31.1 → 0.32.0,
  `opentelemetry_sdk` 0.31.0 → 0.32.1, `tracing-opentelemetry` 0.32.1 → 0.33.0.
  These four are version-locked to each other and have to move together.

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

[Unreleased]: https://github.com/gabloe/felix/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/gabloe/felix/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/gabloe/felix/releases/tag/v0.1.0
