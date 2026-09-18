# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Felix is pre-1.0: the wire protocol and broker semantics may change between
minor versions. See [delivery semantics](docs-site/src/content/docs/architecture/semantics.md)
for what the current release actually guarantees.

## [Unreleased]

### Added

- Idempotent producers (#422). A client asks the broker for a producer id
  (`producer_init`) and sends its batches as `publish_idempotent` with a
  per-shard sequence; the shard's leader appends the sequence it expects,
  answers a re-send of one it already holds with the original outcome and no
  second append, and refuses a gap, an unknown producer, or a sequence older
  than it remembers with a typed `publish_refused`. A batch for a shard led
  elsewhere is refused with the leader's address rather than forwarded.
  Negotiated as `FEATURE_IDEMPOTENT_PRODUCER`. `ClusterClient::idempotent_producer`
  and `Client::idempotent_producer` re-send under the same sequence after an
  ambiguous outcome, follow a not-leader refusal, and end on any other. The
  sequences live in the leader's memory: a new leader answers
  `unknown_producer`, so a batch in flight across a failover is reported rather
  than silently landed or dropped.

- Broker-to-broker mTLS (#125). With `FELIX_INTERNAL_TLS_CERT`,
  `FELIX_INTERNAL_TLS_KEY` and `FELIX_INTERNAL_TLS_CA` set — all three or
  none — every peer connection is mutually authenticated against the CA, and
  the certificate's DNS name is the broker's identity: a dialler verifies the
  listener's certificate against the node id it dials, and the listener checks
  the node id a peer claims in `Hello` against the certificate it presented. A
  peer with no certificate, an untrusted or expired one, or one issued to a
  different name is refused. The certificate and key are re-read every 30s,
  so a renewal is picked up by the next handshake without a restart and
  without dropping connections already up. Without the three variables the
  peer link is encrypted but unauthenticated, as before, and startup now
  warns. Node ids must be valid DNS names under mTLS. The cluster test
  harness runs every cluster test under mTLS.
- A Helm chart, at `deploy/helm/felix`, for the control plane and a broker
  cluster. The control plane is a Deployment over Postgres, rolling one
  instance at a time with none unavailable, or a StatefulSet under the Raft
  backend with a volume per member and the peers map derived from the
  release. Brokers are a StatefulSet whose pod name is the node id, with a
  volume each, the pod IP advertised to peers and the pod's DNS name to
  clients, the credential and Postgres URL taken from Secrets the operator
  creates, probes and a preStop-plus-drain grace period the chart derives,
  PodDisruptionBudgets that keep a replication-factor-three quorum, anti-
  affinity and zone spread, a NetworkPolicy that admits the internal port
  from brokers only, and optional peer mTLS issued per pod by cert-manager's
  CSI driver. Value combinations that are each fine alone and wrong together
  (an even Raft group, a budget wider than the replica count, a drain longer
  than its grace period, a backend without its store) refuse to render.
  `task chart:check` lints and renders it every way `ci/` describes and
  asserts those properties on the output; CI runs it (#131, #132).

### Fixed

- Replica reports — what a shard's leader says about which replicas hold its
  log — are now written to the control plane's store rather than kept in the
  memory of the instance that received them. With several control-plane
  instances over one Postgres, promotion could run on an instance that had
  never seen the report, so a `Quorum` acknowledgement released on it could
  not be made good at failover (#409). Under Raft the report is a log command,
  restamped with the leader's clock like a heartbeat. A report is stamped with
  the store's clock and judged against it, so freshness is no longer a
  subtraction between two hosts' clocks under Postgres; a deleted assignment
  now takes its report with it.

### Security

- A forwarded publish or cache operation now carries the client's own bearer
  token, and the owning broker verifies it before writing — against the
  tenant's keys, for `stream.publish` on that stream or `cache.read` /
  `cache.write` on that cache. The owner used to re-check ownership and
  generation only, so anything that could reach the internal port could
  append to any tenant's stream with no credential (#503). The credential
  rides two new internal kinds, `AuthorizedForwardPublish` (22) and
  `AuthorizedForwardCacheOp` (23); the legacy kinds still decode and are
  refused `Unauthorized`. During a rolling upgrade, an upgraded broker falls
  back to the legacy kind toward an owner that predates it, while an old
  broker's forwards to an upgraded owner fail until it is upgraded — see the
  upgrade notes.
- The control-plane resource API now requires a Felix bearer token on every
  endpoint. Tenants were created, listed and deleted — and namespaces, streams
  and caches managed — with no credential at all, while `/v1/nodes` next to
  them returned `401`. Namespaces, streams and caches take `ns.manage`,
  `stream.manage` or `cache.manage` over the object from a token minted for
  that tenant, with listings filtered to the caller's scope; the tenant catalog
  takes `tenant.manage:cluster:*`; the `snapshot` and `changes` feeds take
  `node.view:cluster:*`, the broker credential that already read the
  shard-assignment watch. The credential is checked before existence, so a
  tenant with no keys answers `401` rather than a `404` that says whether it
  exists.

### Changed

- A broker presents `FELIX_NODE_TOKEN` / `FELIX_NODE_TOKEN_FILE` on the
  metadata sync as well, and accepts one without `FELIX_NODE_ID`: a standalone
  broker pointed at a control plane needs it to learn any stream. Without one
  it still starts, warns once, and has its sync refused.
- The RBAC object grammar accepts `stream:{tenant}/*/*` and `cache:{tenant}/*/*`,
  the tenant-wide forms token exchange already expanded `tenant.manage` to;
  the control plane's own parser had refused them. A wildcard namespace under
  a named leaf is still refused.

## [0.3.1] - 2026-09-16

A real-IdP patch. The 0.3.0 token exchange quietly assumed two things that were
true of the test identity provider and false of Microsoft Entra ID: that a
JWKS key advertises its own algorithm, and that a broker credential minted for
fifteen minutes is long enough. Neither holds against a real Entra tenant, and
0.3.0 could not authenticate against one at all. This release fixes that. No
wire-protocol change; `felix-wire` `VERSION` remains `1`.

### Fixed

- **The control plane rejected every Microsoft Entra ID token** (#371). Entra's
  JWKS keys omit the optional `alg` member (RFC 7517 §4.4), and the exchange
  required it — so a valid RS256 token came back `401 invalid token`. The key's
  algorithm is now checked only when the JWKS actually states one, still bounded
  by the key-type match; an alg-less key verifies against the algorithm the
  token itself declares. 0.3.0 cannot complete a token exchange against Entra;
  0.3.1 can.

### Added

- **`FELIX_EXCHANGE_TOKEN_TTL_SECONDS`** (#371) — the lifetime of an exchanged
  Felix token, default `900`. A broker reads its node credential once and holds
  it for its whole lifetime without refreshing, so the fixed 15-minute TTL
  dropped every broker out of the cluster a quarter-hour in. Raising the TTL
  keeps a long-lived node authenticated; it is a stopgap, with proper token
  refresh tracked as follow-up work.

## [0.3.0] - 2026-09-15

The composed-semantics release. A cache stopped being something you can only
poll: you can subscribe to a key, join with current state already in hand, and
fold deltas into durable sums — three semantics that compose because each one
is a reading of the same log. And a consumer group now survives failover
*whole*: the dead-letter list travels with the shard, not just the cursor.

**No wire-protocol break.** `felix-wire` `VERSION` remains `1`. Every new
capability is a negotiated feature bit (`FEATURE_CACHE_WATCH`,
`FEATURE_CACHE_WATCH_RETAINED`, `FEATURE_COUNTERS`) — a 0.2.0 client and a
0.3.0 broker interoperate byte-for-byte on everything they both know, and a
client never sends a request the broker did not advertise. The internal
broker-to-broker protocol grew six message kinds (16–21), which is how that
protocol evolves; an older peer answers an unknown kind with a typed refusal
rather than a misparse.

### Added

- **Keyed cache watch** (#348). `cache_watch` subscribes to one key or key
  prefix; every applied write is delivered in the shard's write order with its
  log offset, deletes included as tombstoned changes. Resume by offset replays
  from the cache's log and joins live delivery with no gap and no duplicate —
  proven under concurrent writes at join time. An offset compaction has
  collapsed is answered with a marked snapshot of current values
  (`resnapshot`), never a silent gap. A watch that falls behind is **ended
  loudly** with `cache_watch_lagged` naming the first missed offset — filtered
  offsets are sparse, so a drop could never be read from an offset jump — and
  re-watching from that offset is gapless.
- **Retained delivery on a watch** (#349). Ask for `retained` and receive each
  matching key's current value first — at the offset of the write that
  produced it — then live changes: join a presence roster and hold it
  immediately, no polling. `retained_count` makes "your state is now complete"
  an explicit moment, and joining an empty key a definite zero rather than a
  silence. Survives failover: a promoted replica serves the retained value
  from its rebuilt index, at the original offset.
- **Counters** (#350). `counter_add` appends a signed delta and answers with
  the sum *including it* — one round trip to increment and know where you
  stand. Scoped and routed exactly like cache keys, stored beside the cache
  (a counter and a cache value sharing a key are unrelated). The sum survives
  restart, compaction — which collapses applied deltas into a checkpoint
  without renumbering the log — and leader failover, where the promoted
  replica folds the true sum from its shipped log and keeps counting.
  **At-least-once, stated honestly**: a retried add after a lost
  acknowledgement double-counts; the decision and its failure mode are
  recorded in `docs/projections.md`.
- **The dead-letter list replicates with its shard** (#362). Group state now
  fails over whole: a promoted leader resumes each group where it had reached
  *and* lists what it had given up on, and an operator's redrive works there —
  previously the promotion silently forgot exactly the records an operator had
  been told to look at. Entries recorded under the earlier on-disk layout are
  still listed and discardable.
- **Client API**: `watch_cache` / `watch_cache_retained` (typed `Lagged` and
  `resnapshot` surfaces), `counter_add` / `counter_get` — each feature-gated on
  the broker's advertisement before anything is sent.

### Fixed

- A publish forwarded between brokers now lands on the shard it was routed
  for (#356).
- Control-plane readiness is proven against a database that can fail (#358),
  and the rolling-restart and Raft-chaos test harnesses retry a dead
  connection against a re-probed rotation instead of a stale snapshot
  (#359, #364) — the load-balancer behaviour they model.

### Known limitations

- A prefix watch reads one shard; watching a whole multi-shard cache means one
  watch per shard, with no client helper yet.
- Counters carry no dedupe identity — retried adds can double-count — and a
  cache watch does not see counter changes.
- A cache still declares no consistency level: its writes carry the `Leader`
  guarantee, not `Quorum`.

The status table
(https://gabloe.github.io/felix/getting-started/what-felix-is-for/) remains
the per-capability source of truth.

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

[Unreleased]: https://github.com/gabloe/felix/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/gabloe/felix/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/gabloe/felix/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/gabloe/felix/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/gabloe/felix/releases/tag/v0.1.0
