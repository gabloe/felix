# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Felix is pre-1.0: the wire protocol and broker semantics may change between
minor versions. See [delivery semantics](docs-site/src/content/docs/architecture/semantics.md)
for what the current release actually guarantees.

## [Unreleased]

**Wire protocol:** one new frame flag, `FLAG_BINARY_PUBLISH_KEYED` (`0x0040`).
`VERSION` remains `1` and no feature bit is added. The bit is negotiated on the
handshake, so a client and broker that disagree about it exchange the same bytes
they did before.

### Added

- **A routing key rides in the binary publish frame** (#549). A keyed publish
  used to force the JSON encoding — the binary layouts were fixed and had
  nowhere to put a key — and that fallback measured **645.8 MB/s against 917
  MB/s** unkeyed on the same rig, with user CPU up from 20% to 28%. Routing a
  record cost roughly 30% of throughput, which made sharding something you paid
  for rather than something you got.

  `0x0040` is a modifier on `0x0001`, the shape `FLAG_BINARY_PUBLISH_ACKED`
  already established: the body is prefixed with a `u16` key length and the key
  bytes. With both bits set the correlation prefix still comes first, so a
  broker reads `request_id` at offset 0 whether or not a key follows.

  A broker that predates the bit would read `key_len` as `tenant_len`, so the
  client sends the keyed binary frame only to a broker that advertised it and
  uses JSON otherwise — costing throughput, not correctness.

- **A Node.js / TypeScript client** (`crates/felix-typescript`), a napi-rs
  addon over `felix-client` rather than a reimplementation of the protocol —
  the same reasoning as the Python binding, and the same surface: publish
  (keyed, or at-least-once), subscribe, sharded subscribe with per-shard
  resume, cache get/put/delete, counters, cache watches and consumer groups.
  Every call returns a `Promise`, and every handle is disposable.
  Errors arrive as typed classes — `ConnectionError`, `AuthError`,
  `NotFoundError`, `CursorError`, `InvalidArgumentError` — mirroring the
  Python binding's exceptions, so an application branches on identity rather
  than on message text.

  It passes the client conformance suite, and CI and the release pipeline are
  both gated on that: the suite drives a real three-node fixture cluster,
  names the catalogue scenario each test demonstrates, and
  `felix-conformance verify` checks the results against the catalogue, so a
  green run that quietly skipped a required scenario still fails. Not
  published to npm: the addon is built per platform and attached to the GitHub
  release, with the npm step behind `PUBLISH_NPM` for the same reason PyPI is
  behind `PUBLISH_PYPI`.

- **`felix-loadgen --keys <n>`** spreads the ingest scenario's batches over `n`
  routing keys. The scenario published unkeyed, and an unkeyed record resolves
  to shard 0, so every "multi-shard" measurement taken with it was really a
  single-shard one — a 12-shard and a 1-shard stream measured identically
  (920 vs 923 MB/s) because both exercised the same log. Default `0` keeps the
  old behaviour, so existing runs stay comparable.

### Changed

- **The container images are published.** `ghcr.io/gabloe/felix-broker` and
  `ghcr.io/gabloe/felix-controlplane` went out with 0.4.1 and pull without
  credentials. Each release tags the full version, the minor series, and
  `latest` on non-prereleases.

  The docs said they were not published and told you to build your own; that
  was true when written and is not now. The Docker Compose, Kubernetes and
  installation pages now pull instead, and keep the build commands for running
  something unreleased.

  Images are signed with cosign, keyless, **over the digest and never the tag**
  — a tag can be moved and a signature over one would follow it. The Kubernetes
  page carries the `cosign verify` invocation. The chart's image tag defaults to
  its `appVersion`, so a default install resolves to a published image with
  nothing to configure.


- **The data path is binary; JSON is compatibility only** (#550). Now that
  #549 put the routing key in the binary publish frame, the JSON encoding has
  no remaining reason to carry data-plane traffic — it measured **645.8 MB/s
  against 917** for the same keyed workload on the same rig, with user CPU up
  from 20% to 28%, and it buys nothing the binary frames do not now cover.

  `Publisher::publish_json` and `Publisher::publish_batch_json` are
  **deprecated** and will be removed in 0.6.0. Nothing routes through them any
  more: `publish_batch`'s fallback now calls the private keyed form directly, so
  it keeps working once the public surface goes.

  **The broker still accepts JSON publishes and will keep accepting them.**
  `ORIGINAL_V1_FLAGS` is frozen, so a client older than `FLAG_BINARY_PUBLISH_ACKED`
  or `FLAG_BINARY_PUBLISH_KEYED` is entitled to send them forever. What changed is
  that no current Felix client emits one except as a fallback it chooses itself,
  against a broker that did not advertise the frame it wanted.
  `felix_broker_json_publishes_total{frame="publish"|"publish_batch"}` counts what
  still arrives that way — the evidence a deployment would need before the arm
  could ever be dropped.

  `publish_idempotent` is unaffected. It is JSON because no binary layout carries
  a producer id and sequence yet, not for compatibility.

- **Device flushes can go through `io_uring` on Linux** (#548), behind
  `FELIX_STORAGE_IO_URING=1`, default off. `IORING_OP_FSYNC` removes the
  `spawn_blocking` hand-off rather than shrinking it, and unlike running the
  sync inline it keeps the `await` as a yield point, so background rollover and
  retention still get scheduled. A perf session measured **956.7 MB/s against
  917.2** with it on — every run better, no overlap between the distributions.

  The blocking pool stays as the fallback: a kernel too old for the opcode, or a
  container that forbids the syscall, falls back rather than failing. Durability
  must not depend on an optimisation being available.

## [0.4.1] - 2026-09-18

A throughput fix. Durable publishes to one shard were processed strictly one at
a time, so group commit -- the mechanism that lets one device flush serve many
waiters -- never had more than one waiter and every publish paid a full flush
alone. Nothing was lost or misordered; the broker simply used about half a
machine and refused the rest.

Also fixes the release job that shipped 0.4.0 with no Python wheels.

**No wire-protocol change.** `felix-wire` `VERSION` remains `1`,
`INTERNAL_VERSION` remains `1`, and no feature bit is added.

### Fixed

- **Concurrent publishes share a device flush again** (#535). Publishes for a
  shard queued to a single worker, and that worker awaited each one to
  completion -- including the `fsync` -- before taking the next. One publish was
  ever at the sync point, so the group-commit fan-in was **1 by construction**:

  ```
  1 flush / ~300 us = ~3,300 publishes/s per shard
  x 256 KB per batch = ~850 MB/s
  ```

  which is the per-broker ceiling measured on Azure NVMe, at ~50% CPU with half
  the cores idle -- waiting on the disk rather than computing. It got *worse*
  with more publishers, who queued behind each other.

  The publish is now two phases. `claim_publish` consumes the offsets and
  reserves the commit turn; the transport calls it **serially, in arrival
  order**, so the order records land on disk is unchanged, and a client
  pipelining under `AckMode::None` keeps its send order.
  `complete_publish` awaits the flush and then appends and fans out under that
  turn, and is spawned -- so several flushes overlap and group commit has
  something to coalesce. Measured fan-in on the transport-level test:
  **1.000 -> 9.5**.

  Ordering is unchanged and checked rather than assumed: `commit_order`'s unit
  tests, which are the deterministic proof that turns are granted in offset
  order regardless of arrival order, and
  `disk_order_cursor_order_and_delivery_order_agree_under_concurrency`.

- **The release job could never have built the Python wheels** (#534). It
  installed the binding with `maturin develop`, which requires an *active*
  virtualenv; `actions/setup-python` provides an interpreter and no venv. The
  failure took the wheel, sdist, attach and publish jobs with it, so 0.4.0
  shipped with no wheels and nothing on PyPI. Installation is now
  `pip install ./crates/felix-python`, which builds through maturin as the PEP
  517 backend -- the same thing a user does.

  The job also ran *only* in `release.yml`. The Python client merged after
  v0.3.1 and every earlier release predates it, so its first execution in its
  life was the v0.4.0 tag build. It now runs in CI as well, on every change,
  using the same installation steps so the two cannot drift.

### Added

- **`FELIX_BROKER_PUB_FLUSH_CONCURRENCY`** (default `32`) -- durable publishes
  one publish worker may have awaiting their device flush at once. Offsets are
  still claimed serially, so this does not affect the order records land in; it
  decides how many flushes group commit gets to coalesce. `1` restores the
  0.4.0 behaviour of one flush at a time, which is also how the fix is tested:
  at `1` the fan-in is 1.000 and the regression test fails.

### Changed

- `FELIX_BROKER_PUB_WORKERS_PER_CONN` is documented for what it is. The pool is
  **process-wide**, built once before the accept loop, not per connection as
  the name and the old description both say. A stream-shard handle maps to one
  worker, so raising it spreads *different* shards across workers and cannot
  give one shard more than one. The name is misleading and a rename is tracked
  in #535.

### Performance notes

The per-broker figures published for 0.4.0 -- **~977 MB/s at ~48% CPU**, and
the conclusion drawn from them that *"durable throughput scales by adding
brokers, not by adding cores per broker"* -- describe this defect rather than
the design. The measurements were accurate; the architectural inference was
not. Replacement numbers need a rig session against 0.4.1 and are deliberately
not guessed at here.

## [0.4.0] - 2026-09-18

The multi-node hardening release. 0.3.0 made a cache into something you can
subscribe to; 0.4.0 makes the cluster something you can authenticate, package,
and reason about formally. Every broker-to-broker link can now be mutually
authenticated, every control-plane endpoint requires a credential, the whole
deployment ships as a Helm chart, and a TLA+ model of the lease and promotion
protocol found a safety defect that reading had not.

Felix also stopped being a Rust-only project: there is a **Python client**, a
binding over the Rust one rather than a reimplementation, gated by a
conformance catalogue that the next language will have to pass too.

**No wire-protocol break.** `felix-wire` `VERSION` remains `1` and
`INTERNAL_VERSION` remains `1`. The one new capability is a negotiated feature
bit, `FEATURE_IDEMPOTENT_PRODUCER` — a 0.3.x client and a 0.4.0 broker
interoperate byte-for-byte on everything they both know.

### Added

- A TLA+ model of one shard's lease, replication and promotion protocol, at
  `docs/formal/FelixShard.tla`, checked with TLC by `task tla:check` and in
  CI. It holds no-two-leaders, acknowledged-records-survive,
  acknowledged-records-agree and no-truncation-below-the-mark under clock
  drift, lost heartbeats, lost reports, and a broker paused between admitting
  a write and committing it. Three configurations are expected to fail and
  are held to it: one with no safety interval and one with the commit-time
  lease check removed, which show each is load-bearing, and one with
  promotion as the design writes it, which finds that a leader report older
  than the last acknowledgement can promote a replica missing an
  acknowledged `Quorum` record. Promotion by (last generation, length)
  passes, and is the rule to move to (#420).

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

- A leader rebuilds a halted follower itself. A follower whose log diverged
  from the leader's, or that refused a bootstrap because it held records of
  its own, is told to discard its copy of the shard (`ReplicateRebuild`,
  kind 24) and shipped again from the leader's oldest surviving record. It
  happens under a policy: `FELIX_REPLICATION_REBUILD_MAX_CONCURRENT` caps
  how many followers a broker rebuilds at once (default `1`; `0` leaves every
  halt to an operator, as before) and `FELIX_REPLICATION_REBUILD_BYTES_PER_SEC`
  paces the transfer. A fenced halt is never rebuilt, and a follower that
  predates the message stays halted until it is upgraded (#424).
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
- **A Python client** (#392, #393, #395), as a binding over the Rust client
  rather than a reimplementation, so the two cannot drift in behaviour. Both a
  synchronous and an asyncio surface, covering publish and subscribe, consumer
  groups, cache watches and multi-shard subscriptions. It is gated by a
  conformance catalogue (`scenarios.toml`) that every future client must pass:
  Python claims every required scenario and leaves two optional ones
  unclaimed rather than pretending — `at_least_once` does not carry a routing
  key, and a prefix watch over a multi-shard cache needs one watch per shard.
  Release wheels are built in CI.
- **Container images** for the broker and the control plane (#405), built and
  smoke-tested in CI, which is what the Helm chart above deploys.
- **Refresh tokens and `POST /token/refresh`** (#402). 0.3.1 raised the
  exchanged-token TTL as a stopgap because a broker read its credential once
  and held it forever; this is the real fix. A refresh rotates within a family,
  re-evaluates RBAC on every use — so a grant removed since the last exchange
  stops working without waiting for a re-exchange — and a replayed token
  revokes the whole chain. Brokers refresh their node credential rather than
  holding it for their lifetime (#404).
- **Configuration that refuses to be wrong quietly.** An unrecognised
  `FELIX_*` variable is reported as a typo instead of silently taking a default
  (#499), and settings that are each fine alone and wrong together are refused
  at startup (#509). `--print-config` prints the effective configuration with
  every credential redacted, so it can be pasted into an issue (#500).
- **Day-0 bootstrap is audited** (#506), with its scope containment pinned by
  test — it logged nothing at all before.
- **A broker names the replicas replication has stopped for** (#485), so a
  halted follower is visible rather than inferred from lag.
- **Segments record where each leadership generation began** (#466), which is
  what lets a follower resume at a generation boundary and a leader drop a
  divergent suffix.
- **An unknown internal frame kind is refused rather than fatal** (#496). A
  newer peer sending a kind an older one does not know no longer ends its
  control loop.
- `--slow-subscribers` in the load generator (#382), for measuring
  slow-consumer isolation rather than asserting it.
- The protocol decoders are fuzzed, with every target run in CI (#480).

### Performance

- **Replication stopped being a timer.** A pass ships on a durable append
  instead of waiting for the next tick (#457), ships every shard concurrently
  rather than one after another (#471), and advances the quorum mark at the
  **majority** instead of at the slowest follower (#478). A pass's replica
  reports now share one control-plane request (#494). Together these are what
  make a `Quorum` acknowledgement a measurement of replication rather than of a
  2s sweep.
- **The cache write path group-commits** like the stream path (#390).
- **A commit turn is released to one publisher rather than all of them**
  (#511), so a wake-up does not thunder.
- **Placement uses bounded-load rendezvous hashing** (#388), so shards spread
  evenly instead of piling onto whichever node scores highest.

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
- **A `Quorum` acknowledgement could outrun what the replicas actually held.**
  A follower can no longer confirm past the batch it was sent (#427), the
  leader holds the quorum mark when a replica report did not land (#434), and
  a broker may only report positions for shards it actually leads (#430).
- **Clocks are no longer compared across hosts.** The lease is anchored at the
  heartbeat's *send*, not its response (#426); liveness expiry is judged by one
  clock rather than one per instance (#439); the leader stamps a heartbeat
  rather than whichever instance received it (#475); and a replica report is
  judged on the clock that stamped it (#484).
- **Storage durability gaps.** A failed fsync now poisons the segment writer
  rather than letting the next append proceed as if the disk had kept its word
  (#428); the compaction directory swap is crash-safe (#429); and a sparse
  index entry may not point inside the segment header (#483).
- **A follower resumes at the generation boundary rather than at zero** (#477),
  and a leader drops a divergent suffix left by a previous generation (#467).
- A subscriber id is never reused (#397).
- A forwarded publish is bounded by the ack budget (#399).
- An unknown stream reports zero shards rather than one (#400).
- A bootstrap whose ranges *meet* is accepted, rather than requiring an exact
  match (#432).
- Shard assignments cascade on delete in the in-memory store too (#398).
- `felix-common` builds on its own default features (#468).

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
- **Inbound connections on the internal listener are bounded** (#505), by
  `FELIX_INTERNAL_MAX_INBOUND_CONNECTIONS` and
  `FELIX_INTERNAL_MAX_INBOUND_PER_SOURCE`. This outlives mTLS: an
  authenticated peer looping on a bug exhausts a listener exactly as a hostile
  one does.
- **Every refused credential is counted and logged** (#519), so a
  misconfigured broker or an attacker is visible rather than silent.

### Changed

- A broker presents `FELIX_NODE_TOKEN` / `FELIX_NODE_TOKEN_FILE` on the
  metadata sync as well, and accepts one without `FELIX_NODE_ID`: a standalone
  broker pointed at a control plane needs it to learn any stream. Without one
  it still starts, warns once, and has its sync refused.
- The RBAC object grammar accepts `stream:{tenant}/*/*` and `cache:{tenant}/*/*`,
  the tenant-wide forms token exchange already expanded `tenant.manage` to;
  the control plane's own parser had refused them. A wildcard namespace under
  a named leaf is still refused.
- The workspace was reorganised to Rust conventions (#391): `foo.rs` + `foo/`
  throughout, with `unreachable_pub` and `mod_module_files` enforced through
  `[workspace.lints]`.
- The replica-report shapes are shared rather than redeclared, and the
  lifecycle is typed (#447).

### Known limitations

- **Promotion can pick a replica missing an acknowledged `Quorum` record**
  (#527). Promotion reads the leader's last report, so a leader that
  acknowledges and then dies before its next report leaves a fresh report
  naming a replica that never received it. Report expiry does not close it —
  the report is recent, only older than the acknowledgement. The guarantee still
  holds against every fault the suite injects — the TLA+ model added in this
  release is what found the interleaving the suite does not reach. Promotion by greatest (last generation,
  length) closes it and is the rule to move to.
- **Shard rebalancing is not implemented** (#130). A shard whose leader is
  alive is never moved, so adding a broker adds capacity for *new* placements
  only — scaling a broker StatefulSet up will not migrate existing shards onto
  the new pods.
- **A retried Raft proposal can answer `409` for a write that succeeded**
  (#529), so a provisioning script can be told a tenant it just created already
  exists.
- **There is no rate limiting in the control plane** (#524), and an
  unauthenticated caller can force a JWKS fetch per request by presenting a
  token with an unknown `kid`.
- Published throughput and latency figures remain **RF=1 `Leader`**. The
  harness can now seed `Quorum` streams (#526), but the numbers are not taken
  yet (#425, #375).

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

[Unreleased]: https://github.com/gabloe/felix/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/gabloe/felix/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/gabloe/felix/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/gabloe/felix/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/gabloe/felix/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/gabloe/felix/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/gabloe/felix/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/gabloe/felix/releases/tag/v0.1.0
