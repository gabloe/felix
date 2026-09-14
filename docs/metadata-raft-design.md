# Metadata Raft

**Decision: control-plane metadata becomes a Raft-replicated state machine
hosted inside the control-plane instances themselves, built on openraft,
served through the existing store traits as a third backend alongside memory
and Postgres. Brokers and clients cannot tell which backend answered. SWIM
for node liveness is rejected for now — that decision is recorded in
[control-plane.md](control-plane.md#why-liveness-stays-centralized-swim-considered),
because it stands on its own whether or not Raft ships.**

Recorded for [#333](https://github.com/gabloe/felix/issues/333). The
alternatives and what would overturn each choice are below, in the same
spirit as [replication-design.md](replication-design.md) — which decided the
*opposite* for stream payloads, and whose argument this document must not
quietly contradict.

## Why now

[ha-postgres.md](ha-postgres.md) names the triggers for reconsidering the
deferred Raft option. The one that fired: Felix should be deployable where no
database platform exists. M7 made the control plane highly available as N
stateless instances over an HA Postgres; that is the right trade wherever a
managed database is available, and it remains supported. Raft is for
everywhere else — and for removing the last external dependency from a
self-contained cluster.

## What has to be true

**The broker contract is frozen.** Brokers consume snapshots + change feeds,
send heartbeats, and receive leases. None of that may change shape: a broker
must not know or care which backend the control plane runs. The whole design
is behind `ControlPlaneStore`/`AuthStore`, exactly where the Postgres/memory
split already lives.

**Acknowledged metadata writes survive losing a minority.** This matches the
contract ha-postgres.md demands of the database platform (synchronous
replication), for the same reason: a rolled-back shard-assignment
`generation` can be reused for a different owner, and brokers de-duplicate
ownership changes by generation. Raft gives this by construction — an
acknowledged write is committed on a majority.

**The M7 signal must keep holding.** Rolling restart of every instance, and
a kill of any one of three, with zero failed broker watch or heartbeat calls.
The existing `rolling_restart.rs` test is the yardstick; it gets a Raft
variant with no Postgres underneath.

**Writes are rare and small; reads are constant.** Tenants, streams,
assignments, membership — kilobytes that change on operator action, plus
heartbeats on a fixed cadence. The read side (broker watches, snapshots,
routing) dwarfs the write side. The design must put its cost on the path
that can afford it.

## The objection this design must answer first

[replication-design.md](replication-design.md) rejected per-shard Raft
partly because **Raft requires truncating a follower's divergent uncommitted
log suffix**, and the storage layer's load-bearing invariant is that records
are never rewritten. Does that argument not kill a metadata Raft log too?

No, and the distinction is worth stating precisely: that invariant belongs to
`felix-storage`'s segment log, and the metadata Raft log **never touches
`felix-storage`**. It is a separate, purpose-built, kilobyte-scale log whose
semantics are Raft's — suffix truncation included — owned by the control
plane, on the control plane's own volume. The "keep two logs" objection in
that document was about writing every *payload* record twice; here there is
no payload, and the second log holds only metadata commands. Reusing the
segment store for the Raft log was considered and rejected for exactly the
reason per-shard Raft was: it would need a truncate-uncommitted-suffix
operation the segment format deliberately does not have.

## The selected design

### Shape

Every control-plane instance embeds a Raft node. The group is the
control-plane replica set — three for production, one (single-node group)
for development. Each instance persists the Raft log and periodic snapshots
on its own volume; this is the PVC-per-pod StatefulSet shape
[control-plane.md](control-plane.md#kubernetes-deployment-model) has
sketched since the beginning.

```
            writes (forwarded to leader, proposed, committed on majority)
   brokers ──────────────────────────────────────────────┐
      │                                                   ▼
      │  reads (served locally)     ┌─────────┐    ┌───────────┐
      ├────────────────────────────▶│ follower│◀──▶│  leader   │
      │                             └─────────┘    └───────────┘
      └────────────────────────────▶┌─────────┐         ▲
                                    │ follower│◀────────┘
                                    └─────────┘   log + snapshots
                                                  on each node's own volume
```

### The state machine is the in-memory store

`InMemoryStore` already implements every store trait and holds the entire
metadata state. The Raft state machine is that store plus an `apply(command)`
entry point: commands mutate it, snapshots serialize it, and reads are served
from it directly on whichever instance received them. This is not a
convenience — it is the design's main simplification. The state machine does
not need inventing; it needs a command log in front of something that
already exists and is already contract-tested against Postgres.

**Commands are API-shaped, not statement-shaped.** One command per logical
mutation: `CreateStream`, `RegisterNode`, `Heartbeat`, `PlaceShard`,
`BootstrapTenantAuth`, and so on — roughly one per mutating store-trait
method, not one per row touched. Two things fall out:

- **Multi-step atomicity is free.** M7 made tenant bootstrap a single
  Postgres transaction so N racing instances produce exactly one winner.
  Under Raft the same guarantee is a single `BootstrapTenantAuth` command:
  the log totally orders the racers, the first applies, the rest observe
  `already initialized`. Every check-then-act race the Postgres backend
  closes with row locks, the log closes with ordering.
- **Apply must be deterministic.** Three instances apply the same commands
  and must reach identical state. So nothing inside `apply` may read a
  clock, generate randomness, or consult anything outside the command and
  the state. Every timestamp is stamped at *propose* time (the heartbeat
  rule "the recorded time is the control plane's own clock" becomes "the
  leader's clock, carried in the command" — same authority, same property:
  a broker still cannot postpone its own expiry). Every generated value —
  signing-key material, kids — is generated at the API layer and carried in
  the command. Signing keys already live in Postgres rows today; carrying
  them in log entries on the same class of volume changes their exposure
  surface by nothing, but it is stated here so nobody discovers it in a
  review.

### Writes: forwarded, proposed, applied

Any instance accepts a mutating HTTP request; a follower forwards it to the
leader over the control plane's internal channel, the leader proposes,
commits on a majority, applies, and answers. Clients keep talking to one
load-balanced URL. Forwarding is invisible, exactly as it is for
broker-to-broker publishes on the data plane.

Heartbeats go through the log like everything else. The volume argument:
tens of brokers at one heartbeat per 5s is single-digit commands per second;
even hundreds of brokers cost tens per second, on entries of a few dozen
bytes. If that ever dominates, the answer is revisiting liveness (see the
SWIM decision), not carving a side channel that would give the cluster two
disagreeing views of membership. Heartbeats keep their existing rule of
publishing no change-feed event; the state machine updates the row silently.
Replicating them matters for exactly one reason: the expiry sweep on a
newly elected leader must see recent heartbeat times, or a leader change
would look like every broker going silent at once and expire the fleet.

### Reads: local, because the contract already allows it

Broker snapshot/changes polling is *pull-based and eventually consistent by
contract* — a broker behind by one poll is the normal case the resnapshot
rules already handle. So follower-served reads change nothing for brokers,
and they are what keeps read load off the leader.

The change feeds keep their per-entity sequence numbers and eviction
windows, maintained by the state machine exactly as `InMemoryStore`
maintains them today, so all three resnapshot signals
(`first seq > since`, empty-but-advanced, `next_seq < since`) survive
unchanged. Sequence numbers are state-machine state, identical on every
instance at the same applied index — a broker can fail over between
instances mid-poll and the numbers still mean the same thing, which is
better than today, where an in-memory control plane restarting resets them.

Admin reads that feed decisions (the sweep's view before claiming an expiry,
placement's snapshot) run on the leader, which serves them from applied
state — leader-local reads after `ReadIndex`-style confirmation where
staleness would change a decision. The expiry sweep and the placement
reconciler run **only on the leader**, which replaces M7's
"each node claimed by exactly one sweep" cross-instance coordination with
something strictly simpler: there is one sweep because there is one leader,
and its conclusions are commands like everything else.

### Leases come from the leader

[replication-design.md](replication-design.md) leases are granted by "the
control plane"; under Raft that means **the Raft leader**, and the
`t + L + margin` safety arithmetic gets one addition: a new Raft leader must
not grant generation `G+1` earlier than the old leader could have promised
`G` was still valid. Lease grants and their timestamps are in the log, so
the new leader knows every outstanding promise and simply waits out the
same margin. The fencing story does not change; it gains a second
well-defined epoch (Raft term) underneath the one it already has
(assignment generation).

`ReplicaPositions` stay volatile and per-instance, exactly as documented
today — they expire in about a second and are advisory. The existing caveat
(a new instance cannot promote until leaders report to *it*) becomes "a new
Raft leader waits up to one report interval before promoting", which is the
same bound with less ambiguity about who "it" is.

### Snapshots, compaction, recovery

State is small, so snapshots are cheap: serialize the whole store at a log
threshold, truncate the log behind it. An instance that lost its volume
rejoins empty and is caught up by snapshot install plus log replay — the
Raft-native answer to the question backup/restore answers for Postgres. For
disaster recovery beyond quorum loss, the same snapshot format doubles as an
export: the import path below reads either a Postgres database or a snapshot
file.

### Group membership and bootstrap

Initial members come from configuration — for the StatefulSet shape, the
ordinal peers (`felix-controlplane-{0,1,2}`) via the headless service that
control-plane.md already reserves for exactly this. Growing the group is
learner-first: a new instance joins as a non-voting learner, catches up by
snapshot, and is promoted; shrinking is the reverse. A single-member group
serves development with no ceremony, replacing the in-memory backend's role
without its amnesia.

### Migration from Postgres

Dual backends, then an offline cutover. The tool is `felix-controlplane
migrate`, riding the same binary so every image that runs the control plane
carries it; the interchange format is the state machine's own exported
snapshot, produced **through the store traits** so it contains exactly what
the API serves. The ceremony:

1. **Stand up the Raft group**, fresh and empty, with the same peers map on
   every member. Its import guard refuses a group that already holds state,
   so pointing the tool at the wrong cluster is an error message, not a
   catastrophe. *Abort here: tear the group down; nothing has changed.*
2. **Freeze metadata writes** — flip the Postgres-backed instances out of
   rotation (their readiness during a drain already does this). Brokers
   keep serving on their catalogs and leases, exactly as during any
   control-plane blip. *Abort here: put the old instances back in
   rotation; nothing has changed.*
3. **Export**: `felix-controlplane migrate export-postgres state.json`
   (with `FELIX_CONTROLPLANE_POSTGRES_URL` pointing at the frozen
   database). The tool prints a summary — counts per entity — for the
   before/after comparison.
4. **Import**: `felix-controlplane migrate import state.json
   http://<any-member>` — one `ImportState` command proposed through the
   group: atomic on every member, forwarded to the leader from whichever
   address you gave. *Abort here: the group either applied all of it or
   none; tear it down and put Postgres back.*
5. **Verify and repoint**: compare the import summary against the export's,
   spot-check snapshots and feed heads, then point brokers and operators at
   the new instances and retire the database.

Sequence continuity is the part brokers feel: the export carries every
change feed's **high-water mark with an empty retained window**, so a
broker whose checkpoint is at the head continues without noticing, and one
behind the head gets the ordinary "checkpoint predates the window" signal
and resnapshots exactly once. Dragging Postgres's change rows along to
avoid even that single resnapshot was considered and skipped: the signal
path is a contract brokers already honour, and exercising it beats
carrying migration-only code to avoid it.

The freeze window is minutes, and brokers tolerate it by design. Online
dual-write migration was considered and rejected: two sources of truth
during the window is precisely the class of bug this whole design exists to
remove, and the workload (rare writes, pull-based readers) does not need
zero-write-downtime cutover.

**Disaster recovery beyond quorum loss** uses the same two commands, and
this is deliberate: the export file *is* the DR artifact, and
`migrate import ... --overwrite` onto a fresh group is the restore. The
`--overwrite` flag is the loud warning made mechanical — it discards
whatever the target group holds, and consumers' checkpoints with it, so it
belongs in a runbook and nowhere else. Take exports on a schedule the way
database backups are taken; any consistent export is a state the cluster
has actually been in.

### Probes

- `/v1/system/live` — unchanged: process-local, never touches consensus.
  An instance that lost quorum must not be restarted into the same lost
  quorum.
- `/v1/system/ready` — ready when this instance knows a leader and its
  applied index is within a freshness bound of the leader's commit. A
  follower serving watches is ready; an instance partitioned from the group
  is not; during an election the group is briefly all-unready for writes,
  which is the truthful answer and lasts an election timeout, not a cache
  window. The M7 drain semantics (fail readiness first, predrain hold,
  bounded drain) apply as-is.

## Library

**openraft, pinned to the stable 0.9 line** (0.9.25 at time of writing;
the 0.10 line is still alpha), wrapped behind a small crate-local seam so
openraft types never appear in the store traits or handlers — the same
discipline as the storage layer's `AppendOnlyLog` seam, and the insurance
against openraft's documented pre-1.0 API instability.

- **openraft** — async, tokio-native, snapshot/learner/membership machinery
  included, proven as the metadata consensus of Databend among others.
- **raft-rs (TiKV)** — rejected: a sync core that requires hand-building
  the tick loop, transport, storage, and snapshot orchestration openraft
  ships; that is most of the risk for none of the fit.
- **Hand-rolled** — rejected. ha-postgres.md put it as "every line of
  consensus code Felix does not carry is one it cannot get wrong"; that was
  an argument for deferring, and now that the work is scheduled it is an
  argument for a maintained implementation with an existing test corpus.
  Felix's inventiveness budget here goes to the state machine and the
  migration, which nobody else can write.

Transport between Raft nodes rides the control plane's existing HTTP
listener (internal routes), not a new port: the group is small, elections
are rare, and one less listener is one less thing to secure in M8.

## Failure modes

| Failure | Behaviour |
| --- | --- |
| One instance of three dies | Leader (if it was the leader) re-elected in one election timeout; writes pause for that long, reads keep serving; M7 signal holds |
| Instance loses its volume | Rejoins empty, snapshot-installed, promoted back; no operator data surgery |
| Network partition, leader in minority | Old leader steps down (cannot commit), majority elects; minority instances fail readiness rather than serve writes that cannot commit |
| Quorum lost (2 of 3 down) | Writes and readiness fail on survivors; brokers keep serving on catalogs and leases as during any control-plane outage; recovery = restore instances, or restore-from-snapshot ceremony documented with appropriately loud warnings |
| Clock skew between instances | Irrelevant to Raft safety (term-based); lease arithmetic keeps the same drift-rate assumption it has today |
| Disk full on one instance | That instance fails writes → falls out of quorum participation → fails readiness; group continues on the majority |

## Testing

- **Determinism harness**: apply the same command sequence to two state
  machines, assert byte-identical snapshots — the cheap test that catches
  the expensive bug (a clock or a HashMap iteration order leaking into
  apply).
- **The contract suites run against the Raft backend** exactly as they run
  against memory and Postgres (`node_contract`, `shard_contract`) — that is
  what the trait seam is for.
- **`rolling_restart.rs`, Raft variant**: three instances, no Postgres,
  same zero-failed-calls assertion, plus a hard kill of the leader
  specifically.
- **Chaos via the cluster harness**: partition the leader from the group
  under broker traffic; assert no metadata write is lost and no shard gets
  two leaders across the transition (the lease safety interval test, now
  with a moving grantor).
- **Snapshot/restore**: wipe one instance's volume mid-traffic; assert it
  rejoins and converges.

## What would overturn this

- **Heartbeat write volume dominating the log** at broker counts Felix
  actually reaches — the fix is decentralizing liveness (the SWIM decision
  gets reopened), not abandoning Raft for the metadata that is actually
  rare.
- **A multi-region metadata requirement.** One Raft group in one region is
  this design; metadata with region-local write latency everywhere is a
  different problem (and was already out of scope for Postgres HA too).
- **openraft 0.10 stabilizing with a materially better storage API** — the
  seam exists so that upgrade is a contained event, not a redesign.

## Sequencing

Tracked as milestone M13; the issue breakdown mirrors this document's
sections — Raft core and storage, state machine and command set, the store
backend and forwarding, migration tooling, probes and packaging, and the
chaos/conformance pass. [#333](https://github.com/gabloe/felix/issues/333)
is the umbrella.

### Implementation status

| Piece | Issue | State |
| --- | --- | --- |
| Raft core: seam, redb log/vote/snapshot store, HTTP transport, group lifecycle | [#337](https://github.com/gabloe/felix/issues/337) | **Landed** — `services/controlplane/src/raft/`. The store passes openraft's own storage conformance suite; group tests cover election, replication, restart-as-rejoin, wiped-volume rebuild by snapshot, and learner-first growth. Nothing serves metadata from it yet. |
| Metadata state machine | [#338](https://github.com/gabloe/felix/issues/338) | **Landed** — `store/command.rs` (the versioned, API-shaped command set) and `store/state_machine.rs` (`MetadataStateMachine`, the in-memory store behind the seam). The determinism harness applies a full-coverage script to two machines and requires byte-identical snapshots; a real three-node group settles eight concurrent bootstraps by log order alone with byte-identical replicas. Landing it surfaced and fixed real iteration-order leaks: multi-node expiry and cascading deletes published change events in HashMap order. Nothing serves API traffic from it yet. |
| Store backend, forwarding, read semantics | [#339](https://github.com/gabloe/felix/issues/339) | **Landed** — `store/raft_backend.rs` (`RaftStore`), the third backend behind the store traits: reads from local applied state, writes proposed through the seam with follower→leader forwarding inside it, sweep and placement gated to the leader by a linearizable read-index check, and `StorageBackend::Raft` selectable via `FELIX_RAFT_NODE_ID` / `FELIX_RAFT_DATA_DIR` / `FELIX_RAFT_PEERS`. Passes the same node/shard contract suites as memory and Postgres; a binary-level test serves the HTTP API with no database and keeps its metadata across a restart. Finding recorded below. Probes are minimal (leader-known) until #341. |
| Migration from Postgres | [#340](https://github.com/gabloe/felix/issues/340) | **Landed** — `felix-controlplane migrate export-postgres/import`, the generic trait-level export (works against any backend, doubling as the DR artifact), the `ImportState` command with its used-store guard and `--overwrite` restore path, and the ceremony above. The pg-tests E2E migrates a populated Postgres into a Raft group over the real propose route and verifies records, sequence heads, generations, and auth state; a broker at the head continues without a resnapshot. |
| Probes, packaging, configuration | [#341](https://github.com/gabloe/felix/issues/341) | **Landed** — readiness answers from consensus state (leader known, apply-lag bounded, and a leader counts only while a quorum has acknowledged it within 5s — a quorumless leader leaves rotation, proven by test); liveness stays process-local. Timings are tunable (`FELIX_RAFT_HEARTBEAT_MS`, `FELIX_RAFT_ELECTION_TIMEOUT_MIN/MAX_MS`, snapshot/write knobs) with unworkable combinations refused at startup. Consensus position ships as `felix_meta_raft_*` gauges plus forwarded-proposal and write-timeout counters. Kubernetes shape documented on the docs-site page. Known fact below. |
| Chaos and conformance | [#342](https://github.com/gabloe/felix/issues/342) | **Landed** — `tests/raft_chaos.rs`: three real binaries, no database, broker-shaped traffic and metadata writes flowing while every member is SIGTERM-restarted, the leader is SIGKILLed, the leader is frozen (SIGSTOP) past several elections and thawed, and a follower's volume is wiped. Verdict per run: zero failed calls, election gaps bounded, and **every acknowledged write present on every member** — the milestone's completion signal, met. It caught four real bugs before landing (below). |

One deliberate deviation from the sketch above, made while landing #337: the
Raft log lives in **redb** (an embedded, crash-safe, single-file ACID store)
rather than hand-rolled files. Consensus durability plumbing — votes and
entries that must never be acknowledged and then lost — is the last place
Felix should be inventive, and openraft's storage suite now enforces the
semantics against the real store on every test run.

Four findings from landing #342 — each one a bug the chaos suite caught
that no earlier test could see:

- **`loosen-follower-log-revert` is not optional.** A member rejoining with
  a wiped volume reports a log that went backwards; without that openraft
  feature the *leader* trips a debug assertion in its replication-progress
  tracking when the member returns — and a release build would carry the
  inconsistent progress state silently. The feature is now on, with the
  reasoning at the dependency declaration.
- **A restart is not done until the state machine is.** A restarted member
  learned the leader within a heartbeat and reported ready while its
  volatile state machine was still replaying the log — serving a world
  missing entries it had itself committed. Startup now blocks until the
  replay reaches the committed index persisted on its own disk.
- **A wiped member's apply-lag reads zero.** Lag is measured against the
  member's *own* log, which is exactly the blind spot for a member that has
  none of the group's state yet. Readiness now refuses a follower that
  knows a leader but holds an empty log — it has joined an established
  group and nothing has replicated into it yet. (A leader is exempt; a
  genuinely new cluster is leaderless, so formation is never blocked.)
- **One hung hop must not eat the whole write budget.** A leader that is
  frozen — not dead — accepts the forwarded connection and stalls, and a
  single forward could consume the entire proposal budget, leaving nothing
  for the retry after the group elected a successor. Every attempt is now
  individually capped well below the budget.

One known fact recorded while landing #341, inherent to **pre-0.10
openraft**: there is no leadership-transfer API, so a rolling deploy that
restarts the current *leader* pauses metadata writes for one election
timeout (~1.2s at defaults) while a successor elects itself. Reads keep
serving throughout, followers restart with no pause at all, and brokers are
unaffected by construction. openraft 0.10 adds `transfer_leader`; adopting
it is a contained change because the seam owns the shutdown path — until
then this is a documented bound, not a bug.

One finding from landing #339: **openraft's write path waits indefinitely**
— a leader that has lost quorum queues proposals forever rather than
failing them. The seam now owns an overall write deadline (default 10s,
elections and forwarding included), so "no quorum" reaches callers as an
error rather than a hang; the quorum-loss test is what surfaced it.

Two findings from landing #338, recorded because they are the design's
predictions coming true:

- **The iteration-order leak was real.** Multi-node expiry and the
  tenant/namespace cascade deletes published their change events in HashMap
  iteration order — harmless on one instance, state-forking on replicas,
  because each event takes a sequence number as it publishes. They now
  publish in sorted order, and the determinism harness is what holds that
  door shut.
- **Key generation moved to propose time for every backend.**
  `TenantAuthSeed` now carries the candidate signing keys; the API layer
  generates them, and both the Postgres transaction and the state machine
  install them only when the tenant has none. The store layer is now free of
  randomness end to end, not just under Raft.
