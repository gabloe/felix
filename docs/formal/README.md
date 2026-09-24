# A formal model of one shard

`FelixShard.tla` is a TLA+ model of the protocol in
[`docs/replication-design.md`](../replication-design.md): the lease that lets a
broker serve a shard, the replication that puts its records on a majority, and
the promotion that names the next leader when the lease lapses. TLC checks it
in about a minute, and `task tla:check` runs it locally and in CI.

Prose about a safety interval is an argument; a model checker either finds the
interleaving that breaks it or runs out of interleavings to try. This one found
something the prose had not.

## What is modelled

One shard, three brokers, one control plane, discrete time.

- **Clocks.** `now` is real time. Each broker's clock is monotonic and within
  `Drift` of real time, which is the design's drift-rate assumption in the only
  shape a finite model needs. On each tick a broker's clock moves by zero, one
  or two: standing still is slow, two is fast.
- **Leases.** The leader heartbeats; the control plane accepts a heartbeat only
  while the lease it granted has not lapsed, and extends it by `L`. The broker
  extends its own belief from the instant it *sent* the heartbeat, on its own
  clock, and stops serving `Eps` before that belief expires. Heartbeats can be
  lost. The control plane grants the next generation no earlier than `Margin`
  after the expiry it recorded.
- **Writes.** Admission checks the broker is serving. The write then waits,
  and claims its place in the log; with `FenceAtClaim` the claim checks the
  handoff fence again. `AckOnAdmit` acknowledges a `Leader` write when it is
  admitted rather than when it commits, and with `FenceFromAdmit` such a write
  holds the fence from admission. Commit checks the lease again, or not, which is the
  `CheckAtCommit` knob. Anything may happen between admission and the claim,
  and between the claim and the commit: those gaps are a queue and a paused
  process.
- **Replication.** The leader ships the next record a follower is missing. A
  follower whose log disagrees with the leader's keeps what a newer generation
  than its own last accepted one says, above its high-water mark; anything else
  halts it. A follower refuses a leader older than one it has heard from.
- **Acknowledgement.** Under `Quorum`, once a majority including the leader
  holds the record; under `Leader`, on the leader's own commit.
- **Reports.** The leader tells the control plane which followers hold every
  record it does. The report travels on its own: it may arrive after the
  acknowledgements it describes, or never.
- **Promotion.** After the lapse and the margin, the control plane names a new
  leader. `Promotion = "leader-report"` is the design as written: a follower
  the last report named as caught up. `Promotion = "log-order"` is the live
  replica with the greatest (last generation, length): Raft's election
  restriction, which needs no report.
- **Planned handoff**, under `Handoff`. The control plane may move the shard
  while its leader is alive: it fences the leader, which stops serving when
  it sees the fence but keeps its lease and keeps shipping, and names the
  successor only once the leader has reported that its log stopped growing.
  A write claimed before the fence still commits. `WaitForDrained` is that
  wait. The report counts claimed writes only, as the broker's write fence
  does. Reports carry the generation they were made at and one from a
  superseded generation is dropped on arrival, as the store does.
- **Planners.** The control plane decides from a read of the store, not from
  its live state. A decision (promote, fence, cut over) either reads and writes
  in one step, or comes from a read one of `Planners` took earlier (`cpView`:
  the assignment, the last report, whether the lease had lapsed) and still
  holds, one write per read. Every assignment write bumps `ver`, the store's
  generation. With `CasWrites` a write lands only if `ver` is still what its
  read saw, which is `put_shard_assignment_if`. `Planners = {}` is a single
  instance whose reads are never stale; the `StalePlanner` and
  `StalePromotion` configurations hold one read across the other instance's
  writes, as two control-plane instances over one database do.

Not modelled: the storage layer (a commit is a commit), network partitions as
such (they are lost heartbeats, lost reports, and delays), retention, and the
bootstrap of a follower below the leader's base.

## What is checked

| Invariant | Says |
| --- | --- |
| `AtMostOneServing` | No two brokers serve the shard at once. |
| `AckedSurvive` | Whoever is serving holds every acknowledged record. |
| `AckedAgree` | Two brokers never hold different acknowledged records at one offset. |
| `AckedOnMajority` | Every acknowledged `Quorum` record is on a majority. |
| `NoTruncationBelowHwm` | A follower never discards a record below its high-water mark. |
| `NoStaleCommit` | No broker commits at a generation the control plane has superseded. |

## The configurations, and what each must do

`scripts/check_tla.sh` holds each configuration to a declared outcome. A
configuration that must find a violation exists to show a check is
load-bearing, or to pin a finding the design has not acted on yet; a violation
that quietly became a pass would be a model that stopped saying anything.

| Configuration | Knobs | Must |
| --- | --- | --- |
| `FelixShardLease.cfg` | drifting clocks, no writes: heartbeats, lapses, promotions | pass `AtMostOneServing` and `NoStaleCommit` (0.8M states) |
| `FelixShardLogOrder.cfg` | both lease checks, `Quorum`, two writes, promotion by log order | pass every invariant (2.0M states) |
| `FelixShardThinMargin.cfg` | drifting clocks with `Margin = 0` and `Eps = 0` | violate `AtMostOneServing` |
| `FelixShardNoCommitCheck.cfg` | commit-time lease check removed | violate `NoStaleCommit` |
| `FelixShardNoReportOrder.cfg` | the design *before* #268: a `Quorum` ack released before the report describing it lands | violate `AckedSurvive` |
| `FelixShard.cfg` | the design as implemented: report-before-mark, promotion from the leader's report | pass every invariant (2.0M states) |
| `FelixShardHandoff.cfg` | a planned move off a live leader: fence, drained report, cut over | pass every invariant (2.7M states) |
| `FelixShardHandoffNoWait.cfg` | the same move cutting over without waiting for the drained report | violate `AtMostOneServing` |
| `FelixShardStalePlannerCas.cfg` | two instances moving the shard, one acting on a held read; writes conditional on the generation read | pass every invariant (2.6M states) |
| `FelixShardStalePlanner.cfg` | the same, writing unconditionally | violate `AtMostOneServing` |
| `FelixShardStalePromotionCas.cfg` | two instances failing the shard over, one acting on a held read; writes conditional | pass every invariant (29K states) |
| `FelixShardStalePromotion.cfg` | the same, writing unconditionally | violate `AtMostOneServing` |
| `FelixShardHandoffLeaderAck.cfg` | a planned move under `Leader` acknowledgement, the claim checking the fence | pass every invariant (1.3M states) |
| `FelixShardHandoffNoClaimFence.cfg` | the same move with the fence checked at admission only | violate `AckedSurvive` |
| `FelixShardHandoffAdmitAck.cfg` | the same move with the write acknowledged on admission and holding the fence from there | pass every invariant (1.2M states) |
| `FelixShardHandoffAdmitAckClaimFence.cfg` | acknowledged on admission, fenced at the claim | violate `AckedSurvive` |

Drift is checked where it matters and nowhere else. The lease configurations
carry drifting clocks and no writes, so every interleaving of three drifting
clocks is affordable; the replication configurations carry writes and
synchronised clocks, because nothing about which replica holds which record
depends on what time a broker thinks it is. One configuration with both ran
past four hundred million states without finishing.

### What ties this to the code, and what does not

A spec and an implementation are two artifacts in two languages. Nothing in the
toolchain makes one follow the other, and the gap is not hypothetical: the
broker gained the report-before-mark ordering in #268, this model went on
describing the design without it, and `check_tla.sh` pinned the resulting
`AckedSurvive` violation as *expected* — asserting for three weeks that Felix
loses acknowledged records, for a design it no longer had. An issue was then
filed against the model's finding, proposing work the code did not need.

So every configuration carries an `Evidence:` block naming the tests that
establish what it assumes of the implementation, and
`scripts/check_spec_evidence.py` (run by `task docs:evidence`) fails when a
cited test no longer exists or a configuration cites nothing. Rename the test
for a behaviour and the spec is put in front of you.

A configuration that deliberately models something the code does *not* do says
`Evidence: none` and why — the counterexample configurations instead cite the
test proving the check they remove is really there.

Citations do not catch the change that actually drifted: #268 changed the
protocol without renaming a cited test. So a pull request that touches the
code this model describes — `services/felix-broker-service/src/{cluster/lease,replication,shards/lifecycle}`
and `services/felix-controlplane-service/src/cluster/placement`, tests and
metrics aside — must also touch `docs/formal/`, or carry a line

```
Spec-Unaffected: <why>
```

in a commit message or the PR description. `scripts/check_spec_pairing.py`
enforces it in CI (`task tla:pairing BASE=origin/main` locally). It is blunt on
purpose: most edits to those files are not protocol changes, and the marker is
how you say so. What it buys is that nobody changes the protocol without being
asked whether the model still describes it.

**What this does not do.** A cited test can keep its name while its assertions
change, and the spec can model a behaviour wrongly while every citation
resolves. This makes drift harder to introduce silently; it does not detect it.
Checking that the implementation *conforms* to the spec needs trace validation —
emitting protocol events and checking recorded runs are behaviours of the
spec — which is a different and much larger mechanism.

**Trace validation is not planned.** It needs the broker and control plane to
emit protocol events behind a test-only feature, a mapping from those events
onto the spec's variables, and TLC in trace mode in CI — a project, not a
check. And what it buys is bounded: it shows the runs the tests happened to
make are behaviours the spec permits, and says nothing about paths no test
exercises. The drift that actually occurred (#268) is what the two checks
above catch. Worth revisiting if the protocol grows another mechanism of the
size of the planned handoff, or if drift gets past both checks once.

### Two planners, one read

`FelixShardStalePlanner.cfg` finds this. One instance reads the shard while
its leader is live and its report lists both followers caught up, and holds
that read. The other fences towards one follower, the leader reports drained,
and it cuts over. The held read's fence lands next: it names the old leader
again, at a new generation, with the other follower as successor. That leader
reports drained, and the cut-over to the second follower lands while the first
still holds a live lease. `FelixShardStalePromotion.cfg` is the failover
version: two promotions from one report of two caught-up followers. With
`CasWrites`, each late write finds a newer generation and writes nothing.

### The interval that is load-bearing

With no margin on either side, TLC finds a leader whose clock runs slow still
serving when the control plane, whose lease copy has lapsed, names the next
one. `Margin` and `Eps` together have to outlast what `Drift` can do to the
two clocks, which is the design's safety interval, and removing it is two
leaders in one step.

### The check that is load-bearing

With `CheckAtCommit = FALSE`, TLC finds a broker that admits a write while its
lease is valid, is paused while the lease lapses and the next generation is
granted, and then commits. That is the "process suspension" case the design
names, and the second check is what closes it.

### The wait that is load-bearing

`FelixShardHandoffNoWait.cfg` names the successor as soon as the fence is
written. TLC finds two leaders in seven steps: the control plane fences the
leader and cuts over, and the old leader has simply not seen the fence yet —
it holds a valid lease, believes it leads, and is serving. Nothing about the
lease closes this, because the lease has not lapsed; the leader is alive and
was meant to keep it.

What closes it is the leader saying it stopped. `WaitForDrained = TRUE` holds
the cut-over until a report at the fenced generation says the leader has
stopped serving and its log is not growing, and the same configuration then
explores 2.7M states without a violation. The generation on the report matters
as much as the flag: an earlier leader's drained report is about a leadership
that has ended, and believing it lets the next move skip its wait.

The broker's half is `ShardLifecycle::observe`, which closes the shard's write
fence the moment a draining assignment arrives and never serves it again at
that generation, and the replication driver, which withholds the drained
report until the fence is closed with no write inside it.

### The fence at the claim that is load-bearing

`FelixShardHandoffNoClaimFence.cfg` checks the fence at admission only. TLC
finds an acknowledged record lost in eleven steps:

1. The leader admits a write. It waits to be claimed — in the broker, in a
   publish queue.
2. The control plane fences the leader, which sees it and stops serving.
3. The leader reports `drained`. The report counts claimed writes, and this
   one is not claimed yet, so the report is true as far as it goes.
4. The write is claimed and committed. Under `Leader` consistency the commit
   is the acknowledgement.
5. The control plane cuts over on the drained report, to a successor the
   report named caught up. It does not hold the record. `AckedSurvive` fails.

Nothing about waiting longer closes this: however still the leader's tail
holds, a write can wait in the queue for longer. What closes it is the claim
checking the fence — `FenceAtClaim = TRUE`, which is
`FelixShardHandoffLeaderAck.cfg`, passing. The broker's check is
`ShardFence::enter` in `services/felix-broker-service/src/shards/lifecycle/fence.rs`,
entered by every write right before it claims its place in the log and held
until the write is durable, and the drained report waits for the fence to be
closed with nothing inside it. Under `Quorum` the report-before-mark ordering
keeps such a record from being acknowledged, which is why the counterexample
needs `Leader`; the record would still land on the old leader after it said
it had stopped.

### The fence from admission, for a write acknowledged there

The claim check is only safe for a write nobody has been told about yet. The
broker acknowledges a publish when it is queued unless `ack_on_commit` is on,
and refusing that publish at its claim loses a record the client holds an ack
for. `FelixShardHandoffAdmitAckClaimFence.cfg` models it: the write is
acknowledged on admission, the leader sees the fence while it is queued,
refuses the claim and reports `drained`, and the successor takes over without
it. TLC finds `AckedSurvive` violated.

`FelixShardHandoffAdmitAck.cfg` has the write hold the fence from admission,
as `enqueue_publish` does for a publish nobody waits on: the claim is not
refused, and the drained report waits until the write is claimed and
committed. It passes.

### The ordering that is load-bearing

With `ReportBeforeAck = FALSE`, TLC finds this in a second:

1. The leader reports its two followers level with it.
2. It admits and commits a write, ships it to one follower, and acknowledges it
   under `Quorum`: leader plus one follower is a majority of three.
3. It dies before the next report leaves. The report the control plane holds is
   recent, and predates the acknowledgement.
4. The lease lapses, the margin passes, and the control plane promotes the
   *other* follower, which the report named as level. It holds no copy of the
   acknowledged record. `AckedSurvive` fails.

Report expiry does not close this. The design's expiry is about reports older
than the time it takes to notice a leader is gone; this report is fresh, it is
just older than the last acknowledgement.

**What closes it is ordering, not freshness**, and the broker does it: the
leader reports who holds the record, waits for that report to land, and only
then moves the quorum mark that releases the acknowledgement. That is
`publish_mark` in `services/felix-broker-service/src/replication/driver/shard.rs`, which moves the
mark only `if reported`, and `await_quorum`, which blocks the publish on the
mark. With `ReportBeforeAck = TRUE` — `FelixShard.cfg`, the implemented design
— TLC explores 2.0M distinct states and finds no violation.

So the pair is the point. The ordering is not merely present in the code; the
model shows the guarantee fails without it.

> A caution on reading a pass. `FelixShard.cfg` passing is only meaningful if
> acknowledgements actually happen under the added precondition — a
> precondition nothing can satisfy would make `AckedSurvive` vacuously true.
> Checked by hand with a temporary `acked = {}` invariant, which TLC violates
> in 2,307 states: acknowledgements are released, and the pass is about them.

Promotion by log order finds no trace, in the same bounds. A replica holding an
acknowledged `Quorum` record is in every majority that could acknowledge one
after it, so the replica with the greatest (last generation, length) among the
live ones holds every acknowledged record; the generation comes first because
a stale proposal from an older leader can be longer than the log that
superseded it. That rule needs each replica to say where it is — a position on
its own heartbeat — rather than the leader to say where its followers were.

## Running it

```bash
task tla:check          # java or docker; fetches the TLA+ tools once, pinned
```

To explore a configuration by hand, with the trace when a check fails:

```bash
java -jar target/tla/tla2tools-v1.7.4.jar -deadlock -workers auto \
  -config docs/formal/FelixShard.cfg docs/formal/FelixShard.tla
```

The bounds (`MaxTime`, `MaxWrites`, and the `SYMMETRY` over brokers) keep the
whole check to about a minute. Widening them widens what is checked; the
invariants do not change. The script runs TLC with checkpoints off and its
scratch directory outside the tree. Run by hand without `-metadir`, TLC writes
a `states/` directory beside the spec that reaches gigabytes; `.gitignore`
covers it.
