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
- **Writes.** Admission checks the lease. Commit checks it again, or not, which
  is the `CheckAtCommit` knob. Anything may happen between the two: that gap is
  where a paused process lives.
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
| `FelixShardLease.cfg` | drifting clocks, no writes: heartbeats, lapses, promotions | pass `AtMostOneServing` and `NoStaleCommit` (1.0M states) |
| `FelixShardLogOrder.cfg` | both lease checks, `Quorum`, two writes, promotion by log order | pass every invariant (3.0M states) |
| `FelixShardThinMargin.cfg` | drifting clocks with `Margin = 0` and `Eps = 0` | violate `AtMostOneServing` |
| `FelixShardNoCommitCheck.cfg` | commit-time lease check removed | violate `NoStaleCommit` |
| `FelixShard.cfg` | the design as written: promotion from the leader's report | violate `AckedSurvive` |

Drift is checked where it matters and nowhere else. The lease configurations
carry drifting clocks and no writes, so every interleaving of three drifting
clocks is affordable; the replication configurations carry writes and
synchronised clocks, because nothing about which replica holds which record
depends on what time a broker thinks it is. One configuration with both ran
past four hundred million states without finishing.

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

### The finding

With promotion as the design writes it, TLC finds this in a second:

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
just older than the last acknowledgement — and the acknowledgement is released
the moment the quorum mark moves, while the report describing that same pass is
still on its way.

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
