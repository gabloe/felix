# Shard replication and leader failover

**Decision: leadership is a time-bounded lease issued by the control plane, and
replication is log shipping from the leader to its followers. Per-shard Raft is
rejected.**

Recorded for M5.1 (#110). The rejected alternative and what would overturn the
decision are both below.

## What has to be true

Constraints first, because the comparison is only meaningful against them.

**Safety.** At most one broker may commit writes for a shard at a given epoch,
under any combination of partition, pause, and clock error the failure model
admits. A record acknowledged under `Quorum` must survive any failure the
configured majority tolerates.

**Availability.** A shard survives its leader failing. It also survives the
control plane being briefly unreachable — a broker must not stop serving because
a metadata service restarted.

**Write latency.** A publish already crosses ingress, offset assignment,
durability, and fanout in a fixed order. Replication adds one network round trip
for `Quorum` and none for `Leader`. It must not add a *consensus* round trip to
the common path.

**Operational.** A broker holds many shards. Whatever runs per shard runs
hundreds or thousands of times per broker.

**Dependency.** The storage layer is fixed. Its invariants are load-bearing and
documented, and a replication design that violates them is not a replication
design, it is a storage rewrite.

## The constraint that decides it

`docs/storage-format.md` and `docs/durable-storage.md` state it plainly:

> **Records are never rewritten**, which is what lets recovery keep trusting
> "valid bytes end at EOF".

That single property is why torn-tail repair is sound, why a missing index can
be rebuilt, and why preallocation reserves blocks without changing `st_size`.

**The invariant is narrower than "never rewritten", and it has to be.**

An earlier version of this document argued that Felix avoids truncation
entirely, because a leader only ships records it has committed. That is false
under `Quorum`, where shipping is what *produces* the commit: a follower stores
a record before any majority holds it, so a leader that dies mid-flight leaves
that record on some followers and not others. The next leader may legitimately
reuse the offset. Every leader-change scheme has to reconcile that, and Felix is
not an exception — `Divergence::Conflict` exists precisely because it happens.

The invariant that actually holds, and that recovery depends on, is:

> **No record at or below the high-water mark is ever rewritten.**

Below the mark a record is on a majority and can never be un-committed. Above it
a record is a proposal, and discarding a proposal the cluster did not adopt is
not the same act as rewriting history. Torn-tail repair and interior-corruption
detection rest on the *committed* prefix being immutable, which this gives them.

So the case against per-shard Raft is not about truncation, because truncation
is required either way. It is:

1. **Election and clock trade-offs.** Raft elects by term and majority vote,
   which needs no clock assumption but adds a round of voting to every failover
   and a second failure detector beside the one the control plane already runs.
   Leases reuse the heartbeat the broker already sends, and pay for it with the
   safety interval described below.
2. **Two logs, or one.** Raft over the stream log means either the segment log
   *is* the Raft log — with Raft's index and term bookkeeping in the record
   format — or every record is written twice and the two can disagree after a
   crash. The second is a durability path the storage design deliberately does
   not have.

Neither is a tuning problem. Both are a different storage layer.

The storage layer was in fact built anticipating the other answer:
`docs/durable-storage.md` says "Replication (M5) is what `seal`'s checksum and
`read_range`'s bounded paging exist to serve." Sealed-segment checksums and
bounded range reads are the primitives of *shipping*, not of consensus.

## Why the control plane being Raft-backed does not settle this

M7 makes control-plane metadata highly available, and Raft is the right tool
there. It does not follow that payload replication should use Raft, and the
issue asks for this to be said explicitly.

The two have opposite cost profiles:

| | Control-plane metadata | Stream payload |
| --- | --- | --- |
| Volume | Assignments, tenants, streams — kilobytes, changing rarely | The entire data plane |
| Groups | One, for the cluster | One per shard: hundreds or thousands per broker |
| What consistency buys | Linearizable reads of small state everyone must agree on | Durability of records already ordered by a single writer |
| Cost of a round trip | Paid once per metadata change | Paid on every publish |

A stream's records are already totally ordered by their leader. Consensus is not
being asked to *establish* an order — the log has one. It is being asked to
replicate an existing order durably. That is a weaker requirement than Raft
solves, and paying Raft's price for it is paying for agreement that has already
happened.

## The selected design

### Leases

The control plane grants a **lease** on `(shard, generation)` to one broker for a
bounded duration `L`. The lease is the authority to serve; the assignment alone
is not.

A lease is bound to the generation. A new generation is a new lease, never a
renewal of the old one — which is what makes generation the epoch that fences
writes.

The leader renews through the heartbeat it already sends. A renewal that does not
arrive before expiry means the lease lapses; nothing revokes it explicitly,
because a revocation cannot be delivered to a partitioned broker, which is
precisely the case that matters.

### The exact condition under which a broker may accept a write

A broker may accept a write for shard `S` if and only if **all** of:

1. It holds a lease for `S` at generation `G`.
2. Its own monotonic clock reads earlier than `expiry(G) − ε`, where `ε` covers
   clock drift and the time between this check and the record reaching disk.
3. Its local shard state for `S` is open at exactly generation `G` — the check
   `IngressRouter::dispatch` already performs.
4. For a `Quorum` stream, a majority of the replica set for `G` **including the
   leader** has durably stored the record. For a `Leader` stream, the leader's
   own configured durable commit point has been reached.

Conditions 1–3 are checked **twice**: once when the request is admitted, and
again immediately before the record is committed to the log. The second check is
not redundant. Everything between them can take arbitrarily long — a full ingress
queue, a slow fsync, a VM pause — and a lease that was valid on admission may
have expired by the time the bytes reach the disk. Fencing only at the routing
boundary leaves exactly the window this design exists to close.

### Failover

1. The lease lapses (no renewal within `L`).
2. The control plane selects an eligible replica: one whose durable high-water
   mark is within the configured catch-up bound of the last known committed
   offset.
3. It publishes assignment generation `G+1` naming that replica as leader, and
   grants it a lease.

The control plane must not publish `G+1` before it is certain no broker can still
believe it holds `G`. With lease duration `L` granted at control-plane time `t`,
`G+1` may be granted no earlier than `t + L + margin`, where `margin` covers
clock drift between the two parties and the network delay of the grant itself.
The leader stops at `t + L − ε` by its own clock. The gap between those two
instants is the safety interval, and it is why leases are safe without
synchronized clocks.

The broker cannot observe `t`, so it anchors at the instant it **sent** the
heartbeat — always at or before `t`, so the round trip comes out of its own
lease rather than out of the margin. Anchoring at the instant the *response* was
handled runs the other way: a buffered read or a VM pause in between would push
the lease past `t + L`, which is the safety interval being spent by the same
kind of stall the twice-checked conditions above exist to survive.

The safety interval is the whole mechanism, so it is worth seeing:

<p align="center">
  <img src="assets/leader-lease.svg" alt="A lease on one time axis: broker A stops accepting at expiry minus epsilon by its own clock, the control plane grants the next generation no earlier than expiry plus a margin, and the gap between them is a safety interval in which no broker is leader" width="900">
</p>

```mermaid
sequenceDiagram
    participant A as Broker A (leader at G)
    participant CP as Control plane
    participant B as Broker B (replica)

    CP->>A: lease(shard, G) until t+L
    Note over A: serving

    A--xCP: renewal lost (A fails, or is partitioned)

    Note over A: at t+L-ε by A's own clock,<br/>A stops serving
    Note over CP: waits until t+L+margin<br/>before granting again

    CP->>B: assignment G+1, lease until t'+L
    Note over B: serving

    Note over A,B: the gap between A stopping and B starting<br/>is the safety interval: no epoch has two leaders
```

Nothing in that sequence requires A and CP to agree on the time. It requires only
that neither clock runs more than `ρ` faster or slower than real time, so the two
instants cannot cross.

### The clock assumption, stated precisely

Safety requires a bound on clock **drift rate**, not synchronized clocks. For any
two nodes, over a real interval `T`, each monotonic clock advances by between
`T(1−ρ)` and `T(1+ρ)` for a known `ρ`.

This is a real assumption and it can be violated. The realistic violation is not
NTP error — it is **process suspension**: a VM migration, a stop-the-world pause,
a throttled container. A broker suspended past its expiry wakes believing it
still holds a lease.

That is exactly why condition 2 is re-checked at the durable-append boundary
rather than only at admission. A suspended broker's next check is after it wakes,
and it sees an expired lease before its bytes reach disk. A suspension *between*
that check and the write completing is bounded by `ε`, which is the parameter to
tune, and the residual risk to state honestly rather than claim away.

### Replication

The leader ships records to followers over the internal transport that already
exists (#105). Under `Quorum` it ships them *before* they are committed — that
is what makes the majority — so a follower can hold a record no majority ever
acknowledged, from a leader that then died.

A follower therefore truncates, but only **above** the high-water mark. Below it
a record is on a majority and is never discarded: it was ordered by a leader
holding an unexpired lease, and the safety interval means no other leader existed
at that generation. Above it, a record is a proposal that the cluster may not
have adopted, and a new leader reusing the offset is ordinary rather than
alarming.

Reconciling that needs the two sides to agree on where their histories diverge,
which is what the generation of each record establishes — see
[Divergence and truncation](#divergence-and-truncation) (#406).

Catch-up for a new or lagging follower is a bounded `read_range` from the leader,
with sealed-segment checksums to verify wholesale rather than record by record —
the primitives the storage layer already exposes for this purpose.

**This works only while the leader still holds what the follower is missing.**
Once retention has trimmed past a follower's position, shipping cannot reach it:
the records are not on the leader to send, and starting the follower at the
surviving base offset would leave its log with a hole nothing downstream could
detect. Such a follower is offered a log that *begins* at the leader's oldest surviving
offset (`ReplicateBootstrap`). A replica holding nothing takes it and replication
resumes; one holding records of its own refuses, because a log placed over them
would have a hole nothing downstream could detect, and it is halted for an
operator to resolve.

### The `Leader` loss window, precisely

For `ConsistencyLevel::Leader`, a record is acknowledged once it is durable on
the leader alone. If the leader's storage is permanently lost, records
acknowledged but not yet shipped are lost.

The window is bounded by the leader's **replication lag**: the byte range between
its durable high-water mark and the lowest follower's acknowledged mark. It must
be exported as a metric, because a bound nobody can observe is not a bound. An
operator choosing `Leader` is choosing this window, and must be able to see how
large it currently is.

`Quorum` has no such window: a majority including the leader holds every
acknowledged record, so any failure within the configured majority preserves it.

### Divergence and truncation

Two brokers can hold different records at the same offset. It takes a leader
dying mid-flight under `Quorum`: the record reached some followers, no majority
acknowledged it, and the next leader reuses the offset for its own. Nothing is
wrong with either broker.

Finding it is the easy half and is done: a batch's overlap with what a follower
already holds is compared byte for byte, and a mismatch is
`Divergence::Conflict`. A follower answers with the end of the batch it was sent
rather than its own tail, so the leader cannot resume past records neither side
has compared (#406).

Repairing it needs the two to agree on where their histories part, and offsets
alone do not say — both logs have an offset 100, and being told "they differ at
100" does not say how far back the agreement goes. The generation does say,
because a generation belongs to exactly one leader: the highest generation both
brokers hold is the last one they cannot disagree within, and its end offset on
the leader is the furthest point the follower can keep.

So a follower records where each generation began in its log, as a small map
beside the segments. It then repairs itself, with no exchange at all: a
conflict is droppable when the batch that found it comes from a **newer**
generation than the one this follower last accepted, and the divergence sits at
or after where that older generation began. Both conditions matter — a leader
disagreeing with *itself* is an inconsistency rather than a predecessor's
leftovers, and repairing that would let a leader rewrite its own history.

Anything else halts, as before. That is the same shape Kafka arrived at without
Raft (KIP-101, KIP-279), reached without adding a message: the generation is
already on every batch, and the follower's own history supplies the rest.

Not exchanging it is deliberate. A new internal message kind cannot be sent to
a peer that might not understand it — an undecodable frame ends the stream, and
those streams are long-lived lanes multiplexing every in-flight request, so a
probe costs far more than it learns. Repairing from what a follower already
knows needs no negotiation and no rolling-upgrade order.

What it gives up is the case where the follower's history is absent or does not
reach back far enough. Those halt, which is exactly today's behaviour.

Three things this deliberately does not do:

- **It does not go in the record format.** A generation per record would mean a
  segment format bump, and `SegmentHeader::decode` rejects an unknown version
  outright rather than guess — by design, since a moved field produces a
  plausible mis-parse. The map is derived state that can be rebuilt or absent.
- **It does not truncate below the high-water mark.** Everything there is on a
  majority. A truncation point computed below it is a bug, not a repair, and
  should refuse rather than proceed.
- **It does not make a halted follower repair itself automatically.** Truncating
  a divergent suffix is a decision with a policy attached — how many followers
  may rebuild at once, and at what bandwidth (#424).

### Who may be promoted

A leader reports, on every replication pass, which of its followers hold every
record it does. The leader is the only party that can say: it knows both its own
tail and how far each follower has acknowledged, where a follower knows only
where it is.

The bound is **zero** — a follower is caught up when it is missing nothing. A
bound above zero is a bound on how much a promotion may silently lose, and there
is no honest value for it that is not a policy decision; zero needs no such
decision, and a follower reaches it constantly on a healthy shard.

Reports expire, after the node expiry timeout plus one heartbeat. A report says
a follower *was* caught up; the leader kept writing afterwards, and promoting on
a stale report loses whatever was written since. The window is derived from the
liveness settings rather than configured separately, because it has to outlive
exactly one thing: the time it takes to notice the leader is gone.

A halted follower is never reported, however close its last position was. It has
stopped rather than fallen behind.

**If no replica qualifies, the shard is left unplaced.** The alternative is what
the code used to do: fall back to ordinary scoring and hand the shard to
whichever node scores highest, which may never have seen it. That broker then
serves an empty log at a newer generation while the records sit on replicas that
were not chosen — a failover that *is* the data loss, and one nothing downstream
reports as one. Unavailable is visible and recoverable; silently empty is
neither.

A stream that never asked for replication is unaffected. It has no replicas, so
there was never a copy to prefer, and a fresh placement stays the only thing
available.

Positions are held in the control plane's memory. They change constantly, are
advisory, and expire in about a second, so persisting them would cost a write
per report for data that is worthless by the time it could be read back. The
consequence is that a second control-plane instance starts knowing nothing and
cannot promote until leaders have reported to it — which matters for M7's
multi-instance work and not before.

## Failure model

| Situation | Behaviour |
| --- | --- |
| Leader fails | Lease lapses; a caught-up replica is promoted at `G+1` after the safety interval. Unavailable for at most `L + margin + promotion`. |
| Leader partitioned from the control plane | Keeps serving until its lease expires, then stops. A brief control-plane outage costs nothing; a long one costs availability, not safety. |
| Leader partitioned from followers | `Quorum` writes fail — correctly, the majority is unreachable. `Leader` writes succeed and accumulate loss-window exposure, which the lag metric shows. |
| Control plane unavailable | No new leases are granted. Existing leases run to expiry, then shards go unavailable. Deliberate: granting without a functioning authority is how split-brain happens. |
| Broker suspended past expiry | Refused at the durable-append check on waking. |
| Stale broker after reassignment | Its lease has expired, so it refuses. This is what closes #239 by construction rather than by racing a watch. |

## What is implemented so far

`#111` builds the leadership half:

- **Leases**, renewed by the heartbeat, with the duration taken from the control
  plane's expiry window. Checked at admission (cheap, cached) and again at commit
  (authoritative, reads the clock).
- **Replica sets**, chosen by the same score as leadership so the whole set is a
  deterministic function of the shard and the cluster. `replication_factor`
  defaults to 1, so a stream that never asked for replication is unchanged.
- **Promotion**, gated on a caught-up follower.

The gate matters more than the promotion. A replica that holds no log can be
promoted perfectly well and will then serve an empty shard — the failover *is*
the data loss. So promotion requires a follower within the catch-up bound.

Leaders now report which followers hold everything they do, so the gate has real
input and promotion fires: a lost leader is replaced by a replica that holds the
log, in around a second on a local three-node cluster.

Failover works: a lost leader is replaced by a replica that holds the log, and a
quorum-acknowledged record is readable from the replacement.

Getting there needed five separate fixes, and the common thread is worth
recording. A `Quorum` acknowledgement is a promise about *which brokers hold a
record*, and every one of these was a way for the cluster's own account of that
to drift from the truth:

- the commit order was not rebased when records arrived by replication, so the
  first write a promoted broker accepted never completed
- a stream raised to `Quorum` kept acknowledging on the leader alone until the
  broker restarted, because the live stream state was never updated
- the leader's tail was read before shipping and used after, so a follower level
  with the *old* tail was reported caught up for a record it did not have
- the acknowledgement was released before the control plane was told who held
  the record, so a leader could die having promised a client something the
  cluster could not act on
- promotion chose by placement score rather than by how much a replica held

Both halves of this are implemented (#112). The follower's side is the exchange,
the append rule, and the fence at the storing end. The leader's side keeps one
cursor per follower, ships bounded batches from its own log, and moves the
cursor only on the follower's answer — so a follower that has fallen behind or
been rebuilt is caught up by its own `LogGap`, with no separate negotiation and
nothing kept on disk.

Replication to a follower stops on `LogConflict` or `FencedEpoch`. Neither
converges by retrying: the first means the two logs disagree about bytes both
sides hold, the second that this broker is no longer the leader.

`Stream.consistency` is now wired into the acknowledgement path (#113). A
`Leader` publish is acknowledged once the leader's own durability policy is
satisfied, exactly as before. A `Quorum` publish is held until a majority of the
replica set *of the generation it was written at* holds its records durably.

The majority always counts the leader, so `replication_factor: 1` — the default
— makes `Quorum` behave exactly like `Leader` rather than never acknowledging.
A halted follower counts for nothing: it has stopped rather than fallen behind,
and letting its last position count would make an acknowledgement mean less than
it says.

A wait that runs out is reported as a failure, and the distinction matters: the
records *are* durable on the leader and may yet reach a majority. The broker is
not saying the write failed, it is saying it cannot vouch for it at the level the
stream asked for. `FELIX_PUBLISH_QUORUM_TIMEOUT_MS` sets the budget. Leadership
moving mid-wait ends it the same way, immediately, rather than running the clock
out on an answer that can no longer come.

A control plane that sends a consistency level this broker does not recognise is
refused rather than defaulted. Falling back to `Leader` would serve a stream the
operator asked to be quorum-replicated at the weaker guarantee, silently.

The `Leader` loss window is exported as `felix_broker_replication_lag_records`:
how far the slowest follower is behind, across every shard this broker leads. A
halted follower is excluded from it — it has stopped rather than fallen behind,
and `felix_broker_replication_halted` is where that shows.

The rule and its refusals are in `docs/internal-protocol.md`.

## What this does to the other M5 issues

- **#111 (fenced leadership)** — this is now specific: the epoch is the
  assignment generation, the fence is the lease, and it is enforced at both the
  routing and durable-append boundaries. **#239 is subsumed**: a stale ex-owner is
  a broker without a valid lease, and the same check refuses it.
- **#112 (replicate records)** — append-only shipping, not a consensus log. The
  catch-up path is `read_range` plus sealed-segment checksums. **Done**, both
  halves. Catch-up currently re-reads from the offset the follower names rather
  than verifying whole sealed segments by checksum; that is an optimisation for
  #114, not a change to the rule.
- **#113 (Leader and Quorum)** — the majority is over the replica set *of the
  current generation*, and an acknowledgement from a replica at an older
  generation does not count toward it.
- **#114 (bootstrap followers)** — bounded range reads from the leader; no
  snapshot-install protocol is needed, because the log is the snapshot.
- **#115 (failure injection)** — needs lease expiry, clock skew, and suspension
  as injectable faults, not just process kills. The harness can stop and move a
  broker today (#108); it cannot yet pause one or skew its clock.
- **#116 (semantics)** — must document the `Leader` loss window in terms of the
  lag metric, and state that `Quorum` is majority-including-leader.

## What would overturn this

Stated because a decision without one is an opinion.

**Raft's real advantage is that it needs no clock assumption for safety.** Leases
trade that for a simpler data path. If Felix ever needs to run where drift rate
cannot be bounded — or where process suspension is common enough that `ε` cannot
be chosen — that trade stops being worth it.

The other trigger is the storage layer changing. The argument above rests on
"no committed record is ever rewritten." If that invariant is ever relaxed for another
reason, Raft's cost drops sharply and this should be revisited rather than
inherited.

What is *not* a reason to revisit: throughput. Leases were not chosen because
they are faster in the common case. `Quorum` pays one round trip either way, and
the difference between designs is at failover and in the storage layer, not in
steady-state publish latency.
