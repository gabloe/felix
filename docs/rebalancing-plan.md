# Rebalancing: from partial to done

The status table lists online rebalancing as partial. This is the plan for
finishing it: what was wrong with moves beyond the known gaps, and the order the
work lands in. Each phase is one pull request and is useful on its own.

## How a move works

The control plane moves a shard in steps (`cluster/placement/moves.rs`):

1. **Stage.** The destination is added to the shard's replicas and the leader
   starts shipping it the log.
2. **Fence.** Once the destination has caught up, the assignment goes
   `draining`. The old leader stops serving the shard but keeps shipping.
3. **Drained.** The old leader reports that the shard's log has stopped growing.
4. **Cut over.** The control plane names the destination leader at a new
   generation.

Moves start when a broker is drained (`POST /v1/nodes/{id}/drain`) or when a
broker leads more than its share of shards.

Every assignment write bumps the shard's generation, including the fence. So the
old leader sees a fence as a new, draining generation of a shard it already
holds, not as a release of the current one. Anything that has to happen when a
broker stops serving a shard must handle both paths.

## What was wrong beyond the known gaps

The status row named four gaps: publishes refused during the switch, subscribers
ended rather than migrated, one move at a time as the only pacing, and no limit
on the copy's bandwidth. Reading the code turned up correctness problems that
come first.

- **An acknowledged write could be lost.** Admission checks that the broker
  serves the shard, but the write then waits in a queue and was committed
  without checking again. A write queued across the fence could commit on the
  old leader after it reported drained, and the new owner never received it.
  Cache writes, counter adds, consumer-group writes and forwarded writes had
  the same gap. "Drained" was a guess: the log tail holding still for two
  passes.
- **Group positions, dead letters and counters were not waited for.** They are
  copied to the new owner after the drained report, and nothing waits for the
  copy. A dead-letter entry or counter add that had not arrived was lost.
- **Two control-plane instances could undo each other's steps.** Every instance
  runs placement, and assignment writes did not check the generation they were
  planned from. A stale fence could hand a shard back to its old leader after
  the new one had acknowledged writes.
- **Readers on the old leader went silent.** Nothing ended a moved shard's
  subscriptions or cache watches on a broker that stayed up, so a client had no
  reason to look for the new owner.
- **Follower replacements were not counted against the move limit.**
- **A move may never reach the fence under steady writes**, because the
  destination has to be exactly level with the tail.
- **The staged destination counts toward the quorum** while it is still
  copying, so a `Quorum` publish can wait for the whole copy.

The switch-over also takes seconds rather than milliseconds, because three
broker loops poll the control plane every 2 s and placement runs every 5 s.

## Phases

| Phase | What it does | Status |
| --- | --- | --- |
| 0 | Correctness: the write fence, waiting for group state and counters, conditional assignment writes, ending readers on a moved shard | in progress |
| 1 | Fast switch-over: wake the loops instead of polling, long-poll the assignment feed, run placement when a report arrives, warm the destination, publish routes and servable shards together | planned |
| 2 | No refused publishes: hold a publish to a moving shard briefly and forward it, a typed "shard moving" refusal the client retries, the destination not counted toward quorum while it copies | planned |
| 3 | Subscriptions follow the shard: a final frame telling the client where to resume, and the client resuming there with no gap or duplicate | planned |
| 4 | Pacing: count every copy in flight, a per-node limit, drains before rebalancing, start the fence within a lag threshold, a move timeout, a bandwidth limit on copies | planned |
| 5 | Operator controls: list, start, cancel and pause moves over the API and a CLI | planned |
| 6 | Idempotent producers keep their sequences across a planned move | planned |
| 7 | Docs and the status row | planned |

Load-aware placement (moving shards by load rather than by count) is separate
work with its own status row.

### Phase 0: correctness

- **Conditional assignment writes** (merged, #660). A placement write lands only
  if the assignment is still at the generation it was planned from, in memory,
  Postgres and Raft. `FelixShardStalePlanner` and `FelixShardStalePromotion`
  show the race without the check.
- **Readers end when their shard moves.** A broker ends a shard's subscriptions
  and cache watches whenever it stops serving it, after what was already queued
  for them.
- **The write fence.** Every write enters a per-shard fence right before it
  claims its place in the log and stays counted until it is durable. The fence
  closes when the broker stops serving the shard, and a write reaching it after
  that is refused with the refusal its path already had. The drained report
  waits for the fence to be closed and empty, so it is exact.
  `FelixShardHandoffNoClaimFence` loses an acknowledged write without it.
- **Group state and counters.** The drained report also waits until the
  group-cursor, dead-letter and counter logs are level on the destination.

### Phase 1: fast switch-over

- The shard watch wakes the routing feed, and the feed wakes the replication
  driver, instead of each waiting for its next tick.
- The assignment change feed long-polls (`wait_ms`), so a broker hears about a
  fence or cut-over as soon as it is written. Old control planes ignore the
  parameter.
- A drained report, or one showing the destination caught up, wakes placement.
- A broker named as a shard's destination prepares to serve it while it is
  still copying, so taking over is quick.
- Routes and the set of servable shards are published together, so there is no
  moment where one is updated and the other is not.
- Metrics for how long a move takes and how long its switch-over lasts.

The target is a switch-over well under a second on a local cluster.

## Checking the work

Each correctness fix comes with a test that fails without it: a unit test for
the mechanism, and where the behaviour only shows across processes, a
`felix-cluster` test against real brokers. Changes to the replication, lease or
placement code come with a TLA+ configuration that passes, and where the change
closes a hole, a companion configuration that shows the hole
(`docs/formal/README.md`).
