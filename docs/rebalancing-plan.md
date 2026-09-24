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
| 0 | Correctness: the write fence, waiting for group state and counters, conditional assignment writes, ending readers on a moved shard | done |
| 1 | Fast switch-over: wake the loops instead of polling, long-poll the assignment feed, run placement when a report arrives, warm the destination, publish routes and servable shards together | done |
| 2 | No refused publishes: hold a publish to a moving shard briefly and forward it, a typed "shard moving" refusal the client retries, the destination not counted toward quorum while it copies | done |
| 3 | Subscriptions follow the shard: a final frame telling the client where to resume, and the client resuming there with no gap or duplicate | done |
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
- **Readers end when their shard moves** (merged, #664). A broker ends a shard's subscriptions
  and cache watches whenever it stops serving it, after what was already queued
  for them.
- **The write fence** (merged, #664). Every write enters a per-shard fence right before it
  claims its place in the log and stays counted until it is durable. The fence
  closes when the broker stops serving the shard, and a write reaching it after
  that is refused with the refusal its path already had. The drained report
  waits for the fence to be closed and empty, so it is exact.
  `FelixShardHandoffNoClaimFence` loses an acknowledged write without it.
- **Group state and counters** (merged, #665). The drained report also waits until the
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

Progress:

- **Control plane** (merged, #662, #665). The change feed long-polls, a
  caught-up or drained report runs placement at once, and the assignment
  carries `successor`.
- **Broker.** The watch long-polls the feed (20 s) and falls back to the sync
  interval against a control plane that answers at once. The watch wakes the
  routing feed; the feed wakes replication after acting on a change, so the
  drained report goes out on the pass right after the fence. A broker named
  as `successor` opens the shard's log and in-memory state while it is still
  copying. The feed publishes routes and the servable set in one swap. The
  destination records `felix_broker_shard_move_seconds` and
  `felix_broker_shard_switchover_seconds`.
  `a_move_switches_over_in_well_under_a_second` measures fence to first
  accepted publish with every broker on the 2 s default interval: about
  90 ms on a local debug build, against 8 s before.

### Phase 2: no refused publishes

- **Hold and forward.** A publish that reaches any broker while its shard is
  between the fence and the cut-over waits, before it is accepted, until the
  broker's routes show the new owner, then goes there. A forwarded publish
  that reaches the old leader, or a new leader whose routes are behind the
  requester's, waits there the same way. A publish now takes its place in the
  write fence when it is routed rather than when it is claimed, so one routed
  just before the fence lands and the move waits for it, instead of being
  refused at its claim. Bounded by `FELIX_SHARD_MOVE_HOLD_MS` (2 s) and
  `FELIX_SHARD_MOVE_HOLD_MAX` (1024 waiting). Only publishes are held; cache,
  counter and group writes are refused through the switch-over as before.
- **A typed refusal.** Past either bound the publish is refused with
  `shard_unavailable`, reason `moving`, retry class `retry` and a
  `retry_after_ms` hint. There is no separate feature bit: `moving` is a new
  reason under an existing code, and a client that does not know it still
  reads the retry class. A client without error codes gets the refusal it
  always did.
- **The destination does not count toward the quorum while it copies.** A
  leader that saw the destination added leaves it out of the quorum mark, and
  ships the copy in 50 ms slices so a pass never waits for the whole copy.
  `FelixShardStagedMove` and `FelixShardStagedMoveSingle` pass;
  `FelixShardStagedMoveVotes`, which counts it, finds a `Quorum` write held on
  the stream's own majority and waiting for the copy.
- A `Quorum` stream placed with one replica is acknowledged by its leader.
  Nothing ships for such a shard, so the quorum wait used to read the missing
  mark as a lost leadership and refuse every publish.

`continuous_publishing_through_a_move_is_never_refused` runs two publishers
through a move and sees no refusal and no record lost or stored twice.

### Phase 3: subscriptions follow the shard

- **The frame.** A broker that stops serving a shard ends its subscriptions
  and cache watches with `shard_moved {subscription_id, resume_from?,
  node_id?, addr?, generation}`, after everything it committed has fanned out.
  Only a client that offered `FEATURE_SHARD_MOVED` (`0x1000`) gets it; any
  other sees the stream end byte for byte as before.
- **An exact `resume_from`.** For a durable stream it is the replay ring's next
  sequence, read under the log lock that each publish captures its fanout list
  under. So every record below it was offered to the subscriber (delivered, or
  dropped by its queue) and none at or above it was. It is a stream position,
  not the subscriber's: a queue that dropped records does not move it. An
  in-memory stream sends none, since its sequence means nothing elsewhere.
- **The client resumes at `max(last delivered + 1, resume_from)`.** A record
  fanned out just before the move can still arrive, putting `last + 1` past
  `resume_from`; records the queue dropped stay dropped. Nothing is repeated
  and nothing is skipped that would otherwise have been delivered.
  `ClusterClient::subscribe` returns a `ClusterSubscription` that does this on
  its own; an in-memory stream resumes at the new owner's tail.
- **Where to go.** `node_id` and `addr` are the successor during a move, else
  the new leader. The frame goes out at the fence, before the cut-over, so the
  client tries the named broker first, then the entry broker (which redirects),
  retrying with backoff until the new owner takes the subscription or the
  reconnect deadline (30 s without one) passes.
- **Cache watches.** `resume_from` is the shard log's tail once the writes in
  flight have landed, and absent if they had not; the watcher then resumes
  after the last change it saw. `CacheWatchItem::ShardMoved` and
  `ShardedCacheWatchItem::ShardMoved` surface it; a sharded watch moves that
  shard's resume offset.
- **Sharded subscriptions** follow each shard and report
  `ShardEvent::ShardMoved`. The Python and TypeScript bindings wrap
  `ClusterClient`, so their subscriptions follow too, and they surface the move
  as `ShardMoved` / `CacheWatchShardMoved` (Python) and `shardMoved` (Node).
- **Two ways the last frames were lost** in the broker's subscriber writer,
  both fixed. Deliveries queued in the same batch as the subscriber's
  unregister were dropped; the writer now writes them first. And the feeder
  forgot the subscriber's connection before its last deliveries were routed,
  so one still in the lane queue found no connection; the lane now forgets it
  after handling the unregister. The final frame rides on the unregister,
  which is sent with backpressure, so a full lane cannot drop it.

Evidence: `routing::subscriptions_follow`
(`a_subscription_follows_its_shard_to_the_new_owner`) reads a durable stream
from its start while a publisher keeps writing and the shard moves, and checks
every offset arrives once, in order, with every acknowledged record. The
conformance runner checks the frame on the wire, offered and not.

## Checking the work

Each correctness fix comes with a test that fails without it: a unit test for
the mechanism, and where the behaviour only shows across processes, a
`felix-cluster` test against real brokers. Changes to the replication, lease or
placement code come with a TLA+ configuration that passes, and where the change
closes a hole, a companion configuration that shows the hole
(`docs/formal/README.md`).
