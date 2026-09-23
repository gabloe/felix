---
title: "Adding, draining and removing brokers"
---

How shards move between brokers, what to do to make them move, and what to
watch while they do. Applies to any deployment; the Kubernetes page has the
chart-specific commands.

## What moves a shard

Two things:

- **A broker over its share.** Placement counts the shards each live broker
  leads. A broker leading more than `ceil(shards / live brokers)` hands shards,
  one at a time, to a broker under its share, until no broker is over. A new
  broker registering is the usual cause: it starts with nothing and takes
  shards from whoever has the most.
- **A draining broker.** `POST /v1/nodes/{id}/drain` marks a broker as
  leaving. Placement moves every shard it leads to brokers that are staying,
  and replaces it as a follower wherever it holds a copy for someone else.
  The broker keeps serving throughout; nothing stops until each shard's
  handoff completes.

A shard is never simply reassigned while its leader is alive, because a
broker that has not seen the log would serve it empty. It is **moved**:

1. **Stage.** The destination joins the shard's replica set and the leader
   ships it the log.
2. **Fence.** Once the destination is caught up, the leader is told to stop
   serving the shard. It lets what it already accepted land, ships the last
   records, and reports that its log has stopped growing.
3. **Cut over.** The destination is named leader at a new generation and
   opens the shard.

Every step is written to the control plane's store, so a control-plane restart
or leader change resumes the move where it was. A destination that dies before
it leads is passed over: another caught-up replica takes the shard, or the old
leader keeps it and the move is chosen again. A leader that dies mid-move is
an ordinary failover.

## Adding a broker

Start it with a new `FELIX_NODE_ID` and the same control-plane URL and
credential the others use. Once it registers it is a placement target, and
the rebalance above starts on the next placement pass
(`FELIX_SHARD_RECONCILE_INTERVAL_MS`, 5 s by default).

Nothing else is required. A cluster that came up unevenly — every shard on
the first broker to register, say — corrects itself the same way.

## Draining a broker

With an operator token carrying `node.manage` on `cluster:*`:

```bash
curl -X POST -H "Authorization: Bearer $OPERATOR_TOKEN" \
  http://controlplane:8443/v1/nodes/broker-2/drain
```

Then wait until it leads nothing:

```bash
curl -s -H "Authorization: Bearer $OPERATOR_TOKEN" \
  "http://controlplane:8443/v1/shard-assignments?leader=broker-2"
# {"items":[]} when it is done
```

Stopping the process before that point is a failover, not a drain: the
shards it still leads fail over to caught-up replicas, and a shard with no
replica waits for the broker to come back.

Cancel a drain by putting the broker back into placement:

```bash
curl -X PATCH -H "Authorization: Bearer $OPERATOR_TOKEN" \
  -H "Content-Type: application/json" -d '{"lifecycle":"live"}' \
  http://controlplane:8443/v1/nodes/broker-2
```

Moves already staged run to completion; nothing further starts, and the
broker may then take shards back if it is under its share.

A broker registers every time it starts, and registering makes it `live`, so
**any restart of a draining broker also cancels its drain**. A rolling restart
in the middle of a drain has to be followed by draining it again.

## Removing a broker

Drain it, then wait until no assignment names it at all — not as leader, and
not as a follower either. Leading nothing is not enough: the drain also
replaces the broker wherever it holds a copy for another leader, and that
only happens while it is `draining`. Once it has left, a follower slot still
naming it stays as it is, and that shard runs one replica short.

```bash
curl -s -H "Authorization: Bearer $OPERATOR_TOKEN" \
  http://controlplane:8443/v1/shard-assignments |
  jq '[.items[] | select(.leader == "broker-2" or (.replicas | index("broker-2")))] | length'
# 0 when it is done
```

Then stop the process. Its shutdown marks it `left` with
`POST /v1/nodes/{id}/deregister`, which is what tells the control plane this
was intentional rather than a crash. The record is kept, so the identity and
its incarnation survive if a broker with that `FELIX_NODE_ID` is started again.
To remove it for good once the broker has stopped:

```bash
curl -X DELETE -H "Authorization: Bearer $OPERATOR_TOKEN" \
  http://controlplane:8443/v1/nodes/broker-2
```

The delete is refused while the broker is `live` or `draining`, and while any
shard names it: the assignment is the only record of where that shard's data
is, and a follower slot naming a node that no longer exists is never replaced.

## Watching a move

`GET /v1/shard-assignments` shows each shard's `state` and, during a move,
its `successor`:

| `state` | `successor` | Meaning |
| --- | --- | --- |
| `assigning` or `active` | absent | Nothing in progress |
| `assigning` or `active` | set | Staged: the successor is catching up |
| `draining` | set | Fenced: the leader is stopping, the cut-over is next |

Metrics on the control plane:

| Metric | Meaning |
| --- | --- |
| `felix_shard_move_steps_total{step}` | steps written: `stage`, `fence`, `cut_over`, `abandon`, `reseat` |
| `felix_shard_moves_waiting` | moves that could not advance in the last pass |

`felix_shard_moves_waiting` sitting above zero for longer than a couple of
placement intervals means a successor is not catching up (check the leader's
`felix_broker_replication_lag_records` and `/replication/halted`), a fenced
leader is not reporting drained (a publish stuck in flight, or the broker
lost its control-plane connection), or a drain is queued behind the move
limit.

## What clients see

Between the fence and the successor opening the shard, publishes to it are
**refused**, not accepted somewhere the successor cannot see. The window is
a few control-plane sync intervals — under a second on a local cluster, a
few seconds with the default 5 s intervals. Subscriptions and cache reads
on the old leader are ended when it releases the shard; a client with a seed
list reconnects and is routed to the new owner.

Every publish acknowledged before or during a move is on the new owner. The
tests behind that claim are in `crates/testing/felix-cluster/tests/rebalance.rs`.

## Tuning

| Setting | Default | Effect |
| --- | --- | --- |
| `FELIX_SHARD_MOVES_MAX_CONCURRENT` | `1` | Moves in flight across the cluster. Each is a full copy of a shard's log; raise it to drain a broker with many shards faster, at the cost of that much more replication traffic at once. `0` holds every move. |
| `FELIX_SHARD_RECONCILE_INTERVAL_MS` | `5000` | How often a move advances a step. |
| `FELIX_CONTROLPLANE_SYNC_INTERVAL_MS` (broker) | `5000` | How quickly brokers see each step. Bounds the refused-publish window. |

A drain of `n` shards at the default policy takes roughly three placement
intervals per shard plus the time to copy each log.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| A drain never finishes; `felix_shard_moves_waiting` is `1` | The successor is not catching up. Look at the leader's `/replication/halted` and replication lag. |
| A drain never finishes; no successor is ever staged | No live broker under its share has capacity (`max_shards`), or `FELIX_SHARD_MOVES_MAX_CONCURRENT` is `0`. |
| Shards moved, then moved back | The drained broker was put back to `live`, or restarted (which registers it `live`), while under its share. Drain it again. |
| A removed broker is still listed in a shard's `replicas` | It was stopped before the drain reseated that follower. Start it again, drain it, and wait for the check above to reach 0. |
| `POST /v1/nodes/{id}/drain` returns 409 | The broker is `down` or `left`, so there is nothing to drain. |
| `DELETE /v1/nodes/{id}` returns 409 | The broker is still running, or a shard still names it. The message says which. |
