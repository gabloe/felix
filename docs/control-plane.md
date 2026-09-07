# Control Plane + RAFT Plan (Draft)

This document outlines how the Felix control plane will use RAFT to manage
cluster metadata and propagate configuration to dataplane brokers. It is not
the data plane and does not carry user payloads.

## Goals
- Consistent cluster metadata (membership, shard placement, config).
- Simple propagation to brokers with clear ownership and failover.
- Keep dataplane hot path free of RAFT overhead.

## Non-Goals
- Replicating stream payloads via RAFT.
- Strong consistency for data reads and writes (handled by dataplane + storage).

## RAFT Scope (Control Plane Only)
The RAFT log stores authoritative metadata:
- Node membership and health state (up/down, drains).
- Stream/shard placement and leadership.
- Cluster-wide config (retention, limits, feature flags).
- Versioned snapshots of metadata state.

All control plane nodes participate in the RAFT group. A single leader accepts
metadata writes and replicates them to followers.

## Propagation to Dataplane
Dataplane brokers do not run RAFT. They consume committed metadata through a
simple API:
- Watch or stream updates (long-poll / streaming RPC).
- Periodic snapshot fetch for resync.

On update:
- Brokers start/stop shard ownership.
- Routing tables update for publish/subscribe paths.
- Health/metrics reflect new assignments.

## Failure + Recovery
- If the control plane leader fails, a new leader is elected by RAFT.
- Brokers reconnect and resume watching from the latest committed version.
- Metadata snapshots allow new control plane nodes to catch up quickly.

## API Sketch (Control Plane)
- `GetSnapshot()` -> full metadata view + version.
- `WatchUpdates(from_version)` -> stream of incremental updates.
- `ReportHealth(node_id, status)` -> node liveness signals. **Implemented** as
  `POST /v1/nodes/{node_id}/heartbeat`; see [Broker liveness](#broker-liveness).

## Broker liveness

A broker registers on boot and then reports health on an interval. Nothing
reports that a broker has *stopped*, so silence is the only signal: a periodic
sweep marks a node `down` once its last heartbeat is older than the timeout.

### `POST /v1/nodes/{node_id}/heartbeat`

Request carries the reporting process's own `incarnation`, from its last
registration. The response returns the node's lifecycle as the cluster sees it,
plus the cadence expected of it:

```json
{ "incarnation": 3 }
{ "node_id": "broker-1", "lifecycle": "live",
  "heartbeat_interval_ms": 5000, "expiry_timeout_ms": 15000 }
```

Four rules, each of which exists for a reason:

- **The recorded time is the control plane's own clock.** A broker that could
  supply it could postpone its own expiry indefinitely.
- **An older `incarnation` is rejected with 409.** A heartbeat delayed past a
  restart belongs to a process the broker has already replaced; counting it
  would report a dead incarnation as live.
- **A heartbeat never revives a `down` node.** It proves a process is running,
  not that it still owns the identity. A broker that reads `"lifecycle": "down"`
  must register again before it is eligible for placement.
- **A heartbeat publishes no change.** Heartbeats arrive per node per interval;
  putting each in the changefeed would evict every real membership change from
  the retention window. Only the lifecycle move that expiry causes is published.

> **Not yet authenticated.** Any caller that can reach this endpoint can report
> health for any `node_id`. Authenticating broker identity is tracked in #126;
> until then it is only safe on a trusted network.

### Broker-side lifecycle

A broker joins a cluster only when `FELIX_NODE_ID` is set. Membership is opt-in
because a single-node broker has no cluster to join, and registering one would
put a node in the catalog that placement would then try to use.

| Env | Required | Meaning |
| --- | --- | --- |
| `FELIX_NODE_ID` | opt-in | Stable across restarts. This is the identity, not the process. |
| `FELIX_NODE_ADVERTISE_ADDR` | with `FELIX_NODE_ID` | `host:port` peers reach this broker on. Not the bind address: a broker bound to `0.0.0.0` has to advertise something routable. |
| `FELIX_CONTROLPLANE_URL` | with `FELIX_NODE_ID` | Where to register. |
| `FELIX_REGION_ID` | no | Defaults to `local`. |

Startup fails, rather than defaulting, when `FELIX_NODE_ID` is set without an
advertised address or a control-plane URL, or when the address does not parse.
A broker that guessed its own address would register something unreachable, and
the failure would surface later as peers unable to connect to a node the catalog
says is live.

The sequence:

1. **Register after the broker can serve.** Advertising a node placement may
   route to before it can answer is worse than advertising it a moment late, so
   registration waits for the initial catalog sync when readiness is gated on it.
2. **Heartbeat** on the interval the control plane returns, with bounded
   exponential backoff and jitter. A failure is visible
   (`felix_broker_heartbeat_failures_total`) but never fatal: a control plane
   that is briefly unreachable must not take down a broker that is serving fine.
3. **Drain, then deregister** on SIGTERM, before connections are drained, so
   nothing new is placed here while in-flight work finishes.

A registration refused with a 4xx — a duplicate advertised address, say — stops
the broker with a clear error instead of retrying. A wrong identity stays wrong,
and retrying only hides the misconfiguration. An unreachable control plane is
retried, because it may simply be starting.

This is what separates a graceful shutdown from a crash: a broker that
deregisters is `left`, and one that simply stops is found `down` by expiry. Both
remove it from placement, but only the first is intentional.

### Operator endpoints

`GET /v1/nodes` and `GET /v1/nodes/{node_id}` list registered brokers and
explain each one's placement standing:

```json
{ "node": { "node_id": "broker-1", "spec": { ... }, "status": { ... } },
  "placement": { "eligible": false, "heartbeat_age_ms": 41200,
                 "reasons": ["last heartbeat was 41200ms ago, past the 15000ms timeout; expiry has not run yet"] } }
```

`placement.reasons` exists because "this broker is registered but shards are not
landing on it" is otherwise answered by reading a lifecycle string and doing
heartbeat arithmetic by hand. It reports a stale heartbeat separately from the
lifecycle, so the window between a heartbeat lapsing and the sweep noticing —
where a node still reads `live` — is visible rather than inferred.

Filters intersect, and an absent filter matches everything:

```
GET /v1/nodes?lifecycle=live&region=us-west-2&label=rack%3Da1&label=tier%3Dhot
```

Repeating `label` requires all of them. The listing is unpaginated, like the
other listings in this API: a cluster has brokers in the tens, and a cursor no
caller needs is a cursor every caller has to handle.

#### Authorization

Both read endpoints require `node.view:cluster:*` in a Felix bearer token. The
tenant comes from the token's own `tid` claim rather than a path segment,
because the cluster is not a tenant resource; that claim only selects which
tenant's signing keys to verify against, exactly as `kid` selects a key without
conferring one.

`cluster:*` sits outside the tenant hierarchy on purpose. No tenant scope
contains it, and `validate_new_rule_allowed` admits a policy only when its
object is already inside the caller's scope — so a tenant admin cannot grant
themselves cluster access, and nothing in the bootstrap seed grants it either.
The converse also holds: cluster scope confers nothing inside a tenant, so it is
not a backdoor into tenant data.

Note that the registration, heartbeat, drain, and deregister endpoints above are
**not** authenticated yet (#126). Only the operator read endpoints are.

### Shard ownership

`GET /v1/shard-assignments` lists which broker leads each shard, and
`?leader=<node_id>` narrows it to one broker. Requires the same
`node.view:cluster:*` as the node listing, because ownership and membership are
the same view of the cluster.

There is **at most one assignment per (stream, shard)** — that is the primary
key, and it is the invariant placement depends on.

`generation` increments on every write and is owned by the store, never the
caller. A broker reports status against the generation it read, so a broker that
was slow, partitioned, or restarted cannot resurrect an ownership decision that
placement has already replaced.

States are `assigning` (placement decided, the leader has not confirmed),
`active` (the leader is serving), and `draining` (ownership is moving). Only a
shard someone is actually serving can drain, and a drained shard does not return
to the same leader — placement writes a new assignment at a new generation.

#### How shards get placed

Rendezvous hashing: every eligible node is scored against the shard, and the
highest wins. Chosen over a consistent-hash ring because it needs no ring state
and no virtual-node tuning, distributes better at the handful-of-brokers scale a
cluster starts at, and because removing a node moves only the shards that node
held.

Placement is a **pure function of a metadata snapshot**, so two control-plane
instances reading the same rows reach the same decision without coordinating.
It is independent of the order streams, nodes, or existing assignments arrive
in.

The hash is written out rather than taken from `DefaultHasher`, whose seeding is
not part of its contract — a placement decision that changed with the Rust
version, or differed between two instances, would be silently catastrophic. The
shard key and the node id are hashed independently and then mixed, because a
single pass over the concatenation is badly behaved at the size a cluster
actually is: over 300 shards on four nodes it put 43 on one node and 94 on
another, against a spread within 8% of even for the split form.

Three deliberate omissions in v1:

- **No online rebalancing.** An assignment whose leader is still live is kept,
  however uneven that leaves the cluster. Moving a shard costs a log handoff
  that does not exist yet.
- **`NodeCapacity::weight` is ignored.** Weighted rendezvous needs a logarithm,
  and floating point that must agree bit-for-bit across every instance is a bad
  foundation for a decision that has to be identical everywhere. `max_shards` is
  honoured, as a hard cap.
- **No region or label affinity.** Streams carry no placement constraints to
  filter on yet.

Reconciliation is idempotent: a pass over a settled cluster writes nothing, so
running it on a timer does not churn rows or flood the changefeed. A shard with
no eligible leader is left unplaced and logged with the reason — an empty
cluster and a full one are reported differently, because they need different
fixes.

| Setting | Env | Default |
| --- | --- | --- |
| `node_liveness.shard_reconcile_interval_ms` | `FELIX_SHARD_RECONCILE_INTERVAL_MS` | 5000 |

#### How brokers follow ownership

`GET /v1/shard-assignments/snapshot` returns every current assignment plus a
`next_seq`; `GET /v1/shard-assignments/changes?since=N` returns the changes from
there. A broker applies the snapshot and then polls, and the two together
describe every committed change exactly once — the snapshot is read at a
consistent point and `next_seq` is the log position at that same point.

Three things can break that, and a broker has to notice each rather than carry
on from a checkpoint the control plane can no longer honour:

| Signal | Meaning | Response |
| --- | --- | --- |
| first returned `seq` > `since` | changes between were evicted from the window | re-snapshot |
| empty page but `next_seq` > `since` | the whole span was evicted, not empty | re-snapshot |
| `next_seq` < `since` | the sequence reset under us — a control plane restarted onto a store that does not persist it | re-snapshot |

The second is the subtle one: an empty page is only safe when the log has not
moved. Treating it as "nothing new" whenever it is empty silently skips
everything that was evicted.

Changes are applied by generation, not arrival: a change carrying a generation
at or below the one already held is dropped. That is what makes duplicate
delivery harmless and stops a reordered or retried poll rolling ownership
backwards. Falling behind is not an error — a broker that was away long enough
resnapshots and carries on.

Both endpoints require `node.view:cluster:*`.

#### Referential integrity

Two references, two different policies, chosen rather than inherited:

- **Stream**: a foreign key with `ON DELETE CASCADE`. Deleting a stream removes
  its assignments, because the shards no longer exist and keeping ownership
  records for them leaves placement chasing ghosts.
- **Node**: deliberately **no** foreign key. Deleting a node that still leads a
  shard is **rejected**, not cascaded. A cascade would delete the only record of
  where that shard's data lives, turning an operator's tidy-up into silent data
  orphaning. Reassign the shard first.

Shard numbers are validated against the stream's `shards` count on every write.
Streams cannot currently be resized — `StreamPatchRequest` has no `shards` field
— so no assignment can be orphaned by a shrink. When resize arrives, assignments
above the new bound have to be removed in the same transaction.

### Metrics

Every label below is bounded. Lifecycle has four values, region has as many as
an operator configures, and failure kinds have two. **No metric carries
`node_id`** — a fleet view is `felix_node_count`, and a broker's own view is its
own series.

Control plane:

| Metric | Meaning |
| --- | --- |
| `felix_node_count{lifecycle,region}` | the fleet census, refreshed from the same store read the node listing serves |
| `felix_node_transitions_total{from,to}` | lifecycle moves; `live -> down` is failure, `draining -> left` is a deploy |
| `felix_node_registrations_total{outcome}` | `new`, `restart`, or `rejected` |
| `felix_node_expiry_total` | nodes the sweep marked down |
| `felix_node_expiry_failures_total` | sweeps that failed; non-zero means liveness is stale |
| `felix_node_changes_total{op}` | membership changes published to the changefeed |
| `felix_shard_assignment_changes_total{op}` | shard ownership changes: `assigned`, `updated`, `unassigned` |
| `felix_shards_placed_total` | shards given a leader by reconciliation |
| `felix_shards_unplaceable` | shards with no eligible leader right now; non-zero needs attention |
| `felix_shard_reconcile_failures_total` | passes that could not read the catalog at all |

Broker side:

| Metric | Meaning |
| --- | --- |
| `felix_broker_shard_watch_checkpoint` | log position the watch has reached; compare against the control plane to see lag |
| `felix_broker_shard_assignments` | assignments this broker currently believes in |
| `felix_broker_shard_changes_applied_total` | ownership changes applied |
| `felix_broker_shard_changes_stale_total` | changes dropped for a stale generation; small numbers are routine, and are what makes duplicate delivery harmless |
| `felix_broker_shard_watch_resyncs_total{reason}` | forced resnapshots: `gap_in_history` or `sequence_reset` |
| `felix_broker_shard_watch_failures_total` | polls that failed outright |

Broker:

| Metric | Meaning |
| --- | --- |
| `felix_broker_heartbeat_age_seconds` | seconds since this broker's last accepted heartbeat |
| `felix_broker_heartbeats_total` | heartbeats the control plane accepted |
| `felix_broker_heartbeat_failures_total{kind}` | `rejected` or `unavailable` |
| `felix_broker_membership_live` | 1 while the cluster considers this broker placeable |
| `felix_broker_membership_registrations_total{outcome}` | `registered`, `rejected`, or `unavailable` |

The `rejected` / `unavailable` split is the one worth keeping. `rejected` means
the control plane answered and said no — a duplicate address, a superseded
incarnation — and retrying never fixes it. `unavailable` means nothing answered.
Collapsed into one counter, a misconfigured broker looks exactly like a flaky
network.

#### Suggested alerts

| Condition | Why |
| --- | --- |
| `felix_broker_heartbeat_age_seconds > expiry_timeout_ms / 1000` | the earliest point a broker knows it is about to be declared down, and it fires even when the control plane is what is unreachable |
| `increase(felix_node_transitions_total{to="down"}[5m]) > 0` | a broker failed; `to="left"` over the same window is a deploy and is not the same alert |
| `increase(felix_broker_membership_registrations_total{outcome="rejected"}[15m]) > 0` | a broker is misconfigured and will never join; retrying will not clear it |
| `felix_node_expiry_failures_total` rising | liveness is not being evaluated, so the census is stale in a way the census cannot show |
| `sum(felix_node_count{lifecycle="live"}) < expected` | the fleet is smaller than intended, whatever the cause |

Alert on `felix_broker_membership_live == 0` only where a broker is expected to
be a member; it is legitimately 0 during a drain.

### Configuration

| Setting | Env | Default |
| --- | --- | --- |
| `node_liveness.heartbeat_interval_ms` | `FELIX_NODE_HEARTBEAT_INTERVAL_MS` | 5000 |
| `node_liveness.expiry_timeout_ms` | `FELIX_NODE_EXPIRY_TIMEOUT_MS` | 15000 |
| `node_liveness.sweep_interval_ms` | `FELIX_NODE_EXPIRY_SWEEP_INTERVAL_MS` | 2000 |

The timeout is three intervals: one lost heartbeat is a hiccup, three is a
pattern. Startup fails if `expiry_timeout_ms` is not greater than
`heartbeat_interval_ms` — a timeout at or below the interval expires brokers
that are heartbeating exactly as told to.

Running several control-plane instances is safe. Each node is claimed by exactly
one sweep and only that instance publishes the change, so duplicate sweeps cost
a query and produce no duplicate events.

## Evolution Plan
1) Control plane RAFT for membership + placement only.
2) Add richer metadata (tenants, quotas, retention).
3) Introduce safety checks for rebalancing and drains.

## Kubernetes Deployment Model
### Pod Layout
- Control plane runs as a dedicated StatefulSet with stable identities.
- Dataplane brokers run as a separate StatefulSet (or Deployment for stateless
  dev mode).
- Object store access is configured per broker (later), not in the control plane.

### Storage
- Control plane uses a PVC per pod for RAFT logs and snapshots.
- Dataplane brokers use PVCs for durable log segments (when enabled).

### Services
- Headless Service for control plane peer discovery (RAFT).
- ClusterIP Service for control plane client API (watch/snapshot/health).
- Separate Service for broker QUIC ingress.

### Scheduling + Ops
- Control plane replicas: 3 or 5 (odd count for quorum).
- Use PodDisruptionBudgets to preserve quorum and shard ownership.
- Prefer anti-affinity for control plane pods to avoid single-node failure.
- Configure liveness/readiness probes on control plane API endpoints.

## Open Questions
- Snapshot cadence and maximum delta size.
- Placement heuristics and rebalancing triggers.
- Authentication/authorization for control plane APIs.
