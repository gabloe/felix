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

> **This section is the original sketch; the real thing now exists.** The
> control plane runs one of two production-shaped backends: a stateless REST
> service over a highly available Postgres (what that database must provide
> is [ha-postgres.md](ha-postgres.md)), or a Raft group embedded in the
> instances themselves, with no external database. M13 is complete, chaos
> pass included: three instances survive rolling restarts, a SIGKILLed
> leader, a frozen leader, and a wiped volume without losing an
> acknowledged write. The implemented design, which differs from the
> sketch below in the ways that mattered, is
> [metadata-raft-design.md](metadata-raft-design.md); operator-facing usage
> is on the docs-site Metadata Raft page. This sketch is kept only as the
> record of where the design started.

The RAFT log would store authoritative metadata:
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

Requires `node.manage` over the node being reported — see
[Authorizing membership writes](#authorizing-membership-writes).

### Broker-side lifecycle

A broker joins a cluster only when `FELIX_NODE_ID` is set. Membership is opt-in
because a single-node broker has no cluster to join, and registering one would
put a node in the catalog that placement would then try to use.

| Env | Required | Meaning |
| --- | --- | --- |
| `FELIX_NODE_ID` | opt-in | Stable across restarts. This is the identity, not the process. |
| `FELIX_NODE_ADVERTISE_ADDR` | with `FELIX_NODE_ID` | `host:port` peers reach this broker's **internal** listener on. Not the bind address: a broker bound to `0.0.0.0` has to advertise something routable. |
| `FELIX_CONTROLPLANE_URL` | with `FELIX_NODE_ID` | Where to register. |
| `FELIX_REGION_ID` | no | Defaults to `local`. |
| `FELIX_INTERNAL_BIND` | no | Where the internal listener binds. Defaults to `0.0.0.0:5001`. Must not share a port with `FELIX_QUIC_BIND`. |
| `FELIX_INTERNAL_TLS_CERT`, `FELIX_INTERNAL_TLS_KEY`, `FELIX_INTERNAL_TLS_CA` | recommended | Peer mTLS: this broker's certificate (its DNS name must be `FELIX_NODE_ID`), its key, and the CA every peer must chain to. All three or none; without them the peer link is encrypted but unauthenticated. See `docs/internal-protocol.md`. |

The advertised address is the internal listener's, not the client-facing one:
peers are the only thing that reads it. A broker that advertises a port it does
not bind logs a warning at startup rather than refusing, because a deployment
may legitimately map ports.

A broker also polls `/v1/nodes` on the same interval to build its address book.
Shard assignments name an owner by node id, and only the catalog turns that into
an address to forward to — without it a broker knows a shard is owned elsewhere
and cannot reach it. A refresh that fails keeps the previous catalog rather than
emptying it, because a control-plane blip must not turn a healthy cluster into
one that refuses every remote publish. `felix_broker_node_catalog_nodes` reading
zero while assignments are known is exactly that failure.

A three-node cluster can be run locally with
[the cluster harness](cluster-harness.md).

**Ownership is eventually consistent at the broker.** A shard that moves is not
known to its old owner until that broker's next sync, and until then it serves
publishes for the shard locally — those records land in its log and no
subscriber on the new owner sees them. There is no fencing today, and the
generation check in the forwarding protocol does not cover this: a stale ex-owner
never forwards, so nothing compares generations. A publish acknowledged in that
window is durably written to a broker nobody reads it from, so this is write loss
rather than a delay — tracked in
[#239](https://github.com/gabloe/felix/issues/239), and closed by the leases in
[the replication design](replication-design.md): a broker without a valid lease
refuses, rather than racing its watch. What holds today is convergence within the
sync interval. See
[the stale-ownership window](cluster-harness.md#a-gap-the-suite-does-not-paper-over-the-stale-ownership-window).

The internal transport is described in
[the broker-internal forwarding protocol](internal-protocol.md); its remaining
settings (`FELIX_INTERNAL_CONNS_PER_PEER`, `FELIX_INTERNAL_STREAMS_PER_CONN`,
`FELIX_INTERNAL_MAX_INFLIGHT`, `FELIX_INTERNAL_REQUEST_TIMEOUT_MS`,
`FELIX_INTERNAL_IDLE_TIMEOUT_MS`, `FELIX_INTERNAL_RECONNECT_BASE_MS`,
`FELIX_INTERNAL_RECONNECT_MAX_MS`, `FELIX_INTERNAL_HANDSHAKE_TIMEOUT_MS`) all
have working defaults.

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

#### Authorizing membership writes

Registration, heartbeat, drain, deregistration and `PATCH /v1/nodes/{id}` all
require `node.manage` over the node being changed (`DELETE /v1/nodes/{id}`
requires it on `cluster:*`, so a broker cannot remove its own record):

| Object | Who holds it | Can change |
| --- | --- | --- |
| `node:{node_id}` | a broker, for its own identity | that node only |
| `cluster:*` | an operator managing the fleet | every node |

**A node is an RBAC object, not a field the caller asserts.** A broker
presenting `node.manage:node:broker-a` for `broker-b` is refused, so one broker
cannot register, drain, deregister, or report health for another. Registration
authorises the identity in the *request body*, so a broker cannot claim a name
its credential does not cover.

`node:*` is deliberately rejected: it would be `cluster:*` under a second name,
and two spellings for one scope is how a policy review misses one.

A node scope is an island in the same way `cluster:*` is. No tenant scope
contains it, so a tenant admin cannot grant themselves one; and it confers
nothing inside a tenant. `node.view` does not imply `node.manage` — reading the
fleet is not permission to change it.

##### Giving a broker its credential

A broker with `FELIX_NODE_ID` set **must** have a credential, or it refuses to
start. Starting one that will fail every control-plane call on a loop is worse
than refusing.

| Env | Meaning |
| --- | --- |
| `FELIX_NODE_TOKEN` | the token itself |
| `FELIX_NODE_TOKEN_FILE` | a path to read it from, for a mounted secret |

The file form exists so a credential need not sit in an environment variable
visible in a process listing. Whitespace is trimmed, and a blank value is
treated as no credential rather than as an empty one.

The same credential authenticates the shard-assignment watch and the metadata
feeds the broker seeds from — tenants, namespaces, streams and caches — which
are cluster metadata by the same argument. That makes it a broker's credential
rather than a *member's*: a standalone broker with `FELIX_CONTROLPLANE_URL` and
no `FELIX_NODE_ID` still presents it, and without one its sync is refused on
every poll. It is told so once, at startup, and still starts, because the JWKS
fetch that verifies client tokens is unauthenticated and keeps working.

### Metadata API authorization

Every endpoint that reads or changes the catalog takes a Felix bearer token.
The check runs before the existence check, so a caller without a credential
learns nothing from a 404: a tenant that does not exist has no signing keys,
and a request against it answers `401` whatever the token says.

| Endpoint | Requires | Minted from |
| --- | --- | --- |
| `GET/POST /v1/tenants`, `DELETE /v1/tenants/{id}` | `tenant.manage:cluster:*` | any tenant; the `tid` only picks the keys |
| `/v1/tenants/{t}/namespaces[/{ns}]` | `ns.manage` over `namespace:{t}/{ns}` | tenant `t` |
| `/v1/tenants/{t}/namespaces/{ns}/streams[/{s}]` | `stream.manage` over `stream:{t}/{ns}/{s}` | tenant `t` |
| `/v1/tenants/{t}/namespaces/{ns}/caches[/{c}]` | `cache.manage` over `cache:{t}/{ns}/{c}` | tenant `t` |
| `/v1/{tenants,namespaces,streams,caches}/{snapshot,changes}` | `node.view:cluster:*` | any tenant |

A listing returns only what the caller could manage, so a namespace admin sees
their namespace and not the tenant's layout. A tenant admin's token carries the
manage actions already: token exchange expands `tenant.manage:tenant:{t}` to
`ns.manage:namespace:{t}/*`, `stream.manage:stream:{t}/*/*` and
`cache.manage:cache:{t}/*/*`.

**The catalog is the operator's.** Which tenants exist is cluster metadata, not
something any one tenant owns, so creating, listing and deleting tenants sits
in cluster scope alongside membership — and deleting is operator-only even for
the tenant's own admin, since it takes the signing keys with it. Cluster scope
still confers nothing *inside* a tenant: an operator who can create `t1` cannot
read its streams without a `t1` token.

**Day 0** is the bootstrap listener, which is the only thing that works before
any Felix token exists. An operator credential comes out of it the same way a
broker's does: bootstrap an operator tenant with a policy granting
`tenant.manage`, `node.view` and `node.manage` over `cluster:*` to a role,
assign the operator principal to it, and exchange an IdP token. No tenant admin
can write those rules afterwards, because no tenant scope contains `cluster:*`.

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
`active` (the leader is serving), and `draining` (the leader has been told to
stop serving at this generation so the shard can move). A drained shard leaves
only through a fresh `assigning` at a new generation, never back to `active`
where it stands. `successor` names the node a shard is moving to while a move
is in progress; it is always one of `replicas`, and absent otherwise. See
[Moving a shard](#moving-a-shard).

#### Replica reports

`POST /v1/nodes/{node_id}/replica-status` is how a shard's leader tells the
control plane which replicas hold its log and how far each has got. Promotion
is gated on it: a lost leader is replaced only by a replica reported caught up,
and among those by the one reported furthest ahead. A report may also carry
`drained: true`, the leader's word that it has stopped serving the shard at
that generation and its log will not grow — the fence of a planned move. Requires `node.manage` over
the reporting node, and the node must lead the shard it reports on; a report
naming a generation ahead of the assignment is refused, since one claiming
`u64::MAX` would otherwise block every real report after it.

**Reports live in the store**, keyed like the assignment they describe and
cascading from it, not in the memory of the instance that received them. With
several instances over one Postgres, the instance a report reaches and the
instance that runs placement need not be the same process, and a report only
one of them had seen was a position no promoter could use — a `Quorum`
acknowledgement released on it could not be made good at failover. Under Raft
the report is a log command, restamped with the leader's clock as a heartbeat
is. The latest report replaces the previous one; one from an older generation
is dropped, because leadership moved on and it describes a replica set that
may no longer exist.

**One clock on both sides.** A report is stamped with the store's clock and its
freshness is judged, by whichever instance plans, against the store's clock —
`clock_timestamp()` under Postgres, the leader's process clock under Raft. A
report is believed for twice the expiry timeout plus one heartbeat interval:
long enough to outlive the detection of the leader that made it, since a
report that expired sooner would leave a shard unpromotable forever, and no
longer, since a stale one is how a failover loses the records written after
it. Nothing about a report survives the assignment it describes: deleting the
assignment deletes the report, so a shard removed and recreated does not
inherit the old one's promotability.

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

Two deliberate omissions:

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
| `max_concurrent_shard_moves` | `FELIX_SHARD_MOVES_MAX_CONCURRENT` | 1 |

#### Moving a shard

A shard whose leader is alive is never reassigned outright: a node that has
not seen the log would serve it empty. It is **moved**, in three assignment
writes, each at a new generation and each resumable from the store by
whichever instance runs the next pass:

1. **Stage.** The destination joins `replicas` and is recorded as
   `successor`. The leader ships it the log like any other follower.
2. **Fence.** Once the leader's replica report lists the successor as caught
   up, the assignment goes `draining`. The leader stops admitting writes at
   that generation, lets what it already accepted land, keeps shipping, and
   reports `drained: true` once its log has held still with nothing in
   flight — with `caught_up` measured against that final tail.
3. **Cut over.** On a drained report at the fenced generation, the successor
   is named leader in a fresh `assigning` assignment. The old leader keeps a
   follower's seat if it is staying and the replication factor wants one;
   a draining node is dropped from the set.

Two things start a move. A **draining node** (`POST /v1/nodes/{id}/drain`)
gives up everything it leads, one shard per free move slot, preferring a
caught-up live replica under its share as the destination; a follower it
holds for some other shard is reseated onto a node that is staying. A
**live node over its share** of leadership — more than `ceil(shards /
live nodes)` — gives a shard to a node under its share. Moves in flight are
counted as complete for the share calculation, so a node never stages more
moves than it needs, and only over-to-under moves are made, so the process
converges without trading shards back and forth. A follower that is merely
down is left where it is: a rolling restart would otherwise copy every shard
once per node.

A move stops safely at every step. A destination that stops being live is
dropped from the move (`abandon`) or, after the fence, passed over for
another caught-up replica or the old leader itself, which takes the shard
back at a new generation. A leader that dies mid-move is a failover, where
the successor is a candidate like any other replica. An ephemeral stream has
no log to hand off and is reassigned as it always was.

Between the fence and the new owner opening, publishes to the shard are
refused rather than accepted somewhere the successor cannot see — the client
sees an error, never a silent drop. Locally that window is under a second;
in a deployment it is a few control-plane sync intervals.

`max_concurrent_shard_moves` bounds moves in flight across the cluster,
one by default, because each is a full copy of a shard's log. `0` starts
nothing: a drain waits and an imbalance stays, both visibly.

| Metric | Meaning |
| --- | --- |
| `felix_shard_move_steps_total{step}` | move steps written: `stage`, `fence`, `cut_over`, `abandon`, `reseat` |
| `felix_shard_moves_waiting` | moves that could not advance in the last pass — a destination not catching up, a leader not reporting drained, or the move limit holding a drain back |

The fence and the broker's side of it are described in
[replication-design.md](replication-design.md#planned-handoff).

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

#### What a broker does about ownership

The watch says who *should* own each shard. What a broker has actually done
about it is a separate state, because becoming an owner is not instant: the
durable log has to be opened and recovered first, and a broker serving writes in
that window would acknowledge records it cannot yet persist.

So ownership is two-phase in both directions:

```
unassigned -> opening -> active -> draining -> closed
                  |
                  +-> failed
```

A shard serves **only** in `active`, and only at the generation the control
plane currently names. `opening` exists precisely so an assignment arriving is
not the same event as the shard becoming servable.

- **A new generation reopens.** The control plane moved the shard away and back;
  local state is re-established rather than assumed.
- **An older generation is ignored.** A duplicate delivery, a reordered poll, or
  a snapshot replay is not news, so repeated events cost nothing.
- **An open that finishes after a reassignment does not activate.** The shard
  moved on while it was being recovered.
- **A failed open is not retried by the same assignment arriving again.** Every
  poll re-delivers it, and retrying each time buries the failure. A new
  generation is what retries.
- **A vanished assignment releases the shard**, exactly like one reassigned
  away. A snapshot replaces the whole picture, so only the full set can say what
  disappeared.
- **A failed flush still gives up the shard.** Ownership has moved regardless,
  and continuing to serve would be worse than an unflushed tail — but the error
  is logged loudly.

| Metric | Meaning |
| --- | --- |
| `felix_broker_shard_phase{phase}` | shards this broker holds, by phase |
| `felix_broker_shard_transitions_total{from,to}` | local ownership moves |
| `felix_broker_shard_stale_events_total` | events ignored for an old generation |
| `felix_broker_shard_open_failures_total` | non-zero means a shard the cluster believes is placed here is not being served |

#### Resolving a shard to a node

`felix-router` answers *where* a shard lives; the region allowlist is meant to
answer *whether* traffic may cross to it, though nothing consults it yet
(#616). Keeping those separate matters: folding them
together makes a placement decision look like a policy decision, and they fail
for different reasons and need different fixes.

Routes are published as an immutable snapshot and swapped in whole. A lookup is
an atomic load against a table nobody can mutate underneath it, so the publish
path never takes a lock a writer can hold and never makes a control-plane call.
Updates replace the table entirely rather than patching shards, because a
partial update would let a reader see half a rebalance.

Every outcome is explicit — there is deliberately no "not sure, handle it
locally", because that is a broker writing a shard it does not own:

| Outcome | Meaning |
| --- | --- |
| `Local` | this node leads the shard; handle it here |
| `Remote` | another node leads it, with the address to reach it. M4 forwards; until then it is a typed refusal, distinguishable from failure |
| `Stale` | the caller knows a newer generation than this router does. Wait for the watch, do not fail the stream |
| `Unavailable::NoAssignment` | placement has not assigned it |
| `Unavailable::LeaderUnknown` | the assignment names a node with no known address |
| `Unavailable::LeaderNotLive` | the leader is registered but not live |
| `Unavailable::RegionNotRoutable` | region policy forbids reaching the leader |

Two rules that look like edge cases and are not:

- **A shard led by this node resolves `Local` regardless of liveness or region
  policy.** A broker that stopped serving its own shards while waiting to see
  its own heartbeat land would remove itself from the cluster for no reason.
- **An unknown leader is reported differently from a dead one.** Both are
  unroutable, but one is a missing address and the other is a failover in
  progress, and they send an operator to different places.

#### The ingress gate

Every publish passes through `resolve_stream_cached`, which is why the ownership
check lives there: nothing reaches storage without it.

Ownership is checked **outside** the stream-handle cache. That cache exists to
avoid a registry lookup and holds for a TTL; ownership changes the instant the
control plane says so, and caching it would keep a broker serving a reassigned
shard for up to a TTL. The check is two atomic loads, so paying it per publish
costs less than reasoning about staleness.

Both reads are `ArcSwap` loads, so the resolver is synchronous and allocation
free — no lock a writer can hold, and no await added to the publish path. A
single-node broker short-circuits on a null check before either.

One task keeps the two views in step, in a fixed order: reconcile local shard
state, publish what is servable, then publish the routes. Publishing routes
first would advertise this node as the owner of a shard it has not opened.

**A shard this broker does not own is forwarded to the one that does**, and the
publish is acknowledged only once the owner has written it. The owner does not
take the forwarder's word for the caller's authority: it verifies the client's
own credential before writing, so an authenticated peer is *who is calling*, not
what may be written.

**Sharding is reachable from the wire.** A publish may carry a routing key, and
the key picks the shard through `shard_for`, so a stream placed across brokers
spreads across them. Ordering is per key once a stream has more than one shard;
a single-shard stream keeps total order. The key rides the binary frame under
`FLAG_BINARY_PUBLISH_KEYED`, so routing a publish no longer costs the binary fast
path; JSON remains the fallback for a broker that predates the bit.

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
| `felix_shard_move_steps_total{step}` | planned-move steps written |
| `felix_shard_moves_waiting` | moves that could not advance in the last pass |
| `felix_shard_reconcile_failures_total` | passes that could not read the catalog at all |
| `felix_controlplane_auth_rejected_total{reason}` | credentials turned away by any authenticated endpoint: `missing_token`, `malformed_token`, `invalid_token`, `tenant_mismatch`, `forbidden`. Each is also an `info` log line with the reason and the message the caller saw, never the token. A rising `invalid_token` or `forbidden` is a broker with a stale credential, or something that is not a broker |

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

### Why liveness stays centralized (SWIM, considered)

**Decision: Felix keeps hub-and-spoke liveness — brokers heartbeat the control
plane, a sweep evicts on silence — and does not adopt SWIM-style gossip
membership. Recorded here so the question is answered once, with the triggers
that would reopen it.**

SWIM decentralizes failure detection: every node probes a few random peers per
period, a suspect is probed indirectly through other peers before being
declared dead, and membership spreads by gossip. Its two wins are constant
per-node network load regardless of cluster size, and detection latency that
does not degrade as the cluster grows. Mature Rust implementations exist
(`memberlist`, `foca`), so the cost being weighed is architectural, not
implementation effort.

Four reasons it is the wrong trade for Felix today:

- **Detection is not the authority, and splitting them creates two clocks.**
  Eviction only matters when placement acts on it, and placement is
  centralized — rendezvous hashing over control-plane metadata, with shard
  ownership fenced by leases the control plane grants. More than that: **the
  heartbeat is also the lease renewal.** A leader's authority to serve and its
  liveness signal deliberately travel on one channel to one authority, which
  is what makes "the recorded time is the control plane's own clock" a safety
  property. Gossip membership would put a second, eventually-consistent view
  of "alive" next to the one that grants leases, and every disagreement
  window between them is a place to hide the split-brain that the lease
  arithmetic exists to close.
- **Safety already does not rest on detection speed.** Data-plane failover is
  lease-driven: a lost leader is replaced in about a second, bounded by lease
  expiry plus the safety margin — not by the 15s liveness timeout, which only
  gates *placement eligibility*. SWIM's sub-second detection would accelerate
  a decision Felix deliberately does not take quickly, on a signal that
  fencing renders non-load-bearing.
- **The scale that justifies SWIM is not this scale.** SWIM pays off in the
  hundreds-to-thousands of nodes, where heartbeat fan-in to a hub becomes the
  bottleneck. A Felix cluster is tens of brokers; the fan-in is one tiny
  request per broker per 5s. Under metadata Raft that traffic becomes log
  writes, and even at hundreds of brokers it is tens of kilobyte-scale
  commands per second.
- **False positives are already handled where it matters.** The timeout is
  three missed intervals, eviction is non-destructive (a broker re-registers
  and is placeable again), and a wrongly-expired *leader* cannot corrupt
  anything — its lease, not its liveness row, is what lets it write.

What SWIM would genuinely add is evidence about **asymmetric reachability**: a
broker the control plane can see but its peers cannot reads `live` in the
catalog while every forward to it fails. Leases keep that safe, and
`felix_broker_shard_watch_failures_total` plus the forwarding metrics make it
visible, but placement today cannot act on it. The cheap version of SWIM's
insight — brokers reporting peer reachability to the control plane as an
advisory placement input, alongside the replica-status reports they already
send — covers that gap without a second membership protocol, and is the
first thing to build if it starts biting in practice.

Reopen this decision when any of these becomes true: broker counts reach the
hundreds and heartbeat fan-in (or its Raft log traffic) shows up in
measurements; a deployment shape appears with no control plane to heartbeat;
or placement starts needing peer-observed reachability at a fidelity the
advisory reports cannot deliver.

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
- Under the Postgres backend, control-plane pods are stateless: all metadata
  is in the database, and an instance holds nothing worth a volume.
- Under the raft backend, each pod carries a PVC for the Raft log and
  snapshots — that volume is what makes a pod restart a rejoin. The
  reference StatefulSet shape is on the docs-site Metadata Raft page.
- Dataplane brokers use PVCs for durable log segments (when enabled).

### Services
- Headless Service for control-plane peer discovery — required by the raft
  backend (stable per-pod names feed `FELIX_RAFT_PEERS`), unused by the
  Postgres backend, whose instances do not know about each other.
- ClusterIP Service for control plane client API (watch/snapshot/health).
- Separate Service for broker QUIC ingress.

### Scheduling + Ops
- Control plane replicas: two or more. An odd count matters only for the Raft
  end state; instances share nothing today, so any number works and two is
  enough to survive losing one. The database is the half that actually holds
  state — run it HA per [ha-postgres.md](ha-postgres.md).
- Use PodDisruptionBudgets so a rolling deploy cannot take every instance at
  once.
- Prefer anti-affinity for control plane pods to avoid single-node failure.
- Configure liveness and readiness probes as described under
  [Health probes](#health-probes).

## Health probes

Two endpoints, because they drive different actions.

| Path | Question | What fails it | Wire it to |
| --- | --- | --- | --- |
| `/v1/system/live` | Should this process be restarted? | Nothing outside the process. It touches no database and answers `200` whenever the runtime can answer at all. | liveness probe |
| `/v1/system/ready` | Should this instance get traffic? | The store not answering, answering an error, or being on an older schema than this build expects. `503` with a reason. | readiness probe |
| `/v1/system/health` | — | The same check as `/v1/system/ready`. Kept because deployments already point at it. | nothing new |

**Liveness must not check the database.** A liveness probe drives restarts, and
an external database outage that fails one restarts every instance, repeatedly,
for a fault none of them caused and no restart can fix. That is the single most
important line here.

Recommended settings, and why:

| Setting | Value | Reason |
| --- | --- | --- |
| readiness `periodSeconds` | `2`–`5` | The answer is cached for `FELIX_READINESS_CACHE_TTL_MS` (1s), so polling faster costs nothing extra but gains nothing either. |
| readiness `timeoutSeconds` | `3` | Above `FELIX_READINESS_TIMEOUT_MS` (2s), so the service answers before the prober gives up and the reason is reported rather than lost. |
| readiness `failureThreshold` | `2`–`3` | One slow answer during a Postgres failover should not pull an instance out. |
| liveness `periodSeconds` | `10` | It answers from memory; there is nothing to poll hard for. |
| liveness `failureThreshold` | `3` | A restart is the most expensive response available. |

The readiness check is bounded and cached, so its cost is one query per second
per instance no matter how many probers there are. A transient outage clears on
its own: readiness returns as soon as the store answers again, within the cache
window.

Every claim in this section is exercised against a real Postgres reached through
a proxy the test can cut, black-hole, and restore, in
`tests/pg_readiness.rs` (`cargo test -p felix-controlplane-service --features pg-tests --test
pg_readiness`):

| Claim | What the test does |
| --- | --- |
| An instance that cannot reach its database leaves rotation | Cuts connectivity; `/v1/system/ready` turns `503` |
| …and is *not* restarted for it | Asserts `/v1/system/live` stays `200` through the same outage |
| A transient outage recovers with no intervention | Restores connectivity; readiness returns on its own |
| A database older than this build does not get traffic | Hides the newest applied migration row; readiness turns `503`, and returns when it is put back |
| A probe answers rather than hangs | Black-holes the connection — established, then silent — and the probe still comes back inside its own bound |

The mechanism itself (cache window, timeout, draining short-circuit) is covered
separately in `src/api/readiness/tests.rs` against a probe that fails on command;
the tests above are what make those the *database's* behaviour rather than a
fake's.

### During a shutdown

On SIGTERM the instance **fails readiness first**, before it stops accepting
connections. A load balancer sees the change and steers traffic away while this
instance can still serve what it already has, which is what makes a rolling
restart survivable.

Failing readiness first only helps if something has time to notice. A load
balancer learns by polling, so the instance keeps serving for
`FELIX_SHUTDOWN_PREDRAIN_MS` (default `5000`) after the flip and before it stops
accepting; otherwise the listener closes in the same breath and requests already
in flight toward it are refused at the socket. Size it above the prober's
interval times its failure threshold. A second SIGTERM ends the wait, since an
operator restarting by hand is not waiting on a load balancer.

Draining is answered before the store is consulted and before the cache is read:

- **Before the store**, because nothing a database says changes whether this
  process is shutting down, and a struggling database must not delay an instance
  leaving rotation.
- **Before the cache**, because an instance that had just cached a healthy
  answer would otherwise keep taking traffic for a whole cache window after it
  began shutting down.

Both `/v1/system/ready` and the metrics endpoint's `/ready` read **one flag**, so
they cannot disagree about whether this instance is in rotation. The two
*listeners* are shut down separately and deliberately: the metrics endpoint
outlives the API drain, which is how an operator watches the drain happen.

After the hold-off, in-flight requests are given
`FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS` to finish against one shared deadline covering
every subsystem. Anything still running when it expires is aborted, and that is
reported rather than logged as a clean drain.

No control-plane handler long-polls: the `changes` feeds return immediately
with whatever is committed past `since`, and waiting is the caller's loop. That
is a shutdown property as much as an API one — a drain only has to outlast
requests in service, never a watcher parked on a hanging poll.

What a drain looks like on the metrics endpoint, which outlives it:

| Metric | Meaning |
| --- | --- |
| `felix_ready_state` | 1 in rotation, 0 draining — the flag both `/ready` endpoints read |
| `felix_inflight_requests` | requests currently being served, so "waiting on what?" has an answer |
| `felix_drain_duration_ms` | how long the last drain took |
| `felix_drain_forced_total{subsystem}` | subsystems cut off by the deadline; non-zero means work was dropped, and it is the counter to alert on because the warning log dies with the pod |

The rolling-restart guarantee — two instances over one Postgres, every broker
heartbeat and watch served across a restart of each — is exercised end to end
by `tests/rolling_restart.rs` (`cargo test -p felix-controlplane-service --features pg-tests
--test rolling_restart`).

## Open Questions
- Snapshot cadence and maximum delta size.
- Placement heuristics and rebalancing triggers.
- Authentication/authorization for control plane APIs.
