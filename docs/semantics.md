# Felix semantics

The behavioural contract: what an acknowledgement means, what survives what, and
where each guarantee stops.

**Every normative claim below names the test that holds it.** A guarantee with
no test behind it is an intention, and this document has been wrong before by
describing intentions in the present tense. Where something is not enforced, it
says so rather than omitting it.

Written for the replicated, multi-broker system. It replaces the single-node MVP
contract, which described a system that no longer exists — at-most-once
delivery, no authorization, no durability.

## What an acknowledgement means

An acknowledgement is the only promise Felix makes about a record, and what it
promises depends on two settings that are chosen separately.

### Durability: `FsyncMode`

Where the record is when the broker acknowledges it.

| Mode | An ack means | Lost by |
| --- | --- | --- |
| `None` | The record is in the page cache | Losing the machine |
| `Periodic { interval }` | The record is in the page cache, and will be fsynced within `interval` | Losing the machine within `interval` of the write |
| `OnCommit` | The record is fsynced | Nothing short of the disk failing |

`Periodic` is the default. `OnCommit` is the only setting under which an
acknowledged record survives the loss of the machine, and group commit is what
keeps it affordable: one flush serves every waiter queued behind it.

A crash mid-append leaves a **torn tail**, which recovery repairs by discarding
the incomplete record — it was never acknowledged. Corruption in the *interior*
of a segment is refused instead: the broker will not start. Refusing to start
beats silently losing an acknowledged record.

> Held by `crates/felix-storage/src/disk_log/` recovery tests, including
> `a_crash_before_the_header_is_written_leaves_the_log_openable`,
> `a_crash_while_preparing_a_rollover_leaves_the_log_openable`, and
> `a_crash_while_sealing_a_retired_segment_loses_nothing`.

### Consistency: how many brokers hold it

| Level | An ack means | Loss window |
| --- | --- | --- |
| `Leader` | The shard's leader has it durably | Everything the leader had not yet shipped, if its storage is lost |
| `Quorum` | A majority of the replica set, leader included, has it durably | None within the replica set |

`Quorum` waits. A publish is not acknowledged until the leader can show a
majority holds the record; if no majority is reachable it is **refused**, with
an error that says this broker cannot vouch for the write rather than one that
claims it failed.

> `a_quorum_publish_without_a_majority_is_refused` — freeze every follower and
> the publish is refused rather than acknowledged.
> `a_frozen_follower_does_not_block_a_quorum` — losing a *minority* does not
> stop it, which is the case `Quorum` exists to tolerate.
> `a_quorum_acknowledged_record_survives_its_leader` — the acknowledged record
> is readable after the acknowledging broker is killed.

**The `Leader` loss window is bounded by replication lag**, exported as
`felix_broker_replication_lag_records`. An operator choosing `Leader` is
choosing that window, and a bound nobody can observe is not a bound.

A majority is of the replica set, leader included: a set of three needs two, a
set of five needs three, and a set of one needs one — which is why
`replication_factor: 1` costs nothing.

## Failover

A shard's leadership is a **time-bounded lease** issued by the control plane,
with the assignment generation as its epoch. Per-shard Raft was considered and
rejected; `docs/replication-design.md` records why.

**Only a replica that holds every record the leader did may be promoted.** The
catch-up bound is zero. A bound above zero would be a bound on how much a
promotion may silently lose, and there is no honest non-zero value that is not a
policy decision.

> `the_promoted_leader_is_one_of_the_replicas` and
> `an_unreplicated_shard_does_not_fail_over_to_an_empty_broker` — a shard with
> no qualifying replica is left unavailable rather than served empty.
> `failover_completes_within_the_configured_bound`.
> `records_survive_repeated_failovers` — two failovers in a row, not just one.

**A leader that was frozen past its lease and then resumed cannot acknowledge a
write the cluster has lost.** It wakes still believing it leads; the lease and
the generation are what stop it.

> `a_resumed_leader_does_not_acknowledge_writes_the_cluster_loses`.

The lease depends on bounded process suspension, not on synchronised clocks:
each broker measures elapsed time on its own monotonic clock and gives up a
quarter of the lease as margin. See "Where the guarantees stop" below.

## Routing

A publish and a subscribe make opposite choices, deliberately.

- **A publish to a broker that does not own the shard is forwarded** to the one
  that does, and acknowledged only once the owner has answered.
- **A subscribe to a broker that does not own the shard is redirected**, naming
  the owner. It is never served locally: that would deliver nothing while
  looking exactly like a stream with no traffic.

> `a_publish_to_a_non_owner_is_still_forwarded`,
> `a_subscribe_to_a_non_owner_is_redirected`,
> `a_cluster_client_follows_the_redirect_to_the_owner`.
> `docs/subscribe-routing.md` and `docs/internal-protocol.md` record the two
> decisions.

**Every record of a stream currently lands on shard 0**, because the wire
protocol carries no routing key (#240). A stream's shard count is honoured by
placement and ignored by publishing.

## Delivery to subscribers

- **Ordering** is preserved per stream, per subscriber. There is no ordering
  across streams.
- **A slow subscriber is dropped from, not blocked on.** Each subscriber has a
  bounded queue with an explicit overflow policy, `DropNew` by default. A
  publisher never waits for a subscriber.
- **A drop is detectable.** Delivered events on durable streams carry log
  offsets, so a gap in offsets is exactly a drop. This is the only way an
  application can tell, and it is why offsets are on the event rather than
  inferred.
- **A subscription can resume.** `Subscribe` takes `latest`, `earliest`, or an
  offset; stored history joins live delivery with no gap.

**`DeliveryGuarantee` is declared on a stream and not enforced.** The control
plane accepts `AtMostOnce` and `AtLeastOnce`, and no broker code reads either.
What a client gets is what the sections above describe, whichever value is set.
Do not rely on it.

## Clients

A client is told which brokers exist and is redirected to the right one, but the
retry policy is still the application's. `docs/multi-node-client.md` is the
how-to; this is the contract.

- `ClusterClient` takes several broker addresses and rebuilds its connection
  from the rest when the one in use fails.
- It asks a broker which other brokers exist and **adds** them to what it will
  try. The configured seeds are never removed, so a wrong or stale answer cannot
  leave a client with fewer ways in than it started with.
- `publish` reconnects but does **not** resend. `publish_at_least_once`
  resends, and can therefore duplicate a record whose failure it could not prove
  was not applied. The names are the contract.
- **Retries are bounded by an attempt count and a jittered exponential
  backoff**, and optionally by a deadline across every attempt and sleep. The
  deadline is off by default: one shorter than a single attempt's own timeout
  prevents any retry at all, so a useful value depends on the caller's latency
  budget rather than on a number this library can pick.
- **The backoff is full jitter** — uniform over `[0, ceiling]`, not the ceiling.
  Every client notices a failover at the same moment, and an unjittered backoff
  sends all of them at the freshly promoted broker in step.
- **A failure that cannot succeed on another attempt is not retried.** That is
  a short list: a credential without the permission, and an offset retention has
  passed. Everything else is retried, *including errors nobody has classified* —
  the client protocol carries an error as prose with no code, so this is string
  matching, and a wasted attempt is a cheaper mistake than a lost operation.

  **"Not found" is retried**, and deliberately. A broker learns its streams from
  the control plane and opens a shard only once it is given one, so a broker
  promoted a moment ago reports the stream it is about to serve as missing.
  Being named leader and being ready to serve are different moments.

> `a_forbidden_publish_fails_fast_instead_of_retrying`,
> `a_client_given_one_seed_learns_the_other_brokers`,
> `the_configured_seed_is_never_dropped`,
> `a_client_given_one_seed_survives_losing_it`,
> `a_publisher_survives_losing_its_broker`,
> `records_published_across_a_failover_are_all_readable`.

## Cache

- **Scope:** `(tenant_id, namespace, cache, key)`.
- **Storage:** a cache is a log. A write appends a record; a read consults an
  in-memory index of key → offset and reads the log. See `docs/cache-on-log.md`.
- **Durability:** entries survive a restart when the broker runs with
  `FELIX_DURABLE_STORAGE_DIR`. Without one the cache is in memory and is lost,
  because there is nowhere to write a log.
- **TTL:** lazy on access, against an absolute expiry time, so an expiry that
  passes while the process is down is still an expiry.
- **Reclamation:** compaction rewrites the live set and drops superseded and
  expired records, without ever rewriting a record in place.

> `a_cache_survives_a_restart`, `an_expiry_survives_a_restart`,
> `a_later_write_wins`, `compaction_reclaims_overwritten_records`.

**Cache operations are not routed, and two brokers can hold different values for
the same key with nothing to reconcile them** (#278). Every broker serves its
own cache, so whichever broker a client reaches defines the key for that client.
This is the one data path with no ownership, and unlike a missing value the
divergence is not detectable by a reader. **Do not use the cache from more than
one broker.**

## Authorization

Enforced, contrary to what this document said for a long time. Tenant-scoped
tokens are verified at the broker, and publish, subscribe and cache operations
each check a permission. A forwarded publish is authorized at both the ingress
broker and the owner — routing does not launder a credential.

> `crates/felix-cluster/src/scenarios.rs`, including
> `unauthorized_publish_is_refused` across both ingress paths.

Per-tenant quotas are **not** enforced.

## Where the guarantees stop

Stated because a guarantee without its failure model is a slogan.

- **The failure model is process loss and partition.** Kill, graceful stop,
  freeze, and severing a broker from its peers while it keeps running are all
  injectable and tested. A partitioned leader keeps heartbeating, so the control
  plane goes on believing it is healthy while it can reach nobody — and it
  cannot acknowledge a `Quorum` publish, because it is not a majority on its own.

  > `a_partitioned_leader_cannot_reach_a_quorum`,
  > `a_healed_partition_restores_the_quorum`,
  > `a_partitioned_leader_still_serves_a_leader_stream`.

- **Clock skew between brokers cannot affect lease safety**, because no lease
  reads a wall clock. Each broker measures its own elapsed time on a monotonic
  clock and gives up a quarter of the lease as margin, so two brokers'
  disagreement about what time it is has nothing to act on. The assumption that
  *does* matter is bounded **process suspension**, and that is injectable — a
  broker frozen past its lease and resumed is the test above.

  A broker suspended *between* the commit-time lease check and its write
  reaching disk is a residual window bounded by the margin. The margin is a
  choice rather than a proof, and it is the one clock-shaped assumption left.
- **No exactly-once delivery**, and no transactions.
- **No cross-region ordering or routing guarantees.**
- **No queue semantics** — consumer groups, acknowledgements and redelivery are
  not implemented (#280).
- **Retention is per stream and unbounded by default.** A stream with no
  retention policy grows until the disk does not.
