# Threat model: the broker-internal surface

For [#128](https://github.com/gabloe/felix/issues/128). Covers the M4 forwarding
and M5 replication paths as they exist today, and says which controls are
**present**, which are **absent**, and which are **absent by design pending a
named issue**.

Every claim here was checked against the code. Where a control does not exist,
that is stated plainly rather than described as future work.

## The surface

One listener, separate from the client-facing one
(`services/broker/src/peer/server.rs`), speaking `felix-internal/1` over QUIC.
It accepts four families of request:

| Family | Messages | Effect on the receiver |
|---|---|---|
| Handshake | `Hello` | None; names the caller |
| Forwarded publish | `ForwardPublish` | **Appends to a stream's log** |
| Forwarded cache op | `ForwardCacheOp` | Writes a cache key |
| Replication | `ReplicateRecords`, `ReplicateBootstrap`, and their cache, group, dead-letter and counter variants | **Appends to, or replaces the base of, a shard's log** |

## Trust boundary

**There is none yet.** This is the single most important fact in this document
and everything below follows from it.

The listener applies three defences, and `server.rs` is explicit that none of
them is authentication:

1. **A separate port**, with startup refusing a configuration where the peer and
   client listeners share one.
2. **ALPN.** Both ends negotiate `felix-internal/1`; the client-facing endpoint
   negotiates none, so a client pointed at this port fails in the TLS handshake
   before a frame is read.
3. **A distinct magic** (`FLXI` against the client protocol's `FLX1`), so a
   misdirected frame fails to decode rather than parsing into something
   plausible.

All three stop *accidents*. None stops an attacker, because the TLS
configuration accepts any certificate — peer authentication is mTLS, which is
**#125 and not implemented**.

**So the current security boundary is the network.** Anyone who can reach the
peer port is a peer. Every abuse case below should be read with that in mind:
they describe what a *reachable* attacker can do, not what a *compromised* one
can do, because today those are the same thing.

## Identity

`Hello` carries a `node_id` and nothing verifies it. The check runs in the other
direction only: the dialling side compares `HelloOk`'s id against the node it
meant to reach, so a catalog entry that has been reassigned is caught. An
inbound peer's claimed identity is not checked against the catalog, and is not
used for any decision — which is, in a narrow sense, a mercy: nothing is granted
on the strength of a name nobody verified.

## Abuse cases

### 1. Publish to any tenant, through the forwarding path — **unmitigated**

`ForwardPublish` carries the tenant, namespace and stream, and the payloads. The
owner re-checks **ownership and generation** and nothing else: there is no
authorization check in `peer/handler.rs`. The client's credential is not
forwarded, and no peer-level authority is consulted.

So a caller that can reach the port can append to any stream on this broker,
for any tenant, with no credential at all.

This is exactly what #128's own acceptance criteria warn about —
*"peer authentication alone is not treated as authorization for arbitrary tenant
operations"* — and the state is worse than that sentence assumes: there is
neither peer authentication nor a tenant check.

**Mitigation: #126** (authenticate the broker↔control-plane relationship) and
#125 (mTLS between brokers) establish *who*. A separate control is still needed
for *what*: a forwarding broker should not be able to write a tenant the
original client could not. The cheapest form is to carry the client's
authorization decision with the forwarded request and re-check it at the owner,
so the owner's check does not depend on the forwarder's honesty. Filed as **#503**; it is the residual risk mTLS alone does not close.

### 2. Write arbitrary bytes into a shard's log, through replication — **partly mitigated**

The replication path checks more than the forwarding path does:

- **Role.** A broker that is not a replica of the named shard answers
  `Unauthorized` (`peer/replica.rs`).
- **Generation.** A sender at an older epoch is fenced; one naming a newer
  generation than this broker knows is refused rather than believed.
- **Content.** A batch is compared byte-for-byte against what the follower
  already holds, and a mismatch is `LogConflict`.

What those do *not* bound is a sender that names a shard this broker genuinely
replicates, at the generation it genuinely believes, appending records past the
tail. Those are accepted, because that is replication. A reachable attacker can
therefore inject records into any shard this broker is a replica of, and they
will be indistinguishable from the leader's.

**Mitigation: #125.** Unlike case 1, mTLS is close to sufficient here: the role
and generation checks already narrow a peer's authority to shards it is a
replica of, so proving the peer *is* that broker closes most of the gap.

### 3. Discard a follower's log, through bootstrap — **mitigated**

`ReplicateBootstrap` says "the surviving log begins here", which if accepted
blindly would let a peer truncate a replica. A follower holding records of its
own refuses; only one holding nothing accepts. That refusal is a deliberate
control — discarding records is an operator's decision, not a peer's — and it
holds regardless of authentication.

### 4. Memory exhaustion through declared lengths — **mitigated**

Decoding validates before allocating. `MAX_BODY_BYTES` (64 MiB) bounds a frame
before a buffer is sized from it; `MAX_BATCH_PAYLOADS` (65,536) bounds the
payload count, and the decoder additionally refuses a declared count larger than
the remaining bytes could describe (`declared > body.remaining() / LEN_PREFIX`),
so a small frame cannot claim a large array. `MAX_IDENT_BYTES` (512) bounds
tenant, namespace and stream names.

The fuzz target added in #480 exercises exactly this and found nothing.

### 5. Resource exhaustion through connections — **partly mitigated**

Per connection, QUIC caps concurrent streams at `max_streams` (default 1024) and
applies flow-control windows.

**The number of connections is not capped.** `PeerServer::serve` accepts in a
loop with no limit, no per-source accounting, and no backpressure. A reachable
attacker can open connections until the process runs out of file descriptors or
memory; the ceiling is 64 MiB × 1024 streams × unbounded connections.

`max_inflight_per_peer` does not help: it is an *outbound* shed, applied by this
broker's pool to requests it is sending, not a bound on what it will accept.

**Mitigation: #504.** An accept-side connection cap and per-peer accounting,
useful even after mTLS because an authenticated peer looping on a bug is still
unbounded.

### 6. Amplification — **low**

A request produces at most one response, and responses are small except for
bootstrap offers, which carry an offset rather than data. The internal protocol
has no request that returns bulk data to the caller; replication pushes rather
than pulls. There is no amplification vector of note.

### 7. Replay — **partly mitigated, by accident**

Nothing in the protocol is nonce-protected. What limits replay is that the
operations are close to idempotent: a replayed `ReplicateRecords` is compared
against stored bytes and answered as a retry; a replayed `ForwardPublish`
**is not** — it appends again, because publishes are not deduplicated.

Idempotent producers (**#422**) would close the forwarding case. Until then, a
replayed forwarded publish duplicates records, which is within the delivery
guarantee Felix documents (`AtLeastOnce`) but is a capability an attacker has
for free.

### 8. Stream exhaustion and malformed payloads — **mitigated**

An undecodable frame ends the stream rather than desynchronising it, and since
#496 a frame with an unknown *kind* is stepped over and refused without dropping
the connection. Neither leaves the reader mid-frame.

## Summary

| # | Abuse case | Status | Owner |
|---|---|---|---|
| 1 | Publish to any tenant via forwarding | **Unmitigated** | #125, #126, and #503 for tenant authority |
| 2 | Inject records via replication | Partly (role + generation) | #125 |
| 3 | Truncate a follower via bootstrap | Mitigated | — |
| 4 | Memory exhaustion via lengths | Mitigated | — |
| 5 | Connection exhaustion | **Unmitigated** | #504 |
| 6 | Amplification | Low | — |
| 7 | Replay of a forwarded publish | Unmitigated | #422 |
| 8 | Malformed frames | Mitigated | — |

## Residual risk and operating assumptions

Until #125 and #126 land, **the peer port must be treated as a trusted
network**: reachable from other brokers and nothing else. That is an operational
control, not a product one, and it should be stated wherever deployment is
documented rather than assumed.

Two gaps will remain after mTLS:

- **Tenant authority across a forward.** Authenticating the peer says which
  broker is calling, not which tenant the original client was entitled to write.
- **Connection admission.** An authenticated peer that misbehaves — compromised,
  or simply looping on a bug — is still unbounded in connections.

The milestone signal for this review is *"no unauthenticated internal RPC and no
unencrypted cluster-internal link"*. Encryption is satisfied: QUIC requires TLS,
so the link is encrypted even though the peer is unauthenticated. Authentication
is **not** satisfied, and this document should be re-read when #125 lands, since
several cases above change status the moment it does.
