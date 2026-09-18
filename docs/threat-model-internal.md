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

### 1. Publish to any tenant, through the forwarding path — **mitigated (#503)**

A forwarded publish or cache operation carries the client's own bearer token,
and the owner verifies it itself — against the tenant's keys, for the action
the request performs on the stream or cache it names — before it writes
anything (`peer/handler.rs`, `ForwardingHandler::authorize`). A forward with
no credential, a credential that does not verify, or one that does not allow
the action is refused `Unauthorized`. The legacy credential-less kinds are still
decoded, so the refusal is typed rather than a decode failure, but they are
refused all the same.

So the owner's write depends on what the *client* was entitled to, not on
whether the forwarder checked. That is the control #128's acceptance criteria
asked for — *"peer authentication alone is not treated as authorization for
arbitrary tenant operations"* — and it holds today, before #125 and #126 land,
because it does not rest on knowing who the forwarder is at all.

What it does not do: a compromised forwarder that holds a *client's* valid
token can still forward what that client could have published. That is the
client's authority, correctly applied; the token's lifetime and scope bound it.

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
| 1 | Publish to any tenant via forwarding | Mitigated (the owner verifies the client's credential) | #125, #126 for peer identity |
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

One gap will remain after mTLS:

- **Connection admission.** An authenticated peer that misbehaves — compromised,
  or simply looping on a bug — is still unbounded in connections.

Tenant authority across a forward is no longer one: the owner verifies the
client's credential itself, so authenticating the peer is about *who is
calling*, not about what may be written.

The milestone signal for this review is *"no unauthenticated internal RPC and no
unencrypted cluster-internal link"*. Encryption is satisfied: QUIC requires TLS,
so the link is encrypted even though the peer is unauthenticated. Authentication
is **not** satisfied, and this document should be re-read when #125 lands, since
several cases above change status the moment it does.
