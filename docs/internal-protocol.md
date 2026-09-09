# Broker-internal forwarding protocol

The protocol brokers speak to each other. Separate from the client protocol in
`docs/protocol.md`, and deliberately not a superset of it.

## Why it is a separate protocol

A client frame and an internal frame must never be mistaken for one another. A
client that could send `ForwardPublish` could write to a shard on a broker that
never checked ownership; a broker that accepted client frames on its internal
listener would treat a peer as an authenticated publisher.

Three things keep them apart, and only the first is a wire concern:

1. **A distinct magic.** `FLXI` rather than `FLX1`, so a frame from one side
   fails to decode on the other rather than parsing into something plausible.
2. **A distinct listener.** Internal traffic arrives on the broker-internal QUIC
   role, never the client one. The two bind different ports, and startup refuses
   a configuration where they share one. Both internal endpoints negotiate the
   `felix-internal/1` ALPN and the client-facing role negotiates none, so a
   client pointed at the internal port has no protocol in common with it and TLS
   refuses the handshake — before a frame is read, and before any broker state
   is touched.
3. **A distinct credential.** Peer authentication is mTLS between brokers (M8.1).

The magic alone is not security — it is what makes a misdirected connection fail
loudly instead of quietly.

## Framing

```
+--------+---------+------+--------+---------+
| magic  | version | kind | length | body    |
| u32    | u16     | u16  | u32    | length  |
+--------+---------+------+--------+---------+
```

Big-endian throughout, matching the client protocol.

`kind` is a message discriminant, not a flag set. The client protocol uses flag
*bits* because they modify one payload layout; here each kind **is** a layout, and
an enum makes "unknown kind" a single unambiguous check.

**An unknown kind is rejected, never skipped.** Same reasoning as the client
protocol's unknown flag bits: the kind selects how to read the body, so ignoring
one means confidently misparsing it.

### Handshake

The first message on a connection is `Hello`, answered by `HelloOk`. Both carry
a node id.

It exists for *when* a mismatch is found, not for what is found. Every frame
already carries the version, so a mismatched peer would be rejected on its first
request either way — but that request would be a real forwarded publish, which
then has to be failed and retried. Doing it at connect makes the connection
unusable before anything is riding on it.

The node ids make one further check possible: the caller compares `HelloOk`
against the node id it dialled. An address the catalog has since reassigned
answers with a different id, which is a connection to the wrong broker whether
or not it would have served the request.

### Versioning

`version` is this protocol's own, independent of the client protocol's. A peer
speaking a version this broker does not know gets `ProtocolVersion` and the
connection closes; there is no partial understanding.

Additive evolution happens by adding kinds, not by bumping the version. A new
kind on an old peer is an unknown kind, which is already a typed error rather
than a misparse. The version exists for the case additive change cannot cover —
a change to the header or to an existing body layout.

## Correlation

Every request carries a `correlation_id`, unique per connection and chosen by the
requester. Every response echoes it.

**Every request has exactly one terminal response.** `ForwardPublish` is answered
by exactly one of `ForwardPublishOk`, `ForwardPublishError`, or `NotLeader`. A
requester that never receives one, because the connection dropped, treats the
publish as failed — no forwarded publish is silently abandoned as pending.

Correlation ids are per connection, so two connections may reuse a value.
Responses are matched within the connection they arrive on and nowhere else.

## Flows

### Forwarded publish

```mermaid
sequenceDiagram
    participant A as Broker A (ingress)
    participant B as Broker B (owner)

    A->>A: resolve shard -> Remote(B, generation)
    A->>B: ForwardPublish(correlation, shard, generation, payloads)
    B->>B: check own ownership at that generation
    alt B owns the shard at that generation
        B->>B: append and commit locally
        B-->>A: ForwardPublishOk(correlation, first_offset, last_offset)
    else B has moved on
        B-->>A: NotLeader(correlation, owner, its generation)
    else B cannot serve
        B-->>A: ForwardPublishError(correlation, code)
    end
```

The `generation` field is what makes this safe. A forwarding broker names the
assignment generation it resolved against; the owner compares it with its own.

- **Equal** — both agree, and the publish proceeds.
- **Requester behind** — the shard moved. `NotLeader` carries the new owner, so
  the requester can retry against it rather than guess.
- **Requester ahead** — the *owner* is behind, and answers `StaleRoute`. It must
  not accept the write: it would be writing a shard it may no longer hold.

That asymmetry is the point. A generation mismatch in either direction is an
explicit typed answer, and **never a successful ownership claim**.

### Handshake

```mermaid
sequenceDiagram
    participant A as Broker A (caller)
    participant B as Broker B

    A->>B: Hello(correlation, node_id = A)
    alt B speaks this version
        B-->>A: HelloOk(correlation, node_id = B)
        Note over A: check the id matches the peer dialled
    else version B does not know
        B--xA: connection closed
    end
```

### Subscribe

Whether a subscription is redirected to the owner or proxied through the
receiving broker is decided in M4.4, and the two need different message sets.

What both need is the redirect primitive, which is defined here: `NotLeader`
carries the owning node's id, its advertised address, and its generation. A
redirect design uses it as the answer; a proxy design uses it when the shard
moves mid-subscription. Relay kinds for a proxy design are additive.

## Errors

Typed, because they need different responses:

| Code | Meaning | What the requester does |
| --- | --- | --- |
| `NotLeader` | the shard is owned elsewhere | retry against the named owner |
| version mismatch | the peer speaks a version this broker does not know | the connection closes; there is no frame to answer with, because a reply would carry the version the peer just rejected |
| `StaleRoute` | the *responder* is behind the requester's generation | retry shortly; the owner is catching up |
| `Unavailable` | the owner cannot serve right now, e.g. still opening | retry with backoff |
| `Unauthorized` | the peer is not permitted | do not retry |
| `Overload` | the owner is shedding load | retry with backoff |
| `ProtocolVersion` | version not understood | do not retry; close |
| `Malformed` | the body did not decode | do not retry; close |

`NotLeader` is a distinct kind rather than an error code, because it carries a
routing answer rather than only a reason.

## The transport

Peers reach each other over a QUIC endpoint of their own. What it guarantees,
and what it refuses:

**Connections are pooled and reused.** Repeated requests to one peer share a
connection; several multiplexed streams carry them, because a QUIC stream is
ordered and one large forwarded batch would otherwise hold up every smaller
request behind it.

**Every request terminates.** A connection that drops fails every request
waiting on it at that moment, rather than leaving each to reach its own timeout.
This is what makes the correlation rule above true in practice: a forwarded
publish is never left pending.

**An unhealthy peer cannot consume this broker.** Three bounds, each failing
differently:

| Bound | What crossing it means |
| --- | --- |
| In-flight requests per peer | Requests are shed immediately, not queued. Nothing was sent, so the caller may retry elsewhere at once |
| Reconnect backoff | A peer that refuses connections is redialled on a jittered exponential schedule, and requests arriving inside the window fail without a dial. Jitter matters because every broker notices the same peer restart at the same moment |
| Request timeout | A peer that accepts a request and never answers still releases its waiter |

**A dropped connection and a timeout are not retryable.** The peer may have
applied the write before the answer was lost, so retrying is a duplicate rather
than a repair. Only a shed request — where nothing was sent — is safe to retry
as-is.

Idle connections are closed: rebalancing changes which peers a broker forwards
to, and a connection to one it no longer talks to is a file descriptor and a
keepalive with nothing to do.

## Limits and validation

Every peer-provided field is validated before it is used to allocate:

- Declared payload counts are bounded by what the remaining bytes could hold,
  before any allocation, exactly as the client binary path does.
- String fields are length-prefixed and bounded, and must be valid UTF-8.
- The total body is bounded by the frame length, which is itself bounded.

Peer-provided does not mean trusted. A broker is authenticated, not assumed
correct: a compromised or buggy peer must fail decoding rather than be able to
allocate on demand.
