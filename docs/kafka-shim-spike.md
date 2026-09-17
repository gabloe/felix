# A Kafka wire shim: what it would take, and what it would buy

Findings for [#488](https://github.com/gabloe/felix/issues/488). The spike is in
`spikes/kafka-shim/`; every claim below was produced by running it against
librdkafka (`edenhill/kcat:1.7.1`), not by reading the protocol spec.

## The result

`kcat` reads records out of a Felix shard log, with correct offsets:

```console
$ kcat -b felix:19092 -L
 1 topics:
  topic "orders" with 1 partitions:
    partition 0, leader 0, replicas: 0, isrs: 0

$ kcat -b felix:19092 -C -t orders -p 0 -o beginning -e -q -f '%o:%s\n'
0:felix-record-0
1:felix-record-1
2:felix-record-2
3:felix-record-3
4:felix-record-4

$ kcat -b felix:19092 -C -t orders -p 0 -o 3 -e -q
felix-record-3
felix-record-4

$ kcat -b felix:19092 -Q -t orders:0:-1
orders [0] offset 5
```

**533 lines**, four API keys (`ApiVersions`, `Metadata`, `ListOffsets`,
`Fetch`), one afternoon. That is option A from the issue, complete, against a
real client rather than a mock.

## What was easy, and why

The parts that looked like they would be hard were not, because Felix already
has the shapes Kafka assumes:

- **Offsets need no translation.** Felix offsets are contiguous per shard and
  start at zero, which is exactly Kafka's model. `%o` in the output above is
  Felix's own offset, unmapped. A store with non-contiguous or non-numeric
  offsets would need a translation table and its own durability story; Felix
  does not.
- **Shards are partitions.** One shard became one partition with no adaptation.
  `Metadata` is a description of placement, and Felix's placement already has
  the fields Kafka asks for — leader, replicas, in-sync replicas.
- **`Fetch` is `read_from`.** The shim reads with
  `shard_log(..).read_from(offset, max_bytes)`, the same call the replication
  driver ships with. A Kafka fetch and a follower's catch-up want the same
  thing, so no new read path was needed.

## What cost real time

Three things, all in the protocol rather than in Felix:

1. **`ApiVersions` has a bootstrapping exception, and nothing works until you
   hit it.** librdkafka opens with `ApiVersions` **v3**, which is "flexible"
   (KIP-482): compact arrays and tagged fields, a second encoding throughout. A
   broker that does not speak v3 must answer `UNSUPPORTED_VERSION` **in the v0
   response format** — the client cannot know which format to parse until it
   learns the version was refused. Answer in v0 shape without that error, as the
   first attempt did, and librdkafka reports `Bad message format: probably due
   to broker version < 0.10` and gives up. Nothing else in the protocol can be
   reached until this is exactly right.

2. **CRC-32C, not CRC-32.** The v2 record batch uses Castagnoli. `crc32fast`,
   already in the tree, is IEEE. Getting this wrong rejects every batch with an
   error that says nothing about checksums.

3. **The v2 record batch is fiddly and unforgiving.** Zigzag varints,
   `batch_length` counting from after itself, the CRC covering only the bytes
   after the CRC field, per-record length prefixes. Each is simple; there are
   enough of them that the first version is wrong and the client's error message
   does not say where.

None of that is *hard*. It is exacting, and it is bounded — the reason option A
took an afternoon rather than a week is that the list ends.

## Where it stops, and how badly

Asked to join a consumer group, `kcat` does this:

```console
$ kcat -b felix:19092 -G mygroup orders
% Waiting for group rebalance
… hangs …
```

The shim logs `unsupported api key` for `FindCoordinator` and closes the
connection. The client does not report an error. It waits, retries, and waits.

This is exactly the failure the issue named in advance:

> Partial compatibility that fails *deep* — a client connects, negotiates, and
> then dies inside a rebalance — is worse than no compatibility, because the
> failure is unreadable to the person holding it.

That is the finding that matters most, and it is not a bug in the spike: there
is no protocol-level way to tell a Kafka consumer "this broker has no group
coordinator". `FindCoordinator` either names one or the client keeps trying.

## The recommendation

**Option A is worth shipping only if it is framed as a read-only export, and
only alongside something that makes the group case fail loudly.** Option B is
a decision about idempotent producers, not about protocol work. Option C should
not be attempted.

### Option A — read-only. Cheap, honest, narrow.

533 lines got it working; call it 1,500 with multi-shard topics, several
concurrent fetch sessions, error mapping, timeouts and tests. It buys
`kcat`, `kafka-console-consumer`, and any librdkafka consumer reading a Felix
stream — useful for inspection, ad-hoc export, and getting data *out* of Felix
into an existing pipeline.

It does **not** buy the ecosystem. Kafka Connect, Streams, ksqlDB and Debezium
all need groups.

The condition: a consumer that tries to use a group must fail in a way its
operator can read. `FindCoordinator` returning `COORDINATOR_NOT_AVAILABLE`
forever is still a hang. The honest move is to answer it with a real error code
and document loudly that groups are unimplemented — and to accept that some
clients will still hang, because the protocol does not have a "never" in it.

### Option B — plus `Produce`. Blocked on a decision, not on work.

The record-batch decoder is the encoder in reverse, plus compression codecs
(gzip, snappy, lz4, zstd) because clients choose them. Perhaps a week.

The blocker is not that. Recent clients set `enable.idempotence=true` **by
default**, which means `InitProducerId`, producer epochs and sequence numbers.
Felix has none of that, and the shim's three options are all bad:

- Refuse those clients — most modern producers, out of the box.
- Accept the fields and ignore them — the client believes it has exactly-once
  and does not. This is lying to a durability guarantee, and it is the worst of
  the three.
- Implement them — which is [#422](https://github.com/gabloe/felix/issues/422),
  a change to Felix rather than to a shim.

So option B should not be started before #422 lands. Sequenced after it, the
shim work is small and the semantics are real.

### Option C — plus the group coordinator. No.

`FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`,
`OffsetCommit`, `OffsetFetch`, assignment strategies, generation IDs, and a
rebalance protocol whose edge cases are the accumulated bug reports of a decade.

Felix has the storage half — durable per-shard cursors — and none of the
coordination half. Building it as a compatibility layer means building the
rebalance protocol Felix deliberately does not have, *and* owning its behaviour
across Kafka client versions, for a project with two clients and no production
users. The support obligation outlives whoever writes it.

If group semantics are wanted, they should be designed for Felix and exposed
through Felix's own protocol, where the design is free. A Kafka-shaped
coordinator would fix the design to Kafka's.

## What this changes in the docs

`why-felix.md` lists Kafka wire compatibility as a reason not to use Felix.
That stays, and should say so deliberately rather than by omission: the
recommendation is that Felix will not be Kafka-compatible for anything beyond
reading, and that a reader who needs Connect or Streams should use Kafka.

## Prior art, briefly

The comparison worth drawing is not that others did it, but how far they got
before anything worked:

- **Redpanda** reimplemented the broker natively in C++ and treats the Kafka
  protocol as its *only* protocol. Compatibility is the product, not a layer.
- **AutoMQ** keeps Kafka's own broker code and replaces the storage beneath it,
  which sidesteps the protocol entirely — the coordinator is Kafka's.
- **WarpStream** reimplemented the protocol including the coordinator, and
  their published engineering writing is mostly about the coordinator.
- **Kafka-on-Pulsar** built the coordinator on top of Pulsar and found the
  group and transaction semantics to be the long tail.

Every one of them either adopted Kafka's coordinator wholesale or spent most of
the effort rebuilding it. None got the ecosystem without it. That is the
strongest evidence for the recommendation above: the gap between option A and
the thing people actually want from Kafka compatibility is the coordinator, and
it is not a gap a shim closes cheaply.
