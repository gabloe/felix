---
title: "Python Client"
description: "Installing and using the Felix Python client: both surfaces, streams, queues, cache watches, multi-shard consumption, and the failure modes worth writing code for."
---

`felix` is a binding over the Rust client, not a reimplementation. Reconnection,
redirect-following, retry classification and offset bookkeeping live in
`felix-client` and are shared; Python gets the same failover behaviour Rust
does rather than its own approximation. See
[Choosing a Client](/felix/clients/overview/) for why that choice was made and
what the conformance suite does about it.

## Installing

Not published to PyPI yet. The release pipeline builds a wheel for each
platform and attaches it to the GitHub release, so installing needs no Rust
toolchain — point `pip` at the wheel for your platform:

```bash
pip install https://github.com/gabloe/felix/releases/download/v0.5.0/felix_client-0.5.0-<tag>.whl
```

The PyPI step is written and behind an explicit switch, for the same reason the
npm one is: the name is permanent once claimed.

Building from the repository needs a Rust toolchain:

```bash
pip install ./crates/felix-python
```

## Two surfaces

Both wrap the same Rust client and fail over identically. Pick by how your
application is already written, not by expected performance.

| | `Client` | `AsyncClient` |
| --- | --- | --- |
| Calls | block, GIL released | awaitable |
| Subscriptions | `for event in events` | `async for event in events` |
| Suits | threads, `asyncio.to_thread`, scripts | an existing event loop |

```python
import felix

with felix.Client("127.0.0.1:5000", tenant_id="t1", token=tok) as client:
    client.publish("t1", "default", "events", b"hello")
```

```python
client = await felix.AsyncClient.connect(
    "127.0.0.1:5000", tenant_id="t1", token=tok
)
async with client:
    await client.publish("t1", "default", "events", b"hello")
```

The synchronous surface releases the GIL while it blocks, so threads genuinely
run in parallel — that is what makes `concurrent.futures.ThreadPoolExecutor`
over one shared client a reasonable design rather than a queue behind a lock.

## Connecting

```python
felix.Client(
    addrs,                    # "host:port", or a list of them
    tenant_id="t1",
    token=tok,
    server_name="localhost",  # the name the broker's certificate carries
    ca_file=None,             # trust a specific CA; omit for the system store
)
```

One reachable address is enough — the client discovers the rest of the cluster
and will use brokers it was never told about. Passing several only helps the
*first* connection, for the case where the one you named is the one that is
down.

**TLS is not optional.** QUIC has no unencrypted mode, so there are two trust
choices and no third: the platform trust store, or an explicit `ca_file` for a
self-signed development broker. There is deliberately no "skip verification"
switch — it is the one setting that silently turns a secure deployment
insecure, and a CA file covers development without it.

## Publishing

```python
client.publish("t1", "default", "orders", payload)                      # acked
client.publish("t1", "default", "orders", payload, ack="none")          # fire and forget
client.publish("t1", "default", "orders", payload, key=customer_id)     # routed
```

`ack` selects what the broker must have done before the call returns:
`"per_message"` (the default), `"per_batch"`, or `"none"`, which promises
nothing and does not wait to find out.

### The routing key decides the shard

**Without a key every record lands on shard 0**, so a multi-shard stream
behaves like a single-shard one. If you created a stream with several shards to
get throughput, and you are not passing a key, you are not getting it.

```python
for order in orders:
    client.publish(
        "t1", "default", "orders",
        order.encode(),
        key=order.customer_id.encode(),
    )
```

Records sharing a key share a shard and stay ordered with respect to each
other. Records with different keys do not, once a stream has more than one
shard. A consumer needing total order wants a single-shard stream.

### At-least-once duplicates, and says so

By default a publish whose outcome was ambiguous — the broker may or may not
have written it before the connection went — is **reported, not re-sent**,
because nothing downstream can tell two copies apart.

```python
client.publish("t1", "default", "orders", payload, at_least_once=True)
```

That is the opt-in. The record is then certain to land and **may land twice**.
It cannot be combined with `key`: the re-send path does not carry one, and
honouring the key on the first attempt but not the re-send would put the
duplicate on a different shard, so the combination raises rather than
resolving.

## Subscribing

```python
with client.subscribe("t1", "default", "events") as events:
    for event in events:
        handle(event.payload)
```

`start` is `"latest"` (the default), `"earliest"`, or an integer offset — **the
first record you have not seen**, so a resuming consumer passes the offset it
last handled *plus one*.

### Offsets are how you notice a drop

Subscriber queues shed under the default policy rather than blocking the
publisher, so a subscriber can silently miss records. On a durable stream every
delivered event carries its log offset, and **a jump in them is exactly a
drop**:

```python
expected = None
with client.subscribe("t1", "default", "events") as events:
    for event in events:
        if expected is not None and event.offset != expected:
            log.warning("dropped %d records", event.offset - expected)
        expected = event.offset + 1
        handle(event.payload)
```

That is worth writing even when you do not resume from offsets. It is the only
signal that the queue overflowed.

### A consumer that survives a restart

The pattern the offset semantics exist for: checkpoint what you handled, and
resume at the next one.

```python
def run(client, checkpoint):
    start = checkpoint.load()            # None on a cold start
    start = start + 1 if start is not None else "earliest"

    while True:
        try:
            with client.subscribe("t1", "default", "events", start=start) as events:
                for event in events:
                    handle(event.payload)
                    checkpoint.save(event.offset)
                    start = event.offset + 1
        except felix.ConnectionError:
            # Worth retrying, and the client has already tried other brokers.
            # Resuming from `start` is what makes it lossless.
            time.sleep(1)
        except felix.CursorError:
            # Retention discarded the offset. The only recovery is to accept
            # the gap and say so — silently restarting at the tail would lose
            # records without telling anyone.
            log.error("checkpoint %s is past retention; restarting at earliest", start)
            start = "earliest"
```

Note the two `except` clauses do different things. That is the point of typed
errors: the recovery differs, so the identity has to.

## Errors you can act on

```python
try:
    client.publish("t1", "default", "orders", payload)
except felix.ConnectionError:
    retry()          # worth another attempt, against another broker
except felix.AuthError:
    give_up()        # no amount of retrying grants a permission
except felix.NotFoundError:
    create_stream()  # the stream will not start existing because you retried
```

| Exception | What it means | Retry? |
| --- | --- | --- |
| `ConnectionError` | broker unreachable, or the connection died mid-call | yes, elsewhere |
| `AuthError` | token rejected, or missing the permission | no |
| `NotFoundError` | no such tenant, namespace, stream or cache | no |
| `CursorError` | the start offset is gone; retention discarded it | no — restart at `earliest` |
| `FelixError` | the base, and anything unclassified | judge by case |

Never match on the message. It is prose and it will be reworded; that is
exactly what the exception types exist to spare you.

## Queues

A queue is not a subscription with extra steps. Records are **pulled**, because
only the consumer knows when it has capacity; each is claimed by one member
until settled; and an unsettled record comes back.

```python
while True:
    records = client.group_poll(
        "t1", "default", "orders", shard, "billing",
        max_records=32,
        wait=5.0,          # long poll; an empty list is an answer, not an error
    )
    for record in records:
        try:
            charge(record.payload)
            client.group_ack("t1", "default", "orders", shard, "billing", record.offset)
        except Retryable:
            # Back to the queue now, rather than after the visibility timeout.
            client.group_nack("t1", "default", "orders", shard, "billing", record.offset)
```

`record.attempts` counts deliveries **including this one**, so `1` is a first
attempt and anything higher is a redelivery. It is how a consumer tells a retry
from a first try — worth branching on before doing anything expensive or
side-effecting:

```python
if record.attempts > 3:
    quarantine(record)
    client.group_ack(...)      # settle it; it is not coming back
else:
    process(record)
```

**A group is bound to one shard.** Consuming a multi-shard stream means polling
each shard's group — `stream_shards` says how many there are.

### Dead letters

Past the attempt bound a record is set aside rather than retried forever:

```python
for offset in client.group_dead_letters("t1", "default", "orders", shard, "billing"):
    if fixed_the_cause:
        client.group_redrive("t1", "default", "orders", shard, "billing", offset)
    else:
        client.group_discard("t1", "default", "orders", shard, "billing", offset)
```

## Cache watches

What makes the cache a state-synchronisation primitive rather than a
notification: resume by offset, and loss that is loud.

```python
with client.watch_cache(
    "t1", "default", "sessions",
    felix.CacheWatchFilter.prefix("room:42:"),
    retained=True,
) as watch:
    # Retained values arrive first, and the count says exactly how many. Zero is
    # a definite answer — the prefix is empty — not a silence to wait through.
    roster = {}
    for _ in range(watch.retained_count):
        change = watch.recv()
        roster[change.key] = change.value

    # State is now complete. Everything after this is live.
    for item in watch:
        if isinstance(item, felix.CacheWatchLagged):
            # Not an error: the watch did its job by saying so. Offsets on a
            # filtered watch are sparse, so loss cannot be inferred the way a
            # stream subscriber infers it — this is the only signal.
            watch = client.watch_cache(..., start=item.resume_from)
            continue
        if item.value is None:
            roster.pop(item.key, None)     # a delete is a change with no value
        else:
            roster[item.key] = item.value
```

Three things in that example are load-bearing:

- **`retained_count`** tells you the moment your state is complete. Without it
  you are guessing when to start trusting the map.
- **`value is None` means removed**, and is deliberately distinguishable from
  an empty value. A watcher mirroring a cache has to tell those apart.
- **`CacheWatchLagged` is a value, not an exception.** Re-watching from
  `resume_from` is gapless.

A prefix watch reads **one shard**, so a prefix spanning a multi-shard cache
needs one watch per shard.

## Multi-shard streams

A subscription reads one shard. `subscribe_sharded` opens one per shard,
follows each shard's own owner, and merges them:

```python
with client.subscribe_sharded("t1", "default", "orders") as stream:
    for item in stream:
        if isinstance(item, felix.ShardRecord):
            handle(item.event.payload)
        elif isinstance(item, felix.ShardLost):
            # Surfaced rather than swallowed: the other shards carry on, so a
            # consumer that ignored this would be reading part of the stream
            # while believing it read all of it.
            alert(item.shard, item.error)
        elif isinstance(item, felix.ShardRecovered):
            log.info("shard %d back", item.shard)
```

Resuming is a **map**, not a number, because offsets are per shard:

```python
positions = stream.positions()     # {shard: last offset handled}
# ... later, after a restart
resumed = client.subscribe_sharded("t1", "default", "orders", resume=positions)
```

A single offset carried across shards would replay on every shard but one.

:::caution[A shard that delivered nothing has no position]
`positions()` only lists shards that handed something over. On resume, a shard
not in the map starts wherever `start` says — which is the live tail by
default, so records published to it while you were away are missed. If that
matters, pass `start="earliest"` alongside `resume`, or make sure every shard
has reported before you checkpoint.
:::

## Concurrency

One client is meant to be shared. The synchronous surface blocks with the GIL
released, so a thread pool over one client is genuine parallelism:

```python
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    pool.map(lambda p: client.publish("t1", "default", "events", p), payloads)
```

Creating a client per thread is the thing to avoid: each is a QUIC endpoint
with its own connection pool, and they do not share discovery.

## What is not wrapped

- **`at_least_once` with a routing key** — the re-send path does not carry one,
  and the client refuses the combination rather than silently dropping the key.
- **Idempotent producers** (`producer_init` / `publish_idempotent`) — the Rust
  client has them; this binding does not wrap them yet.

Both are marked in the conformance catalogue, so the binding reports them as
unclaimed rather than passing over them in silence.

## Conformance

Python passes every required scenario in the
[client conformance catalogue](/felix/clients/overview/#the-conformance-suite),
and CI is gated on it. The suite runs against a real three-node cluster rather
than a mock, because what it is checking — reconnection, redirect-following,
offset accounting — only exists in a cluster.
