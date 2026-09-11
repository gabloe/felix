# The cache is a projection over the log

**Decision: a cache is a log. `put` and `delete` append records, an in-memory
index maps key to offset, and `get` reads the log. The separate in-memory
key/value store is kept only for brokers configured without durable storage.**

This is what `docs/architecture.md` has claimed from the start — "one core log,
many semantics", with the cache as "key → latest value with TTL, backed by the
same log". Until now that was aspiration stated as description: the cache was a
`RwLock<HashMap>` alongside the stream registry, sharing no code, no durability,
and no replication with it.

## What the log already gives, free

Everything a durable cache needs is already built and already tested, because
streams needed it first:

- **Crash safety.** CRC-verified records, torn-tail repair, interior corruption
  refused rather than silently accepted.
- **Group commit and fsync policy.** A cache write is an append, so it inherits
  the same throughput lever.
- **Replication.** The driver ships log records and does not care what semantic
  reads them. A cache on the log is replicated by the code that already
  replicates streams, once its shards are placed (#240 and the cache-routing
  work that follows this).

That last point is the argument for doing it this way rather than bolting
durability onto a hash map: the alternative is a second durability path and a
second replication path, which is exactly what the storage design set out not to
have.

## Where a cache lives on disk

Under `<root>/caches/`, with a provider of its own; streams stay at `<root>/`.

A shard directory is named from a hash of `(tenant, namespace, stream)`, so a
cache and a stream with the same name in the same namespace would otherwise
land in the same directory and interleave their records. Separating the roots
makes that impossible rather than unlikely, costs nothing, and leaves the
existing stream layout untouched — no migration, because no stream path
changes.

Nothing enumerates the storage root (recovery scans a *shard* directory for
segment files), so an extra subdirectory under it is inert.

## The record format

One record per write. Little-endian, and versioned in its first byte, because
this is a durable format and a reader years from now has only these bytes.

<p align="center">
  <img src="assets/storage/cache-record.svg" alt="Cache record byte layout: a one-byte version, a one-byte op, an eight-byte absolute expiry, a four-byte key length, then the key and — for a put — the value" width="760">
</p>

- `op` is `0` for a put and `1` for a delete. A delete carries no value.
- `expires_at_millis` is absolute Unix milliseconds, `0` meaning "never".
  Absolute rather than a duration: a record read back after a restart has to
  mean the same thing it meant when written, and a duration would silently
  restart its life on every recovery.
- `key_len` bounds the key so the value needs no length of its own — it is
  simply the rest of the record, which the segment framing already delimits.

An unknown version is refused rather than guessed at, for the same reason an
unknown frame flag is: a misread record is worse than an unreadable one.

## The index is derived, never trusted

`key -> (offset, expires_at)`, held in memory and rebuilt by replaying the log
when the cache is opened. This is the same rule the segment indexes follow, and
for the same reason: anything that can be recomputed from the log must be,
because then it can never be stale in a way that matters.

**The index holds offsets, not values.** The log is the store; memory holds only
where to look. This costs a read per `get` that a hash map would not pay, and it
is what makes the durability real rather than a write-behind of an in-memory
map that is still the actual source of truth. Caching hot values in memory in
front of this index is a straightforward later optimisation, and deliberately
not part of making the claim true.

## TTL

Checked at read against the record's own `expires_at_millis`, which is lazy
expiry — the same behaviour the in-memory cache always had. An expired entry is
reported as absent immediately; the space it occupies is reclaimed by
compaction.

## Compaction

Without it the log grows forever, and "the cache is a log" would be a slow leak
rather than a design.

Compaction writes the live set — the newest surviving record for each key that
is neither deleted nor expired — into a fresh log, then swaps it in. **Records
are never rewritten**, which is the invariant everything in the storage layer
rests on: compaction produces new segments and drops old ones, and never edits a
byte in place.

It runs when the log has grown past a multiple of its live bytes, so the cost is
proportional to the garbage and a cache that is mostly live is never compacted.
Writes are excluded for the duration; a cache write is already serialised behind
the index lock, so this adds no new contention, only a longer hold.

## What this does not do yet

Cache operations are not routed. Every broker serves its own cache, so a `put`
on one broker is invisible to a `get` on another, and the control plane does not
place cache shards. That is the next piece, and it is the same machinery
publishes and subscribes already use — which is the point of putting the cache
on the log first.
