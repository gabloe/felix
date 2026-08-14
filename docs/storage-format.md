# Felix Durable Segment Format (v2)

This document defines the on-disk representation of a durable Felix stream. It is
the source of truth for anyone reading, writing, repairing, or replicating
segment files, and it is intentionally independent of `felix-wire`: network
framing is allowed to change shape for latency reasons, while bytes already on
disk must stay readable by every later build.

Implementation: [`crates/felix-storage/src/segment/format.rs`](../crates/felix-storage/src/segment/format.rs).
The two must move together — a change to one without the other is a bug.

## Goals

- **Versioned.** A reader that does not understand a file says so instead of
  guessing.
- **Self-validating.** Every record carries a checksum over its own header and
  payload, so bit rot and torn writes are detected on read, not inferred.
- **Skippable.** The next record's position is derivable without decoding the
  current one's payload, which is what makes index rebuilds and recovery scans
  cheap.
- **Bounded.** No length read from disk is used to size an allocation before it
  has been range-checked.

## Conventions

- All integers are **big-endian**, matching `felix-wire`.
- Offsets are **logical**: a `u64` that is stable for the life of a record and
  independent of where it lands in a file.
- A *segment* is one file of records. A *shard* is a directory of segments plus
  their indexes.

### Magic numbers

Both file kinds start with a four-byte ASCII magic sharing the prefix `FLS` —
**F**e**L**ix **S**egment — with the last byte naming the kind:

| Magic | ASCII | Expands to | File |
| --- | --- | --- | --- |
| `0x464C5347` | `FLSG` | Felix Se**g**ment | segment data (`*.log`) |
| `0x464C5349` | `FLSI` | Felix Segment **I**ndex | sparse offset index (`*.index`) |

They are deliberately distinct from `felix-wire`'s frame magic `0x464C5831`
(`FLX1`): storage bytes and network bytes are separate formats with separate
versioning, and a file that turned up on a socket — or a frame that turned up in
a segment — should be rejected on its first four bytes rather than misparsed.

## Directory layout

```text
<root>/
  acme_default_orders_0-0d3aed4b998d2798/     ← one directory per stream shard
    00000000000000000000.log                 ← segment data
    00000000000000000000.index               ← sparse offset index
    00000000000000000001.log
    00000000000000000001.index
```

The directory name is a readable rendering of the `ShardKey` plus an FNV-1a hash
of the exact key. The readable part is lossy — anything outside `[A-Za-z0-9-]`
becomes `_`, and components are truncated — so the hash is what guarantees
uniqueness. Dots are excluded deliberately: with no dots in the name, a
component like `..` is not merely escaped but unrepresentable.

File names are zero-padded to 20 digits so lexicographic and numeric order agree.
Recovery still parses the number and sorts on it rather than trusting directory
iteration order, which is filesystem-defined.

## Segment file

A 32-byte header, then records back to back, with a sparse index in a companion
file:

<p align="center">
  <img src="assets/storage/segment-file.svg" alt="A segment file: a 32-byte header followed by variable-length records, with a sparse index file whose entries point at record boundaries" width="900">
</p>

### Segment header (32 bytes)

<p align="center">
  <img src="assets/storage/segment-header.svg" alt="Segment header byte layout: magic, version, flags, base_offset, created_at_micros, header_crc, reserved" width="882">
</p>

| Offset | Size | Field | Value |
| --- | --- | --- | --- |
| 0 | 4 | `magic` | `0x464C5347` (`"FLSG"`) |
| 4 | 2 | `version` | `2` |
| 6 | 2 | `flags` | `0`; any other value is rejected |
| 8 | 8 | `base_offset` | logical offset of this segment's first record |
| 16 | 8 | `created_at_micros` | wall clock at creation, informational |
| 24 | 4 | `header_crc` | CRC-32 (IEEE) over bytes `0..24` |
| 28 | 4 | `reserved` | `0` |

The header is written once and fsynced before any record claims to live in the
segment, so damage here is never a torn write — it is always an error.

### Record

<p align="center">
  <img src="assets/storage/record.svg" alt="Record byte layout: payload_len, offset, timestamp_micros, checksum, then the payload. The checksum covers bytes 0 to 20 and the payload" width="882">
</p>

| Offset | Size | Field | Notes |
| --- | --- | --- | --- |
| 0 | 4 | `payload_len` | ≤ `MAX_PAYLOAD_BYTES` (64 MiB) |
| 4 | 8 | `offset` | logical offset; ascends by exactly 1 within a segment |
| 12 | 8 | `timestamp_micros` | publish time |
| 20 | 4 | `header_crc` | CRC-32 over bytes `0..20` |
| 24 | 4 | `checksum` | CRC-32 over bytes `0..24` **followed by** the payload |
| 28 | n | `payload` | opaque bytes |

`header_crc` is what makes recovery decidable. It is verified *before* any other
field is used, so `payload_len` is only ever acted on once it is known to be
intact. Without it, a bit flip in the length field produced exactly the same
symptom as an unfinished write — a record claiming more bytes than the file
holds — and recovery had to guess. Guessing wrong meant silently truncating a
record that had been fsynced and acknowledged.

`payload_len` comes first and is covered by both checksums, so a reader can step
to the next record with `position + 28 + payload_len` without touching payload
bytes. That property is what makes the index rebuild and the recovery scan cost
proportional to record *count* rather than to bytes decoded.

The checksum covers the header prefix as well as the payload, so a corrupted
offset or timestamp is caught by the same check as a corrupted payload.

## Index file

Index files accelerate reads and are **never trusted**. Every entry is used only
as a starting position for a scan that re-validates real records, and any index
that fails to load — missing, short, wrong generation, garbage — is rebuilt from
its segment. Consequently they carry no checksums.

### Index header (24 bytes)

<p align="center">
  <img src="assets/storage/index-header.svg" alt="Index header byte layout: magic, version, flags, base_offset, reserved" width="672">
</p>

| Offset | Size | Field | Value |
| --- | --- | --- | --- |
| 0 | 4 | `magic` | `0x464C5349` (`"FLSI"`) |
| 4 | 2 | `version` | `2` |
| 6 | 2 | `flags` | `0` |
| 8 | 8 | `base_offset` | must equal the segment's `base_offset` |
| 16 | 8 | `reserved` | `0` |

### Index entry (16 bytes)

<p align="center">
  <img src="assets/storage/index-entry.svg" alt="Index entry byte layout: an eight-byte logical offset and an eight-byte file position" width="672">
</p>

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | `offset` |
| 8 | 8 | `position` — byte position of that record in the segment |

Entries are emitted for the segment's first record and thereafter every
`index_spacing_bytes` of segment data. They are strictly ascending by offset,
which is what `seek_position`'s binary search relies on.

A torn final entry — the signature of a crash mid-append — is tolerated on load:
the file is read up to the last whole entry.

## Compatibility and corruption behaviour

| Condition | Behaviour |
| --- | --- |
| Unknown `magic` | Reject: `CorruptionKind::SegmentMagic` / `IndexMagic` |
| Unknown `version` | Reject: `SegmentVersion` / `IndexVersion`. Never "best effort" |
| Non-zero `flags` | Reject: `SegmentFlags`. Unknown bits may change the layout behind them, so they are not masked off |
| Bad header CRC | Reject: `SegmentHeaderChecksum` |
| Short read | `Truncated { needed, available }` — the one shape recovery may repair |
| Bad record header CRC | `RecordHeaderChecksum` — the length cannot be trusted |
| Bad record CRC | `RecordChecksum` |
| `payload_len` over the limit | `RecordTooLarge`, raised *before* any allocation |
| Offset gap within a segment | `OffsetOutOfOrder` |

Every error carries a `CorruptionSite` naming the shard, segment id and byte
position, because "corruption detected" is not enough to act on at 3am.

### What recovery may repair

Recovery truncates **only** damage confined to the end of the newest segment,
because only there can a record have been mid-write when the process died:

- A `Truncated` failure is always repairable. The header verified, so
  `payload_len` is the length the writer intended, and a file ending short of it
  is *provably* an unfinished write — nothing can have acknowledged a record that
  was never finished. This is the ordinary crash case and needs no operator
  involvement.
- A `RecordHeaderChecksum` failure is **not** repairable by default. The header
  cannot be trusted, so this may equally be an unfinished write or a complete,
  acknowledged record whose header rotted. `repair_checksum_tail` opts in.
- A `RecordChecksum` or `OffsetOutOfOrder` failure is likewise opt-in: the header
  verified, so the record is complete on disk and the damage is rot rather than a
  torn write.
- A `RecordTooLarge` failure is opt-in for the same reason — the writer rejects
  oversized records, so a verified header carrying an impossible length is damage
  the checksums did not catch.
- Segment-header damage is never repairable.

The dividing line is whether the length is trustworthy. When it is, recovery can
prove the write was unfinished; when it is not, recovery refuses to choose
between "unfinished" and "rotted" and fails loudly instead.

The full rules live in `is_repairable_tail` in
[`segment/reader.rs`](../crates/felix-storage/src/segment/reader.rs).

## Golden vectors

`format.rs` pins exact bytes for a segment header and a record. Changing either
is a format change: bump `FORMAT_VERSION`, update this document, and state the
migration path.

```text
SegmentHeader::new(base_offset = 1, created_at_micros = 2):
  46 4C 53 47  00 02  00 00
  00 00 00 00 00 00 00 01
  00 00 00 00 00 00 00 02
  7A 09 E5 B1
  00 00 00 00

encode_record(offset = 7, timestamp = 9, payload = "hi"):
  00 00 00 02
  00 00 00 00 00 00 00 07
  00 00 00 00 00 00 00 09
  C6 54 DE 27
  24 02 15 2C
  68 69
```

## Version history

| Version | Change |
| --- | --- |
| 1 | Initial format. Unreleased. |
| 2 | Added `header_crc` to the record header (24 → 28 bytes), making a corrupted length field detectable without reading the payload. |

A v1 segment is rejected on open with `CorruptionKind::SegmentVersion`, naming
the version found. v1 was only ever written by unreleased builds, so the
migration path is to discard the data directory rather than carry a second
decoder — and rejecting is the safe failure, because a v1 record read as v2
would misparse every field after the length.

## Versioning policy

`FORMAT_VERSION` is a single number covering both the segment and index layouts.

- **Additive changes** that keep existing readers correct — new `flags` bits with
  strictly appended data — still require a version bump, because current readers
  reject unknown flags rather than skipping them. That is deliberate: silently
  ignoring a bit that changes the meaning of following bytes is how formats
  become unreadable.
- **Any change to a field's position, width, or meaning** requires a bump and an
  explicit migration path. Segments already on disk are not rewritable in place.

## Limits

| Limit | Value | Why |
| --- | --- | --- |
| `MAX_PAYLOAD_BYTES` | 64 MiB | Bounds the allocation a corrupt length field can request |
| Max records per segment | `u64` offsets, so effectively unbounded | Rollover is driven by size, not count |
| Oversized records | A record larger than `segment_size_bytes` is written to an otherwise-empty segment of its own | Splitting a record across segments would break the "offsets are contiguous within a segment" invariant that recovery depends on |
