# Storage fuzzing

libFuzzer targets for the durable segment format. They exist because segment
bytes are untrusted input: today from a disk that may have rotted or been cut
mid-write, and from M5 onward from a replication peer over the network. A panic
in this decoder is a remote crash.

## Targets

| Target | Input | Asserts |
| --- | --- | --- |
| `segment_record` | one record's bytes | decoding never panics; a successful decode reports exactly the bytes it consumed and re-encodes identically |
| `segment_recovery` | a whole segment file | recovery returns a contiguous, self-consistent prefix or a typed error — never a log with a hole |
| `sparse_index` | an index file | a malformed index loads as `None`; entries stay strictly ascending; every seek lands at or after the segment header |

## Running

```sh
cargo install cargo-fuzz
cd crates/felix-storage/fuzz

# A few minutes each is enough to catch regressions.
cargo +nightly fuzz run segment_record   -- -max_total_time=300
cargo +nightly fuzz run segment_recovery -- -max_total_time=300
cargo +nightly fuzz run sparse_index     -- -max_total_time=300

# Reproduce a crash the fuzzer found.
cargo +nightly fuzz run segment_record artifacts/segment_record/crash-<hash>
```

The crate is deliberately outside the workspace: `cargo-fuzz` requires nightly
and links libFuzzer, and neither belongs in `cargo build --workspace`.

## Seed corpus

`corpus/` is seeded by `cargo test -p felix-storage --test format_fuzz` — the
same properties, driven by a seeded generator so they run in CI on stable and
reproduce exactly. Add interesting inputs there rather than relying on the
fuzzer to rediscover them; a corpus entry is the cheapest possible regression
test.

## What is *not* fuzzed here

Concurrency. Interleavings of append, flush and rollover are covered by
`tests/crash_recovery.rs`, which kills a real process mid-write, because
libFuzzer's single-threaded model cannot express them.
