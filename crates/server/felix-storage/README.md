# felix-storage

The log-structured segment store behind [Felix](https://github.com/gabloe/felix)
durable streams: segments, sparse indexes, torn-tail repair, and group commit.

Records are never rewritten, so recovery can trust that valid bytes end at EOF.
A torn tail is repaired; interior corruption is fatal, because refusing to start
beats silently losing acknowledged records. Indexes are derived and never
trusted — a missing, short or stale one is rebuilt from the segment it
describes.

**This crate is on crates.io so that `felix-client` can be**, through its
optional `in-process` feature. See
[LICENSING.md](https://github.com/gabloe/felix/blob/main/LICENSING.md).

AGPL-3.0-only.
