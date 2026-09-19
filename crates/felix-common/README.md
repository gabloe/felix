# felix-common

Identifiers, configuration types, and errors shared across the
[Felix](https://github.com/gabloe/felix) crates.

You do not normally depend on this directly — `felix-client` re-exports what a
client needs. It is a separate crate so that the client and the broker cannot
drift on what a tenant id, a shard key, or a typed error *is*.

Apache-2.0.
