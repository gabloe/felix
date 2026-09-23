# felix-broker

The broker core of [Felix](https://github.com/gabloe/felix): stream registry,
fanout, per-subscriber queues, and the commit ordering a publish passes through.

**This crate is on crates.io so that `felix-client` can be.** `felix-client`'s
optional `in-process` feature declares it as a dependency, and crates.io
resolves an optional dependency like any other. Running a broker means the
`felix-broker` binary from a [release](https://github.com/gabloe/felix/releases)
or a container image, not this library.

AGPL-3.0-only — running a modified Felix as a network service means publishing
your changes. `felix-client` itself is Apache-2.0 and a default build pulls none
of this. See
[LICENSING.md](https://github.com/gabloe/felix/blob/main/LICENSING.md).
