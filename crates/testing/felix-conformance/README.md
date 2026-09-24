# felix-conformance

The Felix client conformance kit: a catalogue of scenarios every client must
pass, and a verifier for a client's results.

```bash
cargo run -p felix-conformance                        # protocol suite against a real broker over QUIC
cargo run -p felix-conformance -- scenarios           # print the catalogue
cargo run -p felix-conformance -- verify results.json # check a client's results
```

With no arguments it drives a broker over QUIC and checks publish, subscribe and
cache behaviour against [`docs/protocol.md`](../../../docs/protocol.md). The
catalogue and `verify` are how the Python and TypeScript clients, and any third
party's, show they behave like the Rust client. `felix-cluster client-fixture`
starts something to run a client against. Byte-level wire fixtures are separate:
they live in [`felix-wire`'s `tests/vectors/`](../../protocol/felix-wire/tests/vectors).

## Licensing

This crate is Apache-2.0, matching `felix-wire`, so that protocol conformance is
not gated behind a copyleft licence. It links against AGPL-3.0 crates
(`felix-broker-service`, `felix-broker`, `felix-storage`, `felix-authz`) to run
its checks against the reference broker — normal for a dev/CI tool, and it does not change this crate's
own licence. See [`LICENSING.md`](../../../LICENSING.md).

Because of those dependencies it is marked `publish = false`: it is a test
harness rather than something to depend on from a registry.
