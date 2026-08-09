# felix-conformance

Conformance runner for the Felix wire protocol.

It drives a real broker over QUIC and asserts that publish, subscribe, and cache
behaviour match the v1 protocol described in [`docs/protocol.md`](../../docs/protocol.md),
using the test vectors in [`crates/felix-wire/tests/vectors/`](../felix-wire/tests/vectors/).
The intent is that a third-party client or server implementation in any language
can be validated against the same checks the reference implementation passes.

```bash
cargo run -p felix-conformance
```

## Licensing

This crate is Apache-2.0, matching `felix-wire`, so that protocol conformance is
not gated behind a restrictive licence. It links against Elastic-2.0 crates
(`felix-broker`, `felix-storage`, `felix-authz`) to run its checks against the
reference broker — normal for a dev/CI tool, and it does not change this crate's
own licence. See [`LICENSING.md`](../../LICENSING.md).

Because of those dependencies it is marked `publish = false`: it is a test
harness rather than something to depend on from a registry.
