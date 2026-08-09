# Licensing

Felix uses a split license: the wire protocol and client SDK are
permissively licensed to keep the ecosystem open, while the broker and
control-plane server components are source-available under a license that
prevents third parties from offering Felix as a competing hosted/managed
service.

| Path | License | Why |
|---|---|---|
| `crates/felix-wire/` | Apache-2.0 | The wire protocol. Anyone should be able to implement a Felix client or server in any language without friction. |
| `crates/felix-client/` (default build) | Apache-2.0 | The Rust client SDK. Embeddable in your own products without restriction. |
| `crates/felix-transport/` | Apache-2.0 | Generic QUIC transport plumbing, not Felix-specific server logic. |
| `crates/felix-common/` | Apache-2.0 | Shared IDs/config/error types used by both client and server code. |
| `crates/felix-conformance/` | Apache-2.0 | Conformance test runner and wire-format test vectors, so third-party client/server implementations can validate interop. (It links against the Elastic-2.0 crates below to run its checks against the reference broker — that's normal for a dev/CI tool and doesn't change its own license.) |
| `crates/felix-broker/`, `felix-storage`, `felix-metadata`, `felix-authz`, `felix-crypto`, `felix-consensus`, `felix-router` | Elastic License 2.0 | Server-side core logic. |
| `services/broker/`, `services/controlplane/`, `services/agent/` | Elastic License 2.0 | The runnable server binaries. |

The root [`LICENSE`](LICENSE) file is Elastic License 2.0 (the license for
the project as a whole / the deployable server). [`LICENSE-APACHE`](LICENSE-APACHE)
holds the Apache-2.0 text; each Apache-licensed directory above also carries
its own `LICENSE` file, which takes precedence for that subtree.

## What Elastic License 2.0 actually restricts

In short: you can self-host, modify, and build on Felix's server components
freely. The one thing you can't do is offer Felix itself to third parties as
a hosted/managed service that gives them substantially all of its
functionality — i.e., you can't stand up "Felix as a Service" and compete
with an official hosted offering. See the full text in [`LICENSE`](LICENSE)
or https://www.elastic.co/licensing/elastic-license.

(The license's "license key functionality" clause is boilerplate from the
standard ELv2 text — Felix has no license-key mechanism today, so it's
inapplicable rather than something to work around.)

## `felix-client`'s `in-process` feature

`felix-client` has an optional `in-process` Cargo feature that embeds a
`felix-broker::Broker` directly for local dev/testing without a real QUIC
connection. `felix-broker` is Elastic-2.0, so enabling this feature pulls
Elastic-2.0 code into your build — it is **off by default** specifically so
the default `felix-client` dependency graph stays 100% Apache-2.0. Only
enable `in-process` if you're comfortable with that (Felix's own demos and
test suite do).

## Contributions

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the CLA process. Because of the
split above, which license a given contribution ultimately sits under
depends on which path it touches.
