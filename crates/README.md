# Crates

Every library and tool in the workspace, grouped by who uses it. The
deployable binaries are in [`../services`](../services).

| Group | Crates | What they are |
|---|---|---|
| [`protocol/`](protocol) | `felix-wire`, `felix-transport` | How a client and a broker talk: the frame codec and the QUIC layer under it. |
| [`server/`](server) | `felix-broker`, `felix-storage`, `felix-router`, `felix-authz`, `felix-common` | The libraries the broker and control plane are built from. |
| [`sdk/`](sdk) | `felix-client`, `felix-python`, `felix-typescript` | What an application links to use Felix. |
| [`testing/`](testing) | `felix-cluster`, `felix-conformance`, `felix-loadgen` | Harnesses, the client conformance kit, and the load generator. |

Each directory is named after the package it holds, so `-p felix-wire` lives in
`crates/protocol/felix-wire`. A new crate goes in the group whose users it
shares; if none fits, that is worth a conversation before adding a fifth group.

Dependencies point one way: `testing` and `sdk` depend on `protocol`, `server`
depends on `protocol`, and nothing in `protocol` depends on anything else here.
The one exception is `felix-client`'s off-by-default `in-process` feature,
which embeds `felix-broker`.
