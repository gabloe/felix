# Services

The two deployables. Each is a thin process around the libraries in
[`../crates/server`](../crates/server).

| Package | Binary | What it is |
|---|---|---|
| [`felix-broker-service`](felix-broker-service) | `felix-broker` | A broker node: serves clients over QUIC, registers with the control plane, owns and replicates shards. |
| [`felix-controlplane-service`](felix-controlplane-service) | `felix-controlplane` | Cluster metadata over REST: tenants, streams, nodes, and which broker leads each shard. |

The package names end in `-service` so they cannot be confused with the
`felix-broker` library, which holds the broker's semantics without any of the
networking.
