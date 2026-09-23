# felix-router

Placement and routing for multi-node [Felix](https://github.com/gabloe/felix)
deployments: which broker owns a shard, and where a request goes when the broker
that received it does not.

Placement is a pure function of a metadata snapshot, so two control-plane
instances reading the same catalog reach the same answer without having to agree
on one. Region awareness here is an allowlist of permitted pairs; explicit
cross-region bridges are not built.

Part of the server side, published so the workspace resolves from the registry
as it does from a checkout.

AGPL-3.0-only. See
[LICENSING.md](https://github.com/gabloe/felix/blob/main/LICENSING.md).
