# felix-authz

Token issuance and authorization for [Felix](https://github.com/gabloe/felix):
OIDC token exchange, tenant-scoped EdDSA tokens, and RBAC evaluated at the
broker with delegation rules that prevent privilege escalation.

Part of the server side. A client does not need this — it holds a token and the
broker decides what it may do. Published so the workspace resolves from the
registry as it does from a checkout.

AGPL-3.0-only. See
[LICENSING.md](https://github.com/gabloe/felix/blob/main/LICENSING.md).
