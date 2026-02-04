# Auth Design: OIDC Exchange + Felix Tokens

This document describes the current Felix authentication and authorization design as implemented in the control plane and the broker. It aligns with the `felix-authz` crate, Casbin RBAC model, and the token exchange flow in the control plane.

Detailed RBAC mutation/delegation rules and canonical grammar are documented in `docs/security/rbac.md`.

## Goals

- Multi-tenant authentication and authorization.
- Support multiple OIDC issuers per tenant.
- Brokers validate only Felix tokens (never upstream IdP tokens).
- Embed effective permissions in Felix tokens for fast broker-side enforcement.

## Components

- **Control plane**: Validates upstream OIDC tokens, evaluates RBAC policies, and mints Felix tokens.
- **felix-authz**: JWT mint/verify utilities and permission parsing/matching shared by control plane and broker.
- **Broker**: Verifies Felix token signature/claims and enforces permissions locally.

## Supported Identity Providers

Felix supports any OIDC-compliant IdP that exposes a JWKS endpoint (either via OIDC discovery or a direct `jwks_url`) and uses an allowed upstream OIDC signing algorithm.

Supported upstream OIDC JWT signing algorithms:
- `ES256` (default)
- `RS256`, `RS384`, `RS512`
- `PS256`, `PS384`, `PS512`

Control plane configuration:
- YAML: `oidc_allowed_algorithms: ["ES256"]`
- Env: `FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS=ES256,RS256,...`

This includes common providers like:

- Microsoft Entra ID
- Okta
- Auth0
- Google
- Apple

Other providers are supported as long as they provide standard OIDC discovery or a JWKS URL and the tokens include `iss`, `sub`, and `aud` claims.

## Token Types

### Upstream OIDC token

- Provided by an external IdP (Entra ID, Okta, Auth0, Google, Apple, etc.).
- Validated by the control plane using issuer discovery + JWKS.
- Claims used:
  - `iss` (issuer)
  - `sub` (subject)
  - optional groups/roles claim

### Felix token (JWT)

Minted by the control plane and verified by brokers.

Claims:
- `iss`: `felix-auth`
- `aud`: `felix-broker`
- `sub`: `principal_id` (sha256 of `iss|sub`)
- `tid`: tenant id
- `exp`, `iat`
- `perms`: array of permission strings

Algorithm:
- **EdDSA (Ed25519)** (tenant-specific signing key).
- Header includes `kid`.

## Principal Normalization

- `principal_id = sha256(iss + "|" + sub)` (hex encoding).
- Groups/roles are extracted from a configured claim (optional).
- During token exchange, each extracted group is added as a transient role link:
  - `g, <principal_id>, group:<group_name>, <tenant>`
  - This allows RBAC bindings like `g, group:g1, role:reader, <tenant>`.

## RBAC Model (Casbin)

Casbin is used with domains for tenant scoping.

- **Domain**: tenant id
- **Role link**: `g = user, role, domain`
- **Policy rule**: `p = role, domain, object, action`
- **Matcher**: `keyMatch2` for object pattern matching

Objects:
- Tenant: `tenant:{tenant_id}`
- Namespace: `namespace:{tenant_id}/{namespace}` or `namespace:{tenant_id}/*`
- Stream: `stream:{tenant_id}/{namespace}/{stream}` or `stream:{tenant_id}/{namespace}/*`
- Cache: `cache:{tenant_id}/{namespace}/{cache}` or `cache:{tenant_id}/{namespace}/*`

Actions:
- `rbac.view`, `rbac.policy.manage`, `rbac.assignment.manage`
- `tenant.manage`, `ns.manage`, `stream.manage`, `cache.manage`
- `stream.publish`, `stream.subscribe`
- `cache.read`, `cache.write`

### RBAC Security Model

- RBAC mutation is **not** authorized by resource-management actions.
  - Example: `ns.manage` does not permit policy/grouping writes.
- RBAC APIs are split by intent:
  - list policies/groupings: `rbac.view`
  - mutate policy rules: `rbac.policy.manage`
  - mutate assignments/groupings: `rbac.assignment.manage`
- Delegation is scope-bound:
  - callers can only create/delete rules within their own object scope
  - callers cannot assign a role if any role policy exceeds caller scope
- Write-time validation rejects broad or malformed wildcards (for example `tenant:*`).

### Inheritance Expansion

When building effective permissions for a principal:
- `tenant.manage:tenant:{T}` implies tenant-scoped namespace/stream/cache manage + read/write actions.
- `ns.manage:namespace:{T}/{X}` implies stream/cache manage + read/write within namespace `{X}`.

### Group-Based Role Assignment (from IdP `groups_claim`)

You can assign roles to IdP groups without per-user RBAC bindings:

- Add RBAC grouping rules that bind `group:<name>` to a role.
- Configure `groups_claim` for the tenant IdP issuer.
- At token exchange time, Felix maps each incoming group value to `group:<name>`
  (if not already prefixed) and evaluates RBAC through that link.

Example:
- `g, group:g1, role:reader, tenant-a`
- `p, role:reader, tenant-a, stream:tenant-a/payments/*, stream.subscribe`

## Control Plane Token Exchange Flow

1) Client obtains an upstream OIDC JWT from its IdP.
2) Client calls:
   - `POST /v1/tenants/{tenant_id}/token/exchange`
   - `Authorization: Bearer <oidc_jwt>`
3) Control plane validates:
   - `iss` matches a tenant-allowed issuer
   - signature using JWKS (cached with TTL)
   - `exp/nbf` with clock skew
   - `aud` matches configured audiences
4) Control plane derives `principal_id` and loads RBAC policies/groupings for the tenant.
5) Effective permissions are computed from Casbin and expanded for inheritance.
6) Optional request filters reduce the permission set (`requested` / `resources`).
7) A Felix token is minted and returned.

If no permissions remain, the exchange returns `403`.

## Bootstrap Mode (Day-0)

Felix includes a **one-time operator bootstrap** flow to initialize tenant auth before any admin tokens exist.

Why it exists:
- Admin endpoints require a Felix token with explicit management permissions.
- But Felix tokens require IdP issuer configuration and signing keys.
- Bootstrap fills that gap once per tenant, then is disabled.

### How it works

1) Operator enables bootstrap on the control plane:
   - `FELIX_BOOTSTRAP_ENABLED=true`
   - `FELIX_BOOTSTRAP_BIND_ADDR=127.0.0.1:9095` (or cluster-internal address)
   - `FELIX_BOOTSTRAP_TOKEN=<random secret>`
2) Operator calls the internal endpoint:
   - `POST /internal/bootstrap/tenants/{tenant_id}/initialize`
   - header `X-Felix-Bootstrap-Token: <token>`
3) Control plane:
   - creates the tenant if missing
   - generates signing keys (EdDSA / Ed25519)
   - seeds IdP issuers
   - seeds RBAC policies + groupings
   - marks the tenant as bootstrapped
4) Operator disables bootstrap after use.

### Example

```
POST /internal/bootstrap/tenants/t1/initialize
X-Felix-Bootstrap-Token: <secret>
Content-Type: application/json

{
  "display_name": "Tenant One",
  "idp_issuers": [
    {
      "issuer": "https://login.microsoftonline.com/<tenant>/v2.0",
      "audiences": ["api://felix-controlplane"],
      "discovery_url": null,
      "jwks_url": null,
      "claim_mappings": {
        "subject_claim": "sub",
        "groups_claim": "groups"
      }
    }
  ],
  "initial_admin_principals": ["p:alice"]
}
```

### After bootstrap

Auth admin permissions:
- IdP issuer admin endpoints require `tenant.manage`.
- RBAC policy/grouping endpoints allow either:
  - `rbac.policy.manage` / `rbac.assignment.manage` at tenant scope (`tenant:{tenant_id}`), or
  - those same actions at narrower namespace/stream/cache scopes.
Bootstrap tokens **never** authorize normal admin endpoints.

## Broker Validation Flow

1) Client connects to a broker with `tenant_id` and a Felix token.
2) Broker verifies the token using tenant JWKS fetched from:
   - `GET /v1/tenants/{tenant_id}/.well-known/jwks.json`
3) Broker validates claims:
   - `iss = felix-auth`
   - `aud = felix-broker`
   - `exp/nbf`
   - `tid` matches connection tenant
4) Broker parses `perms` once at connect time and enforces per operation using keyMatch2.

## Data Stored Per Tenant

In the control plane store (memory or Postgres):
- Allowed OIDC issuers + audiences + claim mappings
- Casbin policies and groupings
- Tenant signing keys (current + previous for rotation)

## Allowing Particular Upstream IdPs

IdP trust is **configured per tenant**. The control plane only accepts tokens from issuers listed for that tenant, and only for configured audiences.

### Admin API (preferred)

IdP issuer endpoints require a Felix token with `tenant.manage` for the tenant.
RBAC endpoints require explicit RBAC actions (`rbac.view`, `rbac.policy.manage`, `rbac.assignment.manage`).

Use the control plane admin endpoints to manage IdP issuers per tenant:

```
POST /v1/tenants/{tenant_id}/idp-issuers
Content-Type: application/json

{
  "issuer": "https://login.microsoftonline.com/<tenant>/v2.0",
  "audiences": ["api://felix-controlplane"],
  "discovery_url": null,
  "jwks_url": null,
  "claim_mappings": {
    "subject_claim": "sub",
    "groups_claim": "groups"
  }
}
```

Delete an issuer:

```
DELETE /v1/tenants/{tenant_id}/idp-issuers/{issuer}
```

### Postgres (control plane store)

Insert or update the issuer in the `idp_issuers` table:

```sql
INSERT INTO idp_issuers (
  tenant_id,
  issuer,
  audiences,
  discovery_url,
  jwks_url,
  subject_claim,
  groups_claim
) VALUES (
  't1',
  'https://login.microsoftonline.com/<tenant>/v2.0',
  '["api://felix-controlplane"]'::jsonb,
  NULL,
  NULL,
  'sub',
  'groups'
)
ON CONFLICT (tenant_id, issuer) DO UPDATE SET
  audiences = EXCLUDED.audiences,
  discovery_url = EXCLUDED.discovery_url,
  jwks_url = EXCLUDED.jwks_url,
  subject_claim = EXCLUDED.subject_claim,
  groups_claim = EXCLUDED.groups_claim;
```

Notes:
- If `discovery_url` is `NULL`, the control plane uses `{iss}/.well-known/openid-configuration`.
- `jwks_url` can be set directly if your IdP doesn’t support discovery.
- `subject_claim` defaults to `sub`. `groups_claim` is optional.

### In-Memory Store (dev/tests)

Use the store API to add issuers during initialization:

```rust
store
  .upsert_idp_issuer(\"t1\", IdpIssuerConfig {
      issuer: \"https://example-idp\".to_string(),
      audiences: vec![\"felix-controlplane\".to_string()],
      discovery_url: None,
      jwks_url: Some(\"https://example-idp/.well-known/jwks.json\".to_string()),
      claim_mappings: ClaimMappings {
          subject_claim: \"sub\".to_string(),
          groups_claim: Some(\"groups\".to_string()),
      },
  })
  .await?;
```

The token exchange endpoint will reject upstream tokens if:
- the issuer is not listed for the tenant
- the audience does not match
- the signature or standard claims are invalid

## HTTP Endpoints

### Token Exchange

```
POST /v1/tenants/{tenant_id}/token/exchange
Authorization: Bearer <oidc_jwt>
Content-Type: application/json

{
  "requested": ["stream.publish", "cache.read"],
  "resources": ["namespace:t1/payments", "stream:t1/payments/orders/*"]
}
```

Response:

```
{
  "felix_token": "<jwt>",
  "expires_in": 900,
  "token_type": "Bearer"
}
```

### Tenant JWKS

```
GET /v1/tenants/{tenant_id}/.well-known/jwks.json
```

Response:

```
{
  "keys": [
    {
      "kty": "OKP",
      "kid": "k1",
      "alg": "EdDSA",
      "use": "sig",
      "crv": "Ed25519",
      "x": "..."
    }
  ]
}
```

## Example Policies

```text
# policy rules (p)
# p, <role>, <tenant>, <object>, <action>
p, role:tenant-admin, tenant-a, tenant:tenant-a, tenant.manage
p, role:tenant-admin, tenant-a, tenant:tenant-a, rbac.policy.manage
p, role:payments-admin, tenant-a, namespace:tenant-a/payments, ns.manage
p, role:publisher, tenant-a, stream:tenant-a/payments/*, stream.publish

# groupings (g)
# g, <user>, <role>, <tenant>
g, p:alice, role:tenant-admin, tenant-a
g, p:bob, role:payments-admin, tenant-a
```

## Error Semantics

- `401 Unauthorized`: invalid or missing upstream token
- `403 Forbidden`: tenant not allowed, issuer not allowed, or no permissions
- `500 Internal Server Error`: unexpected backend failures

## Future Work

- Add control plane API auth for administrative endpoints.
- mTLS and service-to-service auth for brokers and internal components.
- Policy editor and audit logging for auth decisions.
- More efficient permission compression for large policy sets.
