# RBAC Model (Control Plane)

This document describes Felix RBAC mutation and delegation semantics.

## Security Goals

- Keep policy evaluation simple and predictable (allow rules only).
- Prevent privilege escalation by enforcing scope containment at write time.
- Separate RBAC-administration permissions from resource-operation permissions.
- Keep tenant boundaries explicit in every object string.

## Actions

Canonical actions:
- `rbac.view`
- `rbac.policy.manage`
- `rbac.assignment.manage`
- `tenant.manage`
- `ns.manage`
- `stream.manage`
- `cache.manage`
- `stream.publish`
- `stream.subscribe`
- `cache.read`
- `cache.write`

## Object Grammar

Valid canonical objects:
- `tenant:{tenant_id}`
- `namespace:{tenant_id}/{namespace}`
- `stream:{tenant_id}/{namespace}/{stream}`
- `cache:{tenant_id}/{namespace}/{cache}`

Allowed wildcards:
- `namespace:{tenant_id}/*`
- `stream:{tenant_id}/{namespace}/*`
- `cache:{tenant_id}/{namespace}/*`

Rejected on writes:
- `tenant:*`
- `stream:*` / `stream:*/*`
- `cache:*` / `cache:*/*`
- any malformed or cross-tenant object

HTTP semantics:
- `400` invalid grammar/action/payload
- `401` missing or invalid bearer token
- `403` caller authenticated but lacks required scope/action

## Delegated RBAC Admin

RBAC endpoints are authorized only by RBAC admin actions:
- list policies/groupings: `rbac.view`
- mutate policies: `rbac.policy.manage`
- mutate assignments: `rbac.assignment.manage`

Delegation safety rule:
- Caller can only mutate policies/assignments whose target objects are within the caller's scope.
- Caller cannot assign a role if any policy on that role exceeds caller scope.

Admin scope examples:
- `rbac.policy.manage:tenant:t1`
  - can create `namespace:t1/*`, `stream:t1/payments/*`, `cache:t1/payments/sessions`
  - cannot create `tenant:*` or any `t2` object
- `rbac.policy.manage:namespace:t1/payments`
  - can create stream/cache policies only under `payments`
  - cannot create `namespace:t1/orders` or `tenant:t1`
- `rbac.policy.manage:stream:t1/payments/orders`
  - can only manage that single stream object
  - cannot create `stream:t1/payments/*`

Examples:
- `rbac.policy.manage:namespace:t1/payments` can manage rules in `payments` only.
- `rbac.policy.manage:stream:t1/payments/orders` cannot grant `stream:t1/payments/*`.
- `rbac.policy.manage:tenant:t1` can manage tenant `t1` RBAC, but still cannot use `tenant:*`.

## Notes

- Strict grammar is enforced for RBAC evaluation and writes.
- Group-claim RBAC is evaluated at token exchange by materializing transient
  `principal -> group:*` links before Casbin permission expansion.
