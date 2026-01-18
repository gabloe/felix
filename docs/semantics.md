# Felix Semantics (MVP)

This document defines the **behavioral contract** for Felix in the single-node MVP.
The goal is predictable p99/p999 behavior, even when the system is under pressure.

## Pub/Sub Delivery Semantics

- **Delivery guarantee:** at-most-once.
- **Ordering:** per-stream ordering is preserved for a given subscriber.
- **Scope:** no ordering guarantees across streams.
- **Acknowledgements:** none in the MVP; publishers do not wait on subscriber acks.

## Backpressure and Slow Subscribers

- **Buffering:** each stream has a bounded in-memory buffer.
- **Slow subscribers:** when a subscriber falls behind the buffer, messages are dropped
  for that subscriber (lag is surfaced to the subscriber).
- **Publisher behavior:** publishing never blocks on subscriber speed.
- **Disconnects:** slow subscribers are not disconnected by default.

## Fanout Fairness

- **Isolation:** slow subscribers do not block fast subscribers.
- **Behavior:** each subscriber progresses independently; lagging subscribers may drop
  messages without affecting other subscribers.

## Tenant and Namespace Model

- **Wire requirements:** tenant and namespace identifiers are required in all data-plane
  operations.
- **Existence enforcement:** the broker rejects operations for unknown tenant/namespace
  resources based on its local registry (synced from the control plane).
- **Authorization:** ACL evaluation is not implemented in the MVP.
- **Quotas:** per-tenant/namespace quotas are not enforced in the MVP.

## Cache Semantics (MVP)

- **Scoping:** cache entries are scoped to `(tenant_id, namespace, cache, key)`.
- **TTL:** entries expire after TTL; expiration is lazy on access.
- **Eviction:** if a cache capacity is configured, eviction is best-effort and not LRU.

## Non-Goals (MVP)

- Exactly-once or at-least-once delivery.
- Cross-region replication or routing guarantees.
- Durability beyond in-memory storage.
