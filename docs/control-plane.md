# Control Plane + RAFT Plan (Draft)

This document outlines how the Felix control plane will use RAFT to manage
cluster metadata and propagate configuration to dataplane brokers. It is not
the data plane and does not carry user payloads.

## Goals
- Consistent cluster metadata (membership, shard placement, config).
- Simple propagation to brokers with clear ownership and failover.
- Keep dataplane hot path free of RAFT overhead.

## Non-Goals
- Replicating stream payloads via RAFT.
- Strong consistency for data reads and writes (handled by dataplane + storage).

## RAFT Scope (Control Plane Only)
The RAFT log stores authoritative metadata:
- Node membership and health state (up/down, drains).
- Stream/shard placement and leadership.
- Cluster-wide config (retention, limits, feature flags).
- Versioned snapshots of metadata state.

All control plane nodes participate in the RAFT group. A single leader accepts
metadata writes and replicates them to followers.

## Propagation to Dataplane
Dataplane brokers do not run RAFT. They consume committed metadata through a
simple API:
- Watch or stream updates (long-poll / streaming RPC).
- Periodic snapshot fetch for resync.

On update:
- Brokers start/stop shard ownership.
- Routing tables update for publish/subscribe paths.
- Health/metrics reflect new assignments.

## Failure + Recovery
- If the control plane leader fails, a new leader is elected by RAFT.
- Brokers reconnect and resume watching from the latest committed version.
- Metadata snapshots allow new control plane nodes to catch up quickly.

## API Sketch (Control Plane)
- `GetSnapshot()` -> full metadata view + version.
- `WatchUpdates(from_version)` -> stream of incremental updates.
- `ReportHealth(node_id, status)` -> node liveness signals.

## Evolution Plan
1) Control plane RAFT for membership + placement only.
2) Add richer metadata (tenants, quotas, retention).
3) Introduce safety checks for rebalancing and drains.

## Kubernetes Deployment Model
### Pod Layout
- Control plane runs as a dedicated StatefulSet with stable identities.
- Dataplane brokers run as a separate StatefulSet (or Deployment for stateless
  dev mode).
- Object store access is configured per broker (later), not in the control plane.

### Storage
- Control plane uses a PVC per pod for RAFT logs and snapshots.
- Dataplane brokers use PVCs for durable log segments (when enabled).

### Services
- Headless Service for control plane peer discovery (RAFT).
- ClusterIP Service for control plane client API (watch/snapshot/health).
- Separate Service for broker QUIC ingress.

### Scheduling + Ops
- Control plane replicas: 3 or 5 (odd count for quorum).
- Use PodDisruptionBudgets to preserve quorum and shard ownership.
- Prefer anti-affinity for control plane pods to avoid single-node failure.
- Configure liveness/readiness probes on control plane API endpoints.

## Open Questions
- Snapshot cadence and maximum delta size.
- Placement heuristics and rebalancing triggers.
- Authentication/authorization for control plane APIs.
