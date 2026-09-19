---
title: "Kubernetes Deployment"
description: "Install the control plane and a broker cluster with the Helm chart, mint the first credential, and run the day-2 operations: rolling upgrades, scaling, replacing a volume."
---

Felix ships a Helm chart, at
[`deploy/helm/felix`](https://github.com/gabloe/felix/tree/main/deploy/helm/felix),
that renders the control plane and a broker cluster with the shape the design
assumes: StatefulSets for stable broker identity, a volume per broker, the
probes and drain behaviour the binaries already ship, and the budgets and
policies that keep a rolling operation from taking a shard's replicas with it.
Every environment variable it wires is real and in the
[environment reference](/felix/reference/environment-variables/); the chart
invents none.

The chart names `ghcr.io/gabloe/felix-broker` and
`ghcr.io/gabloe/felix-controlplane`, which releases publish and which pull
without credentials. The image tag defaults to the chart's `appVersion`, so a
default install resolves to a published image with nothing to configure —
`0.5.0` renders `ghcr.io/gabloe/felix-broker:0.5.0`. To run something you have not released, build from
`docker/` and push to a registry your cluster can reach (the
[Docker Compose page](/felix/deployment/docker-compose/) has the build
commands), then point `image.registry` at it.

**Pin by digest.** `broker.image.digest` and `controlplane.image.digest` take
precedence over the tag, and the digest is what the signature covers — the
release signs `image@digest` and never `image:tag`, because a tag can be moved
to point at something else and a signature over a tag would follow it.

Signing is keyless: cosign takes a short-lived certificate from the release
workflow's OIDC identity, so there is no key to store or rotate and the
signature names the workflow that produced the image. Verify before you pin:

```bash
cosign verify ghcr.io/gabloe/felix-broker:0.5.0 \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com \
  --certificate-identity-regexp='^https://github.com/gabloe/felix/\.github/workflows/release\.yml@refs/tags/v'
```

## What the chart decides for you

| Concern | What renders | Why it is that way |
| --- | --- | --- |
| Broker identity | A StatefulSet whose pod name is `FELIX_NODE_ID` | The name is what the broker registers as, what shards are assigned to, and under peer mTLS the DNS name its certificate must carry. A replaced pod keeps all three and the volume behind them, so a replacement is a rejoin. |
| Addresses | Peers get `$(POD_IP):5001`; clients get `<pod>.<headless>.<ns>.svc:5000` | The broker requires an IP for `FELIX_NODE_ADVERTISE_ADDR` and re-registers a new one on its first heartbeat. Clients are told a name, which survives the pod being replaced. |
| Control plane over Postgres | A Deployment rolling with `maxUnavailable: 0` | Instances are stateless; a new one is ready before an old one goes, and readiness is what the Service routes on. Migrations are additive and run under an advisory lock, so mixed versions serve during the roll. |
| Control plane under Raft | A StatefulSet with a volume per member | Each member's id is its pod ordinal plus one, and every member is handed the same peers map, derived from the replica count. Odd, and at least three. |
| Credentials | Secret references only | The Postgres URL, the bootstrap token and the broker credential are read from Secrets you create. The chart never renders one, and never puts one in a ConfigMap. |
| Shutdown | A preStop sleep, then the drain, inside a derived grace period | The endpoints controller gets a head start before SIGTERM; the drain budget fits before SIGKILL. An explicit grace period that is too short refuses to render. |
| Disruption | PodDisruptionBudgets | At most one broker at a time, which is what keeps a replication-factor-three shard's quorum through node maintenance. A wider budget refuses to render. |
| Placement | Anti-affinity by node, spread by zone | `soft` prefers, `hard` refuses to co-locate. |
| The internal port | On the headless Service only, and a NetworkPolicy admitting it from broker pods | Without peer mTLS, anything that reaches the port is a broker. With it, this is the second fence. |
| Peer mTLS | A cert-manager CSI volume per pod, off by default | Each broker needs a certificate issued to its own name. See [Peer mTLS](#peer-mtls). |

## Prerequisites

- Kubernetes 1.25 or later and Helm 3.8 or later.
- A StorageClass for broker volumes. Brokers fsync; a network disk with
  provisioned IOPS is the usual choice (`gp3`, `pd-ssd`, `Premium_LRS`).
- **A Postgres** with one writable endpoint and synchronous replication, or
  the Raft backend. What the database must provide, and what a failover looks
  like from Felix, is on [Control-plane HA](/felix/deployment/control-plane-ha/).
  The chart does not deploy a database.
- A CNI that enforces `NetworkPolicy`, or the policy is inert.
- For peer mTLS: cert-manager and
  [cert-manager-csi-driver](https://cert-manager.io/docs/usage/csi-driver/).

## Install

Three steps, because a broker refuses to start without a credential and the
credential comes out of the control plane's day-0 bootstrap.

### 1. The control plane, with bootstrap on

```bash
kubectl create namespace felix
kubectl -n felix create secret generic felix-postgres \
  --from-literal=url='postgres://felix:...@postgres-rw.db.svc:5432/felix'
kubectl -n felix create secret generic felix-bootstrap \
  --from-literal=token="$(openssl rand -hex 32)"

helm install felix deploy/helm/felix -n felix \
  --set controlplane.storage.postgres.existingSecret=felix-postgres \
  --set controlplane.bootstrap.enabled=true \
  --set controlplane.bootstrap.existingSecret=felix-bootstrap \
  --set broker.enabled=false
kubectl -n felix rollout status deployment/felix-controlplane
```

For the Raft backend instead of Postgres:

```bash
  --set controlplane.storage.backend=raft --set controlplane.replicas=3
```

The bootstrap listener is on its own ClusterIP Service, never behind the API's,
so it is reachable only through a port-forward.

### 2. Day 0: an operator tenant and the broker credential

Cluster scope (`node.view:cluster:*`, `node.manage`) cannot be granted by a
tenant admin, so it is seeded at bootstrap. Initialise a tenant with a policy
granting the broker role what a broker needs, and an operator role for
yourself:

```bash
kubectl -n felix port-forward svc/felix-controlplane-bootstrap 9095 &
curl -sS -X POST http://127.0.0.1:9095/internal/bootstrap/tenants/ops/initialize \
  -H "X-Felix-Bootstrap-Token: $(kubectl -n felix get secret felix-bootstrap -o jsonpath='{.data.token}' | base64 -d)" \
  -H 'Content-Type: application/json' -d '{
    "display_name": "Operations",
    "idp_issuers": [ { "issuer": "https://login.example.com/", "audiences": ["api://felix-controlplane"],
                       "claim_mappings": { "subject_claim": "sub", "groups_claim": "groups" } } ],
    "initial_admin_principals": ["p:alice"],
    "policies": [
      { "subject": "role:broker",   "object": "cluster:*", "action": "node.view" },
      { "subject": "role:broker",   "object": "cluster:*", "action": "node.manage" },
      { "subject": "role:operator", "object": "cluster:*", "action": "tenant.manage" },
      { "subject": "role:operator", "object": "cluster:*", "action": "node.view" },
      { "subject": "role:operator", "object": "cluster:*", "action": "node.manage" }
    ],
    "groupings": [
      { "user": "p:broker", "role": "role:broker" },
      { "user": "p:alice",  "role": "role:operator" }
    ]
  }'
```

Then exchange an IdP token for the broker principal (the
[token exchange](/felix/features/security/#token-exchange-oidc--felix) flow)
and put the Felix token in a Secret:

```bash
kubectl -n felix create secret generic felix-broker-credential \
  --from-file=token=./felix-node-token
```

One token shared by every broker, carrying `node.manage:cluster:*`, is the
simple form. The stricter one is a token per broker carrying
`node.manage:node:felix-broker-0` and so on, so no broker can register, drain
or report for another: put each under a key named after its pod and set
`broker.credential.perBroker=true`. Either way, a Felix token expires. A broker reads
its token once, at start, so a rotated Secret reaches it only through a
restart (`kubectl rollout restart statefulset/felix-broker`, one pod at a time
under the budget). Give the brokers an IdP refresh token under
`broker.credential.refreshTokenKey` and they re-mint before expiry instead.

### 3. Brokers on, bootstrap off

```bash
helm upgrade felix deploy/helm/felix -n felix --reuse-values \
  --set broker.enabled=true \
  --set broker.credential.existingSecret=felix-broker-credential \
  --set controlplane.bootstrap.enabled=false
kubectl -n felix rollout status statefulset/felix-broker
```

Each broker comes up, registers under its pod name, seeds the catalog from the
control plane, and only then reports ready, so it is never routed traffic for
streams it does not know yet. Verify:

```bash
kubectl -n felix get pods -l app.kubernetes.io/component=broker
kubectl -n felix exec felix-broker-0 -- wget -qO- http://127.0.0.1:8080/ready
# and the fleet as the control plane sees it, with an operator token:
curl -sS -H "Authorization: Bearer $OPERATOR_TOKEN" http://felix-controlplane.felix.svc:8443/v1/nodes
```

## Clients

Clients connect to any broker first and follow discovery to the broker that
owns a shard. The `felix-broker` Service (ClusterIP by default) is that first
hop; discovery then hands out each broker's own name,
`felix-broker-N.felix-broker-headless.felix.svc.cluster.local:5000`.

Clients from outside the cluster need two things: a way in, and an address
that resolves for them. Set `broker.clientService.type=LoadBalancer` on a
provider that balances **UDP**, and `broker.clientAdvertiseAddr` to what
each broker is reachable as from outside, with `$(POD_NAME)` expanded per pod
(`"$(POD_NAME).brokers.example.com:5000"`, say, with one record per broker).

A broker generates its own client-facing certificate at start and exports it
to `/var/lib/felix/export/broker-cert.pem`; clients verify against it. Copy
it out with `kubectl exec felix-broker-0 -- cat ...`.

## Peer mTLS

`FELIX_INTERNAL_BIND` is the port brokers use to forward publishes and ship
replication to each other. With `FELIX_INTERNAL_TLS_CERT`, `_KEY` and `_CA`
set, every peer connection is mutually authenticated: a peer is a broker
holding a certificate the cluster's CA issued to its own node id, checked in
both directions. Without them the port is encrypted but unauthenticated,
anything that can reach it is a broker, and the broker warns at startup.

Each broker needs a certificate issued to its own pod name, and a Secret
cannot vary per pod of one StatefulSet, so the chart uses cert-manager's CSI
driver: one certificate per pod, issued at start, renewed in place.

```bash
kubectl -n felix apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: felix-peer-ca
spec:
  ca:
    secretName: felix-peer-ca   # a CA key pair you hold; never the cluster's default CA
EOF
helm upgrade felix deploy/helm/felix -n felix --reuse-values \
  --set broker.peerTls.enabled=true --set broker.peerTls.issuerName=felix-peer-ca
```

Renewals are picked up from disk without a restart. Whichever mode, the
internal port is never on a routable Service, and the NetworkPolicy admits it
from broker pods only. [`docs/threat-model-internal.md`](https://github.com/gabloe/felix/blob/main/docs/threat-model-internal.md)
sets out what is and is not defended in each mode.

## Operations

### Rolling upgrade

The order, and why, is on [Upgrades & Compatibility](/felix/deployment/upgrades/):
control plane first, then brokers, clients whenever. The chart's budgets and
update strategies make `helm upgrade` do that order one pod at a time, but a
new broker image should still wait on the previous broker being back in its
replica sets:

```bash
helm upgrade felix deploy/helm/felix -n felix --reuse-values \
  --set controlplane.image.digest=sha256:... --set broker.image.digest=sha256:...
kubectl -n felix rollout status deployment/felix-controlplane
kubectl -n felix rollout status statefulset/felix-broker
# Between brokers, and after: nothing halted.
kubectl -n felix exec felix-broker-0 -- wget -qO- http://127.0.0.1:8080/replication/halted
```

`[]` is the answer to want. A release that changes `INTERNAL_VERSION` is not
rolling: scale brokers to zero, upgrade, scale back.

### Scaling out

```bash
helm upgrade felix deploy/helm/felix -n felix --reuse-values --set broker.replicas=5
```

New brokers register and become placement targets; existing shards stay where
they are until something moves them. Scaling **in** removes the highest
ordinals: drain each first (`POST /v1/nodes/{id}/drain` with an operator
token), wait for its shards to fail over, then lower `replicas`. The budget
refuses a value it cannot keep a quorum under.

### Replacing a broker's volume

A broker whose volume is lost comes back empty under the same name and the
same assignments. For every shard it follows, the leader offers a log placed
at its oldest surviving offset; a replica holding nothing takes it and
replication resumes.

```bash
kubectl -n felix delete pvc data-felix-broker-2 --wait=false
kubectl -n felix delete pod felix-broker-2
```

Watch `felix_broker_replication_lag_records` fall and `/replication/halted`
stay empty.

### Control-plane instance loss and database failover

Nothing to do. Survivors serve; readiness takes a failing instance out of
rotation; brokers keep their last-known catalog and retry heartbeats. Keep
`controlplane.liveness.nodeExpiryTimeoutMs` above a database failover plus
one heartbeat interval, so a failover alone never expires brokers that were
serving fine.

### Backups

The control plane's metadata lives in the database (back it up whole, restore
it whole) or, under Raft, in the members' volumes: snapshot those at the
storage layer, and keep the state file the group was seeded from, since
`felix-controlplane migrate import --overwrite` onto a fresh group is the
recovery beyond quorum loss (see [Metadata Raft](/felix/architecture/metadata-raft/)).
Broker volumes hold the streams themselves; snapshot them at the storage
layer, or rely on replication and retention.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Broker pod in `CrashLoopBackOff`, log says a node id needs a credential | `broker.credential.existingSecret` is missing the key the pod reads (`tokenKey`, or the pod's name with `perBroker`). |
| Broker registers, then heartbeats are refused with 403 | The token lacks `node.manage` over `node:<pod name>` or `cluster:*`. |
| Broker never becomes ready | It cannot reach the control plane (`FELIX_CONTROLPLANE_URL`), or the token lacks `node.view:cluster:*`, so it never seeds a catalog. Check its log. |
| Control plane not ready, liveness fine | The store: Postgres unreachable, or the database is behind the build's migrations. That is readiness doing its job. |
| Raft group never forms | Fewer members than the peers map names, or the headless Service was changed. Every member must carry the same map. |
| `helm upgrade` refused with a message about budgets, drains, or members | Deliberate. The message names the values that are wrong together. |
| PVC `Pending` | No default StorageClass, or the named one does not exist in this zone. |
| Peer mTLS pods stuck in `ContainerCreating` | cert-manager-csi-driver is not installed, or the Issuer cannot sign. `kubectl describe pod` shows the CSI error. |

## Next Steps

- **Monitor deployment**: [Observability Guide](/felix/features/observability/)
- **Control-plane HA**: [what the database must provide](/felix/deployment/control-plane-ha/)
- **Graceful shutdown**: [what the probes and drain do](/felix/deployment/graceful-shutdown/)
- **Configure fully**: [Configuration Reference](/felix/reference/configuration/)
- **Secure deployment**: [Security Guide](/felix/features/security/)
