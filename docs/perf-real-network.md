# Real-network performance suite

**Decision: measure Felix on real Azure networks, from release artifacts, with
a real IdP on the hot path — and treat every localhost number as a statement
about the software, never about a deployment.**

Everything published so far (`docs/storage-performance.md`, the benchmarks
page, `scripts/perf/presets.yml` runs) was measured over loopback, most of it
against an in-process broker. Loopback is the right harness for what it
measures — regressions in Felix's own code, isolated from the network — and
the wrong instrument for what users will actually see. It hides RTT (loopback
is ~50µs; an availability zone pair is 1–2ms; a WAN path is tens), hides
congestion control entirely (cwnd never matters when bandwidth is infinite and
loss is zero), hides the cost of TLS and connection establishment at real
distance, and flatters quorum: a `Quorum` acknowledgement over loopback costs
almost nothing extra, which is not the promise a cross-zone deployment is
buying.

This document is the design for closing that gap. The budget is an MSDN
subscription's $200/month of Azure credit, which shapes several decisions
below — this suite provisions, runs, and **tears down**; nothing idles.

## What a run must hold constant

Three disciplines carry over from the local suite, and two are new:

- **Release artifacts, never source builds.** Test machines download the
  GitHub release tarball (`felix-<tag>-linux-x86_64.tar.gz`, produced by
  `release.yml`) and run those bytes. Building on the target machine wastes
  budget, varies with the toolchain, and — the local lesson — compiling
  *during* a run corrupts the measurement. It also makes every result
  attributable to a tag a reader can download.
- **Nothing else runs on the machines.** The same hygiene as local runs, now
  enforced by construction: the VMs exist only for the run.
- **`throughput` vs `delivered_throughput`** keep their meanings, and batched
  runs still measure a throughput profile, not request latency.
- **Compare within a session, never across sessions.** Cloud VMs are a
  hardware lottery (host generation, neighbours). An A/B comparison — leader
  vs quorum, JSON vs binary, watch fanout 1 vs 50 — is valid inside one
  provisioned session on the same VMs, and suspect across sessions. Publish
  per-session baselines beside every delta.
- **Report variance, not just medians.** Real networks jitter. Every scenario
  runs its trials (5, as locally) and publishes p50/p99/p999 with spread;
  a run whose trials disagree wildly is a finding about the environment, and
  is labelled as such rather than averaged into a fiction.

## Topology tiers

Three tiers, provisioned by the same scripts with a flag. Each answers a
question the previous one cannot.

### T1 — one zone, real NICs (the baseline)

The cluster as a latency-sensitive user would deploy it: everything in one
availability zone, in a proximity placement group, accelerated networking on.

| Role | Count | Size | Why |
|---|---|---|---|
| Broker | 3 | `Standard_D4as_v5` (4 vCPU, 16 GiB) | Fixed-CPU (no burstable B-series in a measurement), AMD v5 with accelerated networking; 4 vCPU matches the local runs' scale so numbers are comparable |
| Control plane | 1 | `Standard_D2as_v5` | Off the data path; serves auth, exchange, assignments, watches |
| Load generator | 1–2 | `Standard_D8as_v5` | The generator must never be the bottleneck; 2× at fanout 50 |
| Broker data disk | 3 | Premium SSD v2, 64 GiB, 6k provisioned IOPS | Real durable fsync latency, sized to the run, pay-per-GB |

What T1 answers: publish/subscribe/cache/counter latency and throughput over a
real NIC and switch, fsync against real premium storage, fanout curves, and
the JWT flow — the honest replacements for every published localhost number.

### T2 — three zones (what quorum costs)

Identical, but the three brokers pinned to zones 1/2/3 (no placement group —
the zones are the point) and the stream matrix run twice: `Leader` vs
`Quorum`. Inter-zone RTT (~1–2ms) is exactly the price a quorum
acknowledgement pays per publish, and nobody has measured Felix paying it.

What T2 answers: the Leader/Quorum delta on real inter-zone paths; failover
blackout duration when a zone's broker dies under load; replication lag under
sustained throughput; whether group-commit amortises the quorum wait.

### T3 — a distant client (what users feel)

T1's cluster, plus a load generator in a second region (e.g. cluster in
`eastus2`, client in `westus2`, ~60ms RTT). Latency-focused and
small-payload by design: inter-region *egress is billed* (~$0.09/GB), so
WAN throughput sweeps are bounded explicitly (see budget) rather than run
open-ended.

What T3 answers: client-perceived latency at WAN distance, connection/stream
establishment cost, how the client's pooling and pipelining behave when RTT
is real, and watch/retained delivery lag to a remote subscriber.

## The IdP is real: Microsoft Entra ID

The control plane's token exchange verifies IdP tokens against a configured
allowlist — ES256 by default, **optional RS*/PS*** (`auth/exchange.rs`), which
is exactly what Entra ID issues. The MSDN subscription's tenant provides it
for free: an app registration, client-credentials flow, and the exchange
endpoint turns Entra's RS256 access token into a Felix EdDSA token, which is
what brokers verify per request.

Measured, not just wired:

- **Exchange latency** — Entra token → Felix token, cold and warm (JWKS
  cached), as its own scenario.
- **Steady-state cost** — the data-plane numbers are collected with real
  Felix tokens minted through this flow and refreshed at realistic intervals,
  so verification sits on the hot path exactly as deployed.
- Fallback if the Entra tenant fights back: a Keycloak container on the
  control-plane VM speaks ES256, the default allowlist. The scripts take the
  issuer/JWKS as parameters either way.

## Workload matrix

The local `presets.yml` dimensions carry over (fanout × batch × payload ×
encoding × broker profile), thinned where a dimension is known-flat, plus the
scenarios only a real network can ask:

| Scenario | Tier | What it answers |
|---|---|---|
| Publish latency (batch 1, per-message ack) — fanout {1, 10, 50} × payload {0, 256B, 4KiB} | T1 | The headline numbers, honestly |
| Throughput profile (batch 64) × payload | T1 | Sustained delivery over a real NIC; delivered vs published |
| Cache put/get, counter add/get latency | T1 | Request/response path incl. routed forwards |
| Keyed watch: change-to-delivery latency at watcher fanout {1, 50, 500} | T1 | The fanout claim of the composed semantics |
| Retained join: time-to-complete-state vs roster size {10², 10³, 10⁴} | T1 | What "join and hold the roster" costs |
| Leader vs Quorum publish latency, same stream shape | T2 | What the acknowledgement guarantee costs across zones |
| Failover blackout: kill the leader mid-load, measure publish gap and watch re-establishment | T2 | The ~1s local failover claim, on real infrastructure |
| Replication lag under sustained load | T2 | The `Leader` loss-window, observed not asserted |
| All latency scenarios, small payloads only | T3 | WAN client experience |
| Token exchange latency; authorized vs pre-authorized publish | T1 | What the IdP flow adds |

Each scenario emits the same JSONL the local pipeline consumes, so
`normalize_and_aggregate.py` → `make_charts.py` →
`render_markdown_snippets.py` work unchanged, and results land on the
benchmarks page in the same shape with an environment column (`loopback` |
`azure-t1` | `azure-t2` | `azure-t3`).

## What has to be built

1. **`felix-loadgen`** — the one real gap. Every existing driver is
   loopback-bound: `latency-demo` runs an in-process broker, `soak` spawns its
   own child. The generator this suite needs is the measurement core of
   `latency-demo` pointed at remote addresses through `felix-client` /
   `ClusterClient`: takes cluster endpoints, tenant/token (or the Entra
   client-credential parameters and does the exchange itself), a scenario
   spec, emits the standard JSONL. Ships *in* the release tarball so the
   load-gen VM downloads the same artifact as the brokers.
2. **`scripts/perf/azure/`** — `provision.sh` (one resource group per
   session: VNet, PPG or zones per tier, VMs from `az vm create` with
   cloud-init), `deploy.sh` (fetch release tarball, systemd units for
   broker/control-plane, seed tenant + RBAC, register streams/caches),
   `run.sh` (drive the matrix from the load-gen VM, pull artifacts back),
   `teardown.sh` (`az group delete --yes`). No Terraform state to babysit for
   a create-run-destroy lifecycle; plain `az` + cloud-init is auditable and
   has no standing cost.
3. **A release to point at** — see below.

## Budget

Costed at pay-as-you-go eastus2 list prices; MSDN credit covers it several
times over.

| Item | $/hour |
|---|---|
| 3 × D4as_v5 brokers | 0.52 |
| 1 × D2as_v5 control plane | 0.09 |
| 1 × D8as_v5 load generator | 0.34 |
| 3 × Premium SSD v2 64 GiB | ~0.05 |
| **T1/T2 session total** | **~$1.00** |

A full T1 matrix session is ~4 hours ≈ $4. A T2 quorum/failover session ≈ $3.
T3 adds a second-region generator (+$0.34/hr) and *egress*: at ~$0.09/GB, a
throughput sweep pushing 100 GB would cost more than the VMs — which is why
T3 is latency-scoped and its total transfer is computed and capped in the
runner before a run starts. The whole initial program — T1 twice (repeat to
observe session variance), T2 twice, T3 once, plus debugging headroom — fits
in **under $40 of the $200**. The guardrails that keep it there: everything
lives in one resource group per session, `teardown.sh` is the last line of
`run.sh`, and a scheduled `az group delete` at +8 hours backstops a wedged
session.

## Sequencing

1. **Cut the release first** (Gabriel's call, and the right one): the ladder
   (#348/#349/#350) plus dead-letter replication merge, the workspace version
   bumps to 0.3.0, tag `v0.3.0` triggers `release.yml`, and every VM in this
   suite downloads that tarball. `felix-loadgen` should be in the same
   release, which makes it the one code change gating the suite.
2. Build `felix-loadgen` + `scripts/perf/azure/`, validated against a local
   cluster (the felix-cluster harness) before any Azure spend.
3. T1 session, twice. Publish the first real-network numbers beside the
   loopback ones — expect them to be *worse*, and say so plainly; that is the
   point.
4. T2, then T3. Each produces a results section on the benchmarks page and
   updates any claim the numbers contradict.

## What this deliberately does not do

- **No standing environment.** $200/month buys ~8 always-on days of this
  cluster; a permanent testbed would eat the budget and rot besides.
- **No Kubernetes.** #131 tracks packaging for k8s; this suite measures Felix,
  not an orchestrator's networking. VMs + systemd keep the path from NIC to
  broker legible.
- **No cross-cloud, no multi-region clusters.** The broker's replication is
  zone-scale today; measuring a topology the design does not target yet would
  publish numbers with no owner.
