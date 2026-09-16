# Azure perf sessions

The automation for `docs/perf-real-network.md`. One *session* = one resource
group = one Bicep deployment, created for a run and deleted after it.

### Secrets: a local `session.env`, never committed

```bash
cp scripts/perf/azure/session.env.example scripts/perf/azure/session.env
# edit session.env — fill in the GUIDs and the ONE client secret
set -a; source scripts/perf/azure/session.env; set +a
```

`session.env` is gitignored (only `session.env.example` is tracked). The
client secret lives there and nowhere else — not in the repo, not in shell
history, not echoed by any script. Everything below reads it from the
environment.

### Orchestration: `az vm run-command`, not SSH

The operator never SSHes into a session. Every operator→VM step — waiting for
cloud-init, seeding, starting brokers, running the matrix, reading RTT — goes
through `az vm run-command invoke`, which rides the Azure control plane over
HTTPS. Two reasons the first live run found the hard way: some operator
networks deep-packet-inspect and reset outbound `:22` to arbitrary cloud IPs
(a fresh Azure VM is not whitelisted the way GitHub is), and Ubuntu 24.04's
socket-activated sshd can fail its first start for want of `/run/sshd`.
run-command sidesteps both and needs no inbound port at all; the VNet-private
control plane is reached by running the seed *on* the loadgen, which is inside
the VNet. The NSG still allows SSH from your address so a human *can* open a
shell to debug, but nothing in the automation depends on it. Scripts shipped to
run-command execute under **dash as root**, so `seed-remote.sh` and the inline
snippets are POSIX sh — no `pipefail`, arrays, or `[[ ]]`.

### Run

```bash
az login                                    # the Azure account
# session.env is already sourced (above), so the suite has everything: the
# subscription, the tier/release, and the IdP secrets it mints a token from.
./session.sh                   # provision + seed (~10 min, mostly instrument build)
./run.sh                       # the matrix (~2-4 h); results in sessions/<name>-results/
./teardown.sh                  # ALWAYS. ~$1/hour while it exists.
```

(The manual path still works: set `IDP_TOKEN` plus `IDP_ISSUER` /
`IDP_JWKS_URL` / `IDP_AUDIENCE` yourself and skip the three secrets.)

- **Tiers**: `t1` one-zone proximity-placed baseline; `t2` brokers across
  zones 1/2/3 (set `REPLICATION_FACTOR=3` in the environment before
  `session.sh` so the seeded streams replicate). t3 is a second small
  deployment of a loadgen in another region, pointed at a t1 session.
- **IdP**: an Entra **app registration** (no user — a perf harness wants a
  non-interactive credential), used through the **client-credentials** grant.
  `idp-token.sh` turns `IDP_TENANT_ID` + `IDP_CLIENT_ID` + `IDP_CLIENT_SECRET`
  + `IDP_AUDIENCE` into an access token; the app-only token's `sub`/`oid` is
  the service-principal identity Felix authorizes. The Application ID URI is
  often forced to `api://<client-id>` by tenant policy — that is fine, the
  audience is just a string. Measuring the real exchange is the point, so the
  seed and the exchange-latency scenario both run against this, never demo
  auth.
- **What runs where**: brokers and the control plane run the release
  **tarball** (`RELEASE_TAG`, the measured artifacts), downloaded at provision
  time; the load generator **builds** `felix-loadgen` from `LOADGEN_REF`
  (default `main`) once, before any run. These are deliberately separate:
  the instrument is not in the release, and may not exist at the release tag
  — it does not at v0.3.0 — so it builds from a ref that contains the crate.
- **Budget guardrails**: everything is in the one group; `teardown.sh`
  deletes it; `session.sh` stamps an `autoTeardownAfter` tag at +8h as the
  backstop for a wedged session (enforce it with a subscription automation
  rule, or just check the tag when you log in).
- **Honesty rules**: compare only within a session; keep the machines
  otherwise idle during `run.sh`; every published number cites the
  `session.json` beside it.
