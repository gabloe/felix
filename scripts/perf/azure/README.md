# Azure perf sessions

The automation for `docs/perf-real-network.md`. One *session* = one resource
group = one Bicep deployment, created for a run and deleted after it.

```bash
az login                       # the MSDN subscription
export SESSION=t1-a TIER=t1 RELEASE_TAG=v0.3.0
export IDP_ISSUER=... IDP_JWKS_URL=... IDP_AUDIENCE=... IDP_TOKEN=...
./session.sh                   # provision + seed (~10 min, mostly instrument build)
./run.sh                       # the matrix (~2-4 h); results in sessions/<name>-results/
./teardown.sh                  # ALWAYS. ~$1/hour while it exists.
```

- **Tiers**: `t1` one-zone proximity-placed baseline; `t2` brokers across
  zones 1/2/3 (set `REPLICATION_FACTOR=3` in the environment before
  `session.sh` so the seeded streams replicate). t3 is a second small
  deployment of a loadgen in another region, pointed at a t1 session.
- **IdP**: the `IDP_*` variables describe the real IdP (Entra ID app
  registration; the design doc walks the setup) and `IDP_TOKEN` is a token
  from it for the perf principal. The seed never mints anything — measuring
  the real exchange is the point.
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
