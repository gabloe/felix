# Azure perf sessions

The automation for `docs/perf-real-network.md`. One *session* = one resource
group = one Bicep deployment, created for a run and deleted after it.

```bash
az login                                    # the MSDN account
export AZ_SUBSCRIPTION=<subscription-guid>  # if the login has more than one
export SESSION=t1-a TIER=t1 RELEASE_TAG=v0.3.0 LOADGEN_REF=main

# The IdP, the easy way: three app-registration secrets + the audience, and
# the suite mints the token and derives issuer/JWKS from the tenant. No JWT to
# paste, and it re-mints when one expires.
export IDP_TENANT_ID=<tenant-guid>
export IDP_CLIENT_ID=<felix-perf app client id>
export IDP_CLIENT_SECRET=<a client secret on that app>
export IDP_AUDIENCE=api://<felix-perf app client id>   # the Application ID URI

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
