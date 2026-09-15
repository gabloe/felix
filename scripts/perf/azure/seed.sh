#!/usr/bin/env bash
# Seed one perf session, running ON the load generator (the control plane is
# VNet-private). The flow is the deployment's own, not a harness shortcut:
#
#   1. bootstrap-initialize the tenant with the real IdP's issuer + JWKS
#   2. exchange an IdP token for the Felix operator token
#   3. create the namespace, streams, and cache scopes the matrix uses
#   4. exchange node tokens for the brokers, drop them, start the brokers
#
# The IdP is whatever the operator points at — Entra ID via IDP_* variables
# (docs/perf-real-network.md walks the app registration). IDP_TOKEN is a
# token *from that IdP* for the perf principal; this script never mints
# anything itself, which is the point of measuring the real flow.
set -euo pipefail

: "${CONTROLPLANE_IP:?}"
: "${BROKER_IPS:?}"
: "${BOOTSTRAP_TOKEN:?}"
: "${IDP_ISSUER:?set IDP_ISSUER (e.g. https://login.microsoftonline.com/<tenant>/v2.0)}"
: "${IDP_JWKS_URL:?set IDP_JWKS_URL}"
: "${IDP_AUDIENCE:?set IDP_AUDIENCE (the app registration audience)}"
: "${IDP_TOKEN:?set IDP_TOKEN (a token from the IdP for the perf principal)}"

TENANT="perf"
NAMESPACE="default"
cp="http://${CONTROLPLANE_IP}:8080"
bootstrap="http://${CONTROLPLANE_IP}:8081"

principal="$(python3 - "$IDP_TOKEN" <<'PY'
import base64, json, sys
payload = sys.argv[1].split('.')[1]
payload += '=' * (-len(payload) % 4)
claims = json.loads(base64.urlsafe_b64decode(payload))
print(f"{claims['iss']}#{claims.get('sub', claims.get('oid', ''))}")
PY
)"

echo ">> bootstrap-initialize tenant ${TENANT} (principal ${principal})"
curl -fsS -X POST "${bootstrap}/internal/bootstrap/tenants/${TENANT}/initialize" \
  -H "X-Felix-Bootstrap-Token: ${BOOTSTRAP_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d @- <<JSON
{
  "display_name": "Perf",
  "idp_issuers": [{
    "issuer": "${IDP_ISSUER}",
    "audiences": ["${IDP_AUDIENCE}"],
    "jwks_url": "${IDP_JWKS_URL}",
    "claim_mappings": { "subject_claim": "sub" }
  }],
  "initial_admin_principals": ["${principal}"],
  "policies": [
    { "subject": "role:perf", "object": "stream:${TENANT}/${NAMESPACE}/*", "action": "stream.publish" },
    { "subject": "role:perf", "object": "stream:${TENANT}/${NAMESPACE}/*", "action": "stream.subscribe" },
    { "subject": "role:perf", "object": "cache:${TENANT}/${NAMESPACE}/*", "action": "cache.read" },
    { "subject": "role:perf", "object": "cache:${TENANT}/${NAMESPACE}/*", "action": "cache.write" },
    { "subject": "role:perf", "object": "cluster:*", "action": "node.manage" },
    { "subject": "role:perf", "object": "cluster:*", "action": "node.view" }
  ],
  "groupings": [{ "user": "${principal}", "role": "role:perf" }]
}
JSON
echo

echo ">> exchange the IdP token for the Felix operator token"
exchange() {
  curl -fsS -X POST "${cp}/v1/tenants/${TENANT}/token/exchange" \
    -H "Authorization: Bearer ${IDP_TOKEN}" \
    -H 'Content-Type: application/json' \
    -d '{}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["felix_token"])'
}
token="$(exchange)"
mkdir -p "$HOME/felix-session"
printf '%s' "${token}" > "$HOME/felix-session/token"
auth=(-H "Authorization: Bearer ${token}")

echo ">> namespace, streams, cache scope"
curl -fsS -X POST "${cp}/v1/tenants/${TENANT}/namespaces" "${auth[@]}" \
  -H 'Content-Type: application/json' \
  -d "{\"namespace\": \"${NAMESPACE}\", \"display_name\": \"Perf\"}"
for stream in perf perf-durable; do
  durable=$([ "$stream" = perf-durable ] && echo true || echo false)
  # Replicated when the tier has zones to replicate across; the Leader vs
  # Quorum comparison registers its own streams per run.
  curl -fsS -X POST "${cp}/v1/tenants/${TENANT}/namespaces/${NAMESPACE}/streams" "${auth[@]}" \
    -H 'Content-Type: application/json' \
    -d @- <<STREAM
{
  "stream": "${stream}",
  "kind": "Stream",
  "shards": 1,
  "replication_factor": ${REPLICATION_FACTOR:-1},
  "retention": { "max_age_seconds": null, "max_size_bytes": null },
  "consistency": "Leader",
  "delivery": "AtLeastOnce",
  "durable": ${durable}
}
STREAM
done
curl -fsS -X POST "${cp}/v1/tenants/${TENANT}/namespaces/${NAMESPACE}/caches" "${auth[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"cache": "perf", "display_name": "Perf"}'

echo

echo ">> node tokens to the brokers, then start them"
IFS=',' read -ra brokers <<<"${BROKER_IPS}"
for ip in "${brokers[@]}"; do
  ssh -o StrictHostKeyChecking=accept-new "felix@${ip}" \
    "sudo mkdir -p /etc/felix && printf '%s' '${token}' | sudo tee /etc/felix/node.token > /dev/null && sudo systemctl enable --now felix-broker"
done

echo ">> waiting for the brokers to register and take their shards"
for _ in $(seq 1 60); do
  ready=$(curl -fsS "${cp}/v1/nodes" "${auth[@]}" | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("nodes", [])))' || echo 0)
  [ "${ready}" -ge "${#brokers[@]}" ] && break
  sleep 5
done
echo ">> seeded: tenant ${TENANT}, token at ~/felix-session/token"
