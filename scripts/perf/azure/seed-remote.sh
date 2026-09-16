# POSIX sh body, run ON the load generator under dash as root via run-command.
# seed.sh prepends a header assigning CP / BOOTSTRAP / BOOTSTRAP_TOKEN /
# IDP_TOKEN / IDP_JWKS_URL / IDP_AUDIENCE / TENANT / NAMESPACE /
# REPLICATION_FACTOR, then concatenates this file. Keep it dash-clean: no
# arrays, no pipefail, no bashisms.
#
# The flow is the deployment's own, not a harness shortcut:
#   1. bootstrap-initialize the tenant with the real IdP's issuer + JWKS
#   2. exchange the IdP token for the Felix operator token
#   3. create the namespace, streams, and cache scope the matrix uses
# The felix token is written to ~felix/felix-session/token for run.sh and
# framed on stdout so seed.sh can relay it to the brokers as their node token.
#
# Every step is idempotent (409 "already exists" is tolerated) so a re-run
# after a partial seed converges instead of wedging, and every non-2xx prints
# its status and body so a failure is legible in the run-command message rather
# than a bare stall.

RESP=/tmp/felix-seed-resp

# post_ok <url> [curl args...] — POST, reading any JSON body from stdin via
# `-d @-`. Succeeds on 2xx and on 409 (already exists); prints status+body and
# fails otherwise.
post_ok() {
  _url=$1
  shift
  _code=$(curl -s -o "$RESP" -w '%{http_code}' -X POST "$_url" "$@" -d @-)
  case "$_code" in
    2*) return 0 ;;
    409) echo "   (already exists: HTTP 409, continuing)" ;;
    *) echo "!! POST $_url -> HTTP $_code" >&2; cat "$RESP" >&2; echo >&2; return 1 ;;
  esac
}

# Derive principal (iss#sub) and issuer FROM the token — an app-only Entra
# credential issues a v1 token (iss=https://sts.windows.net/<tenant>/) even
# through the v2 endpoint, so assuming the issuer would reject a valid token.
ISS=$(python3 - "$IDP_TOKEN" <<'PY'
import base64, json, sys
p = sys.argv[1].split('.')[1]; p += '=' * (-len(p) % 4)
print(json.loads(base64.urlsafe_b64decode(p))['iss'])
PY
)
SUB=$(python3 - "$IDP_TOKEN" <<'PY'
import base64, json, sys
p = sys.argv[1].split('.')[1]; p += '=' * (-len(p) % 4)
c = json.loads(base64.urlsafe_b64decode(p)); print(c.get('sub') or c.get('oid') or '')
PY
)
# The control plane keys RBAC on principal_id = hex(sha256(issuer|subject))
# (services/controlplane/src/auth/principal.rs), NOT a human-readable string.
# Registering the admin/role grouping under anything else yields a validated
# token with zero permissions (403 "no permissions"). Compute the same id here
# so the seed's groupings match what the exchange evaluates. subject_claim is
# "sub", so use the token's sub — the same claim the validator maps.
PRINCIPAL=$(python3 - "$ISS" "$SUB" <<'PY'
import hashlib, sys
print(hashlib.sha256(sys.argv[1].encode() + b"|" + sys.argv[2].encode()).hexdigest())
PY
)

echo ">> bootstrap-initialize tenant $TENANT (issuer $ISS, principal $PRINCIPAL)"
post_ok "$BOOTSTRAP/internal/bootstrap/tenants/$TENANT/initialize" \
  -H "X-Felix-Bootstrap-Token: $BOOTSTRAP_TOKEN" \
  -H 'Content-Type: application/json' <<JSON
{
  "display_name": "Perf",
  "idp_issuers": [{
    "issuer": "$ISS",
    "audiences": ["$IDP_AUDIENCE"],
    "jwks_url": "$IDP_JWKS_URL",
    "claim_mappings": { "subject_claim": "sub" }
  }],
  "initial_admin_principals": ["$PRINCIPAL"],
  "policies": [
    { "subject": "role:perf", "object": "stream:$TENANT/$NAMESPACE/*", "action": "stream.publish" },
    { "subject": "role:perf", "object": "stream:$TENANT/$NAMESPACE/*", "action": "stream.subscribe" },
    { "subject": "role:perf", "object": "cache:$TENANT/$NAMESPACE/*", "action": "cache.read" },
    { "subject": "role:perf", "object": "cache:$TENANT/$NAMESPACE/*", "action": "cache.write" },
    { "subject": "role:perf", "object": "cluster:*", "action": "node.manage" },
    { "subject": "role:perf", "object": "cluster:*", "action": "node.view" }
  ],
  "groupings": [{ "user": "$PRINCIPAL", "role": "role:perf" }]
}
JSON

# Two tokens come out of the exchange, because the broker validates a client
# token by rejecting the WHOLE token if any action in it is not client-facing:
#   - the ADMIN token (full perms) creates the namespace/streams/cache and is
#     dropped on each broker as its node token (node.manage/node.view live here);
#   - the CLIENT token is narrowed to stream/cache actions only, and is what the
#     load generator authenticates with — an admin token carrying node.* is
#     refused at the broker's client control stream ("invalid action: node.view").
echo ">> exchange the IdP token for the Felix admin token"
CODE=$(curl -s -o "$RESP" -w '%{http_code}' -X POST "$CP/v1/tenants/$TENANT/token/exchange" \
  -H "Authorization: Bearer $IDP_TOKEN" -H 'Content-Type: application/json' -d '{}')
[ "$CODE" = 200 ] || { echo "!! admin token exchange -> HTTP $CODE" >&2; cat "$RESP" >&2; echo >&2; exit 1; }
TOKEN=$(python3 -c 'import json; print(json.load(open("/tmp/felix-seed-resp"))["felix_token"])')
[ -n "$TOKEN" ] || { echo "!! exchange returned no felix_token" >&2; exit 1; }

echo ">> namespace, streams, cache scope"
post_ok "$CP/v1/tenants/$TENANT/namespaces" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' <<JSON
{ "namespace": "$NAMESPACE", "display_name": "Perf" }
JSON
for stream in perf perf-durable; do
  if [ "$stream" = perf-durable ]; then durable=true; else durable=false; fi
  # Replicated when the tier has zones to replicate across; the Leader vs
  # Quorum comparison registers its own streams per run.
  post_ok "$CP/v1/tenants/$TENANT/namespaces/$NAMESPACE/streams" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' <<STREAM
{
  "stream": "$stream",
  "kind": "Stream",
  "shards": 1,
  "replication_factor": $REPLICATION_FACTOR,
  "retention": { "max_age_seconds": null, "max_size_bytes": null },
  "consistency": "Leader",
  "delivery": "AtLeastOnce",
  "durable": $durable
}
STREAM
done
post_ok "$CP/v1/tenants/$TENANT/namespaces/$NAMESPACE/caches" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' <<JSON
{ "cache": "perf", "display_name": "Perf" }
JSON

echo ">> exchange a client-scoped token for the load generator"
# The exchange narrows to exactly the requested actions (filter_permissions),
# so this token carries only stream/cache perms — no node.* — and the broker
# accepts it on the client control stream.
CODE=$(curl -s -o "$RESP" -w '%{http_code}' -X POST "$CP/v1/tenants/$TENANT/token/exchange" \
  -H "Authorization: Bearer $IDP_TOKEN" -H 'Content-Type: application/json' \
  -d '{"requested":["stream.publish","stream.subscribe","cache.read","cache.write"]}')
[ "$CODE" = 200 ] || { echo "!! client token exchange -> HTTP $CODE" >&2; cat "$RESP" >&2; echo >&2; exit 1; }
CLIENT_TOKEN=$(python3 -c 'import json; print(json.load(open("/tmp/felix-seed-resp"))["felix_token"])')
[ -n "$CLIENT_TOKEN" ] || { echo "!! client exchange returned no felix_token" >&2; exit 1; }
mkdir -p /home/felix/felix-session
printf '%s' "$CLIENT_TOKEN" > /home/felix/felix-session/token
chown -R felix:felix /home/felix/felix-session
chmod 600 /home/felix/felix-session/token

# Relay the ADMIN token to the operator (framed) to drop on the brokers as their
# node token — node auth needs node.manage/node.view, which the client token
# deliberately lacks. It stays inside HTTPS to the Azure control plane.
printf '__FTOKEN_BEGIN__%s__FTOKEN_END__\n' "$TOKEN"
echo __RUNOK__
