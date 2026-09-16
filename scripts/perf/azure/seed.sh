#!/usr/bin/env bash
# Seed one perf session, orchestrated from the operator via `az vm run-command`
# (never SSH — see lib.sh). Three moves:
#
#   1. On the loadgen (inside the VNet, so it can reach the private control
#      plane): bootstrap the tenant with the real IdP, exchange the IdP token
#      for a Felix operator token, create the namespace/streams/cache. This is
#      the deployment's own flow, measured — not demo auth. seed-remote.sh is
#      the body; it frames the felix token on stdout.
#   2. On each broker: drop that token as /etc/felix/node.token and start the
#      broker. run-command reaches each broker directly, so no loadgen->broker
#      SSH (which had no key and rode the same DPI-blocked path) is needed.
#   3. Poll the control plane (from the loadgen) until every broker registers.
set -euo pipefail

: "${CONTROLPLANE_IP:?}"
: "${BROKER_COUNT:?}"
: "${BOOTSTRAP_TOKEN:?}"
: "${IDP_JWKS_URL:?set IDP_JWKS_URL}"
: "${IDP_AUDIENCE:?set IDP_AUDIENCE (the app registration audience)}"
: "${IDP_TOKEN:?set IDP_TOKEN (a token from the IdP for the perf principal)}"
: "${GROUP:?}"

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${here}/lib.sh"

TENANT="perf"
NAMESPACE="default"

# --- 1. bootstrap + exchange + scopes, on the loadgen -----------------------
# A dash header assigns the values seed-remote.sh reads; the tokens are hex/
# base64url with no single quotes, so single-quoting them is safe. Everything
# rides run-command's HTTPS to Azure, never the terminal-visible network.
header="$(cat <<HDR
set -eu
CP='http://${CONTROLPLANE_IP}:8080'
BOOTSTRAP='http://${CONTROLPLANE_IP}:8081'
BOOTSTRAP_TOKEN='${BOOTSTRAP_TOKEN}'
IDP_TOKEN='${IDP_TOKEN}'
IDP_JWKS_URL='${IDP_JWKS_URL}'
IDP_AUDIENCE='${IDP_AUDIENCE}'
TENANT='${TENANT}'
NAMESPACE='${NAMESPACE}'
REPLICATION_FACTOR='${REPLICATION_FACTOR:-1}'
SHARDS='${SHARDS:-12}'
HDR
)"
# Capture without set -e aborting the assignment, so the remote message is
# always printed — a swallowed run-command message is a blind failure.
set +e
seed_out="$(run_on_str "$(loadgen_vm)" "${header}
$(cat "${here}/seed-remote.sh")")"
seed_rc=$?
set -e
printf '%s\n' "${seed_out}"
[ ${seed_rc} -eq 0 ] || { echo "!! loadgen seed step failed (see message above)" >&2; exit 1; }
token="$(printf '%s' "${seed_out}" | extract_between FTOKEN)"
[ -n "${token}" ] || { echo "!! seed did not return a felix token" >&2; exit 1; }

# --- 2. drop the node token on each broker and start it ---------------------
echo ">> node token to each broker, then start"
for i in $(seq 0 $((BROKER_COUNT - 1))); do
  # felix-broker-env.sh regenerates /etc/felix/broker.env before start (the
  # unit's ExecStartPre does too, but a required EnvironmentFile must already
  # exist when the unit activates, so we write it here first). reset-failed
  # clears any earlier give-up. `restart` (not just enable --now) is deliberate:
  # a re-seed drops a *fresh* token, and the broker reads its token once at
  # startup with no refresh, so an already-running broker must be restarted to
  # pick it up — enable --now is a no-op on an active unit.
  broker_out="$(run_on_str "$(broker_vm "${i}")" "set -eu
mkdir -p /etc/felix
printf '%s' '${token}' > /etc/felix/node.token
chmod 600 /etc/felix/node.token
/usr/local/sbin/felix-broker-env.sh
systemctl reset-failed felix-broker 2>/dev/null || true
systemctl enable felix-broker
systemctl restart felix-broker
echo __RUNOK__")" || {
    printf '%s\n' "${broker_out}" >&2
    echo "!! broker-${i} did not start (see message above)" >&2
    exit 1
  }
  echo "   broker-${i}: started"
done

# --- 3. wait for the brokers to register ------------------------------------
echo ">> waiting for the brokers to register and take their shards"
poll_out="$(run_on_str "$(loadgen_vm)" "set -eu
TOKEN=\$(cat /home/felix/felix-session/token)
n=0
i=0
while [ \$i -lt 60 ]; do
  n=\$(curl -fsS 'http://${CONTROLPLANE_IP}:8080/v1/nodes' -H \"Authorization: Bearer \$TOKEN\" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get(\"items\", d.get(\"nodes\", []))))' 2>/dev/null || echo 0)
  [ \"\$n\" -ge ${BROKER_COUNT} ] && break
  i=\$((i + 1)); sleep 5
done
printf '__NODES_BEGIN__%s__NODES_END__\n' \"\$n\"
echo __RUNOK__")"
registered="$(printf '%s' "${poll_out}" | extract_between NODES)"
echo ">> ${registered:-0}/${BROKER_COUNT} brokers registered"
[ "${registered:-0}" -ge "${BROKER_COUNT}" ] || {
  echo "!! not all brokers registered; inspect with: az vm run-command invoke -g ${GROUP} -n $(broker_vm 0) --command-id RunShellScript --scripts 'journalctl -u felix-broker --no-pager | tail -50'" >&2
  exit 1
}
echo ">> seeded: tenant ${TENANT}, token on the loadgen at ~/felix-session/token"
