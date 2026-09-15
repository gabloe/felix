#!/usr/bin/env bash
# Provision one perf session: resource group -> Bicep deployment -> wait for
# cloud-init -> seed -> print the inventory run.sh consumes.
#
# Everything a session creates lives in one resource group, and teardown.sh
# deletes the group. A scheduled auto-teardown backstops a wedged session so
# a forgotten cluster cannot idle through the budget.
set -euo pipefail

: "${SESSION:?SESSION=<name> (becomes the resource group felix-perf-<name>)}"
: "${LOCATION:=eastus2}"
: "${TIER:=t1}"
: "${RELEASE_TAG:=v0.3.0}"
# The instrument builds from a ref that HAS felix-loadgen (not the release
# tag, which predates the crate). main once merged; a branch while in review.
: "${LOADGEN_REF:=main}"
: "${SSH_KEY_FILE:=$HOME/.ssh/id_ed25519.pub}"

here="$(cd "$(dirname "$0")" && pwd)"
group="felix-perf-${SESSION}"
release_url="https://github.com/gabloe/felix/releases/download/${RELEASE_TAG}/felix-${RELEASE_TAG}-linux-x86_64.tar.gz"
bootstrap_token="$(openssl rand -hex 24)"
my_ip="$(curl -fsS https://api.ipify.org)"

# The IdP flow. Two ways in: paste IDP_TOKEN + the issuer/JWKS/audience
# yourself, or (the easy path) give the three app-registration secrets and let
# the suite derive the rest and mint the token. The seed and the token-exchange
# scenario both run against whatever this resolves to — the real flow, not demo
# auth.
if [ -z "${IDP_TOKEN:-}" ] && [ -n "${IDP_TENANT_ID:-}" ]; then
  : "${IDP_CLIENT_ID:?set IDP_CLIENT_ID with IDP_TENANT_ID}"
  : "${IDP_CLIENT_SECRET:?set IDP_CLIENT_SECRET with IDP_TENANT_ID}"
  : "${IDP_AUDIENCE:?set IDP_AUDIENCE (the app Application ID URI)}"
  export IDP_ISSUER="${IDP_ISSUER:-https://login.microsoftonline.com/${IDP_TENANT_ID}/v2.0}"
  export IDP_JWKS_URL="${IDP_JWKS_URL:-https://login.microsoftonline.com/${IDP_TENANT_ID}/discovery/v2.0/keys}"
  export IDP_TOKEN="$("${here}/idp-token.sh")"
  echo ">> minted an IdP token for audience ${IDP_AUDIENCE}"
fi

echo ">> session ${SESSION}: tier ${TIER}, ${LOCATION}, release ${RELEASE_TAG}"
# Land in the right subscription when the login has more than one.
[ -n "${AZ_SUBSCRIPTION:-}" ] && az account set --subscription "${AZ_SUBSCRIPTION}"
az group create --name "${group}" --location "${LOCATION}" --output none

# Compile to plain ARM JSON and deploy THAT, so the deployment never touches
# az's bundled bicep — which ships the wrong architecture on Apple Silicon
# (an ELF binary in a macOS install). Prefer a real `bicep` on PATH; fall back
# to az's, which works once check_version is off and a good binary is in place.
compiled="${here}/main.json"
if command -v bicep >/dev/null 2>&1; then
  bicep build "${here}/main.bicep" --outfile "${compiled}"
else
  az bicep build --file "${here}/main.bicep" --outfile "${compiled}"
fi

deployment=$(az deployment group create \
  --resource-group "${group}" \
  --template-file "${compiled}" \
  --parameters \
    tier="${TIER}" \
    releaseUrl="${release_url}" \
    loadgenRef="${LOADGEN_REF}" \
    sshPublicKey="$(cat "${SSH_KEY_FILE}")" \
    allowedSshCidr="${my_ip}/32" \
    bootstrapToken="${bootstrap_token}" \
  --query properties.outputs --output json)

loadgen_ip=$(jq -r .loadgenPublicIp.value <<<"${deployment}")
cp_ip=$(jq -r .controlPlaneIp.value <<<"${deployment}")
brokers=$(jq -r '.brokerIps.value | join(",")' <<<"${deployment}")

# The inventory is the contract between the session's scripts.
mkdir -p "${here}/sessions"
cat > "${here}/sessions/${SESSION}.env" <<INV
SESSION=${SESSION}
GROUP=${group}
TIER=${TIER}
RELEASE_TAG=${RELEASE_TAG}
LOADGEN_IP=${loadgen_ip}
CONTROLPLANE_IP=${cp_ip}
BROKER_IPS=${brokers}
BOOTSTRAP_TOKEN=${bootstrap_token}
INV
echo ">> inventory: ${here}/sessions/${SESSION}.env"

echo ">> waiting for cloud-init on the load generator (builds the instrument; several minutes)"
for _ in $(seq 1 120); do
  if ssh -o StrictHostKeyChecking=accept-new "felix@${loadgen_ip}" \
      'test -f /var/lib/cloud/instance/felix-provisioned' 2>/dev/null; then
    break
  fi
  sleep 15
done
ssh "felix@${loadgen_ip}" 'test -f /var/lib/cloud/instance/felix-provisioned' \
  || { echo "loadgen never finished provisioning"; exit 1; }

echo ">> seeding through the load generator (the control plane is VNet-private)"
scp -q "${here}/seed.sh" "felix@${loadgen_ip}:/tmp/seed.sh"
ssh "felix@${loadgen_ip}" env \
  CONTROLPLANE_IP="${cp_ip}" \
  BROKER_IPS="${brokers}" \
  BOOTSTRAP_TOKEN="${bootstrap_token}" \
  IDP_ISSUER="${IDP_ISSUER:-}" \
  IDP_JWKS_URL="${IDP_JWKS_URL:-}" \
  IDP_AUDIENCE="${IDP_AUDIENCE:-}" \
  IDP_TOKEN="${IDP_TOKEN:-}" \
  bash /tmp/seed.sh

echo ">> auto-teardown backstop at +8h"
az group update --name "${group}" \
  --set "tags.autoTeardownAfter=$(date -u -v+8H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d '+8 hours' '+%Y-%m-%dT%H:%M:%SZ')" \
  --output none

echo ">> ready. Next: RUN with"
echo "   SESSION=${SESSION} ${here}/run.sh"
echo "   and when finished: SESSION=${SESSION} ${here}/teardown.sh"
