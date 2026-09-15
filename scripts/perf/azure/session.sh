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

echo ">> session ${SESSION}: tier ${TIER}, ${LOCATION}, release ${RELEASE_TAG}"
az group create --name "${group}" --location "${LOCATION}" --output none

deployment=$(az deployment group create \
  --resource-group "${group}" \
  --template-file "${here}/main.bicep" \
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
