#!/usr/bin/env bash
# Provision one perf session: resource group -> Bicep deployment -> wait for
# cloud-init -> seed -> print the inventory run.sh consumes.
#
# Everything a session creates lives in one resource group, and teardown.sh
# deletes the group. A scheduled auto-teardown backstops a wedged session so
# a forgotten cluster cannot idle through the budget.
#
# The operator drives the VMs with `az vm run-command` over HTTPS, never SSH
# (see lib.sh for why). The only inbound port the session needs is nothing:
# the control plane is VNet-private and reached by running on a VM inside it.
set -euo pipefail

: "${SESSION:?SESSION=<name> (becomes the resource group felix-perf-<name>)}"
: "${LOCATION:=eastus2}"
: "${TIER:=t1}"
: "${RELEASE_TAG:=v0.3.1}"
# The instrument builds from a ref that HAS felix-loadgen. The crate merged to
# main in #370, so main is the default again; override for a branch under review.
: "${LOADGEN_REF:=main}"
: "${SSH_KEY_FILE:=$HOME/.ssh/id_ed25519.pub}"

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${here}/lib.sh"
group="felix-perf-${SESSION}"
export GROUP="${group}"
release_url="https://github.com/gabloe/felix/releases/download/${RELEASE_TAG}/felix-${RELEASE_TAG}-linux-x86_64.tar.gz"
bootstrap_token="$(openssl rand -hex 24)"

# The IdP flow. Two ways in: paste IDP_TOKEN + the issuer/JWKS/audience
# yourself, or (the easy path) give the three app-registration secrets and let
# the suite derive the rest and mint the token. The seed and the token-exchange
# scenario both run against whatever this resolves to — the real flow, not demo
# auth.
if [ -z "${IDP_TOKEN:-}" ] && [ -n "${IDP_TENANT_ID:-}" ]; then
  : "${IDP_CLIENT_ID:?set IDP_CLIENT_ID with IDP_TENANT_ID}"
  : "${IDP_CLIENT_SECRET:?set IDP_CLIENT_SECRET with IDP_TENANT_ID}"
  export IDP_SCOPE="${IDP_SCOPE:-${IDP_AUDIENCE:-}}"
  : "${IDP_SCOPE:?set IDP_SCOPE (the app Application ID URI, or the bare client id)}"
  # JWKS is version-agnostic (validates both v1 and v2 tokens). The issuer and
  # the audience are NOT: an app-only credential issues a v1 token
  # (iss=sts.windows.net/<tenant>/, aud=api://<client-id>) even from the v2
  # endpoint, where a v2 token carries the bare client-id GUID. seed-remote.sh
  # derives both from the token rather than assuming them here.
  export IDP_JWKS_URL="${IDP_JWKS_URL:-https://login.microsoftonline.com/${IDP_TENANT_ID}/discovery/v2.0/keys}"
  export IDP_TOKEN="$("${here}/idp-token.sh")"
  echo ">> minted an IdP token for scope ${IDP_SCOPE}"
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

# allowedSshCidr no longer gates a working path in (we use run-command, not
# SSH), but the NSG rule and the parameter remain so a human CAN open a shell
# for debugging from their own address. 0.0.0.0/0 would be the wrong default;
# resolve the operator's address and scope the rule to it.
my_ip="$(curl -fsS https://api.ipify.org)"

deployment=$(az deployment group create \
  --resource-group "${group}" \
  --template-file "${compiled}" \
  --parameters \
    tier="${TIER}" \
    releaseUrl="${release_url}" \
    loadgenRef="${LOADGEN_REF}" \
    brokerCount="${BROKER_COUNT:-3}" \
    loadgenCount="${LOADGEN_COUNT:-1}" \
    brokerVmSize="${BROKER_VM_SIZE:-Standard_D4as_v5}" \
    useLocalNvme="${USE_LOCAL_NVME:-false}" \
    brokerListeners="${BROKER_LISTENERS:-1}" \
    sshPublicKey="$(cat "${SSH_KEY_FILE}")" \
    allowedSshCidr="${my_ip}/32" \
    bootstrapToken="${bootstrap_token}" \
  --query properties.outputs --output json)

loadgen_ip=$(jq -r .loadgenPublicIp.value <<<"${deployment}")
cp_ip=$(jq -r .controlPlaneIp.value <<<"${deployment}")
brokers=$(jq -r '.brokerIps.value | join(",")' <<<"${deployment}")
loadgens=$(jq -r '.loadgenNames.value | join(" ")' <<<"${deployment}")

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
LOADGENS="${loadgens}"
BOOTSTRAP_TOKEN=${bootstrap_token}
INV
echo ">> inventory: ${here}/sessions/${SESSION}.env"

# Wait for cloud-init via run-command (not SSH): the loadgen writes the
# felix-provisioned marker only after building the instrument, which is the
# slow step. The brokers and control plane finish sooner, but we gate on all
# three so seed.sh never races a half-provisioned VM.
wait_provisioned() {
  vm="$1"
  for _ in $(seq 1 120); do
    if run_on_str "${vm}" 'test -f /var/lib/cloud/instance/felix-provisioned && echo __RUNOK__' \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 15
  done
  echo "!! ${vm} never finished cloud-init" >&2
  return 1
}
echo ">> waiting for cloud-init (loadgen builds the instrument; several minutes)"
wait_provisioned "$(cp_vm)"
wait_provisioned "$(loadgen_vm)"
IFS=',' read -ra broker_ip_list <<<"${brokers}"
for i in "${!broker_ip_list[@]}"; do wait_provisioned "$(broker_vm "${i}")"; done

echo ">> seeding (bootstrap + exchange on the loadgen; token-drop + start per broker)"
CONTROLPLANE_IP="${cp_ip}" \
BROKER_COUNT="${#broker_ip_list[@]}" \
BOOTSTRAP_TOKEN="${bootstrap_token}" \
IDP_JWKS_URL="${IDP_JWKS_URL:-}" \
IDP_AUDIENCE="${IDP_AUDIENCE:-}" \
IDP_TOKEN="${IDP_TOKEN:-}" \
GROUP="${group}" \
  bash "${here}/seed.sh"

echo ">> auto-teardown backstop at +8h"
az group update --name "${group}" \
  --set "tags.autoTeardownAfter=$(date -u -v+8H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d '+8 hours' '+%Y-%m-%dT%H:%M:%SZ')" \
  --output none

echo ">> ready. Next: RUN with"
echo "   SESSION=${SESSION} ${here}/run.sh"
echo "   and when finished: SESSION=${SESSION} ${here}/teardown.sh"
