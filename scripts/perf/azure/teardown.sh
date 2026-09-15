#!/usr/bin/env bash
# End the session: delete the resource group and everything in it. The last
# line of every run — nothing in a session is meant to survive it.
set -euo pipefail
: "${SESSION:?SESSION=<name>}"
group="felix-perf-${SESSION}"
echo ">> deleting ${group}"
az group delete --name "${group}" --yes --no-wait
echo ">> deletion running in Azure; verify later with: az group exists --name ${group}"
