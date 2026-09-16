#!/usr/bin/env bash
# Shared plumbing for the session scripts. The one idea here: the operator
# never SSHes into the session. Every operator->VM interaction goes through
# `az vm run-command invoke`, which rides the Azure control plane over HTTPS.
#
# Why not SSH? Two reasons the first live run found the hard way:
#   1. Some operator networks deep-packet-inspect and RST outbound :22 to
#      arbitrary cloud IPs (GitHub's is whitelisted; a fresh Azure VM is not),
#      so `ssh felix@<vm>` resets at key exchange with nothing wrong on the VM.
#   2. Ubuntu 24.04's socket-activated sshd needs /run/sshd, which is not
#      always present on first boot — a second, unrelated way in to fail.
# run-command sidesteps both, needs no inbound :22 at all, and reaches the
# VNet-private control plane by running ON a VM that is already inside the VNet.
#
# Scripts shipped to run-command execute under **dash as root**, so everything
# in cmd_* heredocs must be POSIX sh: no `set -o pipefail`, no arrays, no
# `[[ ]]`. Each remote script ends by printing __RUNOK__; run_on treats its
# absence as failure, because run-command reports the *delivery* as succeeded
# even when the script itself exits non-zero.

# run_on <vm> <local-script-file>
# Execute the script on the VM as root, echo its combined output, and fail
# unless the script reached its __RUNOK__ sentinel.
run_on() {
  _vm="$1"; _script="$2"
  _msg="$(az vm run-command invoke \
    --resource-group "${GROUP}" --name "${_vm}" \
    --command-id RunShellScript --scripts @"${_script}" \
    --query 'value[0].message' -o tsv 2>&1)" || {
    printf '%s\n' "${_msg}" >&2
    return 1
  }
  printf '%s\n' "${_msg}"
  printf '%s' "${_msg}" | grep -q '__RUNOK__' || {
    echo "!! remote script on ${_vm} did not complete (no __RUNOK__)" >&2
    return 1
  }
}

# run_on_str <vm> <script-text>
# Same, for a script passed as a string rather than a file. Writes it to a
# private temp file (the script text can carry tokens), runs it, and removes it.
run_on_str() {
  _vm="$1"; _text="$2"
  _tmp="$(mktemp "${TMPDIR:-/tmp}/felix-run.XXXXXX")"
  chmod 600 "${_tmp}"
  printf '%s' "${_text}" > "${_tmp}"
  run_on "${_vm}" "${_tmp}"
  _rc=$?
  rm -f "${_tmp}"
  return ${_rc}
}

# Extract the payload a remote script framed between __<TAG>_BEGIN__ and
# __<TAG>_END__ markers in its output. Used to bring a value (a token, a count)
# back to the operator from a run-command message without the [stdout] framing.
extract_between() {
  _tag="$1"
  sed -n "s/.*__${_tag}_BEGIN__\(.*\)__${_tag}_END__.*/\1/p" | head -n1
}

# Deterministic VM names — main.bicep's `prefix` is 'felixperf'.
broker_vm() { echo "felixperf-broker-$1"; }
cp_vm() { echo "felixperf-controlplane"; }
loadgen_vm() { echo "felixperf-loadgen"; }
