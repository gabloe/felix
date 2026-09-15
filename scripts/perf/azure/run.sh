#!/usr/bin/env bash
# Drive the matrix from the load generator and bring the data home.
#
# The machines stay otherwise idle for the duration — the same hygiene as a
# local run, enforced by construction. Results land in
# sessions/<name>-results/ as one JSONL of LOADGEN_JSON lines plus a
# session.json of environment metadata, which is what makes a number
# publishable: every row says which tier, SKUs, release, and RTT baseline
# produced it.
set -euo pipefail

: "${SESSION:?SESSION=<name>}"
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1090
source "${here}/sessions/${SESSION}.env"

out="${here}/sessions/${SESSION}-results"
mkdir -p "${out}"
IFS=',' read -ra brokers <<<"${BROKER_IPS}"
broker_addrs=$(printf '%s:5000,' "${brokers[@]}"); broker_addrs="${broker_addrs%,}"

run_case() {
  local label="$1"; shift
  echo ">> ${label}"
  ssh "felix@${LOADGEN_IP}" felix-loadgen \
    --brokers "${broker_addrs}" \
    --tenant perf --token-file '~/felix-session/token' \
    --environment "azure-${TIER}" \
    "$@" | tee "${out}/${label}.out"
}

# RTT baselines first: the number every latency below is read against.
echo ">> network baselines"
{
  echo "{"
  echo "  \"session\": \"${SESSION}\", \"tier\": \"${TIER}\","
  echo "  \"release\": \"${RELEASE_TAG}\","
  echo "  \"rtt_ms\": {"
  first=true
  for ip in "${brokers[@]}"; do
    $first || echo ","; first=false
    rtt=$(ssh "felix@${LOADGEN_IP}" "ping -qc 10 ${ip} | awk -F/ 'END{{print \$5}}'" </dev/null)
    printf '    "%s": %s' "${ip}" "${rtt:-null}"
  done
  echo ""
  echo "  }"
  echo "}"
} > "${out}/session.json"
cat "${out}/session.json"

# --- The matrix. Latency cases are batch 1 with per-message acks (the
# request-latency configuration); throughput cases are batch 64, sized so the
# volume comfortably exceeds the QUIC send window.
for payload in 0 256 4096; do
  for fanout in 1 10 50; do
    run_case "pubsub-lat-p${payload}-f${fanout}" \
      --scenario pubsub --stream perf --payload-bytes "${payload}" \
      --fanout "${fanout}" --batch 1 --warmup 2000 --total 20000
  done
  run_case "pubsub-tp-p${payload}" \
    --scenario pubsub --stream perf --payload-bytes "${payload}" \
    --fanout 10 --batch 64 --binary --warmup 2000 --total 500000
done

run_case "cache-p256" --scenario cache --cache perf --payload-bytes 256 --concurrency 8 --total 20000
run_case "counter" --scenario counter --cache perf --concurrency 8 --total 20000

for watchers in 1 50 500; do
  run_case "watch-f${watchers}" \
    --scenario watch --cache perf --fanout "${watchers}" \
    --payload-bytes 256 --warmup 200 --total 2000
done

# One artifact the analysis reads directly.
grep -h '^LOADGEN_JSON ' "${out}"/*.out | sed 's/^LOADGEN_JSON //' > "${out}/results.jsonl"
echo ">> $(wc -l < "${out}/results.jsonl") result rows in ${out}/results.jsonl"
echo ">> now: SESSION=${SESSION} ${here}/teardown.sh"
