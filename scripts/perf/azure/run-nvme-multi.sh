#!/usr/bin/env bash
# Multi-loadgen aggregate ingest: a single D4 loadgen is crypto-bound at ~1.15
# GB/s, so drive several loadgens at once to push the brokers to their CPU
# ceiling and read MB/s per vCPU (the number that survives a disk-bound
# comparison). Distributes the client token to the extra loadgens, runs the
# ingest scenario on all of them in parallel against the durable stream, samples
# every broker's CPU breakdown at the peak, and sums throughput. Captures full
# per-loadgen output so a failed loadgen is visible, not silently dropped.
set -uo pipefail
: "${SESSION:?SESSION=<name>}"
: "${STREAM:=perf-durable}"
: "${CONC:=16}"                       # publishers per loadgen
# Sourcing the inventory below would clobber a LOADGENS given on the command
# line, and sweeping the generator count is the whole point of this script --
# one, two, then three against the same broker. Remember the caller's value and
# put it back afterwards, so an explicit LOADGENS always wins over the
# inventory's full list.
_loadgens_override="${LOADGENS:-}"
here="$(cd "$(dirname "$0")" && pwd)"
source "${here}/lib.sh"
source "${here}/sessions/${SESSION}.env"
LOADGENS="${_loadgens_override:-${LOADGENS:-felixperf-loadgen}}"
echo ">> generators: ${LOADGENS}"
export GROUP="${GROUP}"

IFS=',' read -ra brokers <<<"${BROKER_IPS}"
broker_addrs="${BROKER_ADDRS:-$(printf "%s:5000," "${brokers[@]}")}"; broker_addrs="${broker_addrs%,}"
TOKEN_FILE="/home/felix/felix-session/token"

echo ">> distributing the client token"
TOK_B64="$(run_on_str felixperf-loadgen "base64 -w0 < ${TOKEN_FILE}; echo" 2>/dev/null | grep -E '^[A-Za-z0-9+/=]{200,}$' | head -1 || true)"
[ -n "${TOK_B64}" ] || { echo "!! could not read the client token from loadgen-0" >&2; exit 1; }
for lg in ${LOADGENS}; do
  [ "$lg" = "felixperf-loadgen" ] && continue
  run_on_str "$lg" "mkdir -p /home/felix/felix-session
printf '%s' '${TOK_B64}' | base64 -d > ${TOKEN_FILE}
chown -R felix:felix /home/felix/felix-session
test -s ${TOKEN_FILE} && echo 'token written' || echo 'TOKEN EMPTY'
echo __RUNOK__" 2>/dev/null | grep -E 'token written|TOKEN EMPTY' | head -1 | sed "s/^/   $lg: /"
done

run_agg() {
  conc="$1"; total=$(( conc * 400000 ))
  ncount=$(echo ${LOADGENS} | wc -w)
  echo ">> aggregate ingest: ${conc} pubs/loadgen x ${ncount} loadgens = $(( conc * ncount )) publishers, stream=${STREAM}"
  rm -f /tmp/agg-felixperf-loadgen*.txt
  pids=""
  for lg in ${LOADGENS}; do
    ( run_on_str "$lg" "export FELIX_MTU_UPPER_BOUND=4096; ulimit -n 1048576 || true
felix-loadgen --brokers '${broker_addrs}' --tenant perf --token-file '${TOKEN_FILE}' --environment 'azure-nvme-multi' --scenario ingest --stream '${STREAM}' --payload-bytes 4096 --batch 64 --concurrency ${conc} --total ${total}
echo __RUNOK__" > "/tmp/agg-${lg}.txt" 2>&1 ) &
    pids="$pids $!"
  done
  # broker CPU breakdown mid-run (us/sy/id/wa/si)
  sleep 14
  echo "   -- broker CPU at load --"
  for i in "${!brokers[@]}"; do
    line=$(run_on_str "$(broker_vm "${i}")" 'top -bn4 -d 1 | grep "^%Cpu" | tail -1; echo __RUNOK__' 2>/dev/null | grep '%Cpu' | head -1 || true)
    echo "      broker-${i}: ${line:-n/a}"
  done
  wait $pids 2>/dev/null || true
  echo "   -- per-loadgen results --"
  total_mb=0; total_ms=0; n=0
  for lg in ${LOADGENS}; do
    line=$(grep '^ingest:' "/tmp/agg-${lg}.txt" 2>/dev/null | head -1 || true)
    if [ -n "$line" ]; then
      echo "      ${lg}: ${line}"
      mb=$(echo "$line" | grep -oE '[0-9.]+ MB/s' | grep -oE '[0-9.]+' | head -1)
      ms=$(echo "$line" | grep -oE '[0-9]+ msg/s' | grep -oE '[0-9]+' | head -1)
      total_mb=$(awk "BEGIN{printf \"%.1f\", ${total_mb} + ${mb:-0}}")
      total_ms=$(awk "BEGIN{printf \"%.0f\", ${total_ms} + ${ms:-0}}")
      n=$((n+1))
    else
      echo "      ${lg}: NO RESULT -- tail:"; tail -4 "/tmp/agg-${lg}.txt" 2>/dev/null | sed 's/^/         /'
    fi
  done
  echo "   AGGREGATE: ${total_ms} msg/s, ${total_mb} MB/s across ${n}/${ncount} loadgens"
  rm -f /tmp/agg-felixperf-loadgen*.txt
}

run_agg "${CONC}"
