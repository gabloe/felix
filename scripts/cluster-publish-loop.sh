#!/usr/bin/env bash
# Publish a burst of messages to a running local cluster, spread across every
# broker in it.
#
# Start a cluster with `task cluster:up` and a subscriber with
# `task -s cluster:subscribe -- orders` first; this fills the subscriber's
# panel.
#
#   task cluster:burst                         30 messages, in order
#   COUNT=60 GAP=0.2 task cluster:burst        slower, for reading on camera
#   PARALLEL=10 task cluster:burst             concurrent, and visibly reordered
#
# Ordering is worth understanding before using this to demonstrate anything.
# At PARALLEL=1 records arrive in the order they were sent. Above that they do
# not, and that is correct rather than a defect: concurrent publishes through
# different brokers have no defined relative order, and the owner assigns
# offsets in the order it commits them. Felix orders a publisher's own sequence,
# not a race between three of them.
set -euo pipefail

STREAM=${STREAM:-orders}
COUNT=${COUNT:-30}
PARALLEL=${PARALLEL:-1}
GAP=${GAP:-0}

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BIN=${BIN:-"$REPO_ROOT/target/debug/felix-cluster"}

if [[ ! -x $BIN ]]; then
  echo "no felix-cluster binary at $BIN" >&2
  echo "build it first: cargo build -p felix-cluster" >&2
  exit 1
fi

# Fail here rather than once per message: without a running cluster every
# publish below would print the same error thirty times.
if ! "$BIN" nodes >/dev/null 2>&1; then
  echo "no cluster is running -- start one with \`task cluster:up\`" >&2
  exit 1
fi

# Ask the cluster which brokers exist rather than assuming three, so this keeps
# working with `task cluster:up -- --nodes 5`.
#
# `nodes`, not `owners`: owners lists shard leaders, and a broker holding no
# shard is exactly the one this demo wants to publish through.
#
# Read with a loop and counted by hand: `mapfile` is bash 4, and `${#arr[@]}` on
# an empty array trips `set -u` under bash 3.2. macOS ships 3.2.
BROKERS=()
BROKER_COUNT=0
while IFS= read -r line; do
  [[ -z $line ]] && continue
  BROKERS[$BROKER_COUNT]=$line
  BROKER_COUNT=$((BROKER_COUNT + 1))
done < <("$BIN" nodes 2>/dev/null)

if (( BROKER_COUNT == 0 )); then
  echo "could not read broker names from \`$BIN owners\`" >&2
  exit 1
fi

echo "publishing $COUNT messages to $STREAM across $BROKER_COUNT broker(s), parallel=$PARALLEL"

for i in $(seq 1 "$COUNT"); do
  via=${BROKERS[$(( (i - 1) % BROKER_COUNT ))]}
  "$BIN" publish "$STREAM" "$(printf 'msg-%03d' "$i")" --via "$via" >/dev/null &

  # Hold in-flight publishes at PARALLEL. Polled rather than `wait -n`, which
  # macOS's bash 3.2 does not have.
  while (( $(jobs -rp | wc -l) >= PARALLEL )); do sleep 0.05; done
  [[ $GAP != 0 ]] && sleep "$GAP"
done

wait
echo "done: $COUNT messages"
