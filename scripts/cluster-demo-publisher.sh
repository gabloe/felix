#!/usr/bin/env bash
# The publisher half of the three-panel demo.
#
# Run by `cluster-demo-tmux.sh` in its own pane. A separate file rather than a
# long `tmux send-keys` string: the shell echoes whatever it is sent, and a
# thirty-line command fills the pane with itself before printing anything.
set -euo pipefail

STREAM=${STREAM:-orders}
PACE=${PACE:-4}
BURST_PARALLEL=${BURST_PARALLEL:-12}

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BIN="$REPO_ROOT/target/debug/felix-cluster"

# `owners` has to reach the control plane, so this waits for the cluster rather
# than for a session file a previous run may have left behind.
until "$BIN" owners >/dev/null 2>&1; do sleep 0.3; done
sleep 2

OWNER=$("$BIN" owners | awk '{print $3}')
OTHER=$("$BIN" nodes | grep -v "^${OWNER}$" | head -1)

echo "owner of $STREAM is $OWNER"
echo
sleep "$PACE"

echo "publishing through $OTHER, which does not own it"
"$BIN" publish "$STREAM" hello --via "$OTHER"
sleep "$PACE"

echo
echo "the same publish, through $OWNER, which does"
"$BIN" publish "$STREAM" direct --via "$OWNER"
sleep "$PACE"

echo
echo "a burst across every broker, one at a time"
PREFIX=seq COUNT=6 GAP=0.35 PARALLEL=1 "$REPO_ROOT/scripts/cluster-publish-loop.sh"
sleep "$PACE"

echo
echo "now $BURST_PARALLEL at once, through all three brokers."
echo "arrival order is no longer guaranteed to match the send order --"
echo "concurrent publishes through different brokers have no relative order."
sleep 1
PREFIX=par COUNT=24 GAP=0 PARALLEL="$BURST_PARALLEL" "$REPO_ROOT/scripts/cluster-publish-loop.sh"

echo
echo "done — every record reached the subscriber"
