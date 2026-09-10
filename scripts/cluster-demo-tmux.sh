#!/usr/bin/env bash
# The three-panel cross-broker demo, driven automatically.
#
#   scripts/cluster-demo-tmux.sh
#
# Opens a tmux session with the cluster top-left, a subscriber bottom-left, and
# a publisher on the right that runs the sequence on its own. Record the whole
# window; nothing needs typing.
#
# `PACE` is the gap between publishes, in seconds. Raise it to narrate over.
set -euo pipefail

PACE=${PACE:-4}
STREAM=${STREAM:-orders}
# The second burst runs concurrently, so the panel shows records arriving out of
# the order they were sent -- which is the point of showing it.
BURST_PARALLEL=${BURST_PARALLEL:-12}
SESSION=${SESSION:-felix-demo}

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BIN="$REPO_ROOT/target/debug/felix-cluster"

command -v tmux >/dev/null || { echo "tmux is not installed" >&2; exit 1; }
[[ -x $BIN ]] || { echo "build first: cargo build -p felix-cluster -p broker" >&2; exit 1; }

tmux kill-session -t "$SESSION" 2>/dev/null || true

# A session file from a previous cluster would otherwise be read by the panes
# before this cluster overwrites it.
rm -f "${TMPDIR:-/tmp}/felix-cluster.json"

# Pane 0: the cluster. Everything else waits for it.
tmux new-session -d -s "$SESSION" -x "${COLUMNS:-200}" -y "${LINES:-50}" \
  "cd '$REPO_ROOT' && '$BIN' up"

# Right half: the publisher. Bottom-left: the subscriber.
tmux split-window -h -t "$SESSION:0.0" -c "$REPO_ROOT"
tmux split-window -v -t "$SESSION:0.0" -c "$REPO_ROOT"

# Both wait on the cluster actually being up rather than on a fixed sleep.
#
# `owners`, not `nodes`: `nodes` only reads the session file, so a file left by
# a previous cluster satisfies it immediately and the pane then talks to a
# control plane that is gone. `owners` has to reach the control plane, so it
# fails until *this* cluster is answering.
wait_for_cluster="until '$BIN' owners >/dev/null 2>&1; do sleep 0.3; done"

tmux send-keys -t "$SESSION:0.1" \
  "clear; $wait_for_cluster; '$BIN' subscribe $STREAM" C-m

# The publisher runs from a file so the pane shows its output rather than a
# screenful of the command that produced it.
tmux send-keys -t "$SESSION:0.2" \
  "clear; STREAM=$STREAM PACE=$PACE BURST_PARALLEL=$BURST_PARALLEL '$REPO_ROOT/scripts/cluster-demo-publisher.sh'" C-m

tmux select-pane -t "$SESSION:0.2"

# ATTACH=0 sets the session up and returns, for scripted checks.
if [[ ${ATTACH:-1} == 0 ]]; then
  echo "session '$SESSION' running detached"
  exit 0
fi

echo "attaching to tmux session '$SESSION' — Ctrl-B then D to detach, Ctrl-C in the cluster pane to stop"
tmux attach -t "$SESSION"
