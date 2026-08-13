#!/usr/bin/env bash
# Durable-log benchmark matrix.
#
# Sweeps the three fsync policies across batch sizes and concurrency levels and
# emits one JSON line per run, so results are directly comparable across
# machines and across time.
#
# Concurrency is in the matrix because it is the axis group commit lives on: at
# concurrency 1 every OnCommit append pays for its own device flush, and the
# whole point of the design is what happens above that. A benchmark that only
# measured a single publisher would report the cost of durability as several
# times what a loaded broker actually pays.
#
# Usage:
#   scripts/bench-durable-log.sh [output.jsonl]
#
# Environment:
#   FELIX_BENCH_RECORDS      records per run          (default 20000)
#   FELIX_BENCH_PAYLOAD      payload bytes            (default 128)
#   FELIX_BENCH_DIR          scratch directory        (default a temp dir)
#   FELIX_BENCH_CONCURRENCY  concurrency levels       (default "1 8 64")
#   FELIX_BENCH_BATCH        batch sizes              (default "1 16")

set -euo pipefail

OUTPUT="${1:-durable-log-bench.jsonl}"
RECORDS="${FELIX_BENCH_RECORDS:-20000}"
PAYLOAD="${FELIX_BENCH_PAYLOAD:-128}"
CONCURRENCY_LEVELS="${FELIX_BENCH_CONCURRENCY:-1 8 64}"
BATCH_SIZES="${FELIX_BENCH_BATCH:-1 16}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Release build only: a debug build measures the optimiser, not the storage.
echo "building felix-log-tool (release)..." >&2
cargo build --release -p felix-storage --bin felix-log-tool >&2
TOOL="$REPO_ROOT/target/release/felix-log-tool"

SCRATCH="${FELIX_BENCH_DIR:-$(mktemp -d)}"
CLEANUP_SCRATCH=0
if [ -z "${FELIX_BENCH_DIR:-}" ]; then
  CLEANUP_SCRATCH=1
fi
cleanup() {
  if [ "$CLEANUP_SCRATCH" = "1" ]; then rm -rf "$SCRATCH"; fi
}
trap cleanup EXIT

: > "$OUTPUT"

# Record the environment alongside the numbers. A latency figure without the
# filesystem and device it was measured on is not reproducible, and this is the
# single most common way benchmark results become unusable six months later.
{
  printf '{"event":"environment"'
  printf ',"date":"%s"' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf ',"git_commit":"%s"' "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  printf ',"os":"%s"' "$(uname -sr | tr -d '"')"
  printf ',"arch":"%s"' "$(uname -m)"
  printf ',"cpus":"%s"' "$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo unknown)"
  # The machine model and CPU matter more than the OS version for storage
  # numbers, and are the details most likely to be misremembered later.
  if command -v sysctl >/dev/null 2>&1; then
    printf ',"model":"%s"' "$(sysctl -n hw.model 2>/dev/null || echo unknown)"
    printf ',"cpu":"%s"' "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
  elif [ -r /proc/cpuinfo ]; then
    printf ',"model":"%s"' "$(cat /sys/devices/virtual/dmi/id/product_name 2>/dev/null || echo unknown)"
    printf ',"cpu":"%s"' "$(awk -F': ' '/model name/{print $2; exit}' /proc/cpuinfo || echo unknown)"
  fi
  printf ',"scratch_dir":"%s"' "$SCRATCH"
  if command -v df >/dev/null 2>&1; then
    printf ',"filesystem":"%s"' "$(df -T "$SCRATCH" 2>/dev/null | awk 'NR==2{print $2}' || df "$SCRATCH" | awk 'NR==2{print $1}')"
  fi
  printf ',"records":%s,"payload_bytes":%s' "$RECORDS" "$PAYLOAD"
  printf '}\n'
} | tee -a "$OUTPUT"

run_one() {
  local fsync="$1" batch="$2" concurrency="$3"
  local label="${fsync}-b${batch}-c${concurrency}"
  local dir="$SCRATCH/$label"
  # A fresh directory per run: reusing one would let an earlier run's segments
  # and page-cache residency skew the next.
  rm -rf "$dir"
  mkdir -p "$dir"

  "$TOOL" bench \
    --dir "$dir" \
    --records "$RECORDS" \
    --payload-bytes "$PAYLOAD" \
    --batch "$batch" \
    --concurrency "$concurrency" \
    --fsync "$fsync" \
    --fsync-interval-ms 250 \
    --warmup-records 2000 \
    --label "$label" | tee -a "$OUTPUT"

  rm -rf "$dir"
}

for fsync in none periodic on_commit; do
  for batch in $BATCH_SIZES; do
    for concurrency in $CONCURRENCY_LEVELS; do
      run_one "$fsync" "$batch" "$concurrency"
    done
  done
done

echo >&2
echo "results written to $OUTPUT" >&2
echo "compare against the budget in docs/storage-performance.md" >&2
