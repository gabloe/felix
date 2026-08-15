#!/usr/bin/env python3
import argparse
import csv
import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW_JSONL = ROOT / "data" / "raw" / "latency_demo_runs.jsonl"
DERIVED_DIR = ROOT / "data" / "derived"
RUNS_CSV = DERIVED_DIR / "latency_demo_runs.csv"
AGG_CSV = DERIVED_DIR / "latency_demo_agg.csv"


def percentile(values, pct):
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    values = sorted(values)
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return float(values[int(k)])
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return float(d0 + d1)


def load_runs(path: Path):
    runs = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            runs.append(json.loads(line))
    return runs


def flatten_run(run: dict) -> dict:
    metrics = run.get("metrics") or {}
    workload = run.get("workload") or {}
    host = run.get("host_info") or {}
    row = {
        "run_id": run.get("run_id"),
        "timestamp": run.get("timestamp"),
        "git_sha": run.get("git_sha"),
        "git_dirty": run.get("git_dirty"),
        "hostname": host.get("hostname"),
        "platform": host.get("platform"),
        "cpu_count": host.get("cpu_count"),
        "cargo_version": run.get("cargo_version"),
        "rustc_version": run.get("rustc_version"),
        "broker_profile": run.get("broker_profile"),
        "preset": run.get("preset"),
        "fanout": workload.get("fanout"),
        "batch": workload.get("batch"),
        "payload_bytes": workload.get("payload_bytes"),
        "warmup": workload.get("warmup"),
        "total": workload.get("total"),
        "binary": workload.get("binary"),
        "trial_index": run.get("trial_index"),
        "exit_code": run.get("exit_code"),
        "parse_error": run.get("parse_error"),
        "stdout_path": run.get("stdout_path"),
        "command": run.get("command_str"),
        "felix_env": json.dumps(run.get("felix_env", {}), sort_keys=True),
        "p50_us": metrics.get("p50_us"),
        "p50_ms": metrics.get("p50_ms"),
        "p99_us": metrics.get("p99_us"),
        "p99_ms": metrics.get("p99_ms"),
        "p999_us": metrics.get("p999_us"),
        "p999_ms": metrics.get("p999_ms"),
        "throughput": metrics.get("throughput"),
        "effective_throughput": metrics.get("effective_throughput"),
        "delivered_throughput": metrics.get("delivered_throughput"),
        "delivered_per_sub_throughput": metrics.get("delivered_per_sub_throughput"),
        "delivered_total": metrics.get("delivered_total"),
        "received": metrics.get("received"),
        "dropped": metrics.get("dropped"),
    }
    return row


def write_csv(path: Path, rows: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def aggregate(rows: list, strict: bool = True):
    """Median each workload group across trials.

    A median is only meaningful over runs that differ by nothing but trial
    index. The grouping key therefore includes the things that change what is
    being measured -- commit, host, toolchain, warmup/total -- and not only the
    workload shape. `run_latency_matrix.py` appends to the same JSONL by
    default, so without this a second run on a different commit or a different
    machine silently merges into the first and the median describes no build
    that ever existed. Recording `git_sha = "mixed"` afterwards labels the
    problem rather than avoiding it.
    """
    grouped = {}
    for row in rows:
        key = (
            row["broker_profile"],
            row["fanout"],
            row["batch"],
            row["payload_bytes"],
            row["preset"],
            row["binary"],
            row.get("git_sha"),
            row.get("git_dirty"),
            row.get("hostname"),
            row.get("platform"),
            row.get("rustc_version"),
            row.get("warmup"),
            row.get("total"),
        )
        grouped.setdefault(key, []).append(row)

    # A dirty tree cannot be attributed to a commit, so its numbers are not
    # comparable to anything -- including a later run of the "same" sha.
    dirty = sum(1 for row in rows if row.get("git_dirty"))
    if dirty:
        print(
            f"warning: {dirty}/{len(rows)} runs were recorded from a dirty "
            "working tree; their git_sha does not identify what was measured",
            file=sys.stderr,
        )
    distinct_builds = {
        (row.get("git_sha"), row.get("hostname")) for row in rows
    }
    if len(distinct_builds) > 1:
        print(
            f"warning: input spans {len(distinct_builds)} (commit, host) pairs; "
            "they are aggregated separately and must not be compared on one chart",
            file=sys.stderr,
        )

    agg_rows = []
    for key, group in grouped.items():
        (
            profile,
            fanout,
            batch,
            payload,
            preset,
            binary,
            key_git_sha,
            key_git_dirty,
            hostname,
            platform,
            rustc_version,
            key_warmup,
            key_total,
        ) = key
        failures = [r for r in group if r.get("parse_error") or r.get("exit_code") != 0]
        successes = [r for r in group if r not in failures]

        def collect(field):
            values = [r[field] for r in successes if r.get(field) is not None]
            return values

        throughput_vals = collect("throughput")
        effective_vals = collect("effective_throughput")
        delivered_vals = collect("delivered_throughput")
        p50_vals = collect("p50_us")
        p99_vals = collect("p99_us")
        p999_vals = collect("p999_us")

        def median(values):
            return statistics.median(values) if values else None

        def median_ms(values):
            return median(values) / 1000.0 if values else None

        # Part of the grouping key now, so it is exact rather than "mixed".
        git_sha = key_git_sha
        git_dirty = key_git_dirty

        agg_rows.append(
            {
                "broker_profile": profile,
                "fanout": fanout,
                "batch": batch,
                "payload_bytes": payload,
                "preset": preset,
                "binary": binary,
                "warmup": key_warmup,
                "total": key_total,
                "hostname": hostname,
                "platform": platform,
                "rustc_version": rustc_version,
                "trial_count": len(group),
                "failure_count": len(failures),
                "git_sha": git_sha,
                "git_dirty": git_dirty,
                "throughput_median": median(throughput_vals),
                "throughput_p10": percentile(throughput_vals, 0.10),
                "throughput_p90": percentile(throughput_vals, 0.90),
                "effective_throughput_median": median(effective_vals),
                "delivered_throughput_median": median(delivered_vals),
                "p50_us_median": median(p50_vals),
                "p50_ms_median": median_ms(p50_vals),
                "p99_us_median": median(p99_vals),
                "p99_ms_median": median_ms(p99_vals),
                "p999_us_median": median(p999_vals),
                "p999_ms_median": median_ms(p999_vals),
            }
        )
    return agg_rows


def write_parquet(rows: list, path: Path):
    if not rows:
        return
    try:
        import pandas as pd  # type: ignore

        df = pd.DataFrame(rows)
        df.to_parquet(path, index=False)
        return
    except Exception:
        pass

    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path)
    except Exception:
        print("Skipping parquet write (pyarrow/pandas not available).")


def main():
    parser = argparse.ArgumentParser(description="Normalize and aggregate latency_demo JSONL.")
    parser.add_argument("--input", default=str(RAW_JSONL))
    args = parser.parse_args()

    raw_path = Path(args.input)
    runs = load_runs(raw_path)
    rows = [flatten_run(r) for r in runs]
    write_csv(RUNS_CSV, rows)
    write_parquet(rows, RUNS_CSV.with_suffix(".parquet"))

    agg_rows = aggregate(rows)
    write_csv(AGG_CSV, agg_rows)
    write_parquet(agg_rows, AGG_CSV.with_suffix(".parquet"))


if __name__ == "__main__":
    main()
