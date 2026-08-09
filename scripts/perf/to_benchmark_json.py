#!/usr/bin/env python3
"""Convert a latency_demo JSONL result set into the customSmallerIsBetter /
customBiggerIsBetter JSON format expected by benchmark-action/github-action-benchmark.

That action expects two separate files when a run has both "smaller is
better" (latency) and "bigger is better" (throughput) metrics, since the
`tool` input sets one direction for the whole file.
"""
import argparse
import json
from pathlib import Path

LATENCY_METRICS = [("p50_us", "p50 (us)"), ("p99_us", "p99 (us)"), ("p999_us", "p999 (us)")]
THROUGHPUT_METRICS = [
    ("throughput", "throughput (msg/s)"),
    ("delivered_throughput", "delivered throughput (msg/s)"),
]


def load_runs(path: Path):
    runs = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            runs.append(json.loads(line))
    return runs


def group_key(run: dict):
    workload = run.get("workload") or {}
    return (
        run.get("broker_profile"),
        workload.get("fanout"),
        workload.get("batch"),
        workload.get("payload_bytes"),
        run.get("preset"),
    )


def label(key: tuple) -> str:
    profile, fanout, batch, payload, preset = key
    return f"{profile}/{preset} fanout={fanout} batch={batch} payload={payload}B"


def median(values):
    values = sorted(values)
    n = len(values)
    mid = n // 2
    if n % 2 == 1:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def stdev(values, mean_value):
    if len(values) < 2:
        return 0.0
    var = sum((v - mean_value) ** 2 for v in values) / (len(values) - 1)
    return var**0.5


def build_entries(runs: list, metric_defs: list, include_run) -> list:
    groups: dict = {}
    for run in runs:
        if run.get("parse_error") or run.get("exit_code") not in (0, None):
            continue
        if not include_run(run):
            continue
        groups.setdefault(group_key(run), []).append(run)

    entries = []
    for key, group in sorted(groups.items(), key=lambda kv: [str(x) for x in kv[0]]):
        for metric, metric_label in metric_defs:
            values = [
                float(g["metrics"][metric])
                for g in group
                if (g.get("metrics") or {}).get(metric) is not None
            ]
            if not values:
                continue
            mean_value = sum(values) / len(values)
            entries.append(
                {
                    "name": f"{label(key)} - {metric_label}",
                    "unit": "us" if metric.endswith("_us") else "msg/s",
                    "value": median(values),
                    "range": f"{stdev(values, mean_value):.2f}",
                    "extra": f"n={len(values)}, mean={mean_value:.2f}",
                }
            )
    return entries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--latency-output", required=True)
    parser.add_argument("--throughput-output", required=True)
    args = parser.parse_args()

    runs = load_runs(Path(args.input))
    latency_entries = build_entries(
        runs,
        LATENCY_METRICS,
        lambda run: (run.get("workload") or {}).get("batch") == 1,
    )
    throughput_entries = build_entries(
        runs,
        THROUGHPUT_METRICS,
        lambda run: (run.get("workload") or {}).get("batch", 0) > 1,
    )

    Path(args.latency_output).write_text(json.dumps(latency_entries, indent=2), encoding="utf-8")
    Path(args.throughput_output).write_text(
        json.dumps(throughput_entries, indent=2), encoding="utf-8"
    )
    print(
        f"Wrote {len(latency_entries)} latency entries, "
        f"{len(throughput_entries)} throughput entries"
    )


if __name__ == "__main__":
    main()
