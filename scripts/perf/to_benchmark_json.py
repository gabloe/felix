#!/usr/bin/env python3
"""Convert a latency_demo JSONL result set into the customSmallerIsBetter /
customBiggerIsBetter JSON format expected by benchmark-action/github-action-benchmark.

That action expects two separate files when a run has both "smaller is
better" (latency) and "bigger is better" (throughput) metrics, since the
`tool` input sets one direction for the whole file.
"""
import argparse
import hashlib
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


def common_value(values):
    unique = {str(value) for value in values if value not in (None, "")}
    if not unique:
        return "unknown"
    if len(unique) == 1:
        return unique.pop()
    return "mixed"


def config_id(run: dict) -> str:
    config = {
        "broker_env": run.get("broker_env") or {},
        "felix_env": run.get("felix_env") or {},
        "preset_args": run.get("preset_args") or [],
        "workload": run.get("workload") or {},
    }
    encoded = json.dumps(config, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:12]


def format_extra(group: list, values: list, mean_value: float, metric: str) -> str:
    deviation = stdev(values, mean_value)
    cv = (deviation / mean_value * 100.0) if mean_value else 0.0
    hosts = [run.get("host_info") or {} for run in group]
    platform_name = common_value(host.get("platform") for host in hosts)
    machine = common_value(host.get("machine") for host in hosts)
    cpu_count = common_value(host.get("cpu_count") for host in hosts)
    rustc = common_value(run.get("rustc_version") for run in group)
    configs = {config_id(run) for run in group}
    config = configs.pop() if len(configs) == 1 else "mixed"
    direction = "lower" if metric.endswith("_us") else "higher"
    workload = group[0].get("workload") or {}
    semantics = (
        "publish-to-delivery latency"
        if metric.endswith("_us")
        else (
            "aggregate subscriber deliveries"
            if metric == "delivered_throughput"
            else "publisher message rate"
        )
    )
    return "\n".join(
        [
            f"trials: {len(values)}",
            f"median: {median(values):.2f}",
            f"mean: {mean_value:.2f}",
            f"stdev: {deviation:.2f}",
            f"cv: {cv:.2f}%",
            f"direction: {direction} is better",
            f"semantics: {semantics}",
            f"runner: {platform_name} ({machine}, {cpu_count} CPUs)",
            f"rustc: {rustc}",
            f"config: {config}",
            f"binary: {str(bool(workload.get('binary'))).lower()}",
        ]
    )


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
                    "extra": format_extra(group, values, mean_value, metric),
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
