#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "scripts" / "perf" / "presets.yml"
AGG_CSV = ROOT / "data" / "derived" / "latency_demo_agg.csv"
CHARTS_DIR = ROOT / "charts" / "latency_demo"


def load_config(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            import yaml  # type: ignore

            return yaml.safe_load(text)
        except ImportError as exc:
            raise SystemExit(
                "Failed to parse presets.yml as JSON and PyYAML is not installed. "
                "Install PyYAML or keep presets.yml JSON-compatible."
            ) from exc


def read_csv(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
    return rows


def to_int(value):
    if value is None or value == "":
        return None
    return int(value)


def to_float(value):
    if value is None or value == "":
        return None
    return float(value)


def cast_rows(rows):
    casted = []
    for row in rows:
        row = dict(row)
        for key in ("fanout", "batch", "payload_bytes", "trial_count", "failure_count", "warmup", "total"):
            row[key] = to_int(row.get(key))
        for key in (
            "throughput_median",
            "throughput_p10",
            "throughput_p90",
            "effective_throughput_median",
            "delivered_throughput_median",
            "p50_ms_median",
            "p99_ms_median",
            "p999_ms_median",
        ):
            row[key] = to_float(row.get(key))
        row["binary"] = True if row.get("binary") == "True" else False
        casted.append(row)
    return casted


def unique_or_mixed(values):
    values = {v for v in values if v is not None}
    if len(values) == 1:
        return values.pop()
    if not values:
        return None
    return "mixed"


def percentile(values, pct):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return float(values[f])
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return float(d0 + d1)


def chart_group(
    rows,
    metric_key,
    ylabel,
    title_prefix,
    output_path,
    presets,
    payloads,
    footer,
    cap_percentile=None,
    scale=None,
):
    payload_positions = list(range(len(payloads)))
    group_width = 0.82
    bar_width = group_width / max(1, len(presets))
    offsets = [
        (i - (len(presets) - 1) / 2) * bar_width
        for i in range(len(presets))
    ]

    raw_values = [
        r.get(metric_key)
        for r in rows
        if r.get(metric_key) is not None
    ]
    cap_value = None
    if cap_percentile is not None and raw_values:
        cap_value = percentile(raw_values, cap_percentile)

    fig, ax = plt.subplots(figsize=(10, 5))

    for idx, preset in enumerate(presets):
        heights = []
        for payload in payloads:
            match = next(
                (
                    r
                    for r in rows
                    if r["preset"] == preset and r["payload_bytes"] == payload
                ),
                None,
            )
            heights.append(match.get(metric_key) if match else None)
        values = []
        for value in heights:
            if value is None:
                values.append(0.0)
            elif cap_value is not None and value > cap_value:
                values.append(cap_value / scale if scale else cap_value)
            else:
                values.append(value / scale if scale else value)
        ax.bar(
            [p + offsets[idx] for p in payload_positions],
            values,
            width=bar_width * 0.95,
            label=preset,
        )

    ax.set_xticks(payload_positions)
    ax.set_xticklabels([str(p) for p in payloads])
    ax.set_xlabel("payload (bytes)")
    ax.set_ylabel(ylabel)
    if cap_value is not None:
        ax.set_title(f"{title_prefix} (clipped p{int(cap_percentile * 100)})")
    else:
        ax.set_title(title_prefix)
    ax.legend(ncol=3, fontsize=8)
    fig.text(0.99, 0.01, footer, ha="right", va="bottom", fontsize=7)
    fig.tight_layout(rect=[0, 0.02, 1, 1])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path.with_suffix(".png"), dpi=140)
    fig.savefig(output_path.with_suffix(".svg"))
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Generate clustered charts from latency_demo_agg.csv.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--input", default=str(AGG_CSV))
    parser.add_argument("--cap-percentile", type=float, default=None)
    args = parser.parse_args()

    config = load_config(Path(args.config))
    presets = config.get("preset_order") or list(config.get("presets", {}).keys())
    payloads = config.get("workload", {}).get("payload_bytes", [0, 256, 1024, 4096])
    cap_percentile = args.cap_percentile
    if cap_percentile is None:
        cap_percentile = config.get("chart_outlier_cap_percentile")

    rows = cast_rows(read_csv(Path(args.input)))

    groups = {}
    for row in rows:
        key = (
            row["broker_profile"],
            row["fanout"],
            row["batch"],
            row["binary"],
        )
        groups.setdefault(key, []).append(row)

    date_str = dt.datetime.utcnow().date().isoformat()

    for key, group_rows in groups.items():
        profile, fanout, batch, binary = key
        warmup = unique_or_mixed([r.get("warmup") for r in group_rows])
        total = unique_or_mixed([r.get("total") for r in group_rows])
        trials = unique_or_mixed([r.get("trial_count") for r in group_rows])
        git_sha = unique_or_mixed([r.get("git_sha") for r in group_rows])

        title_prefix = (
            f"latency_demo {profile} fanout={fanout} batch={batch} "
            f"binary={binary} warmup={warmup} total={total} trials={trials}"
        )
        footer = f"git {git_sha} | {date_str}"

        base_dir = CHARTS_DIR / profile
        base_name = base_dir / f"f{fanout}_b{batch}"

        chart_group(
            group_rows,
            "p50_ms_median",
            "p50 latency (ms)",
            title_prefix,
            base_name.with_name(base_name.name + "_p50"),
            presets,
            payloads,
            footer,
            cap_percentile,
        )
        chart_group(
            group_rows,
            "p99_ms_median",
            "p99 latency (ms)",
            title_prefix,
            base_name.with_name(base_name.name + "_p99"),
            presets,
            payloads,
            footer,
            cap_percentile,
        )
        throughput_values = [
            r.get("throughput_median")
            for r in group_rows
            if r.get("throughput_median") is not None
        ]
        scale = None
        ylabel = "throughput (msg/s)"
        if throughput_values and max(throughput_values) >= 1_000_000:
            scale = 1_000_000.0
            ylabel = "throughput (M msg/s)"
        chart_group(
            group_rows,
            "throughput_median",
            ylabel,
            title_prefix,
            base_name.with_name(base_name.name + "_throughput"),
            presets,
            payloads,
            footer,
            cap_percentile,
            scale,
        )


if __name__ == "__main__":
    main()
