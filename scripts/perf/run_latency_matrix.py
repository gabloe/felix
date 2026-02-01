#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import platform
import re
import shlex
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "scripts" / "perf" / "presets.yml"
RAW_DIR = ROOT / "data" / "raw"
STDOUT_DIR = RAW_DIR / "stdout"
RAW_JSONL = RAW_DIR / "latency_demo_runs.jsonl"

DURATION_RE = re.compile(r"^([0-9]*\.?[0-9]+)\s*(us|ms)$")
RESULTS_RE = re.compile(
    r"Results \(publish n = (?P<publish>\d+), sampled (?P<sampled>\d+), received (?P<received>\d+), dropped (?P<dropped>\d+)\) "
    r"payload=(?P<payload>\d+)B fanout=(?P<fanout>\d+) batch=(?P<batch>\d+) binary=(?P<binary>true|false):"
)

TIMING_RE = re.compile(
    r"(\w+)\s+p50=([0-9.]+\s*(?:us|ms)|n/a)\s+p99=([0-9.]+\s*(?:us|ms)|n/a)(?:\s+p999=([0-9.]+\s*(?:us|ms)|n/a))?"
)


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


def run_cmd(cmd, env=None) -> str:
    return subprocess.check_output(cmd, env=env, text=True).strip()


def safe_cmd(cmd, env=None) -> str:
    try:
        return run_cmd(cmd, env=env)
    except Exception:
        return "unknown"


def collect_host_info() -> dict:
    info = {
        "hostname": safe_cmd(["hostname"]),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "uname": safe_cmd(["uname", "-a"]),
    }
    lscpu = safe_cmd(["lscpu"])
    if lscpu != "unknown":
        info["lscpu"] = lscpu
    return info


def parse_duration(value: str):
    value = value.strip()
    if value == "n/a":
        return None
    match = DURATION_RE.match(value)
    if not match:
        raise ValueError(f"Unrecognized duration: {value}")
    amount = float(match.group(1))
    unit = match.group(2)
    micros = amount if unit == "us" else amount * 1000.0
    return micros


def parse_metrics(stdout: str) -> dict:
    results_matches = list(RESULTS_RE.finditer(stdout))
    if not results_matches:
        raise ValueError("Could not find results header")
    results = results_matches[-1]

    def find_line(pattern: str):
        match = re.search(pattern, stdout, re.MULTILINE)
        if not match:
            raise ValueError(f"Missing metric line for pattern: {pattern}")
        return match.group(1)

    metrics = {
        "publish_total": int(results.group("publish")),
        "sample_total": int(results.group("sampled")),
        "received": int(results.group("received")),
        "dropped": int(results.group("dropped")),
        "payload_bytes": int(results.group("payload")),
        "fanout": int(results.group("fanout")),
        "batch": int(results.group("batch")),
        "binary": results.group("binary") == "true",
        "delivered_total": int(find_line(r"^\s*delivered total = (\d+)$")),
        "p50_raw": find_line(r"^\s*p50\s*=\s*([0-9.]+\s*(?:us|ms))$"),
        "p99_raw": find_line(r"^\s*p99\s*=\s*([0-9.]+\s*(?:us|ms))$"),
        "p999_raw": find_line(r"^\s*p999\s*=\s*([0-9.]+\s*(?:us|ms))$"),
        "throughput": float(find_line(r"^\s*throughput\s*=\s*([0-9.]+) msg/s$")),
        "effective_throughput": float(
            find_line(r"^\s*effective throughput\s*=\s*([0-9.]+) msg/s$")
        ),
        "delivered_throughput": float(
            find_line(r"^\s*delivered throughput\s*=\s*([0-9.]+) msg/s$")
        ),
        "delivered_per_sub_throughput": float(
            find_line(r"^\s*delivered per-sub throughput\s*=\s*([0-9.]+) msg/s$")
        ),
    }

    for key in ("p50", "p99", "p999"):
        micros = parse_duration(metrics[f"{key}_raw"])
        metrics[f"{key}_us"] = micros
        metrics[f"{key}_ms"] = micros / 1000.0 if micros is not None else None

    timing_line = None
    for line in stdout.splitlines():
        if line.strip().startswith("timings:"):
            timing_line = line
    if timing_line:
        timings = {}
        for match in TIMING_RE.finditer(timing_line):
            name = match.group(1)
            for idx, suffix in ((2, "p50"), (3, "p99"), (4, "p999")):
                raw = match.group(idx)
                if raw is None:
                    continue
                try:
                    micros = parse_duration(raw)
                except ValueError:
                    micros = None
                timings[f"{name}_{suffix}_us"] = micros
        metrics["timings_us"] = timings

    return metrics


def parse_filter(expr: str):
    if not expr:
        return {}
    filters = {}
    for part in expr.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise ValueError(f"Invalid filter expression: {part}")
        key, value = part.split("=", 1)
        values = [v for v in re.split(r"[|;]", value) if v]
        filters[key.strip()] = set(v.strip() for v in values)
    return filters


def matches_filter(filters: dict, item: dict) -> bool:
    if not filters:
        return True
    for key, allowed in filters.items():
        actual = item.get(key)
        if actual is None:
            if key == "payload":
                actual = item.get("payload_bytes")
            elif key == "profile":
                actual = item.get("broker_profile")
        if actual is None:
            return False
        if str(actual) not in allowed:
            return False
    return True


def build_matrix(config: dict, trials: int, binary_override=None):
    workload = config["workload"]
    profiles = config["profiles"]
    presets = config["presets"]
    binary_default = workload.get("binary", True)
    binary = binary_override if binary_override is not None else binary_default

    matrix = []
    for profile_name, profile in profiles.items():
        for fanout in workload["fanout"]:
            for batch in workload["batch"]:
                for payload in workload["payload_bytes"]:
                    for preset_name, preset in presets.items():
                        for trial_index in range(1, trials + 1):
                            matrix.append(
                                {
                                    "broker_profile": profile_name,
                                    "profile_env": profile.get("env", {}),
                                    "fanout": fanout,
                                    "batch": batch,
                                    "payload_bytes": payload,
                                    "warmup": workload["warmup"],
                                    "total": workload["total"],
                                    "binary": bool(binary),
                                    "preset": preset_name,
                                    "preset_args": preset.get("args", []),
                                    "trial_index": trial_index,
                                }
                            )
    return matrix


def main():
    parser = argparse.ArgumentParser(description="Run latency_demo across a matrix.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--trials", type=int, default=None)
    parser.add_argument("--filter", default="")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--binary", dest="binary", action="store_true")
    parser.add_argument("--no-binary", dest="binary", action="store_false")
    parser.set_defaults(binary=None)
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_config(config_path)
    trials = args.trials or int(config.get("defaults", {}).get("trials", 1))

    filters = parse_filter(args.filter)
    matrix = build_matrix(config, trials, args.binary)
    matrix = [item for item in matrix if matches_filter(filters, item)]

    if not matrix:
        raise SystemExit("No runs selected after applying filters.")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    STDOUT_DIR.mkdir(parents=True, exist_ok=True)

    git_sha = safe_cmd(["git", "rev-parse", "HEAD"], env={"LC_ALL": "C"})
    git_dirty = safe_cmd(["git", "status", "--porcelain"], env={"LC_ALL": "C"})
    host_info = collect_host_info()
    cargo_version = safe_cmd(["cargo", "--version"])
    rustc_version = safe_cmd(["rustc", "--version"])

    if args.dry_run:
        for item in matrix:
            cmd = [
                "cargo",
                "run",
                "--release",
                "-p",
                "broker",
                "--bin",
                "latency_demo",
                "--",
            ]
            if item["binary"]:
                cmd.append("--binary")
            cmd += [
                "--fanout",
                str(item["fanout"]),
                "--batch",
                str(item["batch"]),
                "--payload",
                str(item["payload_bytes"]),
                "--warmup",
                str(item["warmup"]),
                "--total",
                str(item["total"]),
            ]
            cmd += item["preset_args"]
            print(" ".join(shlex.quote(c) for c in cmd))
        return

    completed = set()
    if args.resume and RAW_JSONL.exists():
        with RAW_JSONL.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                workload = record.get("workload") or {}
                completed.add(
                    (
                        record.get("broker_profile"),
                        workload.get("fanout"),
                        workload.get("batch"),
                        workload.get("payload_bytes"),
                        record.get("preset"),
                        record.get("trial_index"),
                        workload.get("binary"),
                    )
                )

    with RAW_JSONL.open("a", encoding="utf-8") as jsonl:
        for idx, item in enumerate(matrix, 1):
            key = (
                item["broker_profile"],
                item["fanout"],
                item["batch"],
                item["payload_bytes"],
                item["preset"],
                item["trial_index"],
                item["binary"],
            )
            if key in completed:
                continue
            run_id = str(uuid.uuid4())
            timestamp = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()

            env = os.environ.copy()
            env.update(item["profile_env"])
            env["FELIX_DISABLE_TIMINGS"] = "1"

            cmd = [
                "cargo",
                "run",
                "--release",
                "-p",
                "broker",
                "--bin",
                "latency_demo",
                "--",
            ]
            if item["binary"]:
                cmd.append("--binary")
            cmd += [
                "--fanout",
                str(item["fanout"]),
                "--batch",
                str(item["batch"]),
                "--payload",
                str(item["payload_bytes"]),
                "--warmup",
                str(item["warmup"]),
                "--total",
                str(item["total"]),
            ]
            cmd += item["preset_args"]

            print(
                f"[{idx}/{len(matrix)}] profile={item['broker_profile']} "
                f"preset={item['preset']} fanout={item['fanout']} "
                f"batch={item['batch']} payload={item['payload_bytes']} trial={item['trial_index']}"
            )

            process = subprocess.run(
                cmd,
                env=env,
                text=True,
                capture_output=True,
                cwd=str(ROOT),
            )

            stdout_path = STDOUT_DIR / f"{run_id}.txt"
            with stdout_path.open("w", encoding="utf-8") as fh:
                fh.write("STDOUT\n")
                fh.write(process.stdout)
                fh.write("\nSTDERR\n")
                fh.write(process.stderr)

            record = {
                "run_id": run_id,
                "timestamp": timestamp,
                "git_sha": git_sha,
                "git_dirty": bool(git_dirty),
                "host_info": host_info,
                "cargo_version": cargo_version,
                "rustc_version": rustc_version,
                "command": cmd,
                "command_str": " ".join(shlex.quote(c) for c in cmd),
                "broker_profile": item["broker_profile"],
                "broker_env": item["profile_env"],
                "workload": {
                    "fanout": item["fanout"],
                    "batch": item["batch"],
                    "payload_bytes": item["payload_bytes"],
                    "warmup": item["warmup"],
                    "total": item["total"],
                    "binary": item["binary"],
                },
                "preset": item["preset"],
                "preset_args": item["preset_args"],
                "trial_index": item["trial_index"],
                "exit_code": process.returncode,
                "stdout_path": str(stdout_path),
                "felix_env": {k: env[k] for k in sorted(env) if k.startswith("FELIX_")},
            }

            if process.returncode != 0:
                record["parse_error"] = "nonzero exit"
                record["raw_stdout"] = process.stdout
                jsonl.write(json.dumps(record) + "\n")
                jsonl.flush()
                raise SystemExit(f"Run failed with exit code {process.returncode}")

            try:
                metrics = parse_metrics(process.stdout)
            except Exception as exc:
                record["parse_error"] = str(exc)
                record["raw_stdout"] = process.stdout
                jsonl.write(json.dumps(record) + "\n")
                jsonl.flush()
                raise SystemExit(f"Parse error: {exc}")

            record["metrics"] = metrics
            jsonl.write(json.dumps(record) + "\n")
            jsonl.flush()


if __name__ == "__main__":
    main()
