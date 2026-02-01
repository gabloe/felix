#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "scripts" / "perf" / "presets.yml"
OUTPUT_MD = ROOT / "charts" / "latency_demo" / "latency_demo_snippet.md"


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


def main():
    parser = argparse.ArgumentParser(description="Render markdown embedding latency_demo charts.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--output", default=str(OUTPUT_MD))
    args = parser.parse_args()

    config = load_config(Path(args.config))
    profiles = list(config.get("profiles", {}).keys())
    fanouts = config.get("workload", {}).get("fanout", [1, 10])
    batches = config.get("workload", {}).get("batch", [1, 64])

    lines = []
    lines.append("## latency_demo charts")
    lines.append("")

    for profile in profiles:
        lines.append(f"### {profile}")
        for fanout in fanouts:
            for batch in batches:
                base = f"charts/latency_demo/{profile}/f{fanout}_b{batch}"
                lines.append("")
                lines.append(f"#### fanout={fanout} batch={batch}")
                lines.append("")
                lines.append(f"![]({base}_p50.png)")
                lines.append("")
                lines.append(f"![]({base}_p99.png)")
                lines.append("")
                lines.append(f"![]({base}_throughput.png)")
                lines.append("")
        lines.append("")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
