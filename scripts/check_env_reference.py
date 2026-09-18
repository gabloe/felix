#!/usr/bin/env python3
"""Verify that every environment variable the code reads is documented.

`docs-site/.../reference/environment-variables.md` calls itself a reference, and
a reference that silently omits a knob is worse than one that says it is partial
-- an operator concludes the setting does not exist. The page had drifted to
missing 59 of them before this check existed.

Variables used only by benchmarks, demos and the test harness are excluded by
name below rather than documented: they are not operational surface, and listing
them would bury the ones that are.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
REFERENCE = REPO / "docs-site/src/content/docs/reference/environment-variables.md"

# Read by benchmarks, demos, or the test harness only. Not operational surface.
NOT_OPERATIONAL = {
    "FELIX_BENCH_EMBED_TS",
    "FELIX_CACHE_BENCH_CONN_STATS",
    "FELIX_CACHE_BENCH_OPS",
    "FELIX_CACHE_BENCH_PAYLOADS",
    "FELIX_CACHE_BENCH_SAMPLES",
    "FELIX_CACHE_BENCH_TIMINGS",
    "FELIX_CACHE_BENCH_TTL_MS",
    "FELIX_CACHE_BENCH_VALIDATE_EACH",
    "FELIX_CACHE_BENCH_WARMUP",
    "FELIX_CLUSTER_VERBOSE",
    "FELIX_DEMO_LOG_CAPACITY",
    "FELIX_LATENCY_DEMO_FAST",
    # Test-only fault injection. Documented in `docs/cluster-harness.md`, and
    # deliberately not advertised as a production setting.
    "FELIX_PEER_PARTITION_FILE",
    "FELIX_TEST_BROKER_OUTPUT",
    "FELIX_TEST_DATABASE_URL",
}


def read_by_code() -> set[str]:
    out = subprocess.run(
        ["git", "grep", "-hoE", r'"FELIX_[A-Z0-9_]+"', "--", "*.rs"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return {line.strip('"') for line in out.splitlines()}


#: One section of the reference exists to show names that do **not** exist —
#: what a typo looks like, and what the startup warning suggests instead. Read
#: literally, every name in it is "documented and read by nothing", which is the
#: very thing it is explaining. Skipped rather than exempted by name, so the
#: examples can change without anyone remembering to update a list here.
EXAMPLES_OF_WRONG_NAMES = "## Typos in variable names"


def documented() -> set[str]:
    text = REFERENCE.read_text()
    if EXAMPLES_OF_WRONG_NAMES in text:
        start = text.index(EXAMPLES_OF_WRONG_NAMES)
        end = text.find("\n## ", start + 1)
        text = text[:start] + (text[end:] if end != -1 else "")
    return set(re.findall(r"FELIX_[A-Z0-9_]+", text))


def main() -> int:
    expected = read_by_code() - NOT_OPERATIONAL
    missing = sorted(expected - documented())
    # A variable that was renamed away leaves a stale entry, which sends an
    # operator to set something nothing reads.
    stale = sorted(documented() - read_by_code() - NOT_OPERATIONAL)

    for name in missing:
        print(f"\033[31mFAIL\033[0m {name} is read by the code and not documented")
    for name in stale:
        print(f"\033[31mFAIL\033[0m {name} is documented and nothing reads it")
    print(
        f"{len(expected)} operational variable(s) checked, "
        f"{len(missing)} undocumented, {len(stale)} stale"
    )
    return 1 if missing or stale else 0


if __name__ == "__main__":
    sys.exit(main())
