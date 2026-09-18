#!/usr/bin/env python3
"""Verify `felix_common::env_registry::KNOWN_VARS` lists every `FELIX_*` read.

The registry is what turns a mistyped variable into a warning naming the
setting that is quietly using its default. A variable added to the code and not
to the list inverts that: the real name gets reported as unknown, which trains
an operator to ignore the warning entirely.

Sibling to `check_env_reference.py`, which checks the *documentation* covers
every variable. This checks the *runtime* does.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
REGISTRY = REPO / "crates/felix-common/src/env_registry.rs"


def read_by_code() -> set[str]:
    out = subprocess.run(
        ["git", "grep", "-hoE", r'"FELIX_[A-Z0-9_]+"', "--", "*.rs"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return {line.strip('"') for line in out.splitlines()}


def registered() -> set[str]:
    body = REGISTRY.read_text()
    listing = body[body.index("KNOWN_VARS: &[&str] = &[") : body.index("];")]
    return set(re.findall(r'"(FELIX_[A-Z0-9_]+)"', listing))


def main() -> int:
    # The registry's own entries are string literals in a .rs file, so
    # `read_by_code` finds them too. That is what makes the two sets directly
    # comparable: a name in the registry is "read by the code" by construction,
    # and anything else in that set is read by something real.
    expected = read_by_code()
    listed = registered()

    missing = sorted(expected - listed)
    stale = sorted(listed - expected)

    for name in missing:
        print(f"read by the code and not in KNOWN_VARS: {name}")
    for name in stale:
        print(f"in KNOWN_VARS and read by nothing: {name}")

    print(
        f"{len(expected)} variable(s) checked, "
        f"{len(missing)} missing, {len(stale)} stale"
    )
    return 1 if missing or stale else 0


if __name__ == "__main__":
    sys.exit(main())
