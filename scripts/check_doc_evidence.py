#!/usr/bin/env python3
"""Verify that every test a doc cites as evidence actually exists.

The architecture docs make normative claims and back them with test names, in
blockquotes like:

    > `a_quorum_acknowledged_record_survives_its_leader` -- the acknowledged
    > record is still there after its leader is killed.

A citation naming a test that no longer exists is worse than no citation: the
claim reads as verified and nothing checks it. Renaming or deleting a test is
easy and updating the prose that cites it is easy to forget, so this closes that
gap rather than relying on remembering.

A blockquote counts as evidence only when it *begins* with a backticked name.
That is how the convention is written, and it is what separates a citation from
an ordinary callout -- `docs/broker-config.md` has blockquotes naming config
keys, and those are not claims about tests.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DOCS = [REPO / "docs", REPO / "docs-site" / "src" / "content" / "docs"]

# An evidence line: a blockquote whose content opens with a backticked name.
EVIDENCE = re.compile(r"^\s*>\s*`[a-z_]")
CITATION = re.compile(r"`([a-z_][a-z0-9_]{6,})`")
# A path or a module reference, which names a place rather than a test.
NOT_A_TEST = re.compile(r"[./:]")


def cited() -> dict[str, list[str]]:
    """Every test name cited, mapped to where it was cited."""
    found: dict[str, list[str]] = {}
    for root in DOCS:
        for path in sorted(root.rglob("*.md")) + sorted(root.rglob("*.mdx")):
            for number, line in enumerate(path.read_text().splitlines(), 1):
                if not EVIDENCE.match(line):
                    continue
                for name in CITATION.findall(line):
                    if NOT_A_TEST.search(name):
                        continue
                    where = f"{path.relative_to(REPO)}:{number}"
                    found.setdefault(name, []).append(where)
    return found


def defined() -> set[str]:
    """Every function defined anywhere in the workspace's Rust sources."""
    out = subprocess.run(
        ["git", "grep", "-hoE", r"fn [a-z_][a-z0-9_]*", "--", "*.rs"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return {line[3:] for line in out.splitlines()}


def main() -> int:
    names = cited()
    known = defined()
    missing = {name: where for name, where in names.items() if name not in known}

    for name, where in sorted(missing.items()):
        for place in where:
            print(f"\033[31mFAIL\033[0m {place}: cites `{name}`, which no test defines")
    print(f"{len(names)} cited test(s) checked, {len(missing)} missing")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
