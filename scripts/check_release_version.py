#!/usr/bin/env python3
"""Verify every version field agrees, and matches the tag being released.

A tag and a version that disagree ship artifacts labelled with neither, and
nothing else notices: the archive is named from the tag, each crate is built
from its own manifest, and both succeed. The wheel, the npm package and the
Helm chart each carry a version of their own, so bumping the workspace is not
enough -- which is the same shape as the lockfiles that sat at 0.4.0-preview
through two releases because nothing looked.

Run with a tag to check a release (`v0.5.0`), or with no argument to check only
that the files agree with each other, which is what a pull request wants.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

#: Every file carrying a version that must track the release, and how to read
#: it. The Helm chart's own `version` is deliberately absent: it versions the
#: chart, not the app, and moves on its own schedule.
SOURCES: list[tuple[str, str]] = [
    ("Cargo.toml", "cargo"),
    ("crates/felix-python/Cargo.toml", "cargo"),
    ("crates/felix-typescript/Cargo.toml", "cargo"),
    ("crates/felix-typescript/package.json", "npm"),
    ("deploy/helm/felix/Chart.yaml", "chart"),
]


def read(path: str, kind: str) -> str | None:
    text = (REPO / path).read_text()
    if kind == "cargo":
        # The first `version = "..."` is the package's own; dependency versions
        # follow it and are not what this checks.
        match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.M)
    elif kind == "npm":
        return json.loads(text).get("version")
    else:
        match = re.search(r'^appVersion:\s*"?([^"\s]+)"?', text, re.M)
    return match.group(1) if match else None


def main() -> int:
    tag = sys.argv[1] if len(sys.argv) > 1 else None
    expected = tag[1:] if tag and tag.startswith("v") else tag

    found: dict[str, str | None] = {path: read(path, kind) for path, kind in SOURCES}
    missing = [path for path, version in found.items() if version is None]
    for path in missing:
        print(f"FAIL {path}: no version found where one was expected")

    versions = {version for version in found.values() if version is not None}
    disagree = len(versions) > 1
    if disagree:
        print("FAIL the version fields do not agree:")
        for path, version in found.items():
            print(f"       {version}  {path}")

    mismatched = expected is not None and versions != {expected}
    if mismatched:
        print(f"FAIL tag {tag} does not match the versions in the tree:")
        for path, version in found.items():
            marker = " " if version == expected else "<-"
            print(f"     {marker} {version}  {path}")

    if missing or disagree or mismatched:
        print("\nBump every file above, then run `task lock:refresh`.")
        return 1

    agreed = versions.pop()
    print(f"{len(SOURCES)} version field(s) checked, all at {agreed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
