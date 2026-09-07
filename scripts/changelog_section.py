#!/usr/bin/env python3
"""Print one version's section from CHANGELOG.md.

The release workflow uses this as the GitHub release body. Without it the body
is auto-generated from merged pull requests, which for a release spanning months
is mostly dependency bumps -- an accurate list of commits and a poor description
of what changed.

Exits non-zero when the section is missing, so a tag whose changelog entry was
forgotten fails the release rather than publishing an empty one.
"""

import re
import sys
from pathlib import Path


def section(changelog: str, version: str) -> str | None:
    # Headings look like `## [0.2.0] - 2026-09-07`; capture until the next `## `.
    pattern = rf"^## \[{re.escape(version)}\][^\n]*\n(.*?)(?=^## |\Z)"
    found = re.search(pattern, changelog, re.MULTILINE | re.DOTALL)
    return found.group(1).strip() if found else None


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: changelog_section.py <version>", file=sys.stderr)
        return 2

    version = sys.argv[1].removeprefix("v")
    changelog = Path("CHANGELOG.md").read_text(encoding="utf-8")

    body = section(changelog, version)
    if body is None:
        print(f"CHANGELOG.md has no section for {version}", file=sys.stderr)
        return 1

    print(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
