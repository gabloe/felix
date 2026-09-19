#!/usr/bin/env python3
"""Every workflow that sets up Rust must ask for the toolchain the repo pins.

Run via `task ci:toolchains`.

# Why this exists
`rust-toolchain.toml` pins the channel, and rustup honours it for every cargo
invocation in this tree. A workflow step that installs a *different* toolchain
still adds `targets:` to that other one, and cargo then runs the pinned
toolchain without them.

Nothing about that is visible until a job cross-compiles: a target that happens
to be the runner's own is already installed, so four legs of a five-leg matrix
pass and the fifth fails with "can't find crate for `core`". It cost the 0.5.0
release pipeline its Node addon job, which took the npm assets with it.
"""

from __future__ import annotations

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github/workflows"
TOOLCHAIN_FILE = REPO_ROOT / "rust-toolchain.toml"

SETUP = re.compile(
    r"uses:\s*dtolnay/rust-toolchain@(?P<ref>\S+)\s*\n"
    r"(?P<body>(?:[ \t]*(?:#[^\n]*|with:|[a-z-]+:[^\n]*)\n)*)"
)

# A job with a real reason to differ says so on the line above, with the reason.
# cargo-fuzz is the one that does: it needs nightly.
EXEMPT = "toolchain-exempt:"


def pinned_channel() -> str:
    m = re.search(r'channel\s*=\s*"([^"]+)"', TOOLCHAIN_FILE.read_text())
    if not m:
        raise SystemExit("rust-toolchain.toml has no channel")
    return m.group(1)


def check() -> list[str]:
    channel = pinned_channel()
    failures: list[str] = []
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        text = wf.read_text()
        for m in SETUP.finditer(text):
            line = text[: m.start()].count("\n") + 1
            ref, body = m.group("ref"), m.group("body")
            # The marker sits in the comment block above the step, which the
            # step's `name:` and `uses:` lines separate from the match start.
            preceding = text[: m.start()].split("\n")[-6:]
            if any(EXEMPT in l for l in preceding) or EXEMPT in body:
                continue
            asked = re.search(r'toolchain:\s*"?([^"\n]+)"?', body)
            asked = asked.group(1).strip() if asked else ref
            if asked != channel:
                failures.append(
                    f"{wf.relative_to(REPO_ROOT)}:{line}: sets up Rust {asked!r}, but "
                    f"rust-toolchain.toml pins {channel!r}. Cargo will use the pinned "
                    f"one, without any `targets:` added here."
                )
    return failures


def main() -> int:
    failures = check()
    if failures:
        print("Workflow toolchain check FAILED:\n", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 1
    print(f"Every workflow Rust setup asks for {pinned_channel()}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
