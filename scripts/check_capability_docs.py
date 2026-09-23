#!/usr/bin/env python3
"""Every capability the wire can advertise has to be written down somewhere.

Four separate passes over this repository's docs have each found the same shape
of problem: a capability ships, the page its author was looking at gets updated,
and the *reference* pages — the ones organised by operation type rather than by
feature — silently keep the old shape. Consumer groups had shipped for a month
with no section in the client-facing API reference. `cache_delete` was listed as
"(future)" in one place while being documented in another.

Reading for it does not work; it has been tried. So this checks the two things
that are mechanical:

1. **Every `FEATURE_*` bit is mentioned in the docs.** A capability a broker can
   advertise and nothing documents is a capability nobody outside this repo can
   use.
2. **Every `Message` variant is mentioned in the protocol spec.** That document
   calls itself the source of truth for anyone implementing a client, so a
   message it does not mention is a hole in the spec.

Neither proves a page is *good*. They prove nothing was forgotten wholesale,
which is the failure that actually keeps happening.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DOC_ROOTS = ["docs", "docs-site/src", "README.md"]
SPEC = REPO / "docs/protocol.md"

# Response-only messages a client receives but never sends. They are part of the
# protocol and are all currently named in the spec; this list exists so that
# adding one does not silently become the exception that erodes the check.
ALLOWED_UNSPECIFIED: set[str] = set()


def red(text: str) -> str:
    return f"\033[31m{text}\033[0m"


def feature_bits() -> list[str]:
    source = (REPO / "crates/protocol/felix-wire/src/frame.rs").read_text()
    return re.findall(r"pub const (FEATURE_[A-Z_]+): u32", source)


def message_variants() -> list[str]:
    """Variant names of `enum Message`, by brace matching rather than by line."""
    source = (REPO / "crates/protocol/felix-wire/src/message.rs").read_text()
    start = source.index("pub enum Message {")
    depth = 0
    for offset, char in enumerate(source[start:], start):
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                body = source[start:offset]
                break
    else:  # pragma: no cover - a malformed enum would fail to compile first
        raise SystemExit("could not find the end of `enum Message`")
    return re.findall(r"^    ([A-Z][A-Za-z0-9]*)\s*[{,(]", body, re.M)


def snake(name: str) -> str:
    """`StreamShardsView` -> `stream_shards_view`, the wire's `type` tag."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def documented_anywhere(needle: str) -> bool:
    """`git grep` so untracked scratch files cannot satisfy the check."""
    found = subprocess.run(
        ["git", "grep", "-l", needle, "--", *DOC_ROOTS],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    return bool(found.stdout.strip())


def main() -> int:
    failures = 0

    bits = feature_bits()
    for bit in bits:
        if not documented_anywhere(bit):
            failures += 1
            print(
                f"{red('FAIL')} {bit} is advertised on the wire and named in no doc. "
                f"A capability nobody can read about is one nobody outside this "
                f"repository can use."
            )

    spec = SPEC.read_text()
    variants = message_variants()
    unspecified = [
        variant
        for variant in variants
        if variant not in ALLOWED_UNSPECIFIED
        and snake(variant) not in spec
        and variant not in spec
    ]
    for variant in unspecified:
        failures += 1
        print(
            f"{red('FAIL')} Message::{variant} (`{snake(variant)}`) is absent from "
            f"docs/protocol.md, which is the spec a client implementer reads."
        )

    print(
        f"{len(bits)} feature bit(s) and {len(variants)} message variant(s) checked, "
        f"{failures} undocumented"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
