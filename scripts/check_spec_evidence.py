#!/usr/bin/env python3
"""Verify every TLA+ configuration says what it assumes of the code, and cites it.

The spec and the implementation are two artifacts in two languages, and nothing
in the toolchain connects them. That gap is not hypothetical: the broker gained
the report-before-mark ordering in #268, the model went on describing the design
without it, and CI pinned the resulting `AckedSurvive` violation as *expected*
for three weeks -- asserting that Felix loses acknowledged records, for a design
it no longer had. An issue was then filed against the model's finding proposing
work the code did not need.

This closes the cheap half of that gap, the same way `check_doc_evidence.py`
does for prose: a configuration's constants are claims about how the
implementation behaves, so each one names the tests that establish the claim.
Rename or delete such a test and this fails, which puts the spec in front of
whoever is changing the behaviour.

What it does NOT do, stated plainly so nobody reads more into a pass: a cited
test can keep its name while its assertions change, and the spec can model a
behaviour wrongly while every citation resolves. Checking that the
implementation *conforms* needs trace validation against the spec, which is a
different and much larger mechanism. See #598.

The convention, in any `docs/formal/*.cfg`:

    \\* Evidence: `a_failed_replica_report_holds_the_quorum_mark_back` -- the
    \\*   mark is held when the report does not land.

A configuration modelling a design the code deliberately does *not* have --
a counterexample, or an alternative rule -- says so instead:

    \\* Evidence: none, this models an alternative the implementation does not use.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SPECS = REPO / "docs" / "formal"

# An evidence line, and the citations on it. Comments in a TLA+ config open
# with `\*`.
EVIDENCE = re.compile(r"^\s*\\\*\s*Evidence:")
CITATION = re.compile(r"`([a-z_][a-z0-9_]{6,})`")
# Any comment line, which is how an evidence block continues.
COMMENT = re.compile(r"^\s*\\\*")
# The explicit way to say a configuration models something the code does not do.
EXEMPT = re.compile(r"^\s*\\\*\s*Evidence:\s*none\b", re.IGNORECASE)


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
    known = defined()
    failures = 0
    cited_total = 0

    configs = sorted(SPECS.glob("*.cfg"))
    if not configs:
        print(f"\033[31mFAIL\033[0m no configurations found under {SPECS}")
        return 1

    for path in configs:
        where = path.relative_to(REPO)
        lines = path.read_text().splitlines()
        # An evidence *block*: the `Evidence:` line and the comment lines under
        # it, so a citation can have its sentence without being crammed onto
        # one line. Ends at the first line that is not a `\*` comment.
        evidence: list[str] = []
        collecting = False
        for line in lines:
            if EVIDENCE.match(line):
                collecting = True
            elif collecting and not COMMENT.match(line):
                collecting = False
            if collecting:
                evidence.append(line)

        if not evidence:
            print(
                f"\033[31mFAIL\033[0m {where}: no `\\* Evidence:` line. A configuration "
                "makes claims about how the code behaves; name the tests that "
                "establish them, or say `Evidence: none` and why."
            )
            failures += 1
            continue

        if any(EXEMPT.match(line) for line in evidence):
            continue

        names = [name for line in evidence for name in CITATION.findall(line)]
        if not names:
            print(
                f"\033[31mFAIL\033[0m {where}: an Evidence line citing no test. "
                "Name it in backticks, or say `Evidence: none` and why."
            )
            failures += 1
            continue

        cited_total += len(names)
        for name in names:
            if name not in known:
                print(
                    f"\033[31mFAIL\033[0m {where}: cites `{name}`, which no test defines"
                )
                failures += 1

    print(
        f"{len(configs)} spec configuration(s) checked, "
        f"{cited_total} citation(s), {failures} problem(s)"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
