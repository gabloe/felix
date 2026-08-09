#!/usr/bin/env python3
"""Enforce the licence split and crates.io publish-readiness across the workspace.

Run via `task publish:check`.

# Why this exists
The licence split in LICENSING.md is a legal boundary that lives in fifteen
separate manifests. Nothing structural stops a new crate from silently landing on
the wrong side of it, and the failure is invisible until someone reads the
manifest. This turns the LICENSING.md table into an assertion.

# Why it shells out to `cargo metadata`
Manifests use workspace inheritance (`license.workspace = true`), so parsing the
raw TOML sees the string "workspace" rather than the resolved licence and would
validate nothing while appearing to pass. `cargo metadata` reports resolved
values, which is the only trustworthy source.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# The authoritative table, mirroring LICENSING.md. Adding a crate to the
# workspace without adding it here is itself a failure: the point is that
# classification is deliberate rather than inherited by accident.
APACHE = {
    "felix-wire",
    "felix-client",
    "felix-transport",
    "felix-common",
    "felix-conformance",
}
ELASTIC = {
    "felix-broker",
    "felix-storage",
    "felix-metadata",
    "felix-authz",
    "felix-crypto",
    "felix-consensus",
    "felix-router",
    "broker",
    "controlplane",
    "agent",
}

# Service binaries and a dev/CI tool with hard Elastic-2.0 dependencies. Nobody
# consumes these from a registry, and `broker`/`controlplane`/`agent` are generic
# names that would collide besides.
NOT_PUBLISHABLE = {"broker", "controlplane", "agent", "felix-conformance"}

# crates.io hard requirement is `description`; the rest are discoverability
# fields we want set before a first publish rather than bolted on after.
REQUIRED_FIELDS = ("description", "repository", "keywords", "categories")


def workspace_members() -> list[dict]:
    raw = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    meta = json.loads(raw)
    ids = set(meta["workspace_members"])
    return [p for p in meta["packages"] if p["id"] in ids]


def check() -> list[str]:
    failures: list[str] = []
    packages = workspace_members()
    seen = {p["name"] for p in packages}

    unclassified = seen - APACHE - ELASTIC
    for name in sorted(unclassified):
        failures.append(
            f"{name}: not listed in this script's licence table. Add it to APACHE or "
            f"ELASTIC and to the table in LICENSING.md — a new crate must be "
            f"classified deliberately."
        )

    stale = (APACHE | ELASTIC) - seen
    for name in sorted(stale):
        failures.append(
            f"{name}: listed in this script's licence table but is no longer a "
            f"workspace member; remove it here and from LICENSING.md."
        )

    for pkg in sorted(packages, key=lambda p: p["name"]):
        name = pkg["name"]
        expected = (
            "Apache-2.0" if name in APACHE else "Elastic-2.0" if name in ELASTIC else None
        )
        actual = pkg.get("license")
        if expected and actual != expected:
            failures.append(
                f"{name}: licence is {actual!r}, expected {expected!r} per LICENSING.md."
            )

        # `publish` is null when unrestricted, or a list of allowed registries.
        # An empty list is how `publish = false` surfaces here.
        publishable = pkg.get("publish") != []
        should_publish = name not in NOT_PUBLISHABLE
        if publishable and not should_publish:
            failures.append(
                f"{name}: must set `publish = false` — it is a service binary or a "
                f"dev tool, not a library to consume from a registry."
            )
        if not publishable and should_publish:
            failures.append(
                f"{name}: has `publish = false` but is expected to be publishable."
            )

        # Only crates that could actually reach crates.io need the metadata.
        if should_publish:
            for field in REQUIRED_FIELDS:
                if not pkg.get(field):
                    failures.append(
                        f"{name}: missing `{field}`. crates.io rejects a publish "
                        f"without `description`; the rest are required before a "
                        f"first publish."
                    )

        # `cargo package` only bundles files inside the crate directory, so a
        # crate without its own LICENSE ships with no licence text even though
        # the repository root has one.
        crate_dir = pathlib.Path(pkg["manifest_path"]).parent
        if not (crate_dir / "LICENSE").is_file():
            failures.append(
                f"{name}: no LICENSE file in {crate_dir.relative_to(REPO_ROOT)}/. "
                f"cargo package does not reach the repository root."
            )

        readme = pkg.get("readme")
        if should_publish and readme and not (crate_dir / readme).is_file():
            failures.append(
                f"{name}: readme is {readme!r} but that file does not exist in "
                f"{crate_dir.relative_to(REPO_ROOT)}/."
            )

    return failures


def main() -> int:
    failures = check()
    if failures:
        print("Publish-readiness check FAILED:\n", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        print(
            "\nSee LICENSING.md for the authoritative licence table.",
            file=sys.stderr,
        )
        return 1
    count = len(workspace_members())
    print(f"Publish-readiness check passed for {count} workspace members.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
