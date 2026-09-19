#!/usr/bin/env python3
"""Assert the Node client's platform packages agree with what CI builds.

Run via `task ts:packages`.

# Why this exists
`@felix/client` publishes as six packages: a JavaScript one that declares five
platform binaries as optional dependencies, and the five that carry them. The
version appears in every one of them, the file name inside each has to match
what `index.js` looks for, and the set has to match the targets the release
workflow actually builds.

None of that is enforced by anything. A bump that misses `npm/`, a target added
to the build matrix and not to the package list, or a renamed binary all publish
cleanly and fail at `npm install` on someone else's machine -- which is the
worst place to find out, because an npm version is permanent.
"""

from __future__ import annotations

import json
import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
PACKAGE_DIR = REPO_ROOT / "crates/felix-typescript"
RELEASE_WORKFLOW = REPO_ROOT / ".github/workflows/release.yml"

# napi names a binary by platform, architecture and -- where one platform has
# more than one ABI -- the ABI. This is the whole mapping the project uses, not
# napi's full table: a target nobody builds has no package to check.
TARGET_TAGS = {
    "aarch64-apple-darwin": "darwin-arm64",
    "x86_64-apple-darwin": "darwin-x64",
    "aarch64-unknown-linux-gnu": "linux-arm64-gnu",
    "x86_64-unknown-linux-gnu": "linux-x64-gnu",
    "x86_64-pc-windows-msvc": "win32-x64-msvc",
}


def addon_matrix_targets() -> set[str]:
    """The Rust targets the release workflow's Node addon job builds."""
    text = RELEASE_WORKFLOW.read_text()
    job = re.search(r"\n  typescript-addon:\n(.*?)(?=\n  [a-z][\w-]*:\n)", text, re.S)
    if not job:
        return set()
    return set(re.findall(r"target:\s*([\w-]+)\s*\}", job.group(1)))


def check() -> list[str]:
    failures: list[str] = []
    main = json.loads((PACKAGE_DIR / "package.json").read_text())
    version = main["version"]
    binary = main["napi"]["name"]

    declared = main["napi"]["triples"].get("additional", [])
    if main["napi"]["triples"].get("defaults"):
        failures.append(
            "package.json: napi.triples.defaults is true, which declares platforms "
            "the release workflow does not build. List the targets explicitly."
        )
    unknown = set(declared) - set(TARGET_TAGS)
    for target in sorted(unknown):
        failures.append(
            f"package.json: napi.triples lists {target}, which this script has no "
            f"tag for. Add it to TARGET_TAGS."
        )

    built = addon_matrix_targets()
    if not built:
        failures.append(
            f"{RELEASE_WORKFLOW.relative_to(REPO_ROOT)}: could not read the "
            f"typescript-addon build matrix. This check reads the targets from it."
        )
    for target in sorted(built - set(declared)):
        failures.append(
            f"{target}: built by the release workflow but not declared in "
            f"package.json's napi.triples, so its binary is never published."
        )
    for target in sorted(set(declared) - built - unknown):
        failures.append(
            f"{target}: declared in package.json's napi.triples but not built by the "
            f"release workflow, so the optional dependency resolves to nothing."
        )

    tags = sorted(TARGET_TAGS[t] for t in declared if t in TARGET_TAGS)
    expected_optional = {f"@felix/client-{tag}": version for tag in tags}
    actual_optional = main.get("optionalDependencies", {})
    if actual_optional != expected_optional:
        failures.append(
            f"package.json: optionalDependencies is {actual_optional!r}, expected "
            f"{expected_optional!r} -- one entry per declared target, at this "
            f"package's own version."
        )

    npm_dir = PACKAGE_DIR / "npm"
    present = {p.name for p in npm_dir.iterdir() if p.is_dir()} if npm_dir.is_dir() else set()
    for tag in sorted(set(tags) - present):
        failures.append(f"npm/{tag}/: missing. Every declared target needs a package.")
    for tag in sorted(present - set(tags)):
        failures.append(f"npm/{tag}/: no target declares it. Remove it or declare the target.")

    for tag in sorted(set(tags) & present):
        path = npm_dir / tag / "package.json"
        if not path.is_file():
            failures.append(f"npm/{tag}/package.json: missing.")
            continue
        pkg = json.loads(path.read_text())
        node = f"{binary}.{tag}.node"
        if pkg.get("name") != f"@felix/client-{tag}":
            failures.append(f"npm/{tag}/package.json: name is {pkg.get('name')!r}.")
        if pkg.get("version") != version:
            failures.append(
                f"npm/{tag}/package.json: version is {pkg.get('version')!r}, but the "
                f"main package is at {version!r}. They publish together."
            )
        # `index.js` builds this name from the running platform, so a mismatch
        # here is an installed package that cannot find its own binary.
        if pkg.get("main") != node or node not in pkg.get("files", []):
            failures.append(
                f"npm/{tag}/package.json: must carry {node!r}, which is what index.js "
                f"looks for."
            )
        # A package whose page is blank is one nobody can tell apart from a
        # squat, and these are what npm actually downloads.
        for field in ("description", "license", "repository"):
            if not pkg.get(field):
                failures.append(f"npm/{tag}/package.json: missing `{field}`.")
        for name in ("README.md", "LICENSE"):
            if not (npm_dir / tag / name).is_file():
                failures.append(f"npm/{tag}/{name}: missing.")

    for field in ("description", "license", "author", "homepage", "repository", "bugs", "keywords"):
        if not main.get(field):
            failures.append(
                f"package.json: missing `{field}`. It is what npm shows on the "
                f"package page."
            )
    for name in ("README.md", "LICENSE"):
        if not (PACKAGE_DIR / name).is_file():
            failures.append(f"crates/felix-typescript/{name}: missing.")
    return failures


def main() -> int:
    failures = check()
    if failures:
        print("Node package check FAILED:\n", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("Node platform packages agree with the release build matrix.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
