# Contributing to Felix

Thanks for your interest in contributing. Before your first pull request is
merged, please read this — it's short.

## License Split

Felix uses a split license: the wire protocol, client SDK, transport layer,
shared types, and conformance suite are Apache-2.0; the broker and
control-plane server components are Elastic License 2.0. See
[LICENSING.md](LICENSING.md) for the full breakdown of which path is under
which license. Know which part of the tree your PR touches before you start.

## Contributor License Agreement

Every contribution needs two things, regardless of which license path it
lands in — this keeps the project able to evolve its licensing over time
without ever needing to track down past contributors individually:

1. **DCO sign-off** — certify you wrote (or have the right to submit) the
   code, by adding `-s` to your commit:

   ```bash
   git commit -s -m "your message"
   ```

   This adds a `Signed-off-by: Your Name <you@example.com>` trailer. It's the
   same mechanism used by the Linux kernel and Docker.

2. **CLA grant** — on your first pull request, the CLA Assistant bot will
   comment asking you to reply with a fixed phrase to sign. The full text is
   in [CLA.md](CLA.md); in short, you confirm the contribution is your
   original work (or you have the right to submit it) and grant the project
   a broad, non-exclusive license to use and relicense it — **without**
   transferring your copyright. You only sign once, not per-PR.

## AI-Assisted Contributions

AI tools (Claude, Copilot, etc.) are fine to use — this project does. Two
things to keep in mind:

- **You're responsible for what you submit.** Review AI-generated or
  AI-assisted code as if you wrote it yourself; the CLA/DCO sign-off is
  still your assertion that you have the right to submit it.
- **Disclose substantial AI assistance** in the PR description (tool used,
  roughly how much of the change). This is about transparency for
  reviewers, not a restriction — a one-line note like "drafted with Claude
  Code, reviewed and tested by me" is enough.

## Getting Started

- `cargo build --workspace` builds everything.
- `task test` runs the full test suite (spins up Postgres locally if Docker
  is available).
- `task lint` runs `cargo fmt --check` and `cargo clippy -D warnings` — both
  must pass in CI.
- See [README.md](README.md) for an architecture overview and
  [docs/](docs/) for design docs.

## Pull Requests

- Keep PRs focused; a bug fix doesn't need an unrelated refactor along for
  the ride.
- Add tests for new behavior.
- `task lint` and `task test` should pass locally before you open a PR — CI
  runs both plus `cargo-deny`.
