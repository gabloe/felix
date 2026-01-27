#!/usr/bin/env bash
set -euo pipefail

rustup toolchain install 1.92.0
rustup default 1.92.0
rustup component add llvm-tools

sudo apt update
sudo apt install -y linux-perf

cargo install flamegraph samply cargo-samply cargo-llvm-cov

bash scripts/setup-githooks.sh

mkdir -p /workspaces/felix/.tmp