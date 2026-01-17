#!/usr/bin/env bash
set -euo pipefail

echo "Felix demo: in-process pub/sub and cache"
echo "Building..."
cargo build --workspace

echo "Running demo..."
cargo run -p broker --bin demo
