# Installation

This guide covers building Felix from source and verifying your installation.

## System Requirements

- **Operating System:** Linux, macOS, or Windows (WSL2 recommended)
- **Rust:** 1.92.0 or later
- **Memory:** 4 GB minimum, 8 GB recommended for development
- **Disk:** 2 GB for build artifacts
- **Network:** For QUIC, ensure UDP traffic is allowed on your firewall

## Install Rust

Felix requires Rust 1.92.0 or later. Install using [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Follow the prompts to complete installation. Then verify:

```bash
rustc --version
cargo --version
```

Expected output:

```
rustc 1.92.0 (or later)
cargo 1.92.0 (or later)
```

## Clone the Repository

```bash
git clone https://github.com/gabloe/felix.git
cd felix
```

## Build from Source

### Development Build

For development and debugging with full error information:

```bash
cargo build --workspace
```

Binaries will be in `target/debug/`.

### Release Build

For performance testing and production use:

```bash
cargo build --workspace --release
```

Binaries will be in `target/release/`.

!!! warning "Performance Difference"
    Release builds are **significantly faster** than debug builds. Always use `--release` for any performance testing or benchmarking.

### Build Specific Crates

Build only the broker service:

```bash
cargo build -p broker --release
```

Build only the client library:

```bash
cargo build -p felix-client --release
```

## Verify Installation

### Run Tests

Verify everything is working:

```bash
cargo test --workspace
```

You should see all tests passing:

```
running 150 tests
...
test result: ok. 150 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### Run the Conformance Suite

Felix includes a wire protocol conformance runner to validate correct framing and message encoding:

```bash
cargo run -p felix-conformance
```

Expected output:

```
Running wire protocol conformance tests...
✓ Frame envelope encoding
✓ Publish message encoding
✓ Subscribe message encoding
✓ Event message encoding
✓ Cache operations encoding
All conformance tests passed!
```

### Start the Broker

Run the broker service:

```bash
cargo run --release -p broker
```

You should see startup logs:

```
2026-01-25T10:00:00.000Z INFO felix_broker: Starting Felix broker
2026-01-25T10:00:00.001Z INFO felix_broker: QUIC listening on 0.0.0.0:5000
2026-01-25T10:00:00.001Z INFO felix_broker: Metrics server on 0.0.0.0:8080
```

Press `Ctrl+C` to stop the broker.

### Run a Demo

Verify end-to-end functionality with a self-contained demo (no separate broker required):

```bash
cargo run --release -p broker --bin pubsub-demo-simple
```

Other demos you can try (including a control-plane RBAC mutation demo):

```bash
cargo run --release -p broker --bin cache-demo
cargo run --release -p broker --bin latency-demo
cargo run --release -p broker --bin pubsub-demo-notifications
cargo run --release -p broker --bin pubsub-demo-orders
cargo run --manifest-path demos/rbac-live/Cargo.toml
cargo run --manifest-path demos/cross_tenant_isolation/Cargo.toml
```

Note: the cross-tenant isolation demo uses a Postgres-backed control plane.

See the [Demos Overview](../demos/overview.md) for details and expected output.

## Optional Tools

### Task Runner

Install [Task](https://taskfile.dev/) for convenient commands:

**macOS/Linux:**

```bash
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
```

**Using Homebrew:**

```bash
brew install go-task/tap/go-task
```

Then you can use:

```bash
task build      # Build everything
task test       # Run tests
task fmt        # Format code
task lint       # Run linters
```

### Cargo Tools

Install additional cargo extensions for development:

```bash
# Code coverage
cargo install cargo-llvm-cov

# Security auditing
cargo install cargo-deny --version 0.19.0 --locked

# Benchmarking
cargo install cargo-criterion
```

## Build Customization

### Feature Flags

Felix supports optional feature flags:

#### Telemetry

Enable detailed per-stage timing instrumentation:

```bash
cargo build --release --features telemetry
```

!!! note "Performance Impact"
    Telemetry adds instrumentation overhead. Validate on your workload—high fanout and batching can amplify tail latency effects. Disabled by default for production.

### Environment-Specific Builds

#### Minimal Build

Build only what you need:

```bash
# Just the broker
cargo build --release -p broker

# Just the client library
cargo build --release -p felix-client
```

#### All Demos

Build all demonstration binaries:

```bash
cargo build --release --bins
```

## Platform-Specific Notes

### Linux

Felix works best on Linux with modern kernel support for QUIC/UDP optimization:

- Kernel 5.8+ recommended
- Increase UDP buffer sizes for high throughput:

```bash
sudo sysctl -w net.core.rmem_max=26214400
sudo sysctl -w net.core.wmem_max=26214400
```

### macOS

Works well on macOS 11 (Big Sur) and later. No special configuration needed.

### Windows (WSL2)

Use WSL2 for best compatibility:

1. Install WSL2: [Microsoft Guide](https://learn.microsoft.com/en-us/windows/wsl/install)
2. Install Ubuntu or Debian
3. Follow Linux instructions inside WSL2

Native Windows support is not currently tested.

## Docker (Alternative)

Docker images can be built locally for quick testing (not recommended for production):

```bash
# Build the broker image
docker build -t felix-broker -f docker/broker.Dockerfile .

# Run the broker
docker run -p 5000:5000/udp -p 8080:8080 felix-broker
```

### Control Plane Container

Build and run the control plane in a separate container:

```bash
# Build the control plane image
docker build -t felix-controlplane -f docker/controlplane.Dockerfile .

# Run the control plane (example uses a local Postgres)
docker run -p 8443:8443 \
  -e FELIX_CONTROLPLANE_POSTGRES_URL=postgres://postgres:postgres@host.docker.internal:55432/postgres \
  felix-controlplane
```

See [Docker Compose Guide](../deployment/docker-compose.md) for orchestrated deployments.

## Troubleshooting

### OpenSSL Errors (Linux)

If you see OpenSSL-related build errors:

```bash
# Ubuntu/Debian
sudo apt-get install pkg-config libssl-dev

# RHEL/CentOS/Fedora
sudo yum install pkg-config openssl-devel
```

### Linker Errors

Use `lld` for faster linking (optional):

```bash
# Install lld
sudo apt-get install lld  # Debian/Ubuntu
brew install llvm         # macOS

# Configure Rust to use it
mkdir -p .cargo
cat > .cargo/config.toml << EOF
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
EOF
```

### Out of Memory

If the build runs out of memory:

```bash
# Reduce parallel jobs
cargo build --release -j 2
```

### Slow Builds

Enable incremental compilation for development:

```bash
export CARGO_INCREMENTAL=1
cargo build
```

## Next Steps

- [Quickstart Guide](quickstart.md) - Run your first Felix deployment
- [Building & Testing](../development/building.md) - Development workflow
- [Configuration](../reference/configuration.md) - Customize Felix behavior
