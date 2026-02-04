# Project Structure

Understanding Felix's repository layout, crate organization, and architectural conventions.

## Repository Overview

```
felix/
├── crates/              # Rust crates (libraries)
├── services/            # Runnable services (binaries)
├── docs/                # Design and architecture docs
├── docs-site/           # User documentation (MkDocs)
├── scripts/             # Build and automation scripts
├── docker/              # Docker configuration
├── .github/             # GitHub Actions workflows
├── githooks/            # Git hooks for development
├── Cargo.toml           # Workspace manifest
├── Cargo.lock           # Dependency lock file
├── Taskfile.yml         # Task runner configuration
├── deny.toml            # Dependency audit rules
├── mkdocs.yml           # Documentation site config
└── rust-toolchain.toml  # Rust version specification
```

## Crates Directory

The `crates/` directory contains all library crates following a modular architecture:

```
crates/
├── felix-broker/        # Broker core (pub/sub, cache, fanout)
├── felix-wire/          # Wire protocol and framing
├── felix-transport/     # QUIC transport abstraction
├── felix-storage/       # Storage layer (ephemeral + durable)
├── felix-client/        # Client SDK
├── felix-common/        # Shared types and utilities
├── felix-metadata/      # Metadata abstractions
├── felix-router/        # Region-aware routing
├── felix-crypto/        # Encryption and key handling
├── felix-authz/         # Authentication and authorization
├── felix-consensus/     # Consensus coordination (Raft)
└── felix-conformance/   # Wire protocol conformance tests
```

### Core Crates

#### felix-broker

**Purpose**: Broker core logic for pub/sub and cache.

**Responsibilities**:
- Stream registry and subscription management
- Event fanout and batching
- Cache storage with TTL
- Backpressure and flow control
- Connection lifecycle management

**Key modules**:
- `broker.rs`: Main broker type
- `subscription.rs`: Subscription handling
- `cache.rs`: Cache implementation
- `fanout.rs`: Fanout engine

**Dependencies**:
- `felix-wire`: Protocol framing
- `felix-transport`: QUIC abstraction
- `felix-storage`: Data persistence
- `felix-common`: Shared types

#### felix-wire

**Purpose**: Wire protocol definition and frame encoding/decoding.

**Responsibilities**:
- Frame type definitions
- Binary batch encoding/decoding
- Protocol versioning
- Frame validation

**Key types**:
- `Frame`: Top-level frame type
- `BinaryBatch`: Batch frame format
- `FrameCodec`: Encode/decode implementation

**Protocol layers**:
1. **Envelope**: Version, type, length
2. **Binary frames**: Zero-copy fast paths

#### felix-transport

**Purpose**: QUIC transport abstraction and connection pooling.

**Responsibilities**:
- QUIC client/server setup
- Connection lifecycle
- Stream management
- TLS certificate handling
- Flow control configuration

**Key types**:
- `QuicClient`: Client-side connection
- `QuicServer`: Server-side listener
- `StreamPool`: Connection pooling
- `QuicConfig`: Transport configuration

**Based on**: `quinn` (QUIC implementation)

#### felix-storage

**Purpose**: Storage layer abstraction for ephemeral and durable data.

**Responsibilities**:
- Ephemeral in-memory storage
- Durable WAL and log segments
- TTL management
- Retention policies
- Compaction (future)

**Storage types**:
- `EphemeralStore`: In-memory with TTL
- `DurableStore`: Persistent log (planned)
- `CacheStore`: Key-value with expiration

#### felix-client

**Purpose**: Rust client SDK for Felix.

**Responsibilities**:
- Publish API
- Subscribe API
- Cache operations (put/get)
- Connection management
- Stream pooling
- Error handling

**Key types**:
- `Client`: Main client interface
- `Publisher`: Publishing handle
- `Subscription`: Subscription handle
- `InProcessClient`: Embedded testing client
- `ClientConfig`: Client configuration

**Example usage**:
```rust
use felix_client::{Client, ClientConfig};
use felix_wire::AckMode;
use std::net::SocketAddr;

let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig::optimized_defaults(quinn);
let addr: SocketAddr = "127.0.0.1:5000".parse()?;
let client = Client::connect(addr, "localhost", config).await?;
let publisher = client.publisher().await?;
publisher
    .publish("tenant", "namespace", "stream", b"data".to_vec(), AckMode::None)
    .await?;
```

### Supporting Crates

#### felix-common

**Purpose**: Shared types and utilities used across crates.

**Contents**:
- `types.rs`: Common type definitions
- `error.rs`: Error types
- `ids.rs`: ID types (TenantId, StreamId, etc.)
- `config.rs`: Configuration types
- `time.rs`: Time utilities

**Principle**: Minimal dependencies, stable API.

#### felix-metadata

**Purpose**: Metadata abstractions for streams, tenants, and routing.

**Responsibilities**:
- Stream metadata
- Tenant configuration
- Routing policies
- Shard placement
- Region configuration

**Key types**:
- `StreamMetadata`: Stream definition
- `TenantConfig`: Tenant settings
- `ShardInfo`: Shard placement
- `RoutingPolicy`: Region-aware routing

#### felix-router

**Purpose**: Region-aware routing and locality policies.

**Responsibilities**:
- Region topology
- Locality-based routing
- Cross-region bridge configuration
- Request routing logic

**Future**: Control plane integration for dynamic routing.

#### felix-crypto

**Purpose**: Encryption and key management.

**Responsibilities**:
- TLS certificate handling
- Key derivation
- End-to-end encryption (planned)
- Key rotation (planned)

#### felix-authz

**Purpose**: Authentication and authorization.

**Responsibilities**:
- Token-based auth (OIDC exchange + Felix JWTs)
- RBAC policies and permission matching
- Tenant isolation enforcement
- mTLS authentication (planned)

#### felix-consensus

**Purpose**: Consensus and coordination for clustering.

**Responsibilities**:
- Raft implementation (planned)
- Leader election
- Log replication
- Membership management

**Status**: Placeholder for future clustering.

#### felix-conformance

**Purpose**: Wire protocol conformance test suite.

**Responsibilities**:
- Test vector validation
- Cross-implementation testing
- Protocol regression tests

**Usage**:
```bash
cargo run -p felix-conformance
```

## Services Directory

The `services/` directory contains runnable binaries:

```
services/
├── broker/              # Broker service
│   ├── src/
│   │   ├── main.rs      # Broker entrypoint
│   │   ├── config.rs    # Configuration loading
│   ├── Cargo.toml
│   └── README.md        # Performance profiles
├── controlplane/        # Control plane (future)
└── agent/               # Infrastructure agent (future)

demos/
├── broker/              # Demo binaries for the broker crate
│   ├── simple_pubsub_demo.rs
│   ├── cache_demo.rs
│   ├── latency_demo.rs
│   ├── notifications_demo.rs
│   └── orders_demo.rs
├── rbac-live/            # End-to-end RBAC mutation demo crate
│   └── src/main.rs
└── cross_tenant_isolation/  # End-to-end tenant isolation demo crate
    └── src/main.rs
```

### Broker Service

**Location**: `services/broker/`

**Entrypoint**: `src/main.rs`

**Responsibilities**:
- Load configuration from env/YAML
- Initialize broker runtime
- Start QUIC listener
- Expose metrics endpoint
- Handle graceful shutdown

**Demo binaries** (see `demos/broker/`):

- **`simple_pubsub_demo.rs`**: Pub/sub demonstration
- **`cache_demo.rs`**: Cache benchmark
- **`latency_demo.rs`**: Latency measurement tool
- **`notifications_demo.rs`**: Multi-tenant notifications workflow demo
- **`orders_demo.rs`**: Orders/payments pipeline demo
- **`rbac-live/`**: Live RBAC policy change demo (control plane + broker + token exchange)
- **`cross_tenant_isolation/`**: Cross-tenant isolation demo (Postgres + control plane + broker)

## Documentation

### Design Docs (`docs/`)

Architecture and design documentation:

```
docs/
├── architecture.md      # System architecture overview
├── demos.md             # Demo catalog and run instructions
├── protocol.md          # Wire protocol specification
├── control-plane.md     # Control plane design
├── semantics.md         # Delivery semantics
├── design.md            # Product design notes
├── broker-config.md     # Broker configuration
├── client-config.md     # Client configuration
├── todos.md             # Implementation checklist
└── assets/              # Diagrams and images
    └── logo.PNG
```

**Purpose**: Technical design for contributors.

### User Docs (`docs-site/`)

User-facing documentation (MkDocs):

```
docs-site/
├── docs/
│   ├── index.md
│   ├── getting-started/
│   ├── architecture/
│   ├── api/
│   ├── features/
│   ├── deployment/
│   ├── reference/
│   └── development/
└── mkdocs.yml           # Site configuration
```

**Purpose**: End-user guides and API references.

**Build**:
```bash
cd docs-site
mkdocs serve
```

## Scripts Directory

Automation and utility scripts:

```
scripts/
└── perf/                # Performance benchmarking
    ├── run_latency_matrix.py
    ├── normalize_and_aggregate.py
    ├── make_charts.py
    ├── render_markdown_snippets.py
    └── presets.yml      # Benchmark configurations
```

## Docker Directory

Docker build configuration:

```
docker/
├── broker.Dockerfile           # Multi-stage broker build
├── controlplane.Dockerfile     # Control plane build
├── prometheus/
│   ├── prometheus.yml          # Prometheus config
│   └── prometheus.Dockerfile
└── otel-collector/
    ├── config.yml              # OTEL config
    └── otel-collector.Dockerfile
```

## GitHub Workflows

CI/CD configuration:

```
.github/
├── workflows/
│   ├── ci.yml           # Main CI pipeline
│   └── coverage.yml     # Code coverage
└── dependabot.yml       # Dependency updates
```

## Configuration Files

### Cargo.toml (Workspace)

**Purpose**: Define workspace and shared dependencies.

```toml
[workspace]
members = [
    "crates/*",
    "services/*",
]
resolver = "2"

[workspace.dependencies]
tokio = { version = "1.35", features = ["full"] }
anyhow = "1.0"
# ... shared dependencies
```

### rust-toolchain.toml

**Purpose**: Pin Rust version for consistency.

```toml
[toolchain]
channel = "1.92.0"
components = ["rustfmt", "clippy"]
```

### deny.toml

**Purpose**: Configure cargo-deny for dependency auditing.

**Checks**:
- Security vulnerabilities
- License compliance
- Banned crates
- Duplicate dependencies

### Taskfile.yml

**Purpose**: Define common development tasks.

**Tasks**: build, test, lint, fmt, coverage, demos, etc.

## Naming Conventions

### Crate Names

- **Library crates**: `felix-<component>` (e.g., `felix-broker`)
- **Binary crates**: Service name (e.g., `broker`)
- **All lowercase**, hyphen-separated

### Module Structure

```
crate_root/
├── lib.rs              # Public API (library)
├── main.rs             # Entrypoint (binary)
├── module_name.rs      # Single-file module
└── module_name/        # Multi-file module
    ├── mod.rs          # Module root
    ├── submodule.rs
    └── tests.rs        # Module tests
```

### File Naming

- **Snake_case**: `my_module.rs`
- **Tests**: `mod_tests.rs` or `tests/`
- **Binaries**: `bin/my_app.rs`

### Type Naming

- **PascalCase**: Structs, enums, traits (`BrokerConfig`, `FrameType`)
- **snake_case**: Functions, methods, variables (`publish_event`, `config_value`)
- **SCREAMING_SNAKE_CASE**: Constants (`DEFAULT_PORT`, `MAX_BATCH_SIZE`)

## Dependency Management

### Dependency Categories

**Core dependencies**:
- `tokio`: Async runtime
- `quinn`: QUIC implementation
- `serde`: Serialization
- `anyhow`/`thiserror`: Error handling

**Development dependencies**:
- `serial_test`: Test isolation
- `tempfile`: Temporary files in tests
- `criterion`: Benchmarking

### Dependency Rules

1. **Minimize dependencies**: Only add when necessary
2. **Pin versions**: Use exact versions in workspace
3. **Audit regularly**: Run `cargo-deny check`
4. **No unmaintained crates**: Check maintenance status
5. **License compliance**: Only Apache-2.0 / MIT

### Adding Dependencies

```bash
# Add to workspace
cargo add --workspace <crate>

# Add to specific crate
cargo add -p felix-broker <crate>

# Add dev dependency
cargo add --dev <crate>
```

## Testing Structure

### Test Organization

**Unit tests**: Inline in source files
```rust
#[cfg(test)]
mod tests {
    use super::*;
    // Tests here
}
```

**Integration tests**: `tests/` directory
```
crate/
  tests/
    integration_test.rs
    common/
      mod.rs           # Shared test utilities
```

**Conformance tests**: Separate crate (`felix-conformance`)

### Test Naming

- **Functions**: `test_<what_it_does>`
- **Modules**: `tests` or `<module>_tests`
- **Files**: `integration_test.rs`, `e2e_test.rs`

## Build Artifacts

### Target Directory

```
target/
├── debug/              # Debug builds
│   ├── broker          # Binary
│   ├── deps/           # Dependencies
│   └── build/          # Build scripts
├── release/            # Release builds
└── doc/                # Generated docs
```

### Cargo Cache

```
~/.cargo/
├── registry/           # Downloaded crate sources
├── git/                # Git dependencies
└── bin/                # Installed binaries
```

## Development Environment

### Recommended Setup

**Editor**: VS Code with rust-analyzer

**Extensions**:
- rust-analyzer: IntelliSense
- CodeLLDB: Debugging
- Better TOML: TOML syntax

**.vscode/settings.json**:
```json
{
    "rust-analyzer.cargo.features": "all",
    "rust-analyzer.checkOnSave.command": "clippy"
}
```

### Git Hooks

```bash
# Install pre-commit hook
cp githooks/pre-commit .git/hooks/
chmod +x .git/hooks/pre-commit
```

**Pre-commit hook**:
- Format check
- Clippy warnings
- Run tests

## Code Organization Principles

### Modularity

- **Small, focused crates**: Each crate has a single purpose
- **Clear boundaries**: Minimal cross-crate dependencies
- **Public API**: Carefully designed, stable interfaces

### Layering

```
┌─────────────────────────┐
│   Services (binaries)   │
├─────────────────────────┤
│    Application Layer    │
│  (broker, client, etc)  │
├─────────────────────────┤
│     Protocol Layer      │
│   (wire, transport)     │
├─────────────────────────┤
│    Foundation Layer     │
│  (common, storage)      │
└─────────────────────────┘
```

### Dependency Direction

Dependencies flow **downward**:
- Services depend on applications
- Applications depend on protocol
- Protocol depends on foundation
- Foundation has minimal dependencies

**Never**: Lower layers depend on upper layers.

## Future Structure Changes

As Felix evolves:

- **More crates**: New features may add crates
- **Service separation**: Control plane, agents
- **Language clients**: SDKs in other languages
- **Deployment**: Helm charts, Kubernetes operators

The core structure (crates/services separation) will remain stable.

## Next Steps

- **Contributing**: [Contributing Guide](contributing.md)
- **Building**: [Building & Testing](building.md)
- **Architecture**: [System Design](../architecture/system-design.md)
