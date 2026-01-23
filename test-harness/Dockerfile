# Build stage for test harness clients
FROM rust:1.92.0 AS builder

WORKDIR /build

# Copy workspace files
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY services ./services
COPY test-harness ./test-harness

# Build test harness binaries
RUN cargo build --release -p felix-test-harness

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binaries
COPY --from=builder /build/target/release/publisher /usr/local/bin/publisher
COPY --from=builder /build/target/release/subscriber /usr/local/bin/subscriber
COPY --from=builder /build/target/release/init-broker /usr/local/bin/init-broker

# Default to publisher
CMD ["publisher"]
