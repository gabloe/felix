# Build stage
FROM rust:1.92.0 AS builder

WORKDIR /build

# Copy workspace files
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY services ./services

# Build release binaries
RUN cargo build --release -p broker

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy broker binary
COPY --from=builder /build/target/release/broker /usr/local/bin/felix-broker

# Expose default broker port (QUIC)
EXPOSE 5000/udp

# Default command
CMD ["felix-broker"]
