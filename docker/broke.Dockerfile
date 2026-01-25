# syntax=docker/dockerfile:1.7

############################
# Build stage
############################
FROM rust:1.85-bookworm AS builder

ARG PROFILE=release
ARG BIN_NAME=broker
ARG PACKAGE_NAME=broker
ARG CARGO_FEATURES=""
ARG RUSTFLAGS=""

ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    RUSTFLAGS=${RUSTFLAGS}

WORKDIR /src

# System deps (keep minimal; add libssl-dev/pkg-config if you need them)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*

# --- Cache-friendly dependency build ---
# Copy workspace manifests first
COPY Cargo.toml Cargo.lock ./
# Copy each crate manifest (adjust if you have more crates)
COPY crates/broker/Cargo.toml crates/broker/Cargo.toml
COPY crates/felix-broker/Cargo.toml crates/felix-broker/Cargo.toml
COPY crates/felix-wire/Cargo.toml crates/felix-wire/Cargo.toml
COPY crates/felix-storage/Cargo.toml crates/felix-storage/Cargo.toml
# If you have many crates, you can add them here or replace with a script.

# Create dummy src to compile deps without full source
RUN mkdir -p crates/broker/src && echo "fn main(){}" > crates/broker/src/main.rs

# Warm dependency cache
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build -p ${PACKAGE_NAME} --bin ${BIN_NAME} --profile ${PROFILE} ${CARGO_FEATURES}

# Now copy the real source
RUN rm -rf crates/broker/src
COPY . .

# Build real binary
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build -p ${PACKAGE_NAME} --bin ${BIN_NAME} --profile ${PROFILE} ${CARGO_FEATURES}

# Optional: strip (install binutils)
RUN apt-get update && apt-get install -y --no-install-recommends binutils && rm -rf /var/lib/apt/lists/* \
 && strip /src/target/${PROFILE}/${BIN_NAME} || true

############################
# Runtime stage
############################
FROM debian:bookworm-slim AS runtime

ARG BIN_NAME=broker

# Minimal runtime deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tini \
 && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -u 10001 -g nogroup felix

WORKDIR /app

COPY --from=builder /src/target/release/${BIN_NAME} /app/${BIN_NAME}

# If you use config files, mount them at runtime.
# COPY config/broker.yml /etc/felix/broker.yml

USER 10001
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/app/broker"]

# Ports (example)
EXPOSE 5000/udp
EXPOSE 8080
EXPOSE 9090

# Healthcheck assumes an HTTP health endpoint; adjust path/port.
HEALTHCHECK --interval=10s --timeout=2s --retries=6 CMD \
  sh -c 'wget -qO- http://127.0.0.1:8080/healthz >/dev/null 2>&1 || exit 1'