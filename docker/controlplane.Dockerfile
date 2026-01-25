# syntax=docker/dockerfile:1.7

FROM rust:1.85-bookworm AS builder

ARG PROFILE=release
ARG BIN_NAME=controlplane
ARG PACKAGE_NAME=controlplane
ARG CARGO_FEATURES=""
ARG RUSTFLAGS=""

ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    RUSTFLAGS=${RUSTFLAGS}

WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./

# Copy crate manifests for cache warm-up
COPY crates/controlplane/Cargo.toml crates/controlplane/Cargo.toml
# Copy any internal library crates the controlplane depends on:
COPY crates/felix-wire/Cargo.toml crates/felix-wire/Cargo.toml
COPY crates/felix-storage/Cargo.toml crates/felix-storage/Cargo.toml

RUN mkdir -p crates/controlplane/src && echo "fn main(){}" > crates/controlplane/src/main.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build -p ${PACKAGE_NAME} --bin ${BIN_NAME} --profile ${PROFILE} ${CARGO_FEATURES}

RUN rm -rf crates/controlplane/src
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build -p ${PACKAGE_NAME} --bin ${BIN_NAME} --profile ${PROFILE} ${CARGO_FEATURES}

RUN apt-get update && apt-get install -y --no-install-recommends binutils && rm -rf /var/lib/apt/lists/* \
 && strip /src/target/${PROFILE}/${BIN_NAME} || true

FROM debian:bookworm-slim AS runtime

ARG BIN_NAME=controlplane

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tini \
 && rm -rf /var/lib/apt/lists/*

RUN useradd -r -u 10001 -g nogroup felix
WORKDIR /app

COPY --from=builder /src/target/release/${BIN_NAME} /app/${BIN_NAME}

USER 10001
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/app/controlplane"]

EXPOSE 7000
EXPOSE 9091

HEALTHCHECK --interval=10s --timeout=2s --retries=6 CMD \
  sh -c 'wget -qO- http://127.0.0.1:7000/healthz >/dev/null 2>&1 || exit 1'