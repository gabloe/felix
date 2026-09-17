# Kafka wire shim — spike

Evidence for #488. Read-only: `ApiVersions`, `Metadata`, `ListOffsets`, `Fetch`.

```bash
cargo build
SHIM_BIND=0.0.0.0:19092 \
SHIM_ADVERTISE_HOST=host.docker.internal SHIM_ADVERTISE_PORT=19092 \
SHIM_SEED=5 ../../target/debug/felix-kafka-shim-spike

docker run --rm edenhill/kcat:1.7.1 -b host.docker.internal:19092 -L
docker run --rm edenhill/kcat:1.7.1 -b host.docker.internal:19092 \
  -C -t orders -p 0 -o beginning -e -q -f '%o:%s\n'
```

The result, and what it cost, is in `docs/kafka-shim-spike.md`.
