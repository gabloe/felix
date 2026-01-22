# Client Config

This document describes the `felix-client` configuration options and how they are loaded.

## Loading Order

`ClientConfig::from_env_or_yaml(quinn, config_path)` uses:
- Optimized defaults first
- Environment variables next
- YAML overrides last (if provided by `config_path` or `FELIX_CLIENT_CONFIG`)

If a YAML path is provided and cannot be read or parsed, it returns an error.

## Config File Path

Set `FELIX_CLIENT_CONFIG` or pass a path into `ClientConfig::from_env_or_yaml(...)`.

Example:
```rust
let quinn = QuinnClientConfig::with_root_certificates(roots)?;
let cfg = ClientConfig::from_env_or_yaml(quinn, Some("client.yml"))?;
```

## Options

### Publish Parallelism

- `publish_conn_pool` (env: `FELIX_PUB_CONN_POOL`)
  - Number of QUIC connections for publish lanes.
- `publish_streams_per_conn` (env: `FELIX_PUB_STREAMS_PER_CONN`)
  - Number of bidirectional publish streams per connection.
- `publish_chunk_bytes` (env: `FELIX_PUBLISH_CHUNK_BYTES`)
  - Chunk size for large publish writes.
- `publish_sharding` (env: `FELIX_PUB_SHARDING`)
  - Sharding mode across publish streams.
  - Values: `rr` or `hash_stream`.

### Cache Parallelism

- `cache_conn_pool` (env: `FELIX_CACHE_CONN_POOL`)
  - QUIC connection pool size for cache operations.
- `cache_streams_per_conn` (env: `FELIX_CACHE_STREAMS_PER_CONN`)
  - Streams per cache connection.

### Event (Subscribe) Parallelism

- `event_conn_pool` (env: `FELIX_EVENT_CONN_POOL`)
  - QUIC connection pool size for subscription event streams.

### QUIC Flow Control Windows

- `event_conn_recv_window` (env: `FELIX_EVENT_CONN_RECV_WINDOW`)
- `event_stream_recv_window` (env: `FELIX_EVENT_STREAM_RECV_WINDOW`)
- `event_send_window` (env: `FELIX_EVENT_SEND_WINDOW`)
- `cache_conn_recv_window` (env: `FELIX_CACHE_CONN_RECV_WINDOW`)
- `cache_stream_recv_window` (env: `FELIX_CACHE_STREAM_RECV_WINDOW`)
- `cache_send_window` (env: `FELIX_CACHE_SEND_WINDOW`)

All window values are raw bytes.

### Safety / Limits

- `event_router_max_pending` (env: `FELIX_EVENT_ROUTER_MAX_PENDING`)
  - Max pending subscription registrations/streams held by the event router.
- `max_frame_bytes` (env: `FELIX_MAX_FRAME_BYTES`)
  - Hard cap on any single frame length.

### Bench / Telemetry

- `bench_embed_ts` (env: `FELIX_BENCH_EMBED_TS`)
  - Enables embedding publish timestamps into payloads for end-to-end latency histograms.

## YAML Example (Optimized Defaults)

```yaml
publish_conn_pool: 4
publish_streams_per_conn: 2
publish_chunk_bytes: 16384
publish_sharding: "hash_stream"
cache_conn_pool: 8
cache_streams_per_conn: 4
event_conn_pool: 8
event_conn_recv_window: 268435456
event_stream_recv_window: 67108864
event_send_window: 268435456
cache_conn_recv_window: 268435456
cache_stream_recv_window: 67108864
cache_send_window: 268435456
event_router_max_pending: 16384
max_frame_bytes: 16777216
bench_embed_ts: false
```

## Notes

- Client throughput is gated by publish parallelism. A single connection with one stream will bottleneck regardless of broker tuning.
- `publish_sharding=hash_stream` preserves per-stream ordering while spreading across workers; `rr` is best for uniform, high-concurrency loads.
