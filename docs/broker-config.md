# Broker Config Examples

This document shows sample `BrokerConfig` YAML files for common performance goals.

The broker attempts to load a YAML file from:
- `FELIX_BROKER_CONFIG` if set
- otherwise `/usr/local/felix/config.yml`

All fields are optional. Omitted values fall back to defaults.

Defaults favor safety and predictability over maximum throughput.

## General Purpose

Balanced settings for mixed workloads and moderate fanout.

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
controlplane_url: null
controlplane_sync_interval_ms: 2000
ack_on_commit: false
max_frame_bytes: 16777216
publish_queue_wait_timeout_ms: 2000
ack_wait_timeout_ms: 2000
disable_timings: false
control_stream_drain_timeout_ms: 50
cache_conn_recv_window: 268435456
cache_stream_recv_window: 67108864
cache_send_window: 268435456
event_batch_max_events: 64
event_batch_max_bytes: 65536
event_batch_max_delay_us: 250
fanout_batch_size: 64
pub_workers_per_conn: 4
pub_queue_depth: 1024
subscriber_queue_capacity: 128
subscriber_writer_lanes: 4
subscriber_lane_queue_depth: 8192
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
```

## Latency Optimized

Targets low tail latency for small payloads and limited batching.

```yaml
ack_on_commit: true # NOTE: serializes commit/ack; reduces pipeline parallelism
publish_queue_wait_timeout_ms: 1000
ack_wait_timeout_ms: 1000
control_stream_drain_timeout_ms: 25
event_batch_max_events: 8
event_batch_max_bytes: 32768
event_batch_max_delay_us: 100
fanout_batch_size: 8
pub_workers_per_conn: 2
pub_queue_depth: 512
subscriber_queue_capacity: 64
subscriber_writer_lanes: 2
subscriber_lane_queue_depth: 4096
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
```

## Throughput Optimized

Targets max publish throughput and higher batching.

```yaml
ack_on_commit: false
publish_queue_wait_timeout_ms: 3000
ack_wait_timeout_ms: 3000
control_stream_drain_timeout_ms: 100
event_batch_max_events: 256
event_batch_max_bytes: 1048576
event_batch_max_delay_us: 2000
fanout_batch_size: 256
pub_workers_per_conn: 8
pub_queue_depth: 4096
subscriber_queue_capacity: 256
subscriber_writer_lanes: 4
subscriber_lane_queue_depth: 16384
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
```

## Client-Side Parallelism (Important)

Broker throughput and fanout scalability depend on client publish parallelism. For
high throughput or fanout workloads:

- Use multiple QUIC connections
- Use multiple publish streams per connection
- Use round-robin (RR) or hash-based sharding across streams

A single connection with a single publish stream will bottleneck regardless of broker tuning.

## Worker Sizing (Important)

> IMPORTANT: `pub_workers_per_conn` should not exceed the number of active publish streams.
> Excess workers increase contention and can worsen tail latency.

## Notes

- All byte values are raw bytes; use powers of two for MiB values (e.g., 1048576 = 1 MiB).
- Tune `pub_workers_per_conn` and `pub_queue_depth` together; deep queues trade latency for throughput.
- Increasing `pub_workers_per_conn` only helps if publish load is spread across multiple streams or
  connections. Oversubscribing workers relative to streams can degrade performance.
- `subscriber_queue_capacity` controls broker-core per-subscriber buffering and drop behavior.
- `subscriber_writer_lanes` and `subscriber_lane_shard` control outbound event write parallelism.
- `subscriber_lane_shard: auto` is the default and is usually the best starting point.
- Lanes often help high fanout + large payload workloads, but gains can plateau; do not assume
  that more than 8 lanes will improve performance.
- Event delivery uses binary `EventBatch` frames.
- Queue depths directly impact memory usage. Large queue depths combined with large batch sizes and
  high fanout can significantly increase resident memory usage.
