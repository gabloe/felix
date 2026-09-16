---
title: "Observability"
---

Three windows into a running Felix: structured logs, Prometheus metrics, and
optional per-stage telemetry for performance work. This page lists what
actually exists and which signals answer which questions — every metric named
here is one the code emits.

## Logging

Felix logs through `tracing`, filtered by the standard `RUST_LOG` variable:

```bash
RUST_LOG=info                                  # default
RUST_LOG=felix_broker=debug                    # one module, louder
RUST_LOG=felix_broker=trace,felix_wire=debug   # several modules
```

Log lines are structured key-value events (tenant, stream, subscription id,
error), so they grep and parse cleanly. There is no JSON output mode today;
if your aggregation pipeline needs JSON, wrap the process output.

The lines worth knowing on sight:

- Startup: the QUIC listen address, and whether control-plane sync is on.
- `subscriber falling behind` / `events dropped for subscriber` — a
  subscription hit its bounded queue. This is the log-side view of
  `felix_sub_queue_dropped_total`.
- Drain lines during shutdown, saying which subsystems finished in time.

## Metrics

The broker and the control plane each serve Prometheus text on their own
metrics endpoint (`metrics_bind` on the broker; `/metrics`, plus `/live` and
`/ready` for probes).

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'felix-broker'
    static_configs:
      - targets: ['broker-1:8080', 'broker-2:8080', 'broker-3:8080']
```

Rather than an exhaustive list, here are the questions that come up and the
metrics that answer them.

**Is the publish path healthy?**

```prometheus
felix_publish_requests_total
felix_publish_bytes_total
felix_publish_latency_ms                    # histogram
felix_broker_ingress_queue_depth            # publish jobs waiting
felix_broker_ingress_dropped_total          # overflow, by policy
felix_broker_ingress_rejected_total
```

A rising ingress depth means publishers are outrunning the broker; drops and
rejections say the overflow policy fired, which is deliberate and visible.

**Are subscribers keeping up?**

```prometheus
felix_subscribe_requests_total
felix_sub_queue_enqueued_total
felix_sub_queue_dropped_total               # records lost to slow consumers
felix_sub_queue_drop_old_emulated_total     # DropOld configured, DropNew behavior
felix_sub_queue_len
felix_subscriber_disconnect_total
```

`felix_sub_queue_dropped_total` increasing is the signal that a subscriber is
missing records. On a durable stream the subscriber can detect this itself
from offset gaps and resume; on an ephemeral stream this counter is the only
witness.

**Is durability the bottleneck?** The storage layer's metrics are designed
around exactly this question — compare append time against sync time, and
watch the group-commit fan-in:

```prometheus
felix_storage_append_duration_seconds
felix_storage_sync_duration_seconds
felix_storage_sync_batch_appends       # appends served per device flush
felix_storage_unsynced_bytes           # what a crash would lose right now
felix_storage_sync_failures_total      # non-zero: acknowledged durability in doubt
```

If sync dominates append, the fsync policy is the cost. A
`sync_batch_appends` near 1 under concurrent load means appends are
serializing on the device instead of sharing a flush.

**Is the cluster healthy?** Membership from both sides, replication, and
leases:

```prometheus
felix_node_count                            # control plane: fleet size by lifecycle
felix_broker_membership_live                # broker: does the cluster still count me
felix_broker_heartbeat_age_seconds          # alert when this nears the expiry timeout
felix_broker_replication_lag_records
felix_broker_replication_halted
felix_broker_lease_held
felix_broker_lease_refusals_total           # writes refused after a lease lapsed
```

**Example queries**:

```promql
# Publish rate
rate(felix_publish_requests_total[1m])

# p99 publish latency
histogram_quantile(0.99, rate(felix_publish_latency_ms_bucket[5m]))

# Records lost to slow consumers
rate(felix_sub_queue_dropped_total[5m])

# Group-commit effectiveness
rate(felix_storage_sync_batch_appends_sum[5m]) / rate(felix_storage_sync_batch_appends_count[5m])
```

## Shutdown and drain

Four signals, and the reason each one exists.

| Metric | Type | What it tells you |
| --- | --- | --- |
| `felix_ready_state` | gauge | `1` while serving, `0` once draining. Distinguishes an instance that left rotation deliberately from one that vanished. |
| `felix_inflight_requests` | gauge | Requests being served right now. Watch it fall to zero during a drain. |
| `felix_drain_duration_ms` | gauge | How long the last drain took. |
| `felix_drain_forced_total` | counter, by `subsystem` | Subsystems cancelled because the deadline expired. **Non-zero means work was dropped.** |

The last one is the point. A drain that finished in time and a drain that was
cut off both take roughly the deadline to report, so duration alone cannot tell
them apart — and the log line that says which does not survive the pod.

Alert on `felix_drain_forced_total` increasing. Everything else here is for
watching a rolling restart happen.

## Probes

The metrics listener serves `/live` and `/ready`, and they answer different
questions on purpose. `/live` says "this process can respond at all" and
touches nothing outside the process — a liveness probe drives restarts, and
restarting every instance because a dependency is down turns one outage into
a restart loop. `/ready` says "send this instance traffic," and goes false
first thing during shutdown so load balancers steer away before anything
stops working. See [Graceful shutdown](/felix/deployment/graceful-shutdown/).

## Distributed tracing

The broker builds an OTLP tracer provider on startup and installs a
`tracing-opentelemetry` layer when one is available. It is best-effort: if the
collector cannot be reached the broker starts anyway and logs without traces,
because losing telemetry must not stop a broker from serving.

**Configuration** is by environment variable. Felix does take a YAML config file
(`FELIX_BROKER_CONFIG`, see [Configuration](/felix/reference/configuration/)),
but it has no tracing keys — the exporter speaks OTLP over gRPC (tonic) and is
configured entirely through the standard OTel variables:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
export RUST_LOG=info                      # the tracing subscriber's filter
```

Resource attributes are attached from the environment, so a span carries where
it came from without the broker being told twice:

| Variable | Becomes |
|---|---|
| `FELIX_SERVICE_INSTANCE_ID`, falling back to `HOSTNAME` | `service.instance.id` |
| `K8S_CLUSTER_NAME` | `k8s.cluster.name` |
| `K8S_NAMESPACE_NAME` | `k8s.namespace.name` |
| `K8S_POD_NAME` | `k8s.pod.name` |
| `CLOUD_REGION` | `cloud.region` |
| `DEPLOYMENT_ENVIRONMENT` | `deployment.environment` |

## Per-stage telemetry

For performance investigations, both the broker and client can record
per-stage timing samples — decode, fanout, write, and so on. It is off by
default and behind a feature flag, because it is a profiling tool, not a
production metrics system:

```toml
[dependencies]
felix-client = { version = "0.1", features = ["telemetry"] }
```

On the client, `felix_client::frame_counters_snapshot()` returns frame-level
counters, and `felix_client::timings::take_samples()` drains the recorded
per-stage samples. The benchmarks and the `latency-demo` binary are the
worked examples of reading them.

On the broker, `FELIX_CONN_STATS_MS` logs QUIC path statistics (MTU, cwnd,
RTT, loss, flow-control blocking) for healthy connections on an interval —
the data that says whether a throughput problem is transport-side or above
it. Off unless set.

## Debugging quick answers

- **Subscribers receive nothing**: check the subscription was created (log
  line), the stream exists, and the application is actually awaiting
  `next_event()`. Then check `felix_sub_queue_dropped_total`.
- **Publish latency spiked**: check `felix_broker_ingress_queue_depth`
  (broker backed up), then `felix_storage_sync_duration_seconds` (durability
  is the cost), then the client-side telemetry to see which stage grew.
- **Cache misses you didn't expect**: TTL expiry, a broker restart on an
  ephemeral cache, or a key/scope mismatch — in that order of likelihood.
