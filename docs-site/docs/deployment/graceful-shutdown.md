# Process lifecycle and graceful shutdown

Covers how the broker and control plane start, and — mostly — how they stop.
Implemented in `crates/felix-common/src/lifecycle.rs`, which both services share.

## Termination signals

Both services wait on **SIGTERM and SIGINT** on Unix, and Ctrl-C elsewhere.

SIGTERM is the one that matters operationally. Kubernetes, systemd, and
`docker stop` all terminate a process with SIGTERM; SIGINT only covers an
interactive Ctrl-C. A process that handles only SIGINT has no shutdown path under
any of those supervisors — the default SIGTERM handler kills it outright, so every
rolling update drops in-flight publishes, acknowledgements, and subscription
writes.

## Shutdown order

The order matters more than the individual steps.

1. **Readiness goes false.** `/ready` starts returning `503 draining`. Load
   balancers and the Kubernetes endpoints controller stop routing new traffic here
   while the process can still serve it, so clients are steered away from a healthy
   instance rather than discovering a broken one.
2. **Stop admitting new work.** The broker cancels its QUIC accept loop; the
   control plane stops accepting new HTTP connections. Already-accepted work is
   untouched.
3. **Drain, bounded by a deadline.** In-flight connections and requests finish on
   their own.
4. **Force-cancel the remainder and name it.** Anything still running when the
   deadline expires is aborted and logged by name at WARN.

`/live` stays `200` throughout. A draining process is alive and working correctly;
failing liveness would make Kubernetes restart a pod that is shutting down exactly
as intended.

Metrics are torn down **last**, after everything else has drained, so `/metrics`
and `/ready` remain scrapeable for the whole shutdown window. That window is the
only chance an operator has to see what the process was doing while it stopped.

## The drain deadline

`FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS` (default `25000`) is a **single budget shared by
every subsystem**, not a per-subsystem timeout. Each subsystem gets whatever is left
when its turn comes, so N subsystems cannot stretch a 25s deadline into 25N seconds.
Total shutdown time is bounded by this value regardless of how many things hang.

Set it below the platform's kill deadline. Kubernetes defaults
`terminationGracePeriodSeconds` to 30 and sends SIGKILL when it expires, so the
default leaves headroom to finish the drain, log the outcome, and exit first. If you
raise the grace period, raise this to match.

On a clean drain you get:

```
INFO drain complete elapsed_ms=142
```

On a forced one — work was dropped, and this is the line to alert on:

```
WARN drain deadline expired; forcing cancellation elapsed_ms=25001 deadline_ms=25000 unfinished=["quic_connections"]
```

## Kubernetes configuration

Readiness propagation is not instant. The endpoints controller has to observe the
`/ready` failure and update every kube-proxy before traffic actually stops arriving,
and that takes a few seconds. The process cannot wait for something it has no
visibility into, so use a `preStop` hook to hold the pod open while propagation
happens — this is the standard pattern, not a workaround for a gap in Felix:

```yaml
spec:
  terminationGracePeriodSeconds: 30
  containers:
    - name: felix-broker
      env:
        # Leaves ~15s of headroom under the 30s grace period, after the preStop sleep.
        - name: FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS
          value: "20000"
      lifecycle:
        preStop:
          exec:
            # Give the endpoints controller time to remove this pod from rotation
            # before SIGTERM is delivered.
            command: ["/bin/sleep", "5"]
      readinessProbe:
        httpGet:
          path: /ready
          port: 9090
        periodSeconds: 2
      livenessProbe:
        httpGet:
          path: /live
          port: 9090
```

Budget the total: `preStop` sleep + `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS` must fit
inside `terminationGracePeriodSeconds`, or SIGKILL arrives mid-drain and you are
back to dropping in-flight work.

## What is not covered yet

Tracked under [#139](https://github.com/gabloe/felix/issues/139):

- Cancellation is coordinated at the **connection** boundary. The drain waits for
  each connection task to finish, but does not separately signal publish workers,
  acknowledgement waiters, or subscription writers to wind down early. A connection
  that would otherwise sit idle for its full timeout is only cut short by the
  overall deadline.
- Subscription streams are not flushed or closed according to their delivery
  contract; they end when their connection task ends.
- The "an acknowledged publish is never lost solely because SIGTERM arrived"
  guarantee is not yet verified by a test.
- Coverage is at the accept-loop and readiness level
  (`services/broker/tests/graceful_shutdown.rs`). There is no process-level test
  that spawns the real binary, sends it SIGTERM under active publish/subscribe
  traffic, and asserts a bounded clean exit.
