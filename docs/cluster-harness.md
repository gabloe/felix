# The local cluster harness

A three-node Felix cluster on one machine, for integration and failure tests.

```bash
task cluster:status   # start, print membership and ownership, tear down
task cluster:smoke    # publish through a non-owner, receive from the owner
task cluster:up       # start and hold until Ctrl-C
task cluster:test     # the cross-broker integration tests
```

`-- --nodes 5` sets the size. Nothing else is required: no compose file, no
images, no ports to reserve, and no state left behind.

## What is real, and what is not

**Brokers are real processes.** Each gets its own client-facing QUIC port,
internal peer port, metrics port, node identity, credential, and data directory.
They register with the control plane, forward to each other over the internal
transport, and stopping one is a real process exit.

**The control plane runs in the harness process.** Not for convenience: a broker
needs a credential, and the only way to obtain one today is an OIDC token
exchange against a real identity provider. Holding the store in process lets the
harness mint node and client tokens against the tenant's signing keys, which is
what `services/broker/tests/membership_lifecycle.rs` already does for the same
reason.

The consequence, stated plainly: the control plane's router, store, placement,
and HTTP contract are all exercised; its `main`, its own configuration, and its
shutdown are not. A harness that ran it as a process would need a fake IdP, or a
way to issue a first credential without one — the latter is worth having on its
own, and would let this become a fully out-of-process cluster.

## Waiting

Nothing here sleeps for a fixed duration and hopes. Start-up returns only once:

1. every broker answers `/ready`,
2. the control plane considers every broker placeable,
3. every shard has a leader — placement is *stepped*, not waited for, so it does
   not depend on a reconcile timer, and
4. **a publish succeeds**.

The last one is the only honest check for the gap between "assigned" and
"servable": a broker can hold an assignment it has not finished opening, no
control-plane state distinguishes the two, and a publish in that window is
refused. The probe deliberately publishes through an arbitrary broker rather
than the owner, so the routing path is covered by start-up itself.

A wait that times out says what it was still waiting for.

## Two tokens

The harness mints two credentials, and they are not interchangeable:

- A **client token** carrying only `stream.publish` and `stream.subscribe`,
  presented to brokers over QUIC.
- An **admin token** carrying tenant, namespace, and `node.view:cluster:*`,
  presented to the control plane's HTTP API.

A broker validates every action in a token it is given and rejects the whole
token if one is not a client-facing action, so a single credential carrying
`node.view` cannot publish at all.

## Faults

`stop_node` kills a broker and waits until the control plane no longer considers
it placeable, which is the primitive a failure test needs — without it, every
such test races the expiry sweep. Liveness windows are tuned short (a 1s expiry
timeout) because every process is local, so a stopped broker is observable in
about a second.

Blocking a peer link without stopping the process is not supported yet; it needs
either a proxy in front of the internal listener or platform firewall rules, and
nothing in M4 required it.

## Using it from a test

```rust
let cluster = Cluster::start(ClusterConfig::default()).await?;
let (owner, non_owner) = cluster.owner_and_non_owner("orders").await?;
cluster.publish_via(&non_owner, "orders", payload).await?;
```

`Cluster::metric` reads a counter or gauge from one broker's `/metrics`, and
returns `None` when it was never recorded — which for a counter is the
difference between "zero so far" and "this code path never ran". That
distinction is usually what a cross-broker assertion is making: a publish served
locally by the wrong broker looks exactly like one whose delivery is slow.

Dropping a `Cluster` kills its brokers, so a panicking test leaves nothing
running.
