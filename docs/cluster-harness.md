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

## Driving it by hand

`up` writes a session file — the broker addresses and a credential — so a second
terminal has something to attach to. The other commands find it themselves.

```bash
# window 1
task cluster:up

# window 2
task -s cluster:subscribe -- orders

# window 3
task -s cluster:publish -- orders hello
```

```text
# window 2
subscribing to orders on broker-2 (owner)
[broker-2] offset      1  hello

# window 3
published "hello" to orders via broker-0 → forwarded to broker-2 → acknowledged
```

The stream and the payload are named on both sides deliberately. Two terminals
side by side have nothing else linking what was published to what arrived, and a
demo that does not show that linkage is not showing anything.

`-s` keeps Task from echoing its own `cargo run` line above every result.

A subscriber can sit idle indefinitely. That is worth stating because it was not
always true: QUIC closes a connection that has been idle for `max_idle_timeout`,
and a subscription to a quiet stream sends nothing in either direction, so
without a keep-alive the connection died after 30 seconds and took the
subscription with it. The transport now sends keep-alives by default.

That second line is the whole point of a cluster, and it is why the commands say
which broker they went through. A single broker produces the same records; only
the routing differs, so the routing is what the output shows.

`publish` defaults to a broker that does **not** own the shard, because that is
the path a single-node deployment cannot demonstrate. `--via broker-2` (the
owner) prints `written locally, no hop` instead.

The stream is required, not defaulted: a demo where the stream is implicit does
not show that the publisher and the subscriber are talking about the same one.

`subscribe` defaults to the owner, because the owner is the only broker that
serves a subscription today. `--on` a different broker is allowed and says
plainly that nothing will arrive — subscribe routing is M6, decided in
[subscribe routing](subscribe-routing.md).

A burst, for filling a subscriber's panel:

```bash
task cluster:burst                      # 30 messages, in order
COUNT=60 GAP=0.2 task cluster:burst     # slower, readable on camera
PARALLEL=10 task cluster:burst          # concurrent, and visibly reordered
```

It spreads messages across every broker, asking the cluster which ones exist so
it keeps working with `--nodes 5`.

**Ordering is worth understanding before demonstrating anything with it.** At
`PARALLEL=1` records arrive in the order they were sent. Above that they do not,
and that is correct rather than a defect: concurrent publishes through different
brokers have no defined relative order, and the owner assigns offsets in the
order it commits them. Felix orders a publisher's own sequence, not a race
between three of them. Measured at `PARALLEL=10`, all 30 records arrive and the
order interleaves.

`task cluster:owners` prints who leads what, which is worth having on screen
before publishing: the owner is chosen by rendezvous hash, so it differs between
runs.

Two clusters share one session file, so a second `up` takes it over and warns
that it has. Whichever stops first leaves the file alone unless it still
describes that cluster — otherwise stopping the second would leave the first
running and unreachable, holding its ports with no way to address it.

The session file holds a bearer token. It is written owner-only into the temp
directory and removed on teardown; the cluster it opens is loopback-only with dev
certificates, and disappears with the process.

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

## Watching it

A recording of the three-pane demo is on the docs site:
[Demo: Cross-broker Publishing](https://gabloe.github.io/felix/demos/cross-broker-cluster/).
Embedded as video rather than an animated GIF — the same 45 seconds of terminal
output would be tens of megabytes as a GIF, and could not be paused on the line
that matters.

## Running it hands-free

Two ways, depending on whether you want the three-panel view.

```bash
task cluster:demo         # ONE pane, drives itself, narrated headings
task cluster:demo:tmux    # THREE panes: cluster, subscriber, publisher
```

`cluster:demo` runs the whole sequence in a single process: the cluster comes
up, a subscriber attaches to the owner, a record is published through a broker
that does *not* own the shard, then through the one that does, then a burst
across all of them. Records arriving at the subscriber are indented with an
arrow so they read as a separate voice from the publisher's lines. `--pace 0`
runs it flat out, which is what a test wants; the default leaves room to narrate.

`cluster:demo:tmux` does the same thing across three real panes, which reads
better on camera. It needs `tmux`, and `PACE=6 task cluster:demo:tmux` slows it
down to narrate over.

The tmux version ends with two bursts rather than one: six records sent one at a
time, which arrive in order, then twenty-four sent twelve at a time, which do
not. Both prefixes are distinct (`seq-`, `par-`) so the subscriber's panel shows
which is which. The second burst is the interesting one — it makes visible that
concurrent publishes through different brokers have no relative order, which is
a property worth showing rather than a defect worth hiding. Both wait on `owners` rather than `nodes` to decide
the cluster is up: `nodes` only reads the session file, so a file left by a
previous cluster satisfies it immediately and the pane then talks to a control
plane that is gone.

## Running the tests

They are `#[serial]`. Each starts three broker processes, and a two-core CI
runner asked to start nine at once starves them all — the first symptom is a
readiness timeout that looks like a bug in the broker rather than in the test
setup. Serial runs also narrow the window in which two clusters can be handed
the same ephemeral port.

A broker that exits during start-up is started again, up to three times, with
fresh ports. Port selection is inherently racy — a port is probed, released, and
only then handed to the child — so losing it is a retry rather than a cluster
failure. A broker that fails every time is genuinely misconfigured and is
reported with its exit status and the tail of its own log, which is the
difference between "lost a port" and "the control plane refused this identity".

Broker output goes to `broker.log` in each node's data directory rather than
being discarded, so that reason exists to be quoted.

## The conformance suite

`crates/felix-cluster/tests/conformance.rs` runs one set of assertions against
**both** a single broker and a three-node cluster. That equivalence is the
claim being tested: a client must not be able to tell how many brokers there
are, or which one it connected to.

Scenarios live in `scenarios.rs` and are parameterised by which broker the
publish goes through — the owner, or one that is not. A scenario that a
deployment cannot express (a non-owner, on a single node) reports `Skipped` and
says so in the log; it is never quietly run against the owner, which would look
like coverage while asserting nothing.

Everything goes through the client-facing API. A test that reached into broker
internals could not distinguish a correctly routed publish from one the wrong
broker handled locally, which is the failure the suite exists to catch. The
`delivery` scenario checks the forward counter on the ingress broker before
waiting for the record, because a broker that served the publish itself delivers
to its own subscribers and looks correct from any single vantage point.

Failures name the ingress broker, the owner, the shard, and the generation:

```
a non-owner handled the publish locally instead of forwarding it
  (ingress=broker-0 owner=broker-2 shard=t1/ns/orders/0 generation=0)
```

## A gap the suite does not paper over: the stale-ownership window

Ownership reaches a broker through its watch. Between the control plane moving a
shard and the old owner noticing, that broker still believes it owns the shard
and **serves publishes locally**. Those records land in its log and are invisible
to subscribers on the new owner.

Nothing in M4 closes this. There is no fencing, and the generation check protects
only a *forwarded* publish — a stale ex-owner serving locally never forwards, so
nothing checks it. The window is bounded by the broker's control-plane sync
interval.

This is acknowledged-write loss, not merely a routing delay: the client is told
the publish succeeded, and the record is durably on disk on a broker nobody will
read it from. Tracked in
[#239](https://github.com/gabloe/felix/issues/239).

What is promised, and what `a_moved_shard_converges_on_the_new_owner` asserts, is
**convergence**: the old owner starts forwarding within a bounded time. The test
is deliberately written to converge rather than to wait long enough not to
observe the window, so the gap stays visible.

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
