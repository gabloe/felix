# Running a client against a cluster

For application developers. What to configure, what the client does on its own,
and what it will not do for you.

`docs/client-config.md` covers the tuning knobs — pool sizes, windows, queue
policies. This page is about the cluster: how a client finds brokers, what
happens when one dies, and which failures it retries.

Every guarantee named here is defined in [`semantics.md`](semantics.md), which
is the contract; this page is how to reach it from an application.

## Use `ClusterClient`, not `Client`

`Client` holds connection pools to **one** broker. It is the right thing for a
single-node deployment and the wrong thing for a cluster: the broker it was
given is exactly the one that may fail over, and when it does, that client stops
working while every other broker sits there able to serve.

`ClusterClient` owns a `Client` and rebuilds it from the other endpoints when
the one in use fails.

```rust,no_run
use std::time::Duration;
use felix_client::{ClientConfig, ClusterClient, ReconnectPolicy};
use felix_wire::AckMode;

# async fn example(quinn: quinn::ClientConfig) -> anyhow::Result<()> {
let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
config.auth_tenant_id = Some("acme".to_string());
config.auth_token = Some(std::env::var("FELIX_TOKEN")?);
// For a long-running client, set `config.token_provider` so reconnects get a
// fresh token. See docs/auth.md.

// Every broker you know of. One is enough — see "Discovery" below.
let seeds = ["10.0.0.4:5000".parse()?, "10.0.0.5:5000".parse()?];

let client = ClusterClient::connect_with_policy(
    &seeds,
    "broker.internal",
    config,
    ReconnectPolicy {
        attempts: 5,
        backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(2),
        // Off by default. See "Retries" for why there is no useful default.
        deadline: Some(Duration::from_secs(10)),
    },
)
.await?;

client
    .publish_at_least_once("acme", "default", "orders", b"payload".to_vec(), AckMode::PerMessage)
    .await?;
# Ok(())
# }
```

`ClusterClient::connect` uses the default policy, which is the same thing
without the `deadline`.

## Discovery

**One seed address is enough.** On connect, and after every reconnect, the
client asks the broker it reached which other brokers a client may use, and adds
them to what it will try.

- The addresses you configured are **never removed**. A wrong or stale answer
  therefore cannot leave the client with fewer ways in than you gave it, which
  is what makes it safe to take the answer at all.
- A broker is offered only when the cluster considers it able to serve *and* it
  has been told where clients reach it (`FELIX_CLIENT_ADVERTISE_ADDR` on the
  broker). One that has not is left out rather than handed over at an address
  that would refuse the connection.
- An empty answer is normal on a single-node deployment. It is not an error.

`ClusterClient::endpoints()` returns everywhere the client would try, seeds
included. `refresh_topology()` asks again on demand; you do not normally need
it, because reconnecting already does.

Still configure more than one seed if you can. Discovery needs *some* broker to
answer before it can tell you about the others.

## Failover

When the broker in use goes away:

- **`publish` reconnects and does not resend.** It returns the error with the
  connection already replaced, so your next call goes somewhere live. Use it
  when a duplicate is worse than a gap, and handle the error.
- **`publish_at_least_once` reconnects and resends.** It can therefore deliver
  the record twice — the broker cannot tell, because only your application holds
  an identity that would make deduplication possible. The name is the contract.

**`idempotent_producer` reconnects, resends, and does not duplicate.** The
producer takes an id from the broker and numbers its batches; a batch re-sent
under the same number is answered from what the leader remembers rather than
appended again, so the ambiguous publish above stops being ambiguous. A batch
for a shard led elsewhere is refused with the leader's address and the
producer follows it. The one case it does not cover is a leader change while
a batch is in flight: the new leader knows no producers and says so, and the
producer ends on that stream with a typed refusal rather than guessing. See
`docs/protocol.md`, "Idempotent producers".

**A single-shard subscription does not survive a failover.** It is bound to the
connection it was created on. Record `Event.offset` as you go and resubscribe
from `offset + 1`; that is what offsets are for, and it is the only way to
resume without a gap.

**A sharded subscription reconnects each shard on its own** — see below.

## Consuming a whole multi-shard stream

A subscription reads **one shard**. A stream's shards can have different owners
and a subscription is bound to one connection, so reading a four-shard stream
means four subscriptions, each following its own redirect.

`ClusterClient::subscribe_sharded` does that for you:

```rust
let client = Arc::new(ClusterClient::connect(&seeds, "localhost", config).await?);
let mut subscription = client
    .subscribe_sharded("t1", "default", "orders", Some(StartPosition::Earliest))
    .await?;

while let Some(item) = subscription.next().await {
    match item {
        ShardEvent::Record { shard, event } => handle(shard, event),
        // Not an error and not silence: this shard is down and being
        // re-established, and the others are still delivering.
        ShardEvent::ShardLost { shard, error } => warn!(shard, %error, "shard down"),
        ShardEvent::ShardRecovered { shard } => info!(shard, "shard back"),
    }
}
```

Four things about it are deliberate, and each is a choice you would otherwise
have to make yourself:

- **Ordering is per shard, and nothing more.** Two records from one shard arrive
  in the order they were written. Two records from different shards arrive in an
  arbitrary order. Merging cannot restore an order that never existed — Felix
  orders per key, and a key always resolves to one shard. Do not infer
  stream-wide ordering from the fact that these arrive on one channel.

- **Resumption is a vector, not a number.** `Event.offset` is per shard, so one
  number cannot say where a sharded consumer got to.
  `ShardedSubscription::positions` hands back one offset per shard; pass it to
  `resubscribe_sharded` to carry on. Each listed shard resumes at `offset + 1`.

- **An unreachable shard refuses the whole subscription.** Opening covers every
  shard or it fails, naming the shards it could not reach. A subscription
  quietly covering three shards of four looks exactly like a complete one to
  everything downstream, which makes it the worst available answer.

- **Losing one shard's owner does not tear down the others.** That shard is
  re-established on its own, resuming after the last offset it delivered, and
  you are told with `ShardLost` and `ShardRecovered` rather than left to infer
  it from a gap.

It needs a broker advertising `FEATURE_STREAM_SHARDS`, because the shard count
comes from asking one. A broker that has never heard of the stream reports zero
shards and the call fails rather than reading shard 0 and calling it the stream.

## A consumer group across every shard

A group is bound to one shard, and only that shard's leader can serve it: the
group's cursor and its claims live there. `ClusterClient::group_sharded` keeps
one group per shard and follows each shard's redirect to its leader:

```rust,no_run
# async fn example(cluster: std::sync::Arc<felix_client::ClusterClient>) -> anyhow::Result<()> {
let group = cluster.group_sharded("t1", "default", "jobs", "workers").await?;
loop {
    for claimed in group.poll(32).await? {
        // `claimed.shard` travels with the record, so the ack goes to the
        // broker that holds that shard's group.
        group.ack(&claimed).await?;
    }
}
# }
```

- **Each poll takes one shard's batch, visiting shards in turn.** Every shard
  is reached and none can starve the rest; an empty answer means no shard had
  anything.
- **Ordering is per shard.** As with `subscribe_sharded`, records from
  different shards arrive in no particular order relative to each other.
- **A shard that cannot be polled fails the poll** rather than being skipped,
  because a skipped shard looks exactly like an empty one.

It needs brokers that answer a group request for a shard they do not lead with
a redirect, which is any broker from this release on.

## Redirects

A subscribe or a consumer-group request sent to a broker that does not own the
shard is answered with a redirect naming the one that does. `ClusterClient::subscribe` follows it — up to
three hops, never revisiting a broker within one attempt, because a cluster
mid-rebalance can otherwise bounce a client between two brokers that disagree.

If you use `Client` directly, the redirect surfaces as a typed
`NotLeaderError` carrying the owner's node id and address. It is an
instruction, not a failure.

A **publish** to the wrong broker is *forwarded* rather than redirected, so it
needs nothing from you.

## Retries

Bounded by an attempt count and a jittered exponential backoff, and optionally
by a deadline.

- **The backoff is full jitter** — a delay uniform in `[0, ceiling]`, with the
  ceiling doubling to `max_backoff`. Every client of a cluster notices a
  failover at the same instant, and an unjittered backoff would send all of them
  at the freshly promoted broker together.
- **`deadline` is `None` by default, deliberately.** A deadline shorter than one
  attempt's own timeout prevents any retry at all: the first attempt spends the
  budget and the loop exits having tried once. Set it from your own latency
  budget, above the client's publish timeout, or leave it off.
- **Only a credential failure is terminal.** A token without the permission
  fails the same way on every broker and returns immediately.

Everything else is retried, **including "not found"**. A broker learns its
streams from the control plane and opens a shard only when it is given one, so a
broker promoted a moment ago reports the stream it is about to serve as missing.
Being named leader and being ready to serve are different moments.

Errors the client has never seen before are also retried: the protocol carries
an error as prose with no code, so classification is string matching, and a
wasted attempt is a cheaper mistake than a lost operation.

## Consistency, from the client's side

Set per stream on the control plane, not on the client.

- **`Leader`** — acknowledged once the owning broker has it durably. Fast, and
  loses whatever it had not yet replicated if that broker's storage is lost. The
  window is the broker's `felix_broker_replication_lag_records`.
- **`Quorum`** — acknowledged once a majority of the replica set holds it. A
  publish that cannot reach a majority is **refused**, with an error saying this
  broker cannot vouch for the write. That is not the same as "it failed": the
  record may be on disk and may yet reach a majority. Treat it as unknown, and
  let `publish_at_least_once` resend if duplicates are acceptable.

**`DeliveryGuarantee` (`AtMostOnce` / `AtLeastOnce`) is declared on a stream and
not enforced by any broker.** Do not rely on it. What you get is what this page
and `semantics.md` describe.

## Errors you should expect

| What you see | What it means | What to do |
| --- | --- | --- |
| `forbidden` | The token lacks the permission | Fix the credential. Not retried |
| `stream not found` | This broker does not know it *yet* | Retried for you |
| `cannot vouch for the write` | `Quorum` could not reach a majority in time | Unknown, not failed. Resend only if duplicates are acceptable |
| `no eligible leader` / owner unavailable | No replica holds the log, so the shard is unavailable rather than served empty | Wait; this resolves or needs an operator |
| `shard is owned by …` (`NotLeaderError`) | You used `Client`, not `ClusterClient` | Follow it, or use `ClusterClient::subscribe` |
| `gave up after … attempts` | Every endpoint failed | The cluster is unreachable, not merely rebalancing |

## Trying it on three brokers

The cluster harness runs a real three-broker cluster with a control plane:

```bash
task cluster:failover
```

It starts three brokers, configures a `Quorum` stream, connects a client with
**one** address, shows it discovering the other two, kills the broker that
acknowledged the writes, and reads every record back from the broker that took
over. `crates/felix-cluster/tests/` uses the same harness, and those tests are
the worked examples for everything on this page.
