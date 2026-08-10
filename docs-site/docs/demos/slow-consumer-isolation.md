# Demo: Slow-consumer Isolation

## What this shows

- One slow consumer does not degrade the healthy ones — the property in Felix's
  one-line description of itself
- The same workload under both subscriber queue policies, side by side
- What each policy actually costs, measured rather than asserted
- Sequence-gap counting, so "lost events" is a number rather than a claim

This is the demo to run first if you want to know why Felix exists rather than
what it can do. Then run its counterpart,
[Local State Divergence](state-divergence.md), which shows what this trade-off
costs a consumer that holds a local copy of state.

## The question it answers

Every pub/sub system has to decide what happens when one subscriber stops keeping
up. There are only two honest answers, and they are both bad in different ways:

- **Drop** for that subscriber. Everyone else is unaffected; the slow one loses data.
- **Block** until it catches up. Nobody loses data; everyone slows to its speed.

Most systems pick one and bury it in a config file. Felix's default is `drop_new`,
and this demo runs both so the trade-off is visible instead of theoretical.

## Notes

- Starts an in-process broker and QUIC server on a random local port. You do not
  need to run a broker separately.
- Every event carries its own sequence number and publish timestamp. The client API
  exposes neither, so this is what makes gaps observable — a consumer otherwise
  cannot tell it missed anything.
- Numbers are single-node over loopback at fanout 3. They say nothing about
  behaviour at thousands of subscribers or across a real network.
- **Lost events are gone.** Felix is at-most-once today: no replay, no redelivery.

## Architecture (ASCII)

```
                    +------------------+
                    |    Publisher     |  paced at --rate
                    +---------+--------+
                              |
                         QUIC (TLS 1.3)
                              v
                    +---------+--------+
                    |      Broker      |
                    |  fanout to all   |
                    +----+----+----+---+
                         |    |    |
          +--------------+    |    +--------------+
          v                   v                   v
    +-----+-----+       +-----+-----+       +-----+------+
    |  dash-1   |       |  dash-2   |       |  dash-3    |
    |  healthy  |       |  healthy  |       |  STALLS    |
    +-----------+       +-----------+       +------------+
                                             stops draining
                                             mid-run
```

## Run

```bash
task demo:slow-consumer
# or
cargo run --release --manifest-path demos/slow-consumer/Cargo.toml
```

A terminal UI renders live. If stdout is not a terminal the demo falls back to
plain text automatically, so piping it or running it in CI works without flags.

## Configuration flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--rate N` | `20000` | Target publish rate, messages/sec |
| `--subscribers N` | `3` | Consumer count; must be at least 2 |
| `--payload N` | `256` | Payload bytes |
| `--queue-capacity N` | `512` | Subscriber queue depth (the broker default) |
| `--duration N` | `5` | Seconds per phase; a run is 4 phases |
| `--policy P` | `both` | `drop_new`, `block`, or `both` |
| `--no-tui` | off | Plain text instead of the terminal UI |

## Expected output (sample)

```
  policy = drop_new
    publisher           20000 msg/s achieved   (target 20000/s)
    dash-1             237701 received           0 lost
    dash-2             237701 received           0 lost
    dash-3             117462 received      120239 lost   <- stalled

  policy = block
    publisher           16419 msg/s achieved   (target 20000/s)
    dash-1             198703 received           0 lost
    dash-2             198703 received           0 lost
    dash-3             198703 received           0 lost   <- stalled
```

Read the two blocks together:

- Under `drop_new` the publisher hits its target and the healthy consumers lose
  nothing. The stalled consumer loses 120k events permanently.
- Under `block` nothing is lost anywhere — and the publisher drops to 16.4k/s while
  the healthy consumers receive 198k instead of 237k. One sick consumer slowed
  everyone, and all three finish in lockstep.

Neither is the correct answer. Quoting
[how Felix works](../development/how-felix-works.md): *"dropping isolates healthy
publishers and subscribers from a slow consumer; blocking preserves delivery but can
let one slow subscriber throttle every producer of that stream."*

## Configuring the trade-off yourself

The policy is not one switch. Backpressure has to propagate through the whole
chain, and each checkpoint has its own knob and its own default:

| Checkpoint | Setting | Default |
| --- | --- | --- |
| Broker ingress queue | `pub_ingress_wait` | `false` — sheds |
| Broker subscriber queue | `subscriber_queue_policy` | `drop_new` |
| Broker writer lane | `subscriber_lane_queue_policy` | `drop_new` |
| Client subscription queue | `client_sub_queue_policy` | `drop_new` |

Leaving any of them on the shedding default means loss happens there first and the
ones downstream never matter — the lossless configuration requires all four. See
[backpressure internals](../development/internals-concurrency.md) for the full set
of six checkpoints and why each exists.

## Failure injection

The stall is the injection: the consumer holds its subscription open and stops
calling `next_event`, which is what a blocked render loop, a paused container, or a
degraded link looks like from the broker's side. It recovers in the final phase so
you can see it resume — and, under `drop_new`, see that the gap in what it received
is permanent.

## How to extend

- Raise `--subscribers` to see whether isolation holds as fanout grows.
- Lower `--queue-capacity` to make shedding start sooner.
- Stall more than one consumer by editing `run_once` in `src/scenario.rs`, which
  currently marks only the last subscriber as the victim.
- Set `--policy drop_new --rate` high enough to trigger ingress shedding, which
  shows up as a small loss shared by *all* consumers — a different checkpoint from
  the one this demo is about.
