# Demo: Orders/Payments Pipeline

## What this shows

- Pub/sub pipeline across multiple streams (`orders` -> `payments` -> `shipments`)
- Cache-backed last-known state (`order_state`)
- Idempotent workers (dedupe by `event_id`)
- Failure injection with worker restart

## Notes

- This demo starts an in-process broker and QUIC server on a random local port.
- You do not need to run the broker separately.

## Architecture (ASCII)

```
+-----------+      +---------+      +-----------+
| Orders    | ---> | Payments| ---> | Shipments |
| Producer  |      | Worker  |      | Worker    |
+-----------+      +----+----+      +-----+-----+
                        |                 |
                        v                 v
                   orders/payments   shipments
                        |                 |
                        +--------+--------+
                                 v
                           +-----+-----+
                           | Cache KV |
                           | order_state |
                           +-----------+
```

## Run

```bash
task demo:orders
# or
cargo run --release -p broker --bin pubsub-demo-orders
```

Optional failure injection:

```bash
cargo run --release -p broker --bin pubsub-demo-orders -- --kill-worker=payments
```

## Configuration flags

- `--orders=12`: total orders published (default: 12)
- `--duplicate-every=5`: inject a duplicate every N orders (default: 5)
- `--kill-worker=payments`: stop and restart the payments worker mid-run

## Expected output (sample)

```
== Felix Demo: Orders/Payments Pipeline ==
Step 4/9: publishing orders (with intentional duplicates).
Injecting duplicate for order-05
Failure injection: stopping payments worker.
Restarting payments worker.
Step 6/9: reading cache snapshots.
order-01 -> shipment_prepared
order-02 -> shipment_prepared
...
Orders processed: 12 (expected 12)
```

## Failure injection

- `--kill-worker=payments`: stops the payments worker mid-run and restarts it.
- The pipeline continues after restart and cache state remains correct.

## How to extend

- Add a refund stream and worker that rewinds state transitions.
- Store an order timeline in cache instead of last-known state.
- Introduce per-tenant pipelines with isolated worker groups.
