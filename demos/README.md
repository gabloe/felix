# Felix Demos

This directory contains runnable demo binaries and example workflows. The demos are intentionally
self-contained and optimized for clarity over production hardening.

## Broker demos

Located in `demos/broker/` and built from the `broker` crate:

- `pubsub-demo-simple`: End-to-end QUIC publish/subscribe flow.
- `cache-demo`: Cache put/get workflow over QUIC.
- `latency-demo`: Latency and throughput measurement harness.
- `pubsub-demo-notifications`: Multi-tenant real-time alerts demo.
- `pubsub-demo-orders`: Orders/payments pipeline demo.

### Run

```bash
cargo run --release -p broker --bin pubsub-demo-simple
cargo run --release -p broker --bin cache-demo
cargo run --release -p broker --bin latency-demo
cargo run --release -p broker --bin pubsub-demo-notifications
cargo run --release -p broker --bin pubsub-demo-orders
```

### Notes

- Demos rely on demo auth helpers and are not intended for production.
- The broker demos spin up an in-process broker instance for convenience.
