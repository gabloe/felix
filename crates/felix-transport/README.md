# felix-transport

QUIC transport primitives shared by [Felix](https://github.com/gabloe/felix)
clients and brokers: endpoint construction, TLS defaults, and the datagram
sizing that decides whether a connection is fast or stalled.

## What is not obvious here

Linux's UDP GSO batches segments into one IP datagram, so `MTU × segments` must
stay under 65,535. quinn batches up to 10, which puts the ceiling at 6,553
bytes — and a kernel that refuses an oversized batch returns `EMSGSIZE`, which
is not recognised as a GSO failure and stalls delivery rather than degrading it.
MTU discovery is therefore bounded below that ceiling by default.

Receive buffers are sized from the socket's real capacity rather than the
requested one, because a host that silently clamps `SO_RCVBUF` would otherwise
be configured for a window it cannot hold.

## Documentation

- [QUIC transport](https://gabloe.github.io/felix/features/quic-transport/)
- [Environment variables](https://gabloe.github.io/felix/reference/environment-variables/)

Apache-2.0.
