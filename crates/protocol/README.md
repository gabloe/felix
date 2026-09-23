# Protocol

How peers talk. Both crates are Apache-2.0 so that anyone can build a client or
a server without taking on a copyleft dependency.

- [`felix-wire`](felix-wire) is the frame codec: the frame header, the
  `Message` enum, the binary batch formats, and the broker-to-broker messages.
  It does no I/O. `docs/protocol.md` is the specification it implements.
- [`felix-transport`](felix-transport) is the QUIC layer: endpoints,
  connection and stream lifetime, and the transport tuning shared by the client
  and the broker. TLS configuration comes from the caller. It knows nothing
  about Felix messages.

Neither depends on any other crate in this repository.
