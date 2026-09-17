# felix-client for Python

Python bindings for Felix, built as a wrapper over the Rust client rather than
a reimplementation of the protocol.

That distinction is the point. Reconnection, redirect-following, retry
classification and offset bookkeeping are hard to get right and expensive to
get wrong — a second implementation is a second set of subtle bugs in exactly
the places that matter. Here they exist once, in `felix-client`, and every
language binds to them.

## Two surfaces

Synchronous, for threads and `asyncio.to_thread`:

```python
import felix

with felix.Client("127.0.0.1:5000", tenant_id="t1", token=tok, ca_file=ca) as client:
    client.publish("t1", "default", "events", b"hello")
    with client.subscribe("t1", "default", "events") as events:
        for event in events:
            print(event.payload, event.offset)
```

Asynchronous, which is what most Python realtime backends want:

```python
client = await felix.AsyncClient.connect(addr, tenant_id="t1", token=tok, ca_file=ca)
async with client:
    await client.publish("t1", "default", "events", b"hello")
    async with await client.subscribe("t1", "default", "events") as events:
        async for event in events:
            print(event.payload, event.offset)
```

Both wrap the same client and fail over identically. The sync one blocks with
the GIL released; the async one yields to your event loop.

## TLS

QUIC has no unencrypted mode, so there is always a trust decision — and
deliberately no "skip verification" switch, because that is the setting that
silently turns a secure deployment insecure. Either the platform trust store
(the default) or an explicit CA:

```python
felix.Client(addr, tenant_id=..., token=..., ca_file="/path/to/ca.pem")
```

A development broker generates a self-signed certificate at startup; set
`FELIX_TLS_CERT_EXPORT=/path/ca.pem` on the broker and point `ca_file` at it.

## Building

```bash
pip install maturin
task python:develop      # build and install into the active virtualenv
task python:check        # fmt, clippy, build
```

This crate is deliberately outside the Cargo workspace — an extension module
carries `pyo3/extension-module`, which cannot be linked into a test binary —
so `task python:check` is what builds it, not `task test`.

## Conformance

The semantics a Felix client must implement are catalogued in
`crates/felix-conformance/scenarios.toml`, with `task conformance:scenarios`
to read them and `task conformance:verify` to check a client's results. That
catalogue — not this README — is the contract, and it is what the next
language's client is gated on.
