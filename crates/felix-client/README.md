# felix-client

Client-side APIs for interacting with Felix.

Responsibilities:
- In-process client wrapper (current)
- Future networked client SDK
- Publish/subscribe convenience APIs

## Performance defaults (balanced)

These are the built-in defaults used when environment variables are not set:
- `FELIX_EVENT_CONN_POOL=8`
- `FELIX_EVENT_CONN_RECV_WINDOW=268435456` (256 MiB)
- `FELIX_EVENT_STREAM_RECV_WINDOW=67108864` (64 MiB)
- `FELIX_EVENT_SEND_WINDOW=268435456` (256 MiB)
- `FELIX_PUBLISH_CHUNK_BYTES=16384`
