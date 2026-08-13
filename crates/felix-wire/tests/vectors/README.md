# Felix Wire Test Vectors

These vectors are the source of truth for cross-language encoding/decoding.
Every client implementation should validate against them.

Files:
- `publish.json` / `publish.hex`
- `subscribe.json` / `subscribe.hex`
- `event.json` / `event.hex`
- `cache_put.json` / `cache_put.hex`
- `cache_get.json` / `cache_get.hex`
- `cache_value.json` / `cache_value.hex`
- `ok.json` / `ok.hex`
- `error.json` / `error.hex`
- `binary_publish_acked.json` / `.hex` (binary, flags `0x0009`)
- `binary_publish_ack_ok.json` / `.hex` (binary, flags `0x0010`)
- `binary_publish_ack_error.json` / `.hex` (binary, flags `0x0010`)

Each JSON file describes:
- message type and fields
- raw payload (exact bytes)
- expected framed bytes (hex)

Vectors carry an optional `kind` field selecting how they are validated:
- absent or `json_message` — a JSON `Message` payload with `flags` = 0
- `binary_acked_publish_batch` — the binary acked publish encoding
- `binary_publish_ack` — the binary publish acknowledgement

Binary vectors also carry `flags`, plus the decoded field values so a client can
check its decoder as well as its encoder.

Note: payloads are UTF-8 JSON, and binary values are base64-encoded.

Hex files contain lowercase hex without spaces or newlines.
