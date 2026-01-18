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

Each JSON file describes:
- message type and fields
- raw JSON payload (exact bytes)
- expected framed bytes (hex)

Note: payloads are UTF-8 JSON, and binary values are base64-encoded.

Hex files contain lowercase hex without spaces or newlines.
