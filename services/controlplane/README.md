# controlplane service

Executable entrypoint for the Felix control plane.

Responsibilities:
- Hosts management APIs (future)
- Coordinates metadata and placement
- Runs as a small, stateful control cluster

Local dev:
- `cargo run -p controlplane`
- OpenAPI spec: `http://localhost:8443/v1/openapi.json`
- Swagger UI: `http://localhost:8443/docs`
