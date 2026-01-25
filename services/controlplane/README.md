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

## Storage backends

- **Memory (default):** No external dependencies; state lives in-process. Suitable for local development and CI.
- **Postgres:** Durable metadata with monotonic change sequences. Enable by setting `storage.backend: postgres` or providing `FELIX_CONTROLPLANE_POSTGRES_URL` / `DATABASE_URL`.

Example YAML override (`FELIX_CONTROLPLANE_CONFIG`):
```yaml
bind_addr: "0.0.0.0:8443"
storage:
  backend: postgres
postgres:
  url: "postgres://felix:felix@localhost:5433/felix_controlplane"
  max_connections: 10
  connect_timeout_ms: 5000
  acquire_timeout_ms: 5000
changes_limit: 1000
change_retention_max_rows: 10000
```

Environment variables (take precedence over YAML):
- `FELIX_CONTROLPLANE_POSTGRES_URL` (or `DATABASE_URL`)
- `FELIX_CONTROLPLANE_POSTGRES_MAX_CONNECTIONS`
- `FELIX_CONTROLPLANE_POSTGRES_CONNECT_TIMEOUT_MS`
- `FELIX_CONTROLPLANE_POSTGRES_ACQUIRE_TIMEOUT_MS`
- `FELIX_CONTROLPLANE_STORAGE_BACKEND` (`memory`|`postgres`)
- `FELIX_CONTROLPLANE_CHANGES_LIMIT`
- `FELIX_CONTROLPLANE_CHANGE_RETENTION_MAX_ROWS`

## Local Postgres (docker-compose)

A minimal Postgres service is available at `services/controlplane/docker-compose.yml`:
```bash
cd services/controlplane
docker compose up -d
export FELIX_CONTROLPLANE_POSTGRES_URL=postgres://felix:felix@localhost:5433/felix_controlplane
cargo test -p controlplane --features pg-tests -- --ignored   # runs Postgres-backed tests
```
