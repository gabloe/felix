-- Canonical tables
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS namespaces (
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, namespace),
    CONSTRAINT fk_namespaces_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS streams (
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    stream TEXT NOT NULL,
    kind TEXT NOT NULL,
    shards INTEGER NOT NULL,
    retention_max_age_seconds BIGINT NULL,
    retention_max_size_bytes BIGINT NULL,
    consistency TEXT NOT NULL,
    delivery TEXT NOT NULL,
    durable BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, namespace, stream),
    CONSTRAINT fk_streams_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    CONSTRAINT fk_streams_namespace FOREIGN KEY (tenant_id, namespace) REFERENCES namespaces(tenant_id, namespace) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS caches (
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    cache TEXT NOT NULL,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, namespace, cache),
    CONSTRAINT fk_caches_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    CONSTRAINT fk_caches_namespace FOREIGN KEY (tenant_id, namespace) REFERENCES namespaces(tenant_id, namespace) ON DELETE CASCADE
);

-- Change tables
CREATE TABLE IF NOT EXISTS tenant_changes (
    seq BIGSERIAL PRIMARY KEY,
    op TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    payload JSONB,
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS namespace_changes (
    seq BIGSERIAL PRIMARY KEY,
    op TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    payload JSONB,
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stream_changes (
    seq BIGSERIAL PRIMARY KEY,
    op TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    stream TEXT NOT NULL,
    payload JSONB,
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cache_changes (
    seq BIGSERIAL PRIMARY KEY,
    op TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    cache TEXT NOT NULL,
    payload JSONB,
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_namespace_changes_ns ON namespace_changes(tenant_id, namespace);
CREATE INDEX IF NOT EXISTS idx_stream_changes_ns ON stream_changes(tenant_id, namespace);
CREATE INDEX IF NOT EXISTS idx_cache_changes_ns ON cache_changes(tenant_id, namespace);
