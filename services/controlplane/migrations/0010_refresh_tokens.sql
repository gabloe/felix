-- Refresh tokens.
--
-- The secret is not here, only its SHA-256. A read of this table yields
-- nothing a caller could present.
--
-- Rows outlive their usefulness on purpose: a spent or revoked token is kept
-- until it has expired, because refusing a replay is what makes single-use
-- rotation detect theft, and a deleted row would be indistinguishable from a
-- token that never existed. `purge_expired_refresh_tokens` clears them after
-- expiry, when nobody can present them any more.
CREATE TABLE IF NOT EXISTS refresh_tokens (
    tenant_id       TEXT        NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    token_id        TEXT        NOT NULL,
    principal_id    TEXT        NOT NULL,
    -- The IdP group claims presented at exchange. RBAC is re-evaluated against
    -- these on every refresh, so they are claims to re-check rather than
    -- permissions to reuse.
    groups          JSONB       NOT NULL DEFAULT '[]'::jsonb,
    secret_hash     TEXT        NOT NULL,
    -- The rotation chain. A replay revokes every row sharing it.
    family_id       TEXT        NOT NULL,
    issued_at_secs  BIGINT      NOT NULL,
    expires_at_secs BIGINT      NOT NULL,
    used            BOOLEAN     NOT NULL DEFAULT FALSE,
    revoked         BOOLEAN     NOT NULL DEFAULT FALSE,
    PRIMARY KEY (tenant_id, token_id)
);

-- Revoking a family is the response to a replay, so it happens while a caller
-- waits on a refresh that is already going to be refused.
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_family
    ON refresh_tokens (tenant_id, family_id);

-- Revoking everything a principal holds is the operator's cut-off.
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_principal
    ON refresh_tokens (tenant_id, principal_id);

-- The purge sweeps by expiry across every tenant.
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_expiry
    ON refresh_tokens (expires_at_secs);
