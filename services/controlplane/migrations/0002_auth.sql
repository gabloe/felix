CREATE TABLE IF NOT EXISTS idp_issuers (
    tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    issuer TEXT NOT NULL,
    audiences JSONB NOT NULL,
    discovery_url TEXT,
    jwks_url TEXT,
    subject_claim TEXT NOT NULL DEFAULT 'sub',
    groups_claim TEXT,
    PRIMARY KEY (tenant_id, issuer)
);

CREATE TABLE IF NOT EXISTS tenant_signing_keys (
    tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    kid TEXT NOT NULL,
    alg TEXT NOT NULL,
    private_pem TEXT NOT NULL,
    public_pem TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, kid, status)
);

CREATE TABLE IF NOT EXISTS rbac_policies (
    tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    object TEXT NOT NULL,
    action TEXT NOT NULL,
    PRIMARY KEY (tenant_id, subject, object, action)
);

CREATE TABLE IF NOT EXISTS rbac_groupings (
    tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    role TEXT NOT NULL,
    PRIMARY KEY (tenant_id, user_id, role)
);
