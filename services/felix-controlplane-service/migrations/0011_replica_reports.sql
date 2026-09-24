-- What each shard's leader last reported about its replicas.
--
-- In the database rather than in one instance's memory: with several
-- control-plane instances over one Postgres, the instance a report reaches
-- and the instance that later promotes a replica need not be the same
-- process, and a report only one of them had seen was a position no
-- promoter could use.
--
-- One row per shard -- the latest report replaces the previous one -- keyed
-- exactly as the assignment it describes, and cascading from it: a deleted
-- assignment takes its report with it, so a shard that is removed and
-- recreated does not inherit the old one's promotability. Nothing is
-- versioned or logged: a report expires within seconds and the next one
-- replaces it.
CREATE TABLE IF NOT EXISTS replica_reports (
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    kind TEXT NOT NULL,
    stream TEXT NOT NULL,
    shard INTEGER NOT NULL,
    generation BIGINT NOT NULL,
    caught_up JSONB NOT NULL DEFAULT '[]'::jsonb,
    offsets JSONB NOT NULL DEFAULT '{}'::jsonb,
    reported_at_millis BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, namespace, kind, stream, shard),
    CONSTRAINT fk_replica_reports_assignment
        FOREIGN KEY (tenant_id, namespace, kind, stream, shard)
        REFERENCES shard_assignments(tenant_id, namespace, kind, stream, shard)
        ON DELETE CASCADE
);
