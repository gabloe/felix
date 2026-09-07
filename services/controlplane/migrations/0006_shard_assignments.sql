-- Shard ownership: which broker leads each shard of each stream.
--
-- At most one current assignment per (stream, shard) -- that is the primary key,
-- and it is the invariant placement depends on.
CREATE TABLE IF NOT EXISTS shard_assignments (
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    stream TEXT NOT NULL,
    shard INTEGER NOT NULL,
    leader TEXT NOT NULL,
    replicas JSONB NOT NULL DEFAULT '[]'::jsonb,
    generation BIGINT NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, namespace, stream, shard),
    -- Deleting a stream removes its assignments: the shards no longer exist, so
    -- keeping ownership records for them would leave placement chasing ghosts.
    CONSTRAINT fk_shard_assignments_stream
        FOREIGN KEY (tenant_id, namespace, stream)
        REFERENCES streams(tenant_id, namespace, stream) ON DELETE CASCADE
);

-- Placement asks "what does this node own?" on every rebalance and every node
-- failure, so the leader is indexed even though the table is keyed by stream.
CREATE INDEX IF NOT EXISTS idx_shard_assignments_leader ON shard_assignments(leader);
CREATE INDEX IF NOT EXISTS idx_shard_assignments_state ON shard_assignments(state);

CREATE TABLE IF NOT EXISTS shard_assignment_changes (
    seq BIGINT PRIMARY KEY,
    op TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    stream TEXT NOT NULL,
    shard INTEGER NOT NULL,
    payload JSONB,
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_shard_assignment_changes_stream
    ON shard_assignment_changes(tenant_id, namespace, stream);

-- Allocated from a locked row rather than a BIGSERIAL, for the same reason as
-- node_change_seq: a sequence hands out numbers in request order, not commit
-- order, so a snapshot taken between a later commit and an earlier one resumes
-- past a change it never saw. See 0005_nodes.sql.
CREATE TABLE IF NOT EXISTS shard_assignment_change_seq (
    only_row BOOLEAN PRIMARY KEY DEFAULT TRUE
        CONSTRAINT shard_assignment_change_seq_single CHECK (only_row),
    next_seq BIGINT NOT NULL DEFAULT 0
);

INSERT INTO shard_assignment_change_seq (only_row, next_seq) VALUES (TRUE, 0)
    ON CONFLICT (only_row) DO NOTHING;
