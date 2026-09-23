-- Broker membership: a node is a broker process in the cluster.
--
-- Spec columns are operator-declared; status columns are heartbeat-derived.
-- Both live in one row because they describe one node, but only the store
-- writes the status ones.
CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    advertise_addr TEXT NOT NULL,
    region TEXT NOT NULL,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    capacity_max_shards INTEGER NULL,
    capacity_weight INTEGER NOT NULL DEFAULT 1,
    lifecycle TEXT NOT NULL,
    last_heartbeat_at_millis BIGINT NOT NULL,
    registered_at_millis BIGINT NOT NULL,
    incarnation BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Two nodes advertising one address cannot both be reached, so the second is
-- rejected rather than silently shadowing the first.
CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_advertise_addr ON nodes(advertise_addr);

-- Placement filters by region, and by which nodes are eligible.
CREATE INDEX IF NOT EXISTS idx_nodes_region_lifecycle ON nodes(region, lifecycle);

-- Liveness expiry sweeps live nodes ordered by staleness, so the partial index
-- is over exactly the rows that sweep can act on.
CREATE INDEX IF NOT EXISTS idx_nodes_heartbeat_expiry
    ON nodes(last_heartbeat_at_millis)
    WHERE lifecycle IN ('live', 'draining');

CREATE TABLE IF NOT EXISTS node_changes (
    seq BIGINT PRIMARY KEY,
    op TEXT NOT NULL,
    node_id TEXT NOT NULL,
    payload JSONB,
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_node_changes_node ON node_changes(node_id);

-- Why this exists instead of a BIGSERIAL, as the other change tables use:
--
-- A sequence hands out numbers in request order, not commit order. Two writers
-- can take seq 4 and 5, and 5 can commit first. A snapshot taken in between
-- reads MAX(seq)+1 = 6, so the consumer resumes at 6 and never sees 4 -- a
-- committed change silently missed.
--
-- Allocating from a single locked row instead makes the second writer wait for
-- the first to commit, so seq order is commit order and `>= next_seq` cannot
-- skip anything. Membership changes are rare enough for that to cost nothing.
CREATE TABLE IF NOT EXISTS node_change_seq (
    only_row BOOLEAN PRIMARY KEY DEFAULT TRUE CONSTRAINT node_change_seq_single CHECK (only_row),
    next_seq BIGINT NOT NULL DEFAULT 0
);

INSERT INTO node_change_seq (only_row, next_seq) VALUES (TRUE, 0)
    ON CONFLICT (only_row) DO NOTHING;
