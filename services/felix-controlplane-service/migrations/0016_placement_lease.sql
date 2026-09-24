-- Which instance runs placement, and the token placement writes are fenced
-- with.
--
-- One row. `holder` runs placement's timed passes until `expires_at`, and
-- renews on every pass. `token` advances on every placement write and every
-- change of holder; a write lands only at the token its writer read before
-- deciding, so two instances cannot both take the last move slot, and an
-- instance that paused past its lease cannot write after another took over.
CREATE TABLE IF NOT EXISTS placement_lease (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    holder TEXT NULL,
    expires_at TIMESTAMPTZ NULL,
    token BIGINT NOT NULL DEFAULT 0
);
INSERT INTO placement_lease (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
