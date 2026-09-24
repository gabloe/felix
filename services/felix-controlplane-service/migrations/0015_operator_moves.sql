-- What an operator needs to steer shard moves.
--
-- `move_reason` is why the move or replacement in progress started: `drain`,
-- `balance`, `operator` or `replace`. Cleared when it ends.
--
-- `placement_settings` holds cluster-wide switches every instance's placement
-- reads each pass, in one row. `moves_paused` stops placement starting moves
-- of its own; moves already in flight finish, and an operator may still
-- start one.
ALTER TABLE shard_assignments ADD COLUMN IF NOT EXISTS move_reason TEXT NULL;

CREATE TABLE IF NOT EXISTS placement_settings (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    moves_paused BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO placement_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
