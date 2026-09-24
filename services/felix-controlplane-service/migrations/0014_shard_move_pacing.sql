-- What placement needs to pace moves.
--
-- `joining` is a follower being copied in to replace one on a draining node.
-- It is named so the copy counts against the move limit until it has caught
-- up, like a staged successor does.
--
-- `move_started_at_millis` is the store's clock when the move or replacement
-- in progress started, for the move timeout. Kept after a move times out, so
-- that shard waits behind the others for its next slot.
--
-- `leader_offset` is the reporting leader's own tail, so placement can fence
-- a move once the destination is within a lag bound rather than exactly
-- level, which under steady writes it may never be.
ALTER TABLE shard_assignments ADD COLUMN IF NOT EXISTS joining TEXT NULL;
ALTER TABLE shard_assignments ADD COLUMN IF NOT EXISTS move_started_at_millis BIGINT NULL;
ALTER TABLE replica_reports ADD COLUMN IF NOT EXISTS leader_offset BIGINT NULL;
