-- A planned shard move, resumable from the store by any instance.
--
-- `successor` is the node a shard is moving to, staged as a replica first so
-- the log is on it before it leads. Cleared by the assignment that makes it
-- leader.
--
-- `drained` is the leader's word that it has stopped serving at the reported
-- generation and its log will not grow. It rides the replica report because
-- the leader already sends one every pass, and the cut-over reads the same
-- row for who is caught up.
ALTER TABLE shard_assignments ADD COLUMN IF NOT EXISTS successor TEXT NULL;
ALTER TABLE replica_reports ADD COLUMN IF NOT EXISTS drained BOOLEAN NOT NULL DEFAULT false;
