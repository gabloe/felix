-- How many brokers hold a copy of each shard, leader included.
--
-- Defaulted to 1 rather than made NOT NULL without one: every stream that
-- existed before replication was leader-only, and reading them back as anything
-- else would claim copies that do not exist.
ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS replication_factor INTEGER NOT NULL DEFAULT 1;
