-- Caches are placed like streams, because they are the same log underneath.
--
-- Two changes: a cache gains a shard count and a replication factor, and a
-- shard assignment gains the kind of thing it belongs to.

ALTER TABLE caches
    ADD COLUMN IF NOT EXISTS shards INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS replication_factor INTEGER NOT NULL DEFAULT 1;

-- 'stream' is the default so every row written before this reads back as what
-- it was, which is the same reason `ShardKind` deserialises to Stream.
ALTER TABLE shard_assignments
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'stream'
        CONSTRAINT shard_assignments_kind CHECK (kind IN ('stream', 'cache'));

ALTER TABLE shard_assignment_changes
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'stream';

-- The kind joins the key: a cache and a stream may share a name in one
-- namespace, and when they do their shards are unrelated. Without this a cache
-- shard would overwrite the stream shard's ownership row.
ALTER TABLE shard_assignments DROP CONSTRAINT IF EXISTS shard_assignments_pkey;
ALTER TABLE shard_assignments
    ADD PRIMARY KEY (tenant_id, namespace, kind, stream, shard);

-- The old foreign key pointed every row at `streams`, which a cache shard is
-- not in. Referential integrity is kept rather than dropped: each row projects
-- its name into exactly one of two generated columns, and a composite foreign
-- key with a NULL column is not enforced (MATCH SIMPLE), so a row is checked by
-- the one key that applies to it and cascades from the right table.
ALTER TABLE shard_assignments DROP CONSTRAINT IF EXISTS fk_shard_assignments_stream;

ALTER TABLE shard_assignments
    ADD COLUMN IF NOT EXISTS stream_ref TEXT
        GENERATED ALWAYS AS (CASE WHEN kind = 'stream' THEN stream END) STORED,
    ADD COLUMN IF NOT EXISTS cache_ref TEXT
        GENERATED ALWAYS AS (CASE WHEN kind = 'cache' THEN stream END) STORED;

ALTER TABLE shard_assignments
    ADD CONSTRAINT fk_shard_assignments_stream
        FOREIGN KEY (tenant_id, namespace, stream_ref)
        REFERENCES streams(tenant_id, namespace, stream) ON DELETE CASCADE;

ALTER TABLE shard_assignments
    ADD CONSTRAINT fk_shard_assignments_cache
        FOREIGN KEY (tenant_id, namespace, cache_ref)
        REFERENCES caches(tenant_id, namespace, cache) ON DELETE CASCADE;
