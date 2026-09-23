-- A cache can ask for Quorum acknowledgement, the way a stream can. 'Leader'
-- is the default so every cache written before this reads back as what it was.
ALTER TABLE caches
    ADD COLUMN IF NOT EXISTS consistency TEXT NOT NULL DEFAULT 'Leader';
