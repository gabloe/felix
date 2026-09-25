-- The region a stream's data belongs to. Placement keeps its copies there or
-- in a region bridged from it.
--
-- Nullable with no default: every existing stream was placed without regard to
-- region, and reading one back as belonging to some region would start moving
-- it.
ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS region TEXT;
