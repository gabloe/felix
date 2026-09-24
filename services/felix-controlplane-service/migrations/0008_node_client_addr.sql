-- Where a client connects, as opposed to where other brokers connect.
--
-- Nullable with no default: `advertise_addr` is the broker-internal listener
-- and no application may use it, so there is nothing to backfill from. A broker
-- registered before this column existed simply does not offer clients a way in,
-- and is left out of what discovery reports rather than being reported at an
-- address that would refuse them.
ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS client_addr TEXT;
