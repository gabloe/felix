-- Where a Kafka client connects to this broker, when it runs the Kafka listener.
--
-- Nullable with no default: most brokers do not run the listener, and one
-- registered before this column existed has nothing to backfill. A broker
-- without it is simply never handed to a Kafka client.
ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS kafka_addr TEXT;
