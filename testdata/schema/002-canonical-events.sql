-- Vendored from github.com/toudai-dev/toudai at 924da955496398446bc230a1585853e424ce53ba
-- (PR #18), clickhouse/schema/002-canonical-events.sql. Identical to the source except
-- that the former product name in identifiers is the Toudai name. Keep the exporter's
-- INSERT columns in sync with it.
-- Decision 0001 D3: the canonical payload table. It lives on the S3-backed
-- `toudai_payload` storage policy (clickhouse/config.d/toudai-storage.xml):
-- new parts land on the local `cache` volume and the TTL moves a partition to the
-- `object` volume once it is older than the window below. The window is a
-- placeholder for the ten-day retained-scale load to size against the cache
-- volume; it is not a retention limit (the TTL only moves data, it never deletes).
--
-- Lookup is by (tenant_id, dataset, event_id), so the sort key leads with those
-- columns instead of the serving sort key: a payload fetch should find its row
-- by primary-key search without knowing the event time. Daily partitions by event
-- time are what the TTL and tiering act on.
--
-- `tenant_id` holds the globally unique workspace ID, never an account ID.
-- Identifiers stay String (D2).
CREATE TABLE IF NOT EXISTS toudai.canonical_events
(
    tenant_id String,
    dataset LowCardinality(String),
    signal LowCardinality(String),
    event_id String,
    timestamp DateTime64(9),
    schema_version LowCardinality(String),
    payload String,
    payload_sha256 String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, event_id)
TTL toDateTime(timestamp) + INTERVAL 2 DAY TO VOLUME 'object'
SETTINGS storage_policy = 'toudai_payload';
