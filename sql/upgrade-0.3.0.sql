-- pgkv 0.3.0 -> 0.4.0
--
-- Storage: collections move from one JSONB value per key to one row per element, so an
-- operation costs what it touches rather than the size of the collection. This script
-- rewrites pgkv.store and converts existing collections; it holds an exclusive lock on
-- pgkv.store while it runs.

LOCK TABLE pgkv.store IN ACCESS EXCLUSIVE MODE;

-- Expired keys and leftovers do not need converting.
DELETE FROM pgkv.store WHERE expires_at IS NOT NULL AND expires_at < clock_timestamp();

DROP INDEX IF EXISTS pgkv.idx_pgkv_type;
DROP INDEX IF EXISTS pgkv.idx_pgkv_key_pattern;
ALTER TABLE pgkv.store ALTER COLUMN key TYPE TEXT COLLATE "C";
ALTER TABLE pgkv.store ADD COLUMN id BIGINT, ADD COLUMN card BIGINT, ADD COLUMN head BIGINT;
ALTER TABLE pgkv.store SET (fillfactor = 90);

