-- Create dedicated schema for namespace isolation
CREATE SCHEMA IF NOT EXISTS pgkv;

-- One row per key. Strings and JSON documents keep their value here; a collection (hash,
-- list, set, sorted set) keeps one row per element in a side table, found through `id`.
--   card: number of elements of a collection
--   head: position of a list's first element in pgkv.list_items
-- key is COLLATE "C", so the primary key index orders keys by byte and serves prefix scans.
CREATE TABLE pgkv.store (
    key TEXT COLLATE "C" PRIMARY KEY,
    value JSONB,
    type TEXT NOT NULL DEFAULT 'string',
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    id BIGINT,
    card BIGINT,
    head BIGINT,
    CONSTRAINT valid_type CHECK (type IN ('string', 'list', 'set', 'hash', 'zset', 'json'))
) WITH (fillfactor = 90);

-- Index for TTL cleanup
CREATE INDEX idx_pgkv_expires_at ON pgkv.store(expires_at)
    WHERE expires_at IS NOT NULL;

