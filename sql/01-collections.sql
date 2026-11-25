-- Collection elements, one row each, keyed by the owning key's pgkv.store.id.
CREATE SEQUENCE pgkv.collection_id;

CREATE TABLE pgkv.hash_fields (
    id BIGINT NOT NULL,
    field TEXT COLLATE "C" NOT NULL,
    value JSONB NOT NULL,
    PRIMARY KEY (id, field)
);

-- Positions are contiguous: head .. head + card - 1.
CREATE TABLE pgkv.list_items (
    id BIGINT NOT NULL,
    pos BIGINT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (id, pos)
);

CREATE TABLE pgkv.set_members (
    id BIGINT NOT NULL,
    member TEXT COLLATE "C" NOT NULL,
    PRIMARY KEY (id, member)
);

CREATE TABLE pgkv.zset_members (
    id BIGINT NOT NULL,
    member TEXT COLLATE "C" NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (id, member)
);

-- Score order, ties in byte order of the member: the order of ZRANGE.
CREATE INDEX idx_pgkv_zset_score ON pgkv.zset_members (id, score, member);

-- Data lives in these tables, so pg_dump must include their contents. Registering them is
-- only possible (and only needed) when this runs as an extension script; a copy applied as
-- plain SQL is dumped like any other table.
DO $$
BEGIN
    PERFORM pg_catalog.pg_extension_config_dump('pgkv.store', '');
    PERFORM pg_catalog.pg_extension_config_dump('pgkv.hash_fields', '');
    PERFORM pg_catalog.pg_extension_config_dump('pgkv.list_items', '');
    PERFORM pg_catalog.pg_extension_config_dump('pgkv.set_members', '');
    PERFORM pg_catalog.pg_extension_config_dump('pgkv.zset_members', '');
    PERFORM pg_catalog.pg_extension_config_dump('pgkv.collection_id', '');
EXCEPTION WHEN feature_not_supported THEN
    NULL;
END;
$$;

