/**
 * Integration tests need a PostgreSQL database with the pgkv extension and, for the
 * differential test, a real Redis to compare against:
 *
 *   PGKV_TEST_DSN=postgres://postgres:postgres@127.0.0.1:55811/client_ts
 *   PGKV_TEST_REDIS_URL=redis://127.0.0.1:55812/6
 *
 * The tests FLUSHDB both (never FLUSHALL on Redis), so point them at databases of their own.
 */
export const DSN = process.env.PGKV_TEST_DSN ?? "";
export const REDIS_URL = process.env.PGKV_TEST_REDIS_URL ?? "";

if (!DSN) console.warn("PGKV_TEST_DSN is not set: skipping the tests that need PostgreSQL with pgkv");
if (!REDIS_URL) console.warn("PGKV_TEST_REDIS_URL is not set: skipping the differential test against Redis");
