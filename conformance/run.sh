#!/usr/bin/env bash
# Run the conformance suite against throwaway PostgreSQL and Redis containers.
#   ./conformance/run.sh                 scenarios, fuzz corpus, and MULTI/EXEC replay
#   ./conformance/run.sh --fuzz 7 2000   anything after the script name goes to run.py
# Set PGKV_ORACLE_IMAGE=valkey/valkey:9 to use Valkey as the oracle.
set -euo pipefail
cd "$(dirname "$0")/.."

PG=${PGKV_CONFORMANCE_PG_CONTAINER:-pgkv-conformance-pg}
ORACLE=${PGKV_CONFORMANCE_ORACLE_CONTAINER:-pgkv-conformance-oracle}
PG_PORT=${PGKV_CONFORMANCE_PG_PORT:-55442}
ORACLE_PORT=${PGKV_CONFORMANCE_ORACLE_PORT:-55443}
PG_IMAGE=${PGKV_PG_IMAGE:-postgres:17}
ORACLE_IMAGE=${PGKV_ORACLE_IMAGE:-redis:8}
PYTHON=${PYTHON:-python3}

cleanup() {
  if [[ "${PGKV_KEEP_CONTAINER:-0}" != "1" ]]; then
    docker rm -f "$PG" "$ORACLE" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

docker rm -f "$PG" "$ORACLE" >/dev/null 2>&1 || true
docker run -d --name "$PG" -e POSTGRES_PASSWORD=postgres -p "127.0.0.1:${PG_PORT}:5432" "$PG_IMAGE" >/dev/null
docker run -d --name "$ORACLE" -p "127.0.0.1:${ORACLE_PORT}:6379" "$ORACLE_IMAGE" >/dev/null
until docker exec "$PG" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; do sleep 0.25; done

./build.sh >/dev/null
docker cp pgkv.control "$PG:/tmp/pgkv.control"
for sql in pgkv--*.sql; do
  docker cp "$sql" "$PG:/tmp/$sql"
done
docker exec "$PG" bash -lc 'cp /tmp/pgkv.control /usr/share/postgresql/*/extension/ && cp /tmp/pgkv--*.sql /usr/share/postgresql/*/extension/'
docker exec "$PG" psql -h localhost -U postgres -v ON_ERROR_STOP=1 -q -c 'CREATE EXTENSION pgkv;'

if command -v uv >/dev/null 2>&1; then
  uv venv -q .venv-conformance
else
  "$PYTHON" -m venv .venv-conformance
fi
. .venv-conformance/bin/activate
if command -v uv >/dev/null 2>&1; then
  uv pip install -q -r conformance/requirements.txt
else
  pip install -q -r conformance/requirements.txt
fi

export PGKV_DSN="postgresql://postgres:postgres@127.0.0.1:${PG_PORT}/postgres"
export PGKV_ORACLE_URL="redis://127.0.0.1:${ORACLE_PORT}/0"
if [[ $# -gt 0 ]]; then
  python conformance/run.py "$@"
else
  python conformance/run.py --all
  python conformance/run.py --pipeline
fi
