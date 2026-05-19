#!/usr/bin/env bash
set -euo pipefail

CONTAINER=${PGKV_STRESS_CONTAINER:-pgkv-stress}
PORT=${PGKV_STRESS_PORT:-55432}
IMAGE=${PGKV_STRESS_IMAGE:-postgres:16}
PYTHON=${PYTHON:-python3}

cleanup() {
  if [[ "${PGKV_KEEP_CONTAINER:-0}" != "1" ]]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if docker ps -a --format '{{.Names}}' | rg -q "^${CONTAINER}$"; then
  docker rm -f "$CONTAINER" >/dev/null
fi

docker run -d --name "$CONTAINER" -e POSTGRES_PASSWORD=postgres -p "${PORT}:5432" "$IMAGE" >/dev/null
until docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; do sleep 0.25; done

docker cp pgkv.control "$CONTAINER:/tmp/pgkv.control"
docker cp pgkv--0.1.0.sql "$CONTAINER:/tmp/pgkv--0.1.0.sql"
docker exec "$CONTAINER" bash -lc 'cp /tmp/pgkv.control /usr/share/postgresql/*/extension/ && cp /tmp/pgkv--0.1.0.sql /usr/share/postgresql/*/extension/'
docker exec "$CONTAINER" psql -h localhost -U postgres -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION pgkv;'

if command -v uv >/dev/null 2>&1; then
  uv venv -q .venv-stress
else
  "$PYTHON" -m venv .venv-stress
fi
. .venv-stress/bin/activate
if command -v uv >/dev/null 2>&1; then
  uv pip install -q -r stress/requirements.txt
else
  pip install -q -r stress/requirements.txt
fi
export PGKV_DSN="postgresql://postgres:postgres@localhost:${PORT}/postgres"
pytest -q stress "$@"
