from __future__ import annotations

import json
from typing import Any


def scalar(conn, sql: str, params: tuple[Any, ...] = ()) -> Any:
    with conn.execute(sql, params) as cur:
        return cur.fetchone()[0]


def rows(conn, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with conn.execute(sql, params) as cur:
        return cur.fetchall()


def j(value: Any) -> str:
    return json.dumps(value)
