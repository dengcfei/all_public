from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg2

from .config import DBConfig


@contextmanager
def get_connection(config: DBConfig) -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
        sslmode=config.sslmode,
        connect_timeout=12,
    )
    try:
        yield conn
    finally:
        conn.close()
