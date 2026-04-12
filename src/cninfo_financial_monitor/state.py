from __future__ import annotations

import sqlite3
from pathlib import Path


class SeenState:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_reports (
                announcement_id TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                title TEXT NOT NULL,
                release_time TEXT NOT NULL,
                url TEXT NOT NULL,
                file_path TEXT NOT NULL,
                seen_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (announcement_id, stock_code)
            )
            """
        )
        self.conn.commit()

    def has_seen(self, announcement_id: str, stock_code: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM seen_reports WHERE announcement_id = ? AND stock_code = ? LIMIT 1",
            (announcement_id, stock_code),
        ).fetchone()
        return row is not None

    def mark_seen(
        self,
        announcement_id: str,
        stock_code: str,
        title: str,
        release_time: str,
        url: str,
        file_path: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO seen_reports (
                announcement_id,
                stock_code,
                title,
                release_time,
                url,
                file_path
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (announcement_id, stock_code, title, release_time, url, file_path),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
