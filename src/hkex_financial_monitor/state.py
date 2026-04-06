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
            CREATE TABLE IF NOT EXISTS seen_announcements (
                news_id INTEGER NOT NULL,
                stock_code TEXT NOT NULL,
                seen_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (news_id, stock_code)
            )
            """
        )
        self.conn.commit()

    def has_seen(self, news_id: int, stock_code: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM seen_announcements WHERE news_id = ? AND stock_code = ? LIMIT 1",
            (news_id, stock_code),
        ).fetchone()
        return row is not None

    def mark_seen(self, news_id: int, stock_code: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO seen_announcements (news_id, stock_code) VALUES (?, ?)",
            (news_id, stock_code),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
