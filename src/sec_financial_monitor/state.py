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
            CREATE TABLE IF NOT EXISTS seen_filings (
                filing_id TEXT NOT NULL,
                cik TEXT NOT NULL,
                ticker TEXT NOT NULL,
                company_name TEXT NOT NULL,
                form_type TEXT NOT NULL,
                filing_date TEXT NOT NULL,
                report_date TEXT NOT NULL,
                url TEXT NOT NULL,
                file_path TEXT NOT NULL,
                seen_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (filing_id, cik)
            )
            """
        )
        self.conn.commit()

    def has_seen(self, filing_id: str, cik: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM seen_filings WHERE filing_id = ? AND cik = ? LIMIT 1",
            (filing_id, cik),
        ).fetchone()
        return row is not None

    def mark_seen(
        self,
        filing_id: str,
        cik: str,
        ticker: str,
        company_name: str,
        form_type: str,
        filing_date: str,
        report_date: str,
        url: str,
        file_path: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO seen_filings (
                filing_id,
                cik,
                ticker,
                company_name,
                form_type,
                filing_date,
                report_date,
                url,
                file_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                filing_id,
                cik,
                ticker,
                company_name,
                form_type,
                filing_date,
                report_date,
                url,
                file_path,
            ),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
