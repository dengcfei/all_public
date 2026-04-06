from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from .models import DocumentProcessResult


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
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_documents (
                url TEXT NOT NULL PRIMARY KEY,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                release_time TEXT NOT NULL,
                headline_category_code TEXT NOT NULL,
                headline_category_name TEXT NOT NULL,
                title TEXT NOT NULL,
                file_path TEXT NOT NULL,
                status TEXT NOT NULL,
                downloaded INTEGER NOT NULL DEFAULT 0,
                error_message TEXT NOT NULL DEFAULT '',
                currency TEXT NOT NULL DEFAULT '',
                unit TEXT NOT NULL DEFAULT '',
                revenue TEXT NOT NULL DEFAULT '',
                gross_profit TEXT NOT NULL DEFAULT '',
                operating_profit TEXT NOT NULL DEFAULT '',
                profit_for_period TEXT NOT NULL DEFAULT '',
                profit_attributable TEXT NOT NULL DEFAULT '',
                basic_eps TEXT NOT NULL DEFAULT '',
                diluted_eps TEXT NOT NULL DEFAULT '',
                page_count INTEGER NOT NULL DEFAULT 0,
                text_length INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
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

    def has_processed_document(self, url: str, file_path: Path | None = None) -> bool:
        row = self.conn.execute(
            "SELECT status, file_path FROM processed_documents WHERE url = ? LIMIT 1",
            (url,),
        ).fetchone()
        if row is None or row[0] != "done":
            return False

        known_path = Path(row[1]) if row[1] else file_path
        if known_path is None:
            return True
        return known_path.exists() and known_path.stat().st_size > 0

    def record_processed_document(self, result: DocumentProcessResult) -> None:
        extraction = result.extraction
        self.conn.execute(
            """
            INSERT INTO processed_documents (
                url,
                stock_code,
                stock_name,
                release_time,
                headline_category_code,
                headline_category_name,
                title,
                file_path,
                status,
                downloaded,
                error_message,
                currency,
                unit,
                revenue,
                gross_profit,
                operating_profit,
                profit_for_period,
                profit_attributable,
                basic_eps,
                diluted_eps,
                page_count,
                text_length,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(url) DO UPDATE SET
                stock_code = excluded.stock_code,
                stock_name = excluded.stock_name,
                release_time = excluded.release_time,
                headline_category_code = excluded.headline_category_code,
                headline_category_name = excluded.headline_category_name,
                title = excluded.title,
                file_path = excluded.file_path,
                status = excluded.status,
                downloaded = excluded.downloaded,
                error_message = excluded.error_message,
                currency = excluded.currency,
                unit = excluded.unit,
                revenue = excluded.revenue,
                gross_profit = excluded.gross_profit,
                operating_profit = excluded.operating_profit,
                profit_for_period = excluded.profit_for_period,
                profit_attributable = excluded.profit_attributable,
                basic_eps = excluded.basic_eps,
                diluted_eps = excluded.diluted_eps,
                page_count = excluded.page_count,
                text_length = excluded.text_length,
                updated_at = datetime('now')
            """,
            (
                result.url,
                result.stock_code,
                result.stock_name,
                result.release_time,
                result.headline_category_code,
                result.headline_category_name,
                result.title,
                result.file_path,
                result.status,
                1 if result.downloaded else 0,
                result.error_message,
                extraction.currency if extraction else "",
                extraction.unit if extraction else "",
                extraction.revenue if extraction else "",
                extraction.gross_profit if extraction else "",
                extraction.operating_profit if extraction else "",
                extraction.profit_for_period if extraction else "",
                extraction.profit_attributable if extraction else "",
                extraction.basic_eps if extraction else "",
                extraction.diluted_eps if extraction else "",
                extraction.page_count if extraction else 0,
                extraction.text_length if extraction else 0,
            ),
        )
        self.conn.commit()

    def export_processed_documents_csv(self, csv_path: Path) -> None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.conn.execute(
            """
            SELECT
                stock_code,
                stock_name,
                release_time,
                headline_category_name,
                title,
                url,
                file_path,
                status,
                downloaded,
                currency,
                unit,
                revenue,
                gross_profit,
                operating_profit,
                profit_for_period,
                profit_attributable,
                basic_eps,
                diluted_eps,
                error_message,
                updated_at
            FROM processed_documents
            ORDER BY stock_code, release_time DESC, headline_category_name, url
            """
        ).fetchall()
        headers = [
            "stock_code",
            "stock_name",
            "release_time",
            "headline_category_name",
            "title",
            "url",
            "file_path",
            "status",
            "downloaded",
            "currency",
            "unit",
            "revenue",
            "gross_profit",
            "operating_profit",
            "profit_for_period",
            "profit_attributable",
            "basic_eps",
            "diluted_eps",
            "error_message",
            "updated_at",
        ]
        with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            writer.writerows(rows)

    def close(self) -> None:
        self.conn.close()
