from __future__ import annotations

from datetime import date
from typing import Any

from psycopg2.extras import RealDictCursor, execute_values

from .config import DBConfig
from .db import get_connection


class ReferentialRepository:
    def __init__(self, config: DBConfig) -> None:
        self.config = config

    def healthcheck(self) -> dict[str, Any]:
        with get_connection(self.config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT now() AS now_utc, current_database() AS db")
                row = cur.fetchone() or {}
                return dict(row)

    def lookup(self, query: str, limit: int = 20, as_of: date | None = None) -> list[dict[str, Any]]:
        term = query.strip()
        if not term:
            return []

        as_of_date = as_of or date.today()
        wildcard = f"%{term}%"
        with get_connection(self.config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT
                        le.entity_id,
                        le.primary_name_en,
                        le.primary_name_zh,
                        i.instrument_id,
                        i.ticker,
                        i.exchange,
                        i.currency,
                        i.instrument_type,
                        i.is_primary_listing,
                        i.valid_from,
                        i.valid_to
                    FROM legal_entities le
                    JOIN instruments i ON i.entity_id = le.entity_id
                    LEFT JOIN identifiers id ON id.entity_id = le.entity_id
                    WHERE
                        (%s = upper(i.ticker)
                         OR le.primary_name_en ILIKE %s
                         OR coalesce(le.primary_name_zh, '') ILIKE %s
                         OR coalesce(id.value, '') ILIKE %s)
                      AND i.valid_from <= %s
                      AND (i.valid_to IS NULL OR i.valid_to >= %s)
                    ORDER BY le.entity_id, i.is_primary_listing DESC, i.ticker
                    LIMIT %s
                    """,
                    (term.upper(), wildcard, wildcard, wildcard, as_of_date, as_of_date, max(1, min(limit, 200))),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_entity(self, entity_id: str) -> dict[str, Any] | None:
        entity_key = entity_id.strip()
        if not entity_key:
            return None

        with get_connection(self.config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT entity_id, primary_name_en, primary_name_zh, country_of_incorporation, lei
                    FROM legal_entities
                    WHERE entity_id = %s
                    """,
                    (entity_key,),
                )
                entity_row = cur.fetchone()
                if entity_row is None:
                    return None

                cur.execute(
                    """
                    SELECT instrument_id, ticker, exchange, currency, instrument_type,
                           is_primary_listing, valid_from, valid_to
                    FROM instruments
                    WHERE entity_id = %s
                    ORDER BY is_primary_listing DESC, ticker
                    """,
                    (entity_key,),
                )
                instruments = [dict(row) for row in cur.fetchall()]

                cur.execute(
                    """
                    SELECT identifier_id, namespace, value, valid_from, valid_to
                    FROM identifiers
                    WHERE entity_id = %s
                    ORDER BY namespace, value
                    """,
                    (entity_key,),
                )
                identifiers = [dict(row) for row in cur.fetchall()]

                output = dict(entity_row)
                output["instruments"] = instruments
                output["identifiers"] = identifiers
                return output

    def import_payload(self, payload: dict[str, Any]) -> dict[str, int]:
        entities = payload.get("legal_entities") or []
        instruments = payload.get("instruments") or []
        identifiers = payload.get("identifiers") or []

        with get_connection(self.config) as conn:
            with conn.cursor() as cur:
                if entities:
                    execute_values(
                        cur,
                        """
                        INSERT INTO legal_entities (
                            entity_id,
                            primary_name_en,
                            primary_name_zh,
                            country_of_incorporation,
                            lei
                        ) VALUES %s
                        ON CONFLICT (entity_id) DO UPDATE SET
                            primary_name_en = EXCLUDED.primary_name_en,
                            primary_name_zh = EXCLUDED.primary_name_zh,
                            country_of_incorporation = EXCLUDED.country_of_incorporation,
                            lei = EXCLUDED.lei
                        """,
                        [
                            (
                                item["entity_id"],
                                item["primary_name_en"],
                                item.get("primary_name_zh"),
                                item["country_of_incorporation"],
                                item.get("lei"),
                            )
                            for item in entities
                        ],
                    )

                if instruments:
                    execute_values(
                        cur,
                        """
                        INSERT INTO instruments (
                            instrument_id,
                            entity_id,
                            ticker,
                            exchange,
                            currency,
                            instrument_type,
                            is_primary_listing,
                            valid_from,
                            valid_to
                        ) VALUES %s
                        ON CONFLICT (instrument_id) DO UPDATE SET
                            entity_id = EXCLUDED.entity_id,
                            ticker = EXCLUDED.ticker,
                            exchange = EXCLUDED.exchange,
                            currency = EXCLUDED.currency,
                            instrument_type = EXCLUDED.instrument_type,
                            is_primary_listing = EXCLUDED.is_primary_listing,
                            valid_from = EXCLUDED.valid_from,
                            valid_to = EXCLUDED.valid_to
                        """,
                        [
                            (
                                item["instrument_id"],
                                item["entity_id"],
                                item["ticker"],
                                item["exchange"],
                                item["currency"],
                                item["instrument_type"],
                                bool(item.get("is_primary_listing", False)),
                                item["valid_from"],
                                item.get("valid_to"),
                            )
                            for item in instruments
                        ],
                    )

                if identifiers:
                    execute_values(
                        cur,
                        """
                        INSERT INTO identifiers (
                            identifier_id,
                            entity_id,
                            namespace,
                            value,
                            valid_from,
                            valid_to
                        ) VALUES %s
                        ON CONFLICT (identifier_id) DO UPDATE SET
                            entity_id = EXCLUDED.entity_id,
                            namespace = EXCLUDED.namespace,
                            value = EXCLUDED.value,
                            valid_from = EXCLUDED.valid_from,
                            valid_to = EXCLUDED.valid_to
                        """,
                        [
                            (
                                item["identifier_id"],
                                item["entity_id"],
                                item["namespace"],
                                item["value"],
                                item["valid_from"],
                                item.get("valid_to"),
                            )
                            for item in identifiers
                        ],
                    )

            conn.commit()

        return {
            "legal_entities": len(entities),
            "instruments": len(instruments),
            "identifiers": len(identifiers),
        }
