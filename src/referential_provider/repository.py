from __future__ import annotations

from datetime import date
import re
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

    def _safe_token(self, value: str) -> str:
        text = (value or "").strip().upper()
        text = re.sub(r"[^A-Z0-9]+", "_", text)
        return text.strip("_") or "UNKNOWN"

    def _canonical_entity_id(self, exchange: str, ticker: str) -> str:
        return f"ENTITY_{self._safe_token(exchange)}_{self._safe_token(ticker)}"

    def find_entity_convergence_candidates(self) -> dict[str, Any]:
        with get_connection(self.config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        i.exchange,
                        i.ticker,
                        array_agg(DISTINCT i.entity_id ORDER BY i.entity_id) AS entity_ids
                    FROM instruments i
                    GROUP BY i.exchange, i.ticker
                    HAVING count(DISTINCT i.entity_id) > 1
                    ORDER BY i.exchange, i.ticker
                    """
                )
                duplicate_groups = [dict(row) for row in cur.fetchall()]

                cur.execute(
                    """
                    SELECT entity_id, ticker, exchange
                    FROM instruments
                    ORDER BY entity_id, exchange, ticker
                    """
                )
                instrument_rows = [dict(row) for row in cur.fetchall()]

        legacy_sources: dict[str, list[dict[str, str]]] = {}
        for row in duplicate_groups:
            exchange = str(row.get("exchange") or "")
            ticker = str(row.get("ticker") or "")
            ids = [str(x) for x in (row.get("entity_ids") or []) if x]
            if len(ids) < 2:
                continue

            expected_canonical = self._canonical_entity_id(exchange, ticker)
            canonical = expected_canonical if expected_canonical in ids else sorted(ids)[0]
            for entity_id in ids:
                if entity_id == canonical:
                    continue
                legacy_sources.setdefault(entity_id, []).append(
                    {
                        "exchange": exchange,
                        "ticker": ticker,
                        "group_canonical": canonical,
                    }
                )

        instruments_by_entity: dict[str, list[dict[str, str]]] = {}
        for row in instrument_rows:
            entity_id = str(row.get("entity_id") or "")
            if not entity_id:
                continue
            instruments_by_entity.setdefault(entity_id, []).append(
                {
                    "ticker": str(row.get("ticker") or ""),
                    "exchange": str(row.get("exchange") or ""),
                }
            )

        candidates: list[dict[str, Any]] = []
        skipped_ambiguous: list[dict[str, Any]] = []
        for legacy_entity_id, origins in sorted(legacy_sources.items()):
            legacy_instruments = instruments_by_entity.get(legacy_entity_id, [])
            expected_targets = {
                self._canonical_entity_id(item["exchange"], item["ticker"])
                for item in legacy_instruments
                if item.get("exchange") and item.get("ticker")
            }

            if len(expected_targets) != 1:
                skipped_ambiguous.append(
                    {
                        "legacy_entity_id": legacy_entity_id,
                        "reason": "multiple_canonical_targets",
                        "expected_canonical_targets": sorted(expected_targets),
                        "instruments": legacy_instruments,
                        "duplicate_groups": origins,
                    }
                )
                continue

            canonical_entity_id = sorted(expected_targets)[0]
            if canonical_entity_id == legacy_entity_id:
                continue

            candidates.append(
                {
                    "legacy_entity_id": legacy_entity_id,
                    "canonical_entity_id": canonical_entity_id,
                    "instrument_count": len(legacy_instruments),
                    "instruments": legacy_instruments,
                    "duplicate_groups": origins,
                }
            )

        return {
            "duplicate_groups": duplicate_groups,
            "candidates": candidates,
            "skipped_ambiguous": skipped_ambiguous,
        }

    def converge_legacy_entities(self, apply: bool = False) -> dict[str, Any]:
        candidate_bundle = self.find_entity_convergence_candidates()
        candidates = candidate_bundle["candidates"]
        summary: dict[str, Any] = {
            "duplicate_groups": len(candidate_bundle["duplicate_groups"]),
            "candidate_legacy_entities": len(candidates),
            "skipped_ambiguous_entities": len(candidate_bundle["skipped_ambiguous"]),
            "apply": bool(apply),
            "moved_instruments": 0,
            "deleted_instruments": 0,
            "moved_identifiers": 0,
            "deleted_identifiers": 0,
            "deleted_legal_entities": 0,
            "legacy_mappings_added": 0,
            "groups": candidates,
            "skipped_ambiguous": candidate_bundle["skipped_ambiguous"],
        }

        if not apply or not candidates:
            return summary

        with get_connection(self.config) as conn:
            with conn.cursor() as cur:
                for item in candidates:
                                        canonical = str(item["canonical_entity_id"])
                                        legacy_id = str(item["legacy_entity_id"])

                                        cur.execute(
                                                """
                                                UPDATE instruments li
                                                SET entity_id = %s
                                                WHERE li.entity_id = %s
                                                    AND NOT EXISTS (
                                                            SELECT 1
                                                            FROM instruments ci
                                                            WHERE ci.entity_id = %s
                                                                AND ci.ticker = li.ticker
                                                                AND ci.exchange = li.exchange
                                                    )
                                                """,
                                                (canonical, legacy_id, canonical),
                                        )
                                        summary["moved_instruments"] += cur.rowcount

                                        cur.execute(
                                                """
                                                DELETE FROM instruments li
                                                USING instruments ci
                                                WHERE li.entity_id = %s
                                                    AND ci.entity_id = %s
                                                    AND li.ticker = ci.ticker
                                                    AND li.exchange = ci.exchange
                                                """,
                                                (legacy_id, canonical),
                                        )
                                        summary["deleted_instruments"] += cur.rowcount

                                        cur.execute(
                                                """
                                                UPDATE identifiers lid
                                                SET entity_id = %s
                                                WHERE lid.entity_id = %s
                                                    AND NOT EXISTS (
                                                            SELECT 1
                                                            FROM identifiers cid
                                                            WHERE cid.entity_id = %s
                                                                AND cid.namespace = lid.namespace
                                                                AND cid.value = lid.value
                                                    )
                                                """,
                                                (canonical, legacy_id, canonical),
                                        )
                                        summary["moved_identifiers"] += cur.rowcount

                                        cur.execute(
                                                """
                                                DELETE FROM identifiers lid
                                                USING identifiers cid
                                                WHERE lid.entity_id = %s
                                                    AND cid.entity_id = %s
                                                    AND lid.namespace = cid.namespace
                                                    AND lid.value = cid.value
                                                """,
                                                (legacy_id, canonical),
                                        )
                                        summary["deleted_identifiers"] += cur.rowcount

                                        mapping_identifier_id = (
                                                f"ID_{self._safe_token(canonical)}_LEGACY_ENTITY_{self._safe_token(legacy_id)}"
                                        )
                                        cur.execute(
                                                """
                                                INSERT INTO identifiers (
                                                        identifier_id,
                                                        entity_id,
                                                        namespace,
                                                        value,
                                                        valid_from,
                                                        valid_to
                                                ) VALUES (%s, %s, 'ALIAS', %s, CURRENT_DATE, NULL)
                                                ON CONFLICT (identifier_id) DO NOTHING
                                                """,
                                                (mapping_identifier_id, canonical, f"LEGACY_ENTITY:{legacy_id}"),
                                        )
                                        summary["legacy_mappings_added"] += cur.rowcount

                                        cur.execute("DELETE FROM legal_entities WHERE entity_id = %s", (legacy_id,))
                                        summary["deleted_legal_entities"] += cur.rowcount

            conn.commit()

        return summary
