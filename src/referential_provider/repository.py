from __future__ import annotations

from datetime import date
import hashlib
import re
import time
from typing import Any

import akshare as ak
from psycopg2.extras import RealDictCursor, execute_values

from .config import DBConfig
from .db import get_connection
from .text_utils import chinese_variants


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
        terms = chinese_variants(term)
        upper_terms = [item.upper() for item in terms]
        wildcards = [f"%{item}%" for item in terms]
        short_ticker_like = bool(re.fullmatch(r"[A-Za-z0-9.:-]{1,4}", term))
        with get_connection(self.config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (le.entity_id, i.ticker, i.exchange)
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
                                                (upper(i.ticker) = ANY(%s)
                         OR upper(coalesce(id.value, '')) = ANY(%s)
                        OR (%s = false AND (
                            le.primary_name_en ILIKE ANY(%s)
                            OR coalesce(le.primary_name_zh, '') ILIKE ANY(%s)
                        )))
                      AND i.valid_from <= %s
                      AND (i.valid_to IS NULL OR i.valid_to >= %s)
                                        ORDER BY le.entity_id, i.ticker, i.exchange, i.is_primary_listing DESC, i.instrument_id
                    LIMIT %s
                    """,
                    (
                        upper_terms,
                        upper_terms,
                        short_ticker_like,
                        wildcards,
                        wildcards,
                        as_of_date,
                        as_of_date,
                        max(1, min(limit, 200)),
                    ),
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
                            primary_name_zh = COALESCE(EXCLUDED.primary_name_zh, legal_entities.primary_name_zh),
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

    def _stable_identifier_id(self, prefix: str, entity_id: str, value: str) -> str:
        digest = hashlib.md5(value.encode("utf-8")).hexdigest()[:12].upper()
        return f"ID_{self._safe_token(entity_id)}_{prefix}_{digest}"

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

    def backfill_chinese_aliases(self, apply: bool = False) -> dict[str, Any]:
        with get_connection(self.config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT entity_id, primary_name_zh
                    FROM legal_entities
                    ORDER BY entity_id
                    """
                )
                entity_rows = [dict(row) for row in cur.fetchall()]

                cur.execute(
                    """
                    SELECT entity_id, namespace, value
                    FROM identifiers
                    WHERE namespace IN ('COMPANY_NAME_ZH', 'ALIAS')
                    ORDER BY entity_id, namespace, value
                    """
                )
                identifier_rows = [dict(row) for row in cur.fetchall()]

        primary_name_zh_by_entity = {
            str(row["entity_id"]): str(row.get("primary_name_zh") or "").strip()
            for row in entity_rows
        }
        zh_names_by_entity: dict[str, set[str]] = {entity_id: set() for entity_id in primary_name_zh_by_entity}
        alias_values_by_entity: dict[str, set[str]] = {entity_id: set() for entity_id in primary_name_zh_by_entity}
        legacy_group_members: dict[str, set[str]] = {}

        for entity_id, primary_name_zh in primary_name_zh_by_entity.items():
            if primary_name_zh:
                zh_names_by_entity.setdefault(entity_id, set()).update(chinese_variants(primary_name_zh))

        for row in identifier_rows:
            entity_id = str(row.get("entity_id") or "")
            namespace = str(row.get("namespace") or "")
            value = str(row.get("value") or "").strip()
            if not entity_id or not value:
                continue

            alias_values_by_entity.setdefault(entity_id, set()).add(value)
            if namespace == "COMPANY_NAME_ZH":
                zh_names_by_entity.setdefault(entity_id, set()).update(chinese_variants(value))
            if namespace == "ALIAS" and value.startswith("LEGACY_ENTITY:"):
                legacy_group_members.setdefault(value, set()).add(entity_id)

        for _, members in legacy_group_members.items():
            union_names: set[str] = set()
            for entity_id in members:
                union_names.update(zh_names_by_entity.get(entity_id, set()))
            for entity_id in members:
                zh_names_by_entity.setdefault(entity_id, set()).update(union_names)

        updates: list[tuple[str, str]] = []
        inserts: list[tuple[str, str, str, str, None]] = []

        for entity_id, zh_names in sorted(zh_names_by_entity.items()):
            ordered_names = sorted(name for name in zh_names if name)
            if not ordered_names:
                continue

            current_primary_name_zh = primary_name_zh_by_entity.get(entity_id, "")
            preferred_name = current_primary_name_zh or min(ordered_names, key=lambda item: (len(item), item))
            if preferred_name and preferred_name != current_primary_name_zh:
                updates.append((preferred_name, entity_id))

            existing_aliases = alias_values_by_entity.get(entity_id, set())
            for name in ordered_names:
                if name in existing_aliases:
                    continue
                identifier_id = self._stable_identifier_id("ZH_ALIAS", entity_id, name)
                inserts.append((identifier_id, entity_id, "ALIAS", name, None))
                existing_aliases.add(name)

        summary: dict[str, Any] = {
            "apply": bool(apply),
            "entities_with_zh_aliases": sum(1 for value in zh_names_by_entity.values() if value),
            "primary_name_zh_updates": len(updates),
            "alias_inserts": len(inserts),
            "legacy_link_groups": len(legacy_group_members),
        }

        if not apply or (not updates and not inserts):
            return summary

        with get_connection(self.config) as conn:
            with conn.cursor() as cur:
                for primary_name_zh, entity_id in updates:
                    cur.execute(
                        "UPDATE legal_entities SET primary_name_zh = %s WHERE entity_id = %s",
                        (primary_name_zh, entity_id),
                    )

                if inserts:
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
                        ON CONFLICT (identifier_id) DO NOTHING
                        """,
                        [
                            (identifier_id, entity_id, namespace, value, date.today(), valid_to)
                            for identifier_id, entity_id, namespace, value, valid_to in inserts
                        ],
                    )

            conn.commit()

        return summary

    def normalize_hkex_sehk_tickers(self, apply: bool = False) -> dict[str, Any]:
        with get_connection(self.config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT instrument_id, entity_id, ticker
                    FROM instruments
                    WHERE exchange = 'HKEX'
                      AND ticker ~ '^SEHK:[0-9]+$'
                    ORDER BY instrument_id
                    """
                )
                instrument_rows = [dict(row) for row in cur.fetchall()]

                cur.execute(
                    """
                    SELECT identifier_id, entity_id, value
                    FROM identifiers
                    WHERE namespace = 'ALIAS'
                      AND value ~ '^SEHK:[0-9]+$'
                    ORDER BY identifier_id
                    """
                )
                alias_rows = [dict(row) for row in cur.fetchall()]

        def _to_hk_ticker(value: str) -> str:
            code = value.split(":", 1)[1].strip()
            return f"{code.zfill(4)}.HK"

        instrument_updates = [
            {
                "instrument_id": str(row["instrument_id"]),
                "entity_id": str(row["entity_id"]),
                "old_ticker": str(row["ticker"]),
                "new_ticker": _to_hk_ticker(str(row["ticker"])),
            }
            for row in instrument_rows
        ]

        alias_updates = [
            {
                "identifier_id": str(row["identifier_id"]),
                "entity_id": str(row["entity_id"]),
                "old_value": str(row["value"]),
                "new_value": _to_hk_ticker(str(row["value"])),
            }
            for row in alias_rows
        ]

        summary: dict[str, Any] = {
            "apply": bool(apply),
            "instrument_updates": len(instrument_updates),
            "alias_updates": len(alias_updates),
            "removed_duplicate_instruments": 0,
            "removed_duplicate_aliases": 0,
        }

        if not apply or (not instrument_updates and not alias_updates):
            return summary

        with get_connection(self.config) as conn:
            with conn.cursor() as cur:
                for row in instrument_updates:
                    cur.execute(
                        "UPDATE instruments SET ticker = %s WHERE instrument_id = %s",
                        (row["new_ticker"], row["instrument_id"]),
                    )

                for row in alias_updates:
                    cur.execute(
                        "UPDATE identifiers SET value = %s WHERE identifier_id = %s",
                        (row["new_value"], row["identifier_id"]),
                    )

                cur.execute(
                    """
                    DELETE FROM instruments a
                    USING instruments b
                    WHERE a.ctid > b.ctid
                      AND a.entity_id = b.entity_id
                      AND a.ticker = b.ticker
                      AND a.exchange = b.exchange
                    """
                )
                summary["removed_duplicate_instruments"] = cur.rowcount

                cur.execute(
                    """
                    DELETE FROM identifiers a
                    USING identifiers b
                    WHERE a.ctid > b.ctid
                      AND a.entity_id = b.entity_id
                      AND a.namespace = b.namespace
                      AND a.value = b.value
                    """
                )
                summary["removed_duplicate_aliases"] = cur.rowcount

            conn.commit()

        return summary

    def backfill_hkex_chinese_names(self, apply: bool = False, retries: int = 3) -> dict[str, Any]:
        def _norm_hk_code(value: str) -> str:
            text = str(value or "").strip()
            if not text:
                return ""
            digits = "".join(ch for ch in text if ch.isdigit())
            if not digits:
                return ""
            return str(int(digits))

        last_exc: Exception | None = None
        spot_df = None
        for attempt in range(max(1, retries)):
            try:
                spot_df = ak.stock_hk_spot_em()
                break
            except Exception as exc:  # pragma: no cover - network dependent
                last_exc = exc
                if attempt + 1 < max(1, retries):
                    time.sleep(1.5 * (attempt + 1))

        if spot_df is None:
            if last_exc is not None:
                raise last_exc
            raise RuntimeError("Failed to fetch HK spot data")

        code_to_name: dict[str, str] = {}
        for row in spot_df.to_dict("records"):
            code = str(row.get("代码") or "").strip()
            name = str(row.get("名称") or "").strip()
            if code and name:
                norm_code = _norm_hk_code(code)
                if norm_code:
                    code_to_name[norm_code] = name

        with get_connection(self.config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT le.entity_id, i.ticker, le.primary_name_zh
                    FROM legal_entities le
                    JOIN instruments i ON i.entity_id = le.entity_id
                    WHERE i.exchange = 'HKEX'
                      AND i.ticker ~ '^[0-9]{4}\\.HK$'
                    ORDER BY le.entity_id, i.ticker
                    """
                )
                hk_rows = [dict(row) for row in cur.fetchall()]

                cur.execute(
                    """
                    SELECT entity_id, namespace, value
                    FROM identifiers
                    WHERE namespace IN ('ALIAS', 'COMPANY_NAME_ZH')
                    """
                )
                existing_identifiers = [dict(row) for row in cur.fetchall()]

        seen_values = {
            (str(row.get("entity_id") or ""), str(row.get("namespace") or ""), str(row.get("value") or ""))
            for row in existing_identifiers
        }

        primary_updates: list[tuple[str, str]] = []
        identifier_inserts: list[tuple[str, str, str, str, date, None]] = []
        miss_count = 0

        for row in hk_rows:
            entity_id = str(row.get("entity_id") or "")
            ticker = str(row.get("ticker") or "")
            primary_name_zh = str(row.get("primary_name_zh") or "").strip()
            if not entity_id or not ticker:
                continue

            code = _norm_hk_code(ticker.split(".", 1)[0])
            zh_name = code_to_name.get(code)
            if not zh_name:
                miss_count += 1
                continue

            if not primary_name_zh:
                primary_updates.append((zh_name, entity_id))

            for value in chinese_variants(zh_name):
                key_zh = (entity_id, "COMPANY_NAME_ZH", value)
                if key_zh not in seen_values:
                    identifier_id = self._stable_identifier_id("ZH", entity_id, value)
                    identifier_inserts.append((identifier_id, entity_id, "COMPANY_NAME_ZH", value, date.today(), None))
                    seen_values.add(key_zh)

                key_alias = (entity_id, "ALIAS", value)
                if key_alias not in seen_values:
                    identifier_id = self._stable_identifier_id("ALIAS_ZH", entity_id, value)
                    identifier_inserts.append((identifier_id, entity_id, "ALIAS", value, date.today(), None))
                    seen_values.add(key_alias)

        summary: dict[str, Any] = {
            "apply": bool(apply),
            "hk_entities_scanned": len(hk_rows),
            "hk_code_name_map_size": len(code_to_name),
            "primary_name_zh_updates": len(primary_updates),
            "identifier_inserts": len(identifier_inserts),
            "hk_rows_without_name_match": miss_count,
        }

        if not apply or (not primary_updates and not identifier_inserts):
            return summary

        with get_connection(self.config) as conn:
            with conn.cursor() as cur:
                for zh_name, entity_id in primary_updates:
                    cur.execute(
                        """
                        UPDATE legal_entities
                        SET primary_name_zh = %s
                        WHERE entity_id = %s
                          AND (primary_name_zh IS NULL OR btrim(primary_name_zh) = '')
                        """,
                        (zh_name, entity_id),
                    )

                if identifier_inserts:
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
                        ON CONFLICT (identifier_id) DO NOTHING
                        """,
                        identifier_inserts,
                    )

            conn.commit()

        return summary
