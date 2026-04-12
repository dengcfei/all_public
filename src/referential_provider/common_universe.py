from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import akshare as ak
import requests
import yfinance as yf
from bs4 import BeautifulSoup


_WIKI_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; lobster-referential-sync/1.0)"}


def _safe_token(value: str) -> str:
    text = (value or "").strip().upper()
    text = re.sub(r"[^A-Z0-9]+", "_", text)
    return text.strip("_") or "UNKNOWN"


def _norm_text(value: str | None) -> str:
    return (value or "").strip()


def _norm_ticker(value: str) -> str:
    return _norm_text(value).upper()


@dataclass
class ConstituentRecord:
    source_index: str
    ticker: str
    exchange: str
    company_name_en: str
    company_name_zh: str = ""
    country: str = ""
    currency: str = ""
    instrument_type: str = "EQUITY"
    is_primary_listing: bool = True
    valid_from: str = field(default_factory=lambda: date.today().isoformat())


class CommonUniverseCollector:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(_WIKI_HEADERS)

    def collect_all(self, enrich_us: bool = True, us_enrich_limit: int = 0) -> tuple[list[ConstituentRecord], dict[str, int]]:
        records: list[ConstituentRecord] = []
        stats: dict[str, int] = {}

        sp500 = self._fetch_sp500_wiki()
        stats["SP500"] = len(sp500)
        records.extend(sp500)

        ndx = self._fetch_nasdaq100_wiki()
        stats["NASDAQ100"] = len(ndx)
        records.extend(ndx)

        hsi = self._fetch_hsi_wiki()
        stats["HSI"] = len(hsi)
        records.extend(hsi)

        hstech_top = self._fetch_hstech_top10_official()
        stats["HSTECH_TOP10"] = len(hstech_top)
        records.extend(hstech_top)

        csi300 = self._fetch_csi_from_csindex("000300", "CSI300")
        stats["CSI300"] = len(csi300)
        records.extend(csi300)

        csi500 = self._fetch_csi_from_csindex("000905", "CSI500")
        stats["CSI500"] = len(csi500)
        records.extend(csi500)

        if enrich_us:
            self._enrich_us_records(records, limit=us_enrich_limit)

        return records, stats

    def _fetch_sp500_wiki(self) -> list[ConstituentRecord]:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        html = self.session.get(url, timeout=30).text
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", attrs={"id": "constituents"})
        if table is None:
            raise ValueError("SP500 wiki constituents table not found")

        out: list[ConstituentRecord] = []
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue
            ticker = _norm_ticker(cols[0].get_text(strip=True))
            name = _norm_text(cols[1].get_text(" ", strip=True))
            if not ticker or not name:
                continue
            out.append(
                ConstituentRecord(
                    source_index="SP500",
                    ticker=ticker,
                    exchange="US",
                    company_name_en=name,
                    country="US",
                    currency="USD",
                )
            )
        return out

    def _fetch_nasdaq100_wiki(self) -> list[ConstituentRecord]:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        html = self.session.get(url, timeout=30).text
        soup = BeautifulSoup(html, "lxml")

        target_table = None
        ticker_idx = -1
        company_idx = -1
        for table in soup.find_all("table", class_="wikitable"):
            header_row = table.find("tr")
            if header_row is None:
                continue

            header_cells = [th.get_text(" ", strip=True).lower() for th in header_row.find_all("th")]
            if not header_cells:
                continue

            local_ticker_idx = -1
            local_company_idx = -1
            for idx, header in enumerate(header_cells):
                if local_ticker_idx < 0 and "ticker" in header:
                    local_ticker_idx = idx
                if local_company_idx < 0 and ("company" in header or "name" in header):
                    local_company_idx = idx

            if local_ticker_idx >= 0 and local_company_idx >= 0:
                target_table = table
                ticker_idx = local_ticker_idx
                company_idx = local_company_idx
                break
        if target_table is None:
            raise ValueError("NASDAQ100 wiki table not found")

        out: list[ConstituentRecord] = []
        for row in target_table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if not cols:
                continue
            if ticker_idx >= len(cols) or company_idx >= len(cols):
                continue

            ticker = _norm_ticker(cols[ticker_idx].get_text(strip=True))
            company = _norm_text(cols[company_idx].get_text(" ", strip=True))
            if not ticker or not company:
                continue
            out.append(
                ConstituentRecord(
                    source_index="NASDAQ100",
                    ticker=ticker,
                    exchange="NASDAQ",
                    company_name_en=company,
                    country="US",
                    currency="USD",
                )
            )
        return out

    def _fetch_hsi_wiki(self) -> list[ConstituentRecord]:
        url = "https://en.wikipedia.org/wiki/Hang_Seng_Index"
        html = self.session.get(url, timeout=30).text
        soup = BeautifulSoup(html, "lxml")

        target_table = None
        for table in soup.find_all("table", class_="wikitable"):
            header_cells = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
            header_text = "|".join(header_cells)
            if "ticker" in header_text and "name" in header_text:
                target_table = table
                break
        if target_table is None:
            raise ValueError("HSI wiki constituents table not found")

        out: list[ConstituentRecord] = []
        for row in target_table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue
            raw_ticker = _norm_ticker(cols[0].get_text(strip=True))
            ticker = f"{raw_ticker.zfill(4)}.HK" if raw_ticker.isdigit() else raw_ticker
            name = _norm_text(cols[1].get_text(" ", strip=True))
            if not ticker or not name:
                continue
            out.append(
                ConstituentRecord(
                    source_index="HSI",
                    ticker=ticker,
                    exchange="HKEX",
                    company_name_en=name,
                    country="HK",
                    currency="HKD",
                )
            )
        return out

    def _fetch_hstech_top10_official(self) -> list[ConstituentRecord]:
        dashboard = self.session.get("https://www.hsi.com.hk/data/eng/rt/dashboard.do", timeout=30).json()
        index_code = "02083.00"
        for region in dashboard.get("regions") or []:
            for item in region.get("dashboardList") or []:
                if str(item.get("url") or "").strip().lower() == "hstech":
                    index_code = str(item.get("indexCode") or index_code)
                    break

        payload = self.session.get(
            "https://www.hsi.com.hk/api/wsit-hsil-hiip-ea-public-proxy/v1/dataretrieval/e/constituents/v1",
            params={"language": "eng", "indexCode": index_code},
            timeout=30,
        ).json()
        constituents = (((payload.get("data") or {}).get("constituents")) or [])

        out: list[ConstituentRecord] = []
        for item in constituents:
            code = _norm_text(str(item.get("stockCode") or ""))
            if not code:
                continue
            ticker = f"{code.zfill(4)}.HK"
            name = _norm_text(str(item.get("stockName") or ""))
            out.append(
                ConstituentRecord(
                    source_index="HSTECH_TOP10",
                    ticker=ticker,
                    exchange="HKEX",
                    company_name_en=name,
                    country="HK",
                    currency="HKD",
                )
            )
        return out

    def _fetch_csi_from_csindex(self, symbol: str, source_index: str) -> list[ConstituentRecord]:
        df = ak.index_stock_cons_csindex(symbol=symbol)
        out: list[ConstituentRecord] = []
        for row in df.to_dict("records"):
            code = _norm_text(str(row.get("成分券代码") or row.get("品种代码") or ""))
            if not code:
                continue

            exchange_cn = _norm_text(str(row.get("交易所") or ""))
            if "上海" in exchange_cn:
                exchange = "SSE"
                ticker = f"{code}.SH"
            elif "深圳" in exchange_cn:
                exchange = "SZSE"
                ticker = f"{code}.SZ"
            else:
                if code.startswith(("6", "9")):
                    exchange = "SSE"
                    ticker = f"{code}.SH"
                else:
                    exchange = "SZSE"
                    ticker = f"{code}.SZ"

            out.append(
                ConstituentRecord(
                    source_index=source_index,
                    ticker=ticker,
                    exchange=exchange,
                    company_name_en=_norm_text(str(row.get("成分券英文名称") or "")),
                    company_name_zh=_norm_text(str(row.get("成分券名称") or row.get("品种名称") or "")),
                    country="CN",
                    currency="CNY",
                )
            )
        return out

    def _enrich_us_records(self, records: list[ConstituentRecord], limit: int = 0) -> None:
        us_tickers = sorted({r.ticker for r in records if r.country == "US"})
        if limit > 0:
            us_tickers = us_tickers[:limit]

        exchange_map = {
            "NMS": "NASDAQ",
            "NYQ": "NYSE",
            "ASE": "AMEX",
        }

        meta_cache: dict[str, tuple[str, str, str]] = {}
        for ticker in us_tickers:
            query_ticker = ticker.replace(".", "-")
            try:
                tk = yf.Ticker(query_ticker)
                fast = tk.fast_info or {}
                info = tk.info or {}
                exchange = exchange_map.get(str(fast.get("exchange") or "").upper(), str(fast.get("exchange") or "US"))
                currency = _norm_text(str(fast.get("currency") or "USD")) or "USD"
                long_name = _norm_text(str(info.get("longName") or info.get("shortName") or ""))
                meta_cache[ticker] = (exchange, currency, long_name)
            except Exception:
                continue

        for rec in records:
            if rec.country != "US":
                continue
            meta = meta_cache.get(rec.ticker)
            if not meta:
                continue
            exchange, currency, long_name = meta
            if exchange:
                rec.exchange = exchange
            if currency:
                rec.currency = currency
            if long_name:
                rec.company_name_en = long_name


def build_import_payload(records: list[ConstituentRecord]) -> tuple[dict[str, Any], dict[str, int]]:
    consolidated: dict[tuple[str, str], dict[str, Any]] = {}

    for rec in records:
        key = (_norm_ticker(rec.exchange), _norm_ticker(rec.ticker))
        if key not in consolidated:
            consolidated[key] = {
                "source_indexes": {rec.source_index},
                "ticker": _norm_ticker(rec.ticker),
                "exchange": _norm_ticker(rec.exchange),
                "company_name_en": _norm_text(rec.company_name_en),
                "company_name_zh": _norm_text(rec.company_name_zh),
                "country": _norm_text(rec.country) or "UN",
                "currency": _norm_text(rec.currency) or "USD",
                "instrument_type": rec.instrument_type,
                "is_primary_listing": bool(rec.is_primary_listing),
                "valid_from": rec.valid_from,
            }
        else:
            slot = consolidated[key]
            slot["source_indexes"].add(rec.source_index)
            if not slot["company_name_en"] and rec.company_name_en:
                slot["company_name_en"] = rec.company_name_en
            if not slot["company_name_zh"] and rec.company_name_zh:
                slot["company_name_zh"] = rec.company_name_zh
            if slot["exchange"] == "US" and rec.exchange and rec.exchange != "US":
                slot["exchange"] = rec.exchange

    legal_entities: list[dict[str, Any]] = []
    instruments: list[dict[str, Any]] = []
    identifiers: list[dict[str, Any]] = []

    for key, value in sorted(consolidated.items()):
        exchange, ticker = key
        entity_id = f"ENTITY_{_safe_token(exchange)}_{_safe_token(ticker)}"
        instrument_id = f"INST_{_safe_token(exchange)}_{_safe_token(ticker)}"

        name_en = value["company_name_en"] or ticker
        name_zh = value["company_name_zh"]
        country = value["country"]

        legal_entities.append(
            {
                "entity_id": entity_id,
                "primary_name_en": name_en,
                "primary_name_zh": name_zh or None,
                "country_of_incorporation": country,
                "lei": None,
            }
        )

        instruments.append(
            {
                "instrument_id": instrument_id,
                "entity_id": entity_id,
                "ticker": ticker,
                "exchange": exchange,
                "currency": value["currency"],
                "instrument_type": value["instrument_type"],
                "is_primary_listing": value["is_primary_listing"],
                "valid_from": value["valid_from"],
                "valid_to": None,
            }
        )

        identifiers.append(
            {
                "identifier_id": f"ID_{_safe_token(entity_id)}_EN",
                "entity_id": entity_id,
                "namespace": "COMPANY_NAME_EN",
                "value": name_en,
                "valid_from": value["valid_from"],
                "valid_to": None,
            }
        )

        if name_zh:
            identifiers.append(
                {
                    "identifier_id": f"ID_{_safe_token(entity_id)}_ZH",
                    "entity_id": entity_id,
                    "namespace": "COMPANY_NAME_ZH",
                    "value": name_zh,
                    "valid_from": value["valid_from"],
                    "valid_to": None,
                }
            )

        identifiers.append(
            {
                "identifier_id": f"ID_{_safe_token(entity_id)}_TICKER",
                "entity_id": entity_id,
                "namespace": "ALIAS",
                "value": ticker,
                "valid_from": value["valid_from"],
                "valid_to": None,
            }
        )

    payload = {
        "legal_entities": legal_entities,
        "instruments": instruments,
        "identifiers": identifiers,
    }
    metrics = {
        "records_in": len(records),
        "entities": len(legal_entities),
        "instruments": len(instruments),
        "identifiers": len(identifiers),
    }
    return payload, metrics


def write_snapshot(snapshot_dir: Path, stats: dict[str, Any], payload: dict[str, Any]) -> Path:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = snapshot_dir / f"common_universe_{stamp}.json"
    body = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "stats": stats,
        "payload": payload,
    }
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path
