from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from .models import SecFiling


class SecClient:
    """Client for SEC submissions and ticker map APIs."""

    SUBMISSIONS_BASE = "https://data.sec.gov"
    TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"

    def __init__(self, timeout_seconds: int = 20, user_agent: str = "lobster-reports-monitor/1.0 (contact: dev@example.com)") -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json,text/plain,*/*",
            }
        )
        self._ticker_map_cache: dict[str, dict[str, Any]] | None = None

    def resolve_issuer(self, cik: str = "", ticker: str = "") -> tuple[str, str, str]:
        cik_clean = cik.strip()
        ticker_clean = ticker.strip().upper()

        if cik_clean:
            cik10 = cik_clean.zfill(10)
            return cik10, ticker_clean, ""

        if not ticker_clean:
            raise ValueError("Provide either --cik or --ticker")

        ticker_map = self._load_ticker_map()
        item = ticker_map.get(ticker_clean)
        if item is None:
            raise ValueError(f"Ticker not found in SEC mapping: {ticker_clean}")

        cik10 = str(item.get("cik_str", "")).zfill(10)
        name = str(item.get("title", "")).strip()
        return cik10, ticker_clean, name

    def fetch_recent_filings(self, cik: str, ticker: str = "", company_name: str = "") -> list[SecFiling]:
        response = self.session.get(
            f"{self.SUBMISSIONS_BASE}/submissions/CIK{cik}.json",
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        filings_recent = ((payload.get("filings") or {}).get("recent") or {}) if isinstance(payload, dict) else {}

        accessions = self._as_list(filings_recent.get("accessionNumber"))
        forms = self._as_list(filings_recent.get("form"))
        filing_dates = self._as_list(filings_recent.get("filingDate"))
        report_dates = self._as_list(filings_recent.get("reportDate"))
        primary_documents = self._as_list(filings_recent.get("primaryDocument"))
        descriptions = self._as_list(filings_recent.get("primaryDocDescription"))

        count = min(
            len(accessions),
            len(forms),
            len(filing_dates),
            len(report_dates),
            len(primary_documents),
            len(descriptions),
        )

        issuer_name = company_name or str(payload.get("name") or "").strip()
        issuer_ticker = ticker or self._extract_ticker(payload)

        filings: list[SecFiling] = []
        for i in range(count):
            accession = str(accessions[i] or "").strip()
            form_type = str(forms[i] or "").strip().upper()
            filing_date = self._normalize_date(filing_dates[i])
            report_date = self._normalize_date(report_dates[i])
            primary_document = str(primary_documents[i] or "").strip()
            description = str(descriptions[i] or "").strip()

            if not accession or not form_type or not filing_date or not primary_document:
                continue

            filings.append(
                SecFiling(
                    cik=cik,
                    ticker=issuer_ticker,
                    company_name=issuer_name,
                    accession_number=accession,
                    filing_date=filing_date,
                    report_date=report_date,
                    form_type=form_type,
                    primary_document=primary_document,
                    description=description,
                )
            )

        filings.sort(key=lambda item: item.filing_date, reverse=True)
        return filings

    def _load_ticker_map(self) -> dict[str, dict[str, Any]]:
        if self._ticker_map_cache is not None:
            return self._ticker_map_cache

        response = self.session.get(self.TICKER_MAP_URL, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Unexpected SEC ticker mapping payload")

        output: dict[str, dict[str, Any]] = {}
        for item in payload.values():
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("ticker") or "").strip().upper()
            if ticker:
                output[ticker] = item

        self._ticker_map_cache = output
        return output

    @staticmethod
    def _extract_ticker(payload: dict[str, Any]) -> str:
        tickers = payload.get("tickers") if isinstance(payload, dict) else None
        if isinstance(tickers, list) and tickers:
            return str(tickers[0] or "").strip().upper()
        return ""

    @staticmethod
    def _as_list(value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        return []

    @staticmethod
    def _normalize_date(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return text
