from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterable

import requests

from .models import Announcement, TitleSearchResult


RESULT_HEADLINE_CATEGORIES: dict[str, tuple[str, str]] = {
    "final": ("13300", "Final Results"),
    "interim": ("13400", "Interim Results"),
    "quarterly": ("13600", "Quarterly Results"),
}


class HKEXClient:
    """Client for HKEX latest-announcement JSON feed."""

    BASE_URL = "https://www1.hkexnews.hk"
    HSI_BASE_URL = "https://www.hsi.com.hk"

    def __init__(self, timeout_seconds: int = 20) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; hkex-financial-monitor/1.0)",
                "Accept": "application/json,text/plain,*/*",
            }
        )

    def build_feed_url(
        self,
        page: int,
        board: str = "sehk",
        window: str = "latest",
        lang: str = "c",
    ) -> str:
        """Build URL based on HKEX lci.js naming convention.

        Pattern from lci.js:
        /ncms/json/eds/lci + board + window + rels + d + lang + _{page}.json

        Examples:
        - sehk latest zh: lcisehk1relsdc_1.json
        - gem 7days zh: lcigem7relsdc_1.json
        """
        board_part = "sehk" if board.lower() == "sehk" else "gem"
        window_part = "1" if window.lower() == "latest" else "7"
        sort_part = "relsd"  # latest info descending
        lang_part = "e" if lang.lower().startswith("e") else "c"
        file_name = f"lci{board_part}{window_part}{sort_part}{lang_part}_{page}.json"
        return f"{self.BASE_URL}/ncms/json/eds/{file_name}"

    def fetch_page(
        self,
        page: int,
        board: str = "sehk",
        window: str = "latest",
        lang: str = "c",
    ) -> list[Announcement]:
        url = self.build_feed_url(page=page, board=board, window=window, lang=lang)
        response = self.session.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Unexpected HKEX payload format")

        records = payload.get("newsInfoLst") or []
        if not isinstance(records, list):
            return []

        announcements: list[Announcement] = []
        for record in records:
            if not isinstance(record, dict):
                continue

            stock_list = record.get("stock") or []
            if not stock_list:
                stock_list = [{"sc": "", "sn": ""}]

            for stock in stock_list:
                stock_code = str(stock.get("sc") or "").strip()
                stock_name = str(stock.get("sn") or "").strip()
                news_id_raw = record.get("newsId")
                try:
                    news_id = int(news_id_raw)
                except (TypeError, ValueError):
                    continue

                announcements.append(
                    Announcement(
                        news_id=news_id,
                        release_time=str(record.get("relTime") or ""),
                        stock_code=stock_code,
                        stock_name=stock_name,
                        title=str(record.get("title") or ""),
                        category_long=str(record.get("lTxt") or ""),
                        category_short=str(record.get("sTxt") or ""),
                        t1_code=str(record.get("t1Code") or ""),
                        t2_code=str(record.get("t2Code") or ""),
                        file_ext=str(record.get("ext") or ""),
                        file_size=str(record.get("size") or ""),
                        web_path=str(record.get("webPath") or ""),
                        market=str(record.get("market") or ""),
                    )
                )

        return announcements

    def fetch_pages(
        self,
        pages: int,
        board: str = "sehk",
        window: str = "latest",
        lang: str = "c",
    ) -> Iterable[Announcement]:
        for page in range(1, max(pages, 1) + 1):
            for item in self.fetch_page(page=page, board=board, window=window, lang=lang):
                yield item

    def resolve_stock_id(self, stock_code: str, market: str = "SEHK", lang: str = "ZH") -> int:
        """Resolve HKEX internal stockId from user stock code using prefix search API."""
        stock_id, _ = self.resolve_stock(stock_code=stock_code, market=market, lang=lang)
        return stock_id

    def resolve_stock(
        self,
        stock_code: str,
        market: str = "SEHK",
        lang: str = "ZH",
    ) -> tuple[int, str]:
        """Resolve HKEX internal stockId and stock short name for a stock code."""
        code = stock_code.strip().zfill(5)
        url = f"{self.BASE_URL}/search/prefix.do"
        response = self.session.get(
            url,
            params={
                "callback": "callback",
                "lang": lang,
                "type": "A",
                "name": code,
                "market": market,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        text = response.text.strip()
        # JSONP format: callback({...});
        start = text.find("(")
        end = text.rfind(")")
        if start < 0 or end <= start:
            raise ValueError("Unexpected stock search response")

        payload = json.loads(text[start + 1 : end])
        items = payload.get("stockInfo") or []
        for item in items:
            item_code = str(item.get("code") or "").strip().zfill(5)
            if item_code == code:
                return int(item["stockId"]), str(item.get("name") or "").strip()

        raise ValueError(f"Stock code not found on HKEX: {stock_code}")

    def search_result_announcements(
        self,
        stock_code: str,
        from_date: str,
        to_date: str,
        market: str = "SEHK",
        lang: str = "EN",
        result_types: Iterable[str] | None = None,
    ) -> list[TitleSearchResult]:
        stock_id, stock_name = self.resolve_stock(stock_code=stock_code, market=market, lang=lang)
        selected_types = list(result_types or RESULT_HEADLINE_CATEGORIES.keys())

        seen_urls: set[str] = set()
        matches: list[TitleSearchResult] = []
        for result_type in selected_types:
            if result_type not in RESULT_HEADLINE_CATEGORIES:
                raise ValueError(f"Unsupported result type: {result_type}")

            headline_code, headline_name = RESULT_HEADLINE_CATEGORIES[result_type]
            html = self._post_title_search(
                stock_id=stock_id,
                from_date=from_date,
                to_date=to_date,
                market=market,
                lang=lang,
                t1code="10000",
                t2gcode="3",
                t2code=headline_code,
            )
            for item in self._parse_title_search_results(
                html=html,
                stock_code=stock_code,
                stock_name=stock_name,
                headline_category_code=headline_code,
                headline_category_name=headline_name,
            ):
                if item.url in seen_urls:
                    continue
                seen_urls.add(item.url)
                matches.append(item)

        matches.sort(
            key=lambda item: (
                self._parse_release_time(item.release_time),
                item.headline_category_code,
                item.url,
            ),
            reverse=True,
        )
        return matches

    def find_annual_report_url(
        self,
        stock_code: str,
        year: int,
        market: str = "SEHK",
        lang: str = "ZH",
    ) -> str | None:
        """Return a direct HKEX annual-report PDF URL for given stock code and year."""
        stock_id = self.resolve_stock_id(stock_code=stock_code, market=market, lang=lang)

        # Try a few common annual-report keywords from strict to broad.
        keywords = ["年度報告", "年報", "annual report", "annual"]
        for keyword in keywords:
            candidate_urls: list[str] = []
            # Annual reports for fiscal year N are often released in year N+1.
            for from_date, to_date in (
                (f"{year}0101", f"{year}1231"),
                (f"{year + 1}0101", f"{year + 1}1231"),
            ):
                url = self._search_annual_report_url(
                    stock_id=stock_id,
                    year=year,
                    market=market,
                    lang=lang,
                    keyword=keyword,
                    from_date=from_date,
                    to_date=to_date,
                )
                if url:
                    candidate_urls.append(url)

            if candidate_urls:
                def _url_date_key(url: str) -> datetime:
                    match = re.search(r"/(\d{8})[a-zA-Z0-9_]*\.pdf$", url)
                    if not match:
                        return datetime.min
                    try:
                        return datetime.strptime(match.group(1), "%Y%m%d")
                    except ValueError:
                        return datetime.min

                # Deduplicate and return the latest by document date encoded in URL.
                unique_urls = list(dict.fromkeys(candidate_urls))
                return max(unique_urls, key=_url_date_key)

        return None

    def _post_title_search(
        self,
        stock_id: int,
        from_date: str,
        to_date: str,
        market: str,
        lang: str,
        t1code: str,
        t2gcode: str,
        t2code: str,
        title: str = "",
    ) -> str:
        response = self.session.post(
            f"{self.BASE_URL}/search/titlesearch.xhtml",
            params={"lang": lang.lower()},
            data={
                "lang": lang,
                "market": market,
                "searchType": "1",
                "documentType": "",
                "t1code": t1code,
                "t2Gcode": t2gcode,
                "t2code": t2code,
                "stockId": str(stock_id),
                "from": from_date,
                "to": to_date,
                "category": "0",
                "title": title,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.text

    def _parse_title_search_results(
        self,
        html: str,
        stock_code: str,
        stock_name: str,
        headline_category_code: str,
        headline_category_name: str,
    ) -> list[TitleSearchResult]:
        row_blocks = re.findall(r"<tr[^>]*>.*?</tr>", html, flags=re.IGNORECASE | re.DOTALL)
        matches: list[TitleSearchResult] = []
        for row in row_blocks:
            link_match = re.search(
                r'href="(/listedco/listconews/[^"]+\.(?:pdf|doc|docx|xls|xlsx|zip))"[^>]*>(.*?)</a>',
                row,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not link_match:
                continue

            release_match = re.search(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", row)
            if not release_match:
                continue

            title = self._normalize_html_text(link_match.group(2))
            if not title:
                continue

            matches.append(
                TitleSearchResult(
                    stock_code=stock_code.strip().zfill(5),
                    stock_name=stock_name,
                    release_time=release_match.group(1),
                    title=title,
                    headline_category_code=headline_category_code,
                    headline_category_name=headline_category_name,
                    url=f"{self.BASE_URL}{link_match.group(1)}",
                )
            )

        return matches

    @staticmethod
    def _normalize_html_text(raw: str) -> str:
        no_tags = re.sub(r"<[^>]+>", "", raw)
        return re.sub(r"\s+", " ", no_tags).strip()

    @staticmethod
    def _parse_release_time(value: str) -> datetime:
        try:
            return datetime.strptime(value, "%d/%m/%Y %H:%M")
        except ValueError:
            return datetime.min

    def _search_annual_report_url(
        self,
        stock_id: int,
        year: int,
        market: str,
        lang: str,
        keyword: str,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> str | None:
        query_from = from_date or f"{year}0101"
        query_to = to_date or f"{year}1231"
        response = self.session.post(
            f"{self.BASE_URL}/search/titlesearch.xhtml",
            params={"lang": lang.lower()},
            data={
                "lang": lang,
                "market": market,
                "searchType": "0",
                "documentType": "",
                "t1code": "",
                "t2Gcode": "",
                "t2code": "",
                "stockId": str(stock_id),
                "from": query_from,
                "to": query_to,
                "category": "0",
                "title": keyword,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        html = response.text

        row_blocks = re.findall(r"<tr[^>]*>.*?</tr>", html, flags=re.IGNORECASE | re.DOTALL)
        entries: list[tuple[str, str, datetime | None]] = []

        for row in row_blocks:
            link_match = re.search(
                r'href="(/listedco/listconews/[^"]+\.pdf)"[^>]*>(.*?)</a>',
                row,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not link_match:
                continue

            path, label = link_match.group(1), link_match.group(2)
            release_match = re.search(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", row)
            release_dt: datetime | None = None
            if release_match:
                try:
                    release_dt = datetime.strptime(release_match.group(1), "%d/%m/%Y %H:%M")
                except ValueError:
                    release_dt = None

            entries.append((path, label, release_dt))

        if not entries:
            # Fallback: parse links from whole page even if row structure changes.
            fallback_matches = re.findall(
                r'href="(/listedco/listconews/[^"]+\.pdf)"[^>]*>(.*?)</a>',
                html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            entries = [(path, label, None) for path, label in fallback_matches]

        if not entries:
            return None

        def _normalize_title(raw: str) -> str:
            no_tags = re.sub(r"<[^>]+>", "", raw)
            return re.sub(r"\s+", "", no_tags).strip().lower()

        annual_tokens = ["年度報告", "年報", "annualreport"]
        year_token = str(year)
        negative_tokens = ["摘要", "summary", "sustainability", "esg", "持續督導", "督導"]

        # Prefer links whose anchor text contains both year and annual-report wording.
        preferred: list[tuple[str, str, datetime | None]] = []
        for path, label, release_dt in entries:
            label_norm = _normalize_title(label)
            if year_token in label_norm and any(token in label_norm for token in annual_tokens):
                preferred.append((path, label, release_dt))

        candidates = preferred if preferred else entries

        def _quality_score(label: str) -> int:
            label_norm = _normalize_title(label)
            score = 0

            if year_token in label_norm:
                score += 100
            if "年度報告" in label_norm or "年報" in label_norm or "annualreport" in label_norm:
                score += 100

            # Prefer full annual report over summary/supervisory references.
            if any(token in label_norm for token in negative_tokens):
                score -= 80

            # Prefer concise canonical titles like "2025年年度報告".
            if label_norm.endswith("年度報告") or label_norm.endswith("年報"):
                score += 30

            return score

        # When multiple matches exist, choose the newest by release time.
        newest = max(
            candidates,
            key=lambda item: (
                _quality_score(item[1]),
                item[2] if item[2] is not None else datetime.min,
            ),
        )
        return f"{self.BASE_URL}{newest[0]}"

    def get_hsi_dashboard(self, language: str = "eng") -> dict:
        """Return HSI runtime dashboard payload containing index codes and slugs."""
        response = self.session.get(
            f"{self.HSI_BASE_URL}/data/{language}/rt/dashboard.do",
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Unexpected HSI dashboard payload")
        return payload

    def resolve_hsi_index_code(self, index_slug: str, language: str = "eng") -> str:
        """Resolve HSI indexCode (e.g. 00014.00) from route slug (e.g. hscei)."""
        target = index_slug.strip().lower()
        dashboard = self.get_hsi_dashboard(language=language)
        for region in dashboard.get("regions") or []:
            for index_item in region.get("dashboardList") or []:
                if str(index_item.get("url") or "").strip().lower() == target:
                    code = str(index_item.get("indexCode") or "").strip()
                    if code:
                        return code
        raise ValueError(f"HSI index slug not found: {index_slug}")

    def get_hsi_constituents(self, index_code: str, language: str = "eng") -> list[dict]:
        """Get constituent list from HSI constituents API by indexCode."""
        response = self.session.get(
            f"{self.HSI_BASE_URL}/api/wsit-hsil-hiip-ea-public-proxy/v1/dataretrieval/e/constituents/v1",
            params={"language": language, "indexCode": index_code},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return []
        constituents = data.get("constituents") or []
        if not isinstance(constituents, list):
            return []
        return [item for item in constituents if isinstance(item, dict)]

    def get_hscei_stock_codes(self, language: str = "eng") -> list[str]:
        """Reproduce HSCEI page 'Constituents -> View now' flow and return stock codes."""
        index_code = self.resolve_hsi_index_code(index_slug="hscei", language=language)
        constituents = self.get_hsi_constituents(index_code=index_code, language=language)

        seen: set[str] = set()
        codes: list[str] = []
        for item in constituents:
            code = str(item.get("stockCode") or "").strip()
            if not code:
                continue
            code_norm = code.zfill(5)
            if code_norm not in seen:
                seen.add(code_norm)
                codes.append(code_norm)
        return codes
