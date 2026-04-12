from __future__ import annotations

from datetime import datetime
from typing import Iterable

import requests

from .models import Announcement


class CninfoClient:
    """Client for CNINFO history announcement query API."""

    BASE_URL = "http://www.cninfo.com.cn"
    QUERY_PATH = "/new/hisAnnouncement/query"

    def __init__(self, timeout_seconds: int = 20) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; cninfo-financial-monitor/1.0)",
                "Accept": "application/json,text/plain,*/*",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
            }
        )

    def fetch_page(
        self,
        page_num: int,
        page_size: int,
        market: str,
        start_date: str,
        end_date: str,
        stock: str = "",
    ) -> list[Announcement]:
        # CNINFO's history endpoint commonly honors searchkey better than stock.
        search_key = stock.strip()
        response = self.session.post(
            f"{self.BASE_URL}{self.QUERY_PATH}",
            data={
                "pageNum": str(page_num),
                "pageSize": str(page_size),
                "column": market,
                "tabName": "fulltext",
                "plate": "",
                "stock": "",
                "searchkey": search_key,
                "secid": "",
                "category": "",
                "trade": "",
                "seDate": f"{start_date}~{end_date}",
                "sortName": "time",
                "sortType": "desc",
                "isHLtitle": "true",
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        records = payload.get("announcements") if isinstance(payload, dict) else None
        if not isinstance(records, list):
            return []

        announcements: list[Announcement] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            stock_code = str(record.get("secCode") or "").strip()
            stock_name = str(record.get("secName") or "").strip()
            title = str(record.get("announcementTitle") or "").strip()
            adjunct_url = str(record.get("adjunctUrl") or "").strip()
            adjunct_type = str(record.get("adjunctType") or "pdf").strip()
            announcement_id = str(
                record.get("announcementId")
                or record.get("announcementCode")
                or f"{stock_code}_{record.get('announcementTime')}_{adjunct_url}"
            ).strip()
            release_time = self._format_release_time(record.get("announcementTime"))

            if not adjunct_url or not title:
                continue

            announcements.append(
                Announcement(
                    announcement_id=announcement_id,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    title=title,
                    release_time=release_time,
                    adjunct_url=adjunct_url,
                    adjunct_type=adjunct_type,
                )
            )

        return announcements

    def fetch_pages(
        self,
        pages: int,
        page_size: int,
        market: str,
        start_date: str,
        end_date: str,
        stocks: Iterable[str],
    ) -> Iterable[Announcement]:
        stock_list = [s for s in stocks if s]
        if not stock_list:
            stock_list = [""]

        for stock in stock_list:
            for page in range(1, max(1, pages) + 1):
                page_items = self.fetch_page(
                    page_num=page,
                    page_size=page_size,
                    market=market,
                    start_date=start_date,
                    end_date=end_date,
                    stock=stock,
                )
                if not page_items:
                    break
                for item in page_items:
                    yield item

    @staticmethod
    def _format_release_time(raw: object) -> str:
        if raw is None:
            return ""

        try:
            value = int(raw)
        except (TypeError, ValueError):
            return str(raw)

        if value > 10_000_000_000:
            value //= 1000

        try:
            return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M")
        except (OSError, OverflowError, ValueError):
            return str(raw)
