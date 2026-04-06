from __future__ import annotations

from dataclasses import dataclass


HKEX_BASE = "https://www1.hkexnews.hk"


@dataclass(frozen=True)
class Announcement:
    news_id: int
    release_time: str
    stock_code: str
    stock_name: str
    title: str
    category_long: str
    category_short: str
    t1_code: str
    t2_code: str
    file_ext: str
    file_size: str
    web_path: str
    market: str

    @property
    def absolute_url(self) -> str:
        return f"{HKEX_BASE}{self.web_path}"
