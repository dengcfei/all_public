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


@dataclass(frozen=True)
class TitleSearchResult:
    stock_code: str
    stock_name: str
    release_time: str
    title: str
    headline_category_code: str
    headline_category_name: str
    url: str


@dataclass(frozen=True)
class FinancialExtraction:
    source_type: str
    page_count: int
    text_length: int
    currency: str = ""
    unit: str = ""
    revenue: str = ""
    gross_profit: str = ""
    operating_profit: str = ""
    profit_for_period: str = ""
    profit_attributable: str = ""
    basic_eps: str = ""
    diluted_eps: str = ""


@dataclass(frozen=True)
class DocumentProcessResult:
    stock_code: str
    stock_name: str
    release_time: str
    headline_category_code: str
    headline_category_name: str
    title: str
    url: str
    file_path: str
    status: str
    downloaded: bool
    error_message: str = ""
    extraction: FinancialExtraction | None = None
