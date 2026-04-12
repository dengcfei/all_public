from __future__ import annotations

from dataclasses import dataclass


CNINFO_BASE = "http://www.cninfo.com.cn"
CNINFO_FILE_BASE = "http://static.cninfo.com.cn"


@dataclass(frozen=True)
class Announcement:
    announcement_id: str
    stock_code: str
    stock_name: str
    title: str
    release_time: str
    adjunct_url: str
    adjunct_type: str

    @property
    def absolute_url(self) -> str:
        if self.adjunct_url.startswith("http://") or self.adjunct_url.startswith("https://"):
            return self.adjunct_url
        path = self.adjunct_url if self.adjunct_url.startswith("/") else f"/{self.adjunct_url}"
        return f"{CNINFO_FILE_BASE}{path}"
