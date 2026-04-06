from __future__ import annotations

from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests

from .models import Announcement, TitleSearchResult


def _safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)
    return cleaned.strip("_") or "unknown"


def build_target_path(base_dir: Path, item: Announcement) -> Path:
    stock = _safe_name(item.stock_code)
    news_id = str(item.news_id)
    ext = item.file_ext.lower() or "dat"
    filename = f"{item.release_time.replace('/', '-').replace(':', '').replace(' ', '_')}_{stock}_{news_id}.{ext}"
    return base_dir / stock / filename


def download_file(item: Announcement, base_dir: Path, timeout_seconds: int = 30) -> Path:
    target_path = build_target_path(base_dir, item)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    if target_path.exists() and target_path.stat().st_size > 0:
        return target_path

    with requests.get(item.absolute_url, timeout=timeout_seconds, stream=True) as response:
        response.raise_for_status()
        with target_path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)

    return target_path


def download_url(url: str, target_path: Path, timeout_seconds: int = 30) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and target_path.stat().st_size > 0:
        return target_path

    with requests.get(url, timeout=timeout_seconds, stream=True) as response:
        response.raise_for_status()
        with target_path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)

    return target_path


def build_annual_target_path(base_dir: Path, stock_code: str, year: int, url: str) -> Path:
    suffix = Path(urlparse(url).path).suffix or ".pdf"
    safe_code = _safe_name(stock_code.zfill(5))
    filename = f"{safe_code}_{year}_annual_report{suffix}"
    return base_dir / safe_code / filename


def build_title_search_target_path(base_dir: Path, item: TitleSearchResult) -> Path:
    parsed_url = urlparse(item.url)
    suffix = Path(parsed_url.path).suffix or ".pdf"
    basename = Path(parsed_url.path).stem or "document"
    safe_code = _safe_name(item.stock_code.zfill(5))
    category = _safe_name(item.headline_category_name.lower().replace(" ", "-"))

    try:
        stamp = datetime.strptime(item.release_time, "%d/%m/%Y %H:%M").strftime("%Y-%m-%d_%H%M")
    except ValueError:
        stamp = _safe_name(item.release_time.replace("/", "-").replace(":", ""))

    filename = f"{stamp}_{safe_code}_{category}_{basename}{suffix}"
    return base_dir / safe_code / filename
