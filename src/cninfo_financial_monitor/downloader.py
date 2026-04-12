from __future__ import annotations

from pathlib import Path

from src.reports_common.download import safe_name, stream_download

from .models import Announcement


def build_target_path(base_dir: Path, item: Announcement) -> Path:
    stock = safe_name(item.stock_code)
    report_id = safe_name(item.announcement_id)
    ext = (item.adjunct_type or "pdf").lower()
    stamp = item.release_time.replace("/", "-").replace(":", "").replace(" ", "_")
    filename = f"{stamp}_{stock}_{report_id}.{ext}"
    return base_dir / stock / filename


def download_file(item: Announcement, base_dir: Path, timeout_seconds: int = 30) -> Path:
    target_path = build_target_path(base_dir, item)
    return stream_download(item.absolute_url, target_path, timeout_seconds=timeout_seconds)
