from __future__ import annotations

from pathlib import Path

import requests


def safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)
    return cleaned.strip("_") or "unknown"


def stream_download(url: str, target_path: Path, timeout_seconds: int = 30) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and target_path.stat().st_size > 0:
        return target_path

    with requests.get(url, timeout=timeout_seconds, stream=True) as response:
        response.raise_for_status()
        with target_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    handle.write(chunk)

    return target_path
