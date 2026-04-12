from __future__ import annotations

from pathlib import Path

from src.reports_common.download import safe_name, stream_download

from .models import SecFiling


def build_target_path(base_dir: Path, filing: SecFiling) -> Path:
    return build_target_path_with_ext(base_dir, filing, ext=Path(filing.primary_document).suffix.lower() or ".htm")


def build_target_path_with_ext(base_dir: Path, filing: SecFiling, ext: str) -> Path:
    issuer = safe_name((filing.ticker or filing.cik).upper())
    stamp = filing.filing_date.replace("/", "-").replace(":", "").replace(" ", "_")
    filename = f"{stamp}_{issuer}_{safe_name(filing.form_type)}_{safe_name(filing.filing_id)}{ext}"
    return base_dir / issuer / filename


def download_filing(filing: SecFiling, base_dir: Path, timeout_seconds: int = 30) -> Path:
    target_path = build_target_path(base_dir, filing)
    return stream_download(filing.document_url, target_path, timeout_seconds=timeout_seconds)


def download_filing_with_user_agent(
    filing: SecFiling,
    base_dir: Path,
    timeout_seconds: int = 30,
    user_agent: str = "",
) -> Path:
    headers = {"User-Agent": user_agent} if user_agent.strip() else None

    txt_target = build_target_path_with_ext(base_dir, filing, ext=".txt")
    try:
        return stream_download(
            filing.txt_document_url,
            txt_target,
            timeout_seconds=timeout_seconds,
            headers=headers,
        )
    except Exception:
        primary_target = build_target_path(base_dir, filing)
        return stream_download(
            filing.primary_document_url,
            primary_target,
            timeout_seconds=timeout_seconds,
            headers=headers,
        )
