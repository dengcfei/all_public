from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from .client import SecClient
from .downloader import download_filing_with_user_agent
from .filters import looks_like_financial_report
from .state import SeenState

_ALLOWED_TYPES = {"annual", "quarter", "half"}
_DEFAULT_USER_AGENT = "lobster-reports-monitor/1.0 (contact: dev@example.com)"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor and download SEC financial reports")
    parser.add_argument("mode", choices=["pull", "watch"], help="Run one-time pull or continuous watch")
    parser.add_argument("--config", type=str, default=None, help="Path to optional JSON config")
    parser.add_argument("--cik", type=str, default="", help="SEC issuer CIK")
    parser.add_argument("--ticker", type=str, default="", help="US ticker symbol, e.g. AAPL")
    parser.add_argument(
        "--types",
        type=str,
        default="quarter,annual",
        help="Comma-separated report types: quarter, annual, half",
    )
    parser.add_argument("--download-dir", type=str, default="downloads", help="Directory for downloaded files")
    parser.add_argument("--state-file", type=str, default=".data/sec_seen.sqlite3", help="SQLite state file")
    parser.add_argument("--interval", type=int, default=300, help="Polling interval seconds in watch mode")
    parser.add_argument("--timeout-seconds", type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument("--user-agent", type=str, default=_DEFAULT_USER_AGENT, help="SEC-compliant user agent")
    parser.add_argument("--start-date", type=str, default="", help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default="", help="End date YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=1095, help="Default lookback days")
    return parser.parse_args()


def load_config(path: str | None) -> dict:
    if not path:
        return {}
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    return json.loads(config_path.read_text(encoding="utf-8"))


def parse_report_types(raw: str | list[str] | None) -> set[str]:
    if raw is None:
        values = ["quarter", "annual"]
    elif isinstance(raw, list):
        values = [str(value).strip().lower() for value in raw if str(value).strip()]
    else:
        values = [part.strip().lower() for part in str(raw).split(",") if part.strip()]

    if not values:
        values = ["quarter", "annual"]

    invalid = [value for value in values if value not in _ALLOWED_TYPES]
    if invalid:
        raise ValueError(
            "Unsupported report types: " + ", ".join(invalid) + ". Allowed: quarter, annual, half"
        )
    return set(values)


def resolve_date_window(start_date: str, end_date: str, lookback_days: int) -> tuple[str, str]:
    today = datetime.now().date()
    end = end_date.strip() if end_date else today.strftime("%Y-%m-%d")

    if start_date:
        start = start_date.strip()
    else:
        start = (today - timedelta(days=max(1, lookback_days))).strftime("%Y-%m-%d")

    datetime.strptime(start, "%Y-%m-%d")
    datetime.strptime(end, "%Y-%m-%d")
    return start, end


def merge_settings(args: argparse.Namespace, conf: dict) -> dict:
    return {
        "cik": conf.get("cik", args.cik),
        "ticker": conf.get("ticker", args.ticker),
        "types": conf.get("types", args.types),
        "download_dir": conf.get("download_dir", args.download_dir),
        "state_file": conf.get("state_file", args.state_file),
        "interval": int(conf.get("interval", args.interval)),
        "timeout_seconds": int(conf.get("timeout_seconds", args.timeout_seconds)),
        "user_agent": conf.get("user_agent", args.user_agent),
        "start_date": conf.get("start_date", args.start_date),
        "end_date": conf.get("end_date", args.end_date),
        "lookback_days": int(conf.get("lookback_days", args.lookback_days)),
    }


def run_once(
    client: SecClient,
    state: SeenState,
    cik: str,
    ticker: str,
    company_name: str,
    report_types: set[str],
    start_date: str,
    end_date: str,
    download_dir: Path,
) -> tuple[int, int]:
    scanned = 0
    new_hits = 0

    try:
        filings = client.fetch_recent_filings(cik=cik, ticker=ticker, company_name=company_name)
    except Exception as exc:
        print(f"Direct SEC API failed, fallback to sec-edgar-downloader: {exc}", file=sys.stderr)
        return run_once_with_package_fallback(
            state=state,
            cik=cik,
            ticker=ticker,
            report_types=report_types,
            start_date=start_date,
            end_date=end_date,
            download_dir=download_dir,
        )

    for filing in filings:
        scanned += 1

        if filing.filing_date < start_date or filing.filing_date > end_date:
            continue
        if not looks_like_financial_report(filing, report_types=report_types):
            continue
        if state.has_seen(filing.filing_id, filing.cik):
            continue

        try:
            target = download_filing_with_user_agent(
                filing,
                base_dir=download_dir,
                timeout_seconds=client.timeout_seconds,
                user_agent=client.session.headers.get("User-Agent", ""),
            )
            if target.suffix.lower() == ".txt":
                source_url = filing.txt_document_url
            else:
                source_url = filing.primary_document_url

            state.mark_seen(
                filing_id=filing.filing_id,
                cik=filing.cik,
                ticker=filing.ticker,
                company_name=filing.company_name,
                form_type=filing.form_type,
                filing_date=filing.filing_date,
                report_date=filing.report_date,
                url=source_url,
                file_path=str(target),
            )

            new_hits += 1
            print(
                f"[NEW] {filing.filing_date} | {filing.ticker or filing.cik} {filing.company_name} | "
                f"{filing.form_type} | {filing.description or filing.primary_document} | {target}"
            )
        except Exception as exc:
            print(
                f"[WARN] {filing.filing_date} | {filing.form_type} | {filing.filing_id} | {exc}",
                file=sys.stderr,
            )
        time.sleep(0.3)

    return scanned, new_hits


def run_once_with_package_fallback(
    state: SeenState,
    cik: str,
    ticker: str,
    report_types: set[str],
    start_date: str,
    end_date: str,
    download_dir: Path,
) -> tuple[int, int]:
    try:
        from sec_edgar_downloader import Downloader
    except Exception as exc:
        raise RuntimeError("sec-edgar-downloader is not installed") from exc

    issuer = ticker or cik
    package_root = download_dir / ".sec_package"
    package_root.mkdir(parents=True, exist_ok=True)

    # In this environment, SEC may reject email-like tokens in downloader identity.
    downloader = Downloader("LobsterBot", "research-bot", download_folder=package_root)

    form_types: list[str] = []
    if "annual" in report_types:
        form_types.append("10-K")
    if "quarter" in report_types:
        form_types.append("10-Q")
    if "half" in report_types:
        form_types.append("6-K")

    scanned = 0
    for form in form_types:
        try:
            scanned += int(
                downloader.get(
                    form,
                    issuer,
                    after=start_date,
                    before=end_date,
                    download_details=False,
                )
            )
        except Exception as exc:
            print(f"[WARN] fallback get failed for {form}: {exc}", file=sys.stderr)

    source_root = package_root / "sec-edgar-filings" / issuer
    if not source_root.exists():
        return scanned, 0

    target_dir = download_dir / issuer.upper()
    target_dir.mkdir(parents=True, exist_ok=True)

    new_hits = 0
    for form_dir in sorted(source_root.iterdir()):
        if not form_dir.is_dir():
            continue
        form = form_dir.name
        for accession_dir in sorted(form_dir.iterdir()):
            if not accession_dir.is_dir():
                continue
            submission_path = accession_dir / "full-submission.txt"
            if not submission_path.exists():
                continue

            filing_id = accession_dir.name
            if state.has_seen(filing_id, cik):
                continue

            target_path = target_dir / f"{issuer.upper()}_{form}_{filing_id}.txt"
            if not target_path.exists() or target_path.stat().st_size == 0:
                shutil.copy2(submission_path, target_path)

            state.mark_seen(
                filing_id=filing_id,
                cik=cik,
                ticker=issuer.upper(),
                company_name=issuer.upper(),
                form_type=form,
                filing_date="",
                report_date="",
                url=str(submission_path),
                file_path=str(target_path),
            )
            new_hits += 1
            print(f"[NEW] fallback | {issuer.upper()} | {form} | {target_path}")

    return scanned, new_hits


def main() -> int:
    args = parse_args()

    try:
        conf = load_config(args.config)
        settings = merge_settings(args, conf)
        report_types = parse_report_types(settings["types"])
        start_date, end_date = resolve_date_window(
            start_date=settings["start_date"],
            end_date=settings["end_date"],
            lookback_days=settings["lookback_days"],
        )
    except Exception as exc:
        print(f"Invalid settings: {exc}", file=sys.stderr)
        return 2

    user_agent = str(settings["user_agent"] or _DEFAULT_USER_AGENT).strip()
    if "example.com" in user_agent.lower():
        print(
            "Warning: --user-agent is using placeholder contact. Consider setting a real contact for SEC requests.",
            file=sys.stderr,
        )

    client = SecClient(timeout_seconds=settings["timeout_seconds"], user_agent=user_agent)

    try:
        cik, ticker, company_name = client.resolve_issuer(cik=settings["cik"], ticker=settings["ticker"])
    except Exception as exc:
        print(f"Failed to resolve SEC issuer: {exc}", file=sys.stderr)
        return 2

    state = SeenState(Path(settings["state_file"]))
    download_dir = Path(settings["download_dir"])

    try:
        if args.mode == "pull":
            scanned, new_hits = run_once(
                client=client,
                state=state,
                cik=cik,
                ticker=ticker,
                company_name=company_name,
                report_types=report_types,
                start_date=start_date,
                end_date=end_date,
                download_dir=download_dir,
            )
            print(
                f"Done. issuer={ticker or cik}, scanned={scanned}, new={new_hits}, "
                f"window={start_date}~{end_date}"
            )
            return 0

        print(
            "Starting watch mode: "
            f"issuer={ticker or cik} interval={settings['interval']}s window={start_date}~{end_date}"
        )
        while True:
            try:
                scanned, new_hits = run_once(
                    client=client,
                    state=state,
                    cik=cik,
                    ticker=ticker,
                    company_name=company_name,
                    report_types=report_types,
                    start_date=start_date,
                    end_date=end_date,
                    download_dir=download_dir,
                )
                print(f"Cycle done. Scanned={scanned}, new={new_hits}")
            except Exception as exc:
                print(f"Cycle error: {exc}", file=sys.stderr)

            time.sleep(max(5, settings["interval"]))

    finally:
        state.close()


if __name__ == "__main__":
    raise SystemExit(main())
