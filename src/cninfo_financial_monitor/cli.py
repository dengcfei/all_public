from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from .client import CninfoClient
from .downloader import download_file
from .filters import looks_like_financial_report
from .state import SeenState

_ALLOWED_TYPES = {"annual", "half", "quarter"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor and download CNINFO financial reports")
    parser.add_argument("mode", choices=["pull", "watch"], help="Run one-time pull or continuous watch")
    parser.add_argument("--config", type=str, default=None, help="Path to optional JSON config")
    parser.add_argument("--market", type=str, default="szse", choices=["szse", "sse"], help="CNINFO market")
    parser.add_argument("--pages", type=int, default=3, help="Number of pages per stock")
    parser.add_argument("--page-size", type=int, default=30, help="Rows per page")
    parser.add_argument("--stocks", type=str, default="", help="Comma-separated stock codes")
    parser.add_argument(
        "--types",
        type=str,
        default="quarter,half,annual",
        help="Comma-separated report types: quarter, half, annual",
    )
    parser.add_argument("--include-summary", action="store_true", help="Include summary reports")
    parser.add_argument("--download-dir", type=str, default="downloads", help="Directory for downloaded files")
    parser.add_argument("--state-file", type=str, default=".data/cninfo_seen.sqlite3", help="SQLite state file")
    parser.add_argument("--interval", type=int, default=180, help="Polling interval seconds in watch mode")
    parser.add_argument("--timeout-seconds", type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument("--start-date", type=str, default="", help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default="", help="End date YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=30, help="Default lookback days")
    return parser.parse_args()


def load_config(path: str | None) -> dict:
    if not path:
        return {}
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    return json.loads(config_path.read_text(encoding="utf-8"))


def normalize_stock_set(raw: str | list[str] | None) -> set[str]:
    if raw is None:
        return set()
    if isinstance(raw, list):
        values = raw
    else:
        values = [part.strip() for part in str(raw).split(",") if part.strip()]
    return {value.zfill(6) for value in values}


def parse_report_types(raw: str | list[str] | None) -> set[str]:
    if raw is None:
        values = ["quarter", "half", "annual"]
    elif isinstance(raw, list):
        values = [str(value).strip().lower() for value in raw if str(value).strip()]
    else:
        values = [part.strip().lower() for part in str(raw).split(",") if part.strip()]

    if not values:
        values = ["quarter", "half", "annual"]

    invalid = [value for value in values if value not in _ALLOWED_TYPES]
    if invalid:
        raise ValueError(
            "Unsupported report types: " + ", ".join(invalid) + ". Allowed: quarter, half, annual"
        )
    return set(values)


def resolve_date_window(start_date: str, end_date: str, lookback_days: int) -> tuple[str, str]:
    today = datetime.now().date()
    end = end_date.strip() if end_date else today.strftime("%Y-%m-%d")

    if start_date:
        start = start_date.strip()
    else:
        start = (today - timedelta(days=max(1, lookback_days))).strftime("%Y-%m-%d")

    # Validate format.
    datetime.strptime(start, "%Y-%m-%d")
    datetime.strptime(end, "%Y-%m-%d")
    return start, end


def merge_settings(args: argparse.Namespace, conf: dict) -> dict:
    return {
        "market": conf.get("market", args.market),
        "pages": int(conf.get("pages", args.pages)),
        "page_size": int(conf.get("page_size", args.page_size)),
        "stocks": conf.get("stocks", args.stocks),
        "types": conf.get("types", args.types),
        "include_summary": bool(conf.get("include_summary", args.include_summary)),
        "download_dir": conf.get("download_dir", args.download_dir),
        "state_file": conf.get("state_file", args.state_file),
        "interval": int(conf.get("interval", args.interval)),
        "timeout_seconds": int(conf.get("timeout_seconds", args.timeout_seconds)),
        "start_date": conf.get("start_date", args.start_date),
        "end_date": conf.get("end_date", args.end_date),
        "lookback_days": int(conf.get("lookback_days", args.lookback_days)),
    }


def run_once(
    client: CninfoClient,
    state: SeenState,
    market: str,
    pages: int,
    page_size: int,
    stocks: list[str],
    report_types: set[str],
    include_summary: bool,
    start_date: str,
    end_date: str,
    download_dir: Path,
) -> tuple[int, int]:
    scanned = 0
    new_hits = 0

    for item in client.fetch_pages(
        pages=pages,
        page_size=page_size,
        market=market,
        start_date=start_date,
        end_date=end_date,
        stocks=stocks,
    ):
        scanned += 1

        stock_code = item.stock_code.zfill(6)
        if stocks and stock_code not in set(stocks):
            continue

        if not looks_like_financial_report(
            title=item.title,
            report_types=report_types,
            include_summary=include_summary,
        ):
            continue

        if state.has_seen(item.announcement_id, stock_code):
            continue

        target = download_file(item, base_dir=download_dir)
        state.mark_seen(
            announcement_id=item.announcement_id,
            stock_code=stock_code,
            title=item.title,
            release_time=item.release_time,
            url=item.absolute_url,
            file_path=str(target),
        )

        new_hits += 1
        print(
            f"[NEW] {item.release_time} | {stock_code} {item.stock_name} | "
            f"{item.title} | {target}"
        )

    return scanned, new_hits


def main() -> int:
    args = parse_args()

    try:
        conf = load_config(args.config)
        settings = merge_settings(args, conf)
        report_types = parse_report_types(settings["types"])
        stock_set = normalize_stock_set(settings["stocks"])
        start_date, end_date = resolve_date_window(
            start_date=settings["start_date"],
            end_date=settings["end_date"],
            lookback_days=settings["lookback_days"],
        )
    except Exception as exc:
        print(f"Invalid settings: {exc}", file=sys.stderr)
        return 2

    client = CninfoClient(timeout_seconds=settings["timeout_seconds"])
    state = SeenState(Path(settings["state_file"]))
    download_dir = Path(settings["download_dir"])
    stocks = sorted(stock_set)

    try:
        if args.mode == "pull":
            scanned, new_hits = run_once(
                client=client,
                state=state,
                market=settings["market"],
                pages=settings["pages"],
                page_size=settings["page_size"],
                stocks=stocks,
                report_types=report_types,
                include_summary=settings["include_summary"],
                start_date=start_date,
                end_date=end_date,
                download_dir=download_dir,
            )
            print(f"Done. Scanned={scanned}, new={new_hits}, window={start_date}~{end_date}")
            return 0

        print(
            "Starting watch mode: "
            f"interval={settings['interval']}s pages={settings['pages']} page_size={settings['page_size']} "
            f"market={settings['market']} window={start_date}~{end_date}"
        )
        while True:
            try:
                scanned, new_hits = run_once(
                    client=client,
                    state=state,
                    market=settings["market"],
                    pages=settings["pages"],
                    page_size=settings["page_size"],
                    stocks=stocks,
                    report_types=report_types,
                    include_summary=settings["include_summary"],
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
