from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Iterable

from .client import HKEXClient
from .downloader import build_annual_target_path, download_file, download_url
from .filters import looks_like_financial_report
from .models import Announcement
from .state import SeenState


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor and download HKEX financial reports")
    parser.add_argument(
        "mode",
        choices=["pull", "watch", "annual-url", "download-hscei-annuals"],
        help="Run pull/watch, resolve annual URL, or download annual reports for HSCEI constituents",
    )
    parser.add_argument("--config", type=str, default=None, help="Path to optional JSON config")
    parser.add_argument("--board", default="sehk", choices=["sehk", "gem"], help="Market board")
    parser.add_argument("--window", default="latest", choices=["latest", "7days"], help="Data window")
    parser.add_argument("--pages", type=int, default=1, help="Number of feed pages to scan")
    parser.add_argument("--stocks", type=str, default="", help="Comma-separated stock codes")
    parser.add_argument("--stock", type=str, default="", help="Single stock code for annual-url mode")
    parser.add_argument("--year", type=int, default=0, help="Report year for annual-url mode")
    parser.add_argument("--include-non-financial", action="store_true", help="Include non-financial filings")
    parser.add_argument("--download-dir", default="downloads", help="Directory for downloaded files")
    parser.add_argument("--state-file", default=".data/hkex_seen.sqlite3", help="SQLite state file")
    parser.add_argument("--interval", type=int, default=300, help="Polling interval seconds for watch mode")
    parser.add_argument("--timeout-seconds", type=int, default=20, help="HTTP timeout seconds")
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
    return {value.zfill(5) for value in values}


def should_keep(item: Announcement, include_non_financial: bool, stock_set: set[str]) -> bool:
    if stock_set and item.stock_code.zfill(5) not in stock_set:
        return False
    if include_non_financial:
        return True
    return looks_like_financial_report(item)


def run_once(
    client: HKEXClient,
    state: SeenState,
    board: str,
    window: str,
    pages: int,
    include_non_financial: bool,
    stock_set: set[str],
    download_dir: Path,
) -> tuple[int, int]:
    scanned = 0
    new_hits = 0
    for item in client.fetch_pages(pages=pages, board=board, window=window, lang="c"):
        scanned += 1
        if not should_keep(item, include_non_financial=include_non_financial, stock_set=stock_set):
            continue
        if state.has_seen(item.news_id, item.stock_code):
            continue

        target = download_file(item, base_dir=download_dir)
        state.mark_seen(item.news_id, item.stock_code)
        new_hits += 1

        print(
            f"[NEW] {item.release_time} | {item.stock_code} {item.stock_name} | "
            f"{item.title} | {target}"
        )

    return scanned, new_hits


def merge_settings(args: argparse.Namespace, conf: dict) -> dict:
    return {
        "board": conf.get("board", args.board),
        "window": conf.get("window", args.window),
        "pages": int(conf.get("pages", args.pages)),
        "stocks": conf.get("stocks", args.stocks),
        "include_non_financial": bool(
            conf.get("include_non_financial", args.include_non_financial)
        ),
        "download_dir": conf.get("download_dir", args.download_dir),
        "state_file": conf.get("state_file", args.state_file),
        "interval": int(conf.get("interval", args.interval)),
        "timeout_seconds": int(conf.get("timeout_seconds", args.timeout_seconds)),
    }


def main() -> int:
    args = parse_args()

    try:
        conf = load_config(args.config)
    except Exception as exc:
        print(f"Failed to load config: {exc}", file=sys.stderr)
        return 2

    settings = merge_settings(args, conf)
    client = HKEXClient(timeout_seconds=settings["timeout_seconds"])

    if args.mode == "annual-url":
        stock = args.stock.strip() if args.stock else ""
        year = int(args.year or 0)
        if not stock or year <= 0:
            print("annual-url mode requires --stock and --year", file=sys.stderr)
            return 2

        try:
            url = client.find_annual_report_url(stock_code=stock, year=year)
        except Exception as exc:
            print(f"Failed to resolve annual report URL: {exc}", file=sys.stderr)
            return 1

        if not url:
            print(f"No annual report URL found for stock={stock} year={year}")
            return 1

        print(url)
        return 0

    if args.mode == "download-hscei-annuals":
        year = int(args.year or 0)
        if year <= 0:
            print("download-hscei-annuals mode requires --year", file=sys.stderr)
            return 2

        download_dir = Path(settings["download_dir"])
        stocks = client.get_hscei_stock_codes(language="eng")
        print(f"HSCEI constituents loaded: {len(stocks)}")

        ok = 0
        fail = 0
        for stock in stocks:
            try:
                url = client.find_annual_report_url(stock_code=stock, year=year)
                if not url:
                    print(f"[MISS] {stock} no annual report URL for {year}")
                    fail += 1
                    continue
                target = build_annual_target_path(download_dir, stock, year, url)
                download_url(url, target, timeout_seconds=settings["timeout_seconds"])
                print(f"[OK] {stock} | {url} | {target}")
                ok += 1
            except Exception as exc:
                print(f"[ERR] {stock} | {exc}")
                fail += 1

        print(f"Done. HSCEI annual download year={year}, ok={ok}, fail={fail}")
        return 0 if ok > 0 else 1

    stock_set = normalize_stock_set(settings["stocks"])
    state = SeenState(Path(settings["state_file"]))
    download_dir = Path(settings["download_dir"])

    try:
        if args.mode == "pull":
            scanned, new_hits = run_once(
                client=client,
                state=state,
                board=settings["board"],
                window=settings["window"],
                pages=settings["pages"],
                include_non_financial=settings["include_non_financial"],
                stock_set=stock_set,
                download_dir=download_dir,
            )
            print(f"Done. Scanned={scanned}, new={new_hits}")
            return 0

        # watch mode
        print(
            "Starting watch mode: "
            f"interval={settings['interval']}s pages={settings['pages']} board={settings['board']}"
        )
        while True:
            try:
                scanned, new_hits = run_once(
                    client=client,
                    state=state,
                    board=settings["board"],
                    window=settings["window"],
                    pages=settings["pages"],
                    include_non_financial=settings["include_non_financial"],
                    stock_set=stock_set,
                    download_dir=download_dir,
                )
                print(f"Cycle done. Scanned={scanned}, new={new_hits}")
            except Exception as exc:  # keep watcher alive
                print(f"Cycle error: {exc}", file=sys.stderr)
            time.sleep(max(5, settings["interval"]))

    finally:
        state.close()


if __name__ == "__main__":
    raise SystemExit(main())
