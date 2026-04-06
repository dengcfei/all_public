from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Iterable

from .client import HKEXClient, RESULT_HEADLINE_CATEGORIES
from .downloader import (
    build_annual_target_path,
    build_title_search_target_path,
    download_file,
    download_url,
)
from .extractor import PDFFinancialExtractor
from .filters import looks_like_financial_report
from .models import Announcement, DocumentProcessResult, TitleSearchResult
from .state import SeenState


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor and download HKEX financial reports")
    parser.add_argument(
        "mode",
        choices=[
            "pull",
            "watch",
            "annual-url",
            "download-hscei-annuals",
            "download-results-history",
            "download-hscei-results-history",
            "download-latest-results",
        ],
        help="Run pull/watch, resolve annual URL, or download HKEX financial reports by search workflow",
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
    parser.add_argument("--years", type=int, default=10, help="How many calendar years of history to scan")
    parser.add_argument(
        "--result-types",
        type=str,
        default="final,interim,quarterly",
        help="Comma-separated result types: final, interim, quarterly",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional cap on number of stocks to process")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent download/extraction workers")
    parser.add_argument(
        "--log-csv",
        type=str,
        default="",
        help="Optional CSV path for processed-document log; defaults to <download-dir>/hkex_results_log.csv",
    )
    parser.add_argument(
        "--pdf-max-pages",
        type=int,
        default=12,
        help="Maximum number of PDF pages to scan when extracting financial metrics",
    )
    parser.add_argument(
        "--force-reprocess",
        action="store_true",
        help="Ignore prior successful state and reprocess documents",
    )
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


def parse_result_types(raw: str | list[str] | None) -> list[str]:
    if raw is None:
        values = list(RESULT_HEADLINE_CATEGORIES.keys())
    elif isinstance(raw, list):
        values = [str(value).strip().lower() for value in raw if str(value).strip()]
    else:
        values = [part.strip().lower() for part in str(raw).split(",") if part.strip()]

    if not values:
        values = list(RESULT_HEADLINE_CATEGORIES.keys())

    invalid = [value for value in values if value not in RESULT_HEADLINE_CATEGORIES]
    if invalid:
        raise ValueError(
            "Unsupported result types: "
            + ", ".join(invalid)
            + ". Allowed: "
            + ", ".join(RESULT_HEADLINE_CATEGORIES)
        )
    return values


def compute_history_window(years: int) -> tuple[str, str]:
    if years <= 0:
        raise ValueError("--years must be greater than 0")
    today = date.today()
    from_year = today.year - years + 1
    return f"{from_year}0101", today.strftime("%Y%m%d")


def apply_limit(values: list[str], limit: int) -> list[str]:
    if limit <= 0:
        return values
    return values[:limit]


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
        "include_non_financial": bool(conf.get("include_non_financial", args.include_non_financial)),
        "download_dir": conf.get("download_dir", args.download_dir),
        "state_file": conf.get("state_file", args.state_file),
        "interval": int(conf.get("interval", args.interval)),
        "timeout_seconds": int(conf.get("timeout_seconds", args.timeout_seconds)),
        "years": int(conf.get("years", args.years)),
        "result_types": conf.get("result_types", args.result_types),
        "limit": int(conf.get("limit", args.limit)),
        "workers": max(1, int(conf.get("workers", args.workers))),
        "log_csv": conf.get("log_csv", args.log_csv),
        "pdf_max_pages": max(1, int(conf.get("pdf_max_pages", args.pdf_max_pages))),
        "force_reprocess": bool(conf.get("force_reprocess", args.force_reprocess)),
    }


def collect_search_results(
    client: HKEXClient,
    stocks: list[str],
    from_date: str,
    to_date: str,
    result_types: list[str],
    latest_only: bool,
) -> tuple[list[TitleSearchResult], int, int]:
    results: list[TitleSearchResult] = []
    ok = 0
    fail = 0
    for stock in stocks:
        try:
            stock_results = client.search_result_announcements(
                stock_code=stock,
                from_date=from_date,
                to_date=to_date,
                result_types=result_types,
            )
            if latest_only:
                stock_results = stock_results[:1]
            if not stock_results:
                print(f"[MISS] {stock} no matching results in {from_date}-{to_date}")
                fail += 1
                continue
            results.extend(stock_results)
            ok += 1
            print(f"[FOUND] {stock} matched {len(stock_results)} documents")
        except Exception as exc:
            print(f"[ERR] {stock} | search failed | {exc}")
            fail += 1
    return results, ok, fail


def process_document(
    item: TitleSearchResult,
    download_dir: Path,
    timeout_seconds: int,
    extractor: PDFFinancialExtractor,
) -> DocumentProcessResult:
    target = build_title_search_target_path(download_dir, item)
    downloaded = not (target.exists() and target.stat().st_size > 0)

    try:
        download_url(item.url, target, timeout_seconds=timeout_seconds)
        extraction = None
        if target.suffix.lower() == ".pdf":
            extraction = extractor.extract(target)
        return DocumentProcessResult(
            stock_code=item.stock_code,
            stock_name=item.stock_name,
            release_time=item.release_time,
            headline_category_code=item.headline_category_code,
            headline_category_name=item.headline_category_name,
            title=item.title,
            url=item.url,
            file_path=str(target),
            status="done",
            downloaded=downloaded,
            extraction=extraction,
        )
    except Exception as exc:
        return DocumentProcessResult(
            stock_code=item.stock_code,
            stock_name=item.stock_name,
            release_time=item.release_time,
            headline_category_code=item.headline_category_code,
            headline_category_name=item.headline_category_name,
            title=item.title,
            url=item.url,
            file_path=str(target),
            status="error",
            downloaded=downloaded,
            error_message=str(exc),
        )


def process_search_results(
    state: SeenState,
    items: list[TitleSearchResult],
    download_dir: Path,
    timeout_seconds: int,
    workers: int,
    log_csv: Path,
    pdf_max_pages: int,
    force_reprocess: bool,
) -> tuple[int, int, int]:
    skipped = 0
    done = 0
    fail = 0
    extractor = PDFFinancialExtractor(max_pages=pdf_max_pages)

    futures = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for item in items:
            target = build_title_search_target_path(download_dir, item)
            if not force_reprocess and state.has_processed_document(item.url, target):
                skipped += 1
                print(f"[SKIP] {item.stock_code} | {item.release_time} | {item.title}")
                continue
            future = executor.submit(
                process_document,
                item,
                download_dir,
                timeout_seconds,
                extractor,
            )
            futures[future] = item

        for future in as_completed(futures):
            result = future.result()
            state.record_processed_document(result)
            if result.status == "done":
                extraction = result.extraction
                if extraction is not None:
                    print(
                        f"[OK] {result.stock_code} | {result.headline_category_name} | {result.release_time} | "
                        f"revenue={extraction.revenue or '-'} | profit={extraction.profit_attributable or extraction.profit_for_period or '-'} | "
                        f"{result.file_path}"
                    )
                else:
                    print(
                        f"[OK] {result.stock_code} | {result.headline_category_name} | {result.release_time} | "
                        f"{result.file_path}"
                    )
                done += 1
            else:
                print(f"[ERR] {result.stock_code} | {result.release_time} | {result.error_message}")
                fail += 1

    state.export_processed_documents_csv(log_csv)
    return done, fail, skipped


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

    if args.mode in {
        "download-results-history",
        "download-hscei-results-history",
        "download-latest-results",
    }:
        try:
            result_types = parse_result_types(settings["result_types"])
            from_date, to_date = compute_history_window(settings["years"])
        except Exception as exc:
            print(f"Invalid history search settings: {exc}", file=sys.stderr)
            return 2

        if args.mode == "download-hscei-results-history":
            stocks = client.get_hscei_stock_codes(language="eng")
            stocks = apply_limit(stocks, settings["limit"])
            print(f"HSCEI constituents loaded: {len(stocks)}")
        else:
            stocks = sorted(normalize_stock_set(settings["stocks"]))
            if args.stock:
                stocks = sorted(set(stocks) | {args.stock.strip().zfill(5)})
            stocks = apply_limit(stocks, settings["limit"])

        if not stocks:
            print("This mode requires --stocks, --stock, or HSCEI constituents", file=sys.stderr)
            return 2

        collected, search_ok, search_fail = collect_search_results(
            client=client,
            stocks=stocks,
            from_date=from_date,
            to_date=to_date,
            result_types=result_types,
            latest_only=args.mode == "download-latest-results",
        )
        log_csv = Path(settings["log_csv"]) if settings["log_csv"] else Path(settings["download_dir"]) / "hkex_results_log.csv"
        state = SeenState(Path(settings["state_file"]))
        try:
            done, fail, skipped = process_search_results(
                state=state,
                items=collected,
                download_dir=Path(settings["download_dir"]),
                timeout_seconds=settings["timeout_seconds"],
                workers=settings["workers"],
                log_csv=log_csv,
                pdf_max_pages=settings["pdf_max_pages"],
                force_reprocess=settings["force_reprocess"],
            )
        finally:
            state.close()

        print(
            f"Done. mode={args.mode}, stocks={len(stocks)}, search_ok={search_ok}, search_fail={search_fail}, "
            f"documents_done={done}, documents_fail={fail}, skipped={skipped}, workers={settings['workers']}, "
            f"window={from_date}-{to_date}, log_csv={log_csv}"
        )
        return 0 if done > 0 or skipped > 0 else 1

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
            except Exception as exc:
                print(f"Cycle error: {exc}", file=sys.stderr)
            time.sleep(max(5, settings["interval"]))

    finally:
        state.close()


if __name__ == "__main__":
    raise SystemExit(main())
