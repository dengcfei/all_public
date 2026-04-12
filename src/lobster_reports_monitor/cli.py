from __future__ import annotations

import argparse
import sys


def parse_args() -> tuple[str, list[str]]:
    parser = argparse.ArgumentParser(
        description="Unified monitor for HKEX, CNINFO, and SEC listed-company financial reports"
    )
    parser.add_argument("provider", choices=["hkex", "cninfo", "sec"], help="Data source provider")
    args, remaining = parser.parse_known_args()
    return args.provider, remaining


def _run_provider(main_func, argv: list[str]) -> int:
    original_argv = sys.argv[:]
    try:
        sys.argv = [original_argv[0]] + argv
        return int(main_func())
    finally:
        sys.argv = original_argv


def main() -> int:
    provider, provider_argv = parse_args()
    if provider == "hkex":
        from src.hkex_financial_monitor.cli import main as hkex_main

        return _run_provider(hkex_main, provider_argv)
    if provider == "sec":
        from src.sec_financial_monitor.cli import main as sec_main

        return _run_provider(sec_main, provider_argv)
    from src.cninfo_financial_monitor.cli import main as cninfo_main

    return _run_provider(cninfo_main, provider_argv)
