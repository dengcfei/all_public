---
name: lobster-hkex-reports-skill
description: "Use when: monitoring listed-company financial reports from HKEX, CNINFO, or SEC, downloading filings, and tracking new disclosures by stock code or ticker."
---

# Listed Financial Report Monitor Skill

This skill provides a practical workflow and scripts to monitor listed-company disclosures from HKEX, CNINFO, and SEC and download new financial reports.

## What This Skill Does

- Pulls latest disclosure records from HKEX, CNINFO, and SEC endpoints.
- Filters likely financial reports, including quarterly, half-year, and annual reports.
- Tracks seen announcement IDs to avoid duplicate processing.
- Downloads newly discovered report files.

## Quick Start

1. Install dependencies:

   pip install -r requirements.txt

2. Pull once from HKEX:

   python -m src.hkex_financial_monitor.cli pull --pages 2 --download-dir downloads

3. Pull once from CNINFO:

   python -m src.cninfo_financial_monitor.cli pull --stocks 600519 --market sse --lookback-days 365

4. Pull once from SEC:

   python -m src.sec_financial_monitor.cli pull --ticker AAPL --types quarter,annual --lookback-days 1095

5. Watch continuously:

   python -m src.lobster_reports_monitor.cli hkex watch --interval 300 --pages 1

## Typical Usage

- Track all main-board announcements likely to be financial reports.
- Track specific stock codes with `--stocks 00005,00700` (HKEX) or `--stocks 600519` (CNINFO), and tickers with `--ticker AAPL` (SEC).
- Use `watch` mode for periodic monitoring.

## Notes

- Data sources are HKEX public disclosure endpoints, CNINFO history announcement endpoint, and SEC submissions endpoint.
- Endpoint formats may change in future; verify `src/hkex_financial_monitor/client.py`, `src/cninfo_financial_monitor/client.py`, and `src/sec_financial_monitor/client.py` if needed.
