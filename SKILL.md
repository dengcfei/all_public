---
name: lobster-hkex-reports-skill
description: "Use when: monitoring latest HKEX company financial reports, downloading filings from HKEXnews, and tracking new disclosures by stock code."
---

# HKEX Financial Report Monitor Skill

This skill provides a practical workflow and scripts to monitor HKEX listed-company disclosures and download new financial reports.

## What This Skill Does

- Pulls latest disclosure records from HKEX JSON feed used by HKEXnews.
- Filters likely financial reports (annual/interim/results/ESG and related statements).
- Tracks seen announcement IDs to avoid duplicate processing.
- Downloads newly discovered report files.

## Quick Start

1. Install dependencies:

   pip install -r requirements.txt

2. Pull once:

   python -m src.hkex_financial_monitor.cli pull --pages 2 --download-dir downloads

3. Watch continuously:

   python -m src.hkex_financial_monitor.cli watch --interval 300 --pages 1

## Typical Usage

- Track all main-board announcements likely to be financial reports.
- Track specific stock codes with `--stocks 00005,00700`.
- Use `watch` mode for periodic monitoring.

## Notes

- Data source is HKEX public disclosure feed endpoint used by the HKEXnews latest announcements page.
- Endpoint format may change in future; verify `src/hkex_financial_monitor/client.py` if needed.
