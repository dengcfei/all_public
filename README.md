# Lobster HKEX Reports Skill

A starter project to download and monitor newly disclosed company reports from HKEXnews (Disclosure platform).

## Features

- Pull latest records from HKEX JSON data feed.
- Detect financial-report-like announcements.
- Filter by stock code.
- Search HKEX title search for Final Results, Interim Results, and Quarterly Results.
- Extract revenue, profit, and EPS fields from downloaded result PDFs.
- Deduplicate by `newsId` using SQLite state.
- Resume processed document runs from SQLite state.
- Download new report files with concurrent workers.
- Export a CSV log for processed documents and extracted metrics.
- One-shot mode and continuous watch mode.
- Batch download 10-year results history for HSCEI constituents.

## Project Layout

- `SKILL.md`: skill descriptor for usage discovery.
- `requirements.txt`: Python dependencies.
- `src/hkex_financial_monitor/`: core implementation.
- `config.example.json`: optional settings template.

## Setup

```bash
pip install -r requirements.txt
```

## Commands

Pull once:

```bash
python -m src.hkex_financial_monitor.cli pull --pages 2 --download-dir downloads
```

Watch every 5 minutes:

```bash
python -m src.hkex_financial_monitor.cli watch --interval 300 --pages 1 --download-dir downloads
```

Track specific stock codes only:

```bash
python -m src.hkex_financial_monitor.cli pull --stocks 00005,00700
```

Resolve annual report URL by stock code + year:

```bash
python -m src.hkex_financial_monitor.cli annual-url --stock 09988 --year 2025
```

Download 2025 annual reports for all HSCEI constituents:

```bash
python -m src.hkex_financial_monitor.cli download-hscei-annuals --year 2025 --download-dir downloads
```

Download the latest result announcement for one stock code and extract key fields:

```bash
python -m src.hkex_financial_monitor.cli download-latest-results --stock 00700 --download-dir downloads
```

Download 10 years of final/interim/quarterly results for specific stocks:

```bash
python -m src.hkex_financial_monitor.cli download-results-history --stocks 00700,00941 --years 10 --download-dir downloads --workers 6
```

Download 10 years of final/interim/quarterly results for current HSCEI constituents:

```bash
python -m src.hkex_financial_monitor.cli download-hscei-results-history --years 10 --download-dir downloads --workers 8
```

Test the HSCEI workflow on a subset first:

```bash
python -m src.hkex_financial_monitor.cli download-hscei-results-history --years 10 --limit 3 --download-dir downloads --workers 4
```

Restrict to only final and interim results:

```bash
python -m src.hkex_financial_monitor.cli download-results-history --stocks 00700 --result-types final,interim
```

Reprocess documents even if they were already completed before:

```bash
python -m src.hkex_financial_monitor.cli download-hscei-results-history --years 10 --force-reprocess
```

Write the processing log to a dedicated CSV file:

```bash
python -m src.hkex_financial_monitor.cli download-hscei-results-history --years 10 --log-csv downloads/hscei_results_log.csv
```

Include non-financial documents too:

```bash
python -m src.hkex_financial_monitor.cli pull --include-non-financial
```

## Config File Example

You can also pass `--config config.example.json` and override values via CLI.

## Caveats

- HKEX endpoint format can change without notice.
- PDF extraction is heuristic: it works well on the tested English result announcements, but some issuers may use different wording or layouts.
- `download-hscei-results-history` uses the current HSCEI constituent list exposed by HSI, then pulls each constituent's past disclosures from HKEX.
- Resume behavior skips only documents that were already processed successfully and still exist on disk.
- This project is for monitoring public disclosure records and should respect applicable terms of use.
