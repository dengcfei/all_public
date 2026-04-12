# Lobster Listed Reports Skill

A unified project to download and monitor newly disclosed listed-company financial reports from HKEXnews and CNINFO (巨潮资讯).

## Features

- Unified entrypoint for two providers: HKEX and CNINFO.
- Pull latest announcement records and watch continuously.
- Filter by stock code and financial-report type.
- Download newly discovered report files.
- Deduplicate with SQLite state.
- HKEX result-history workflows and PDF metric extraction are preserved.
- CNINFO workflow supports quarterly, half-year, and annual report filters.

## Project Layout

- `SKILL.md`: skill descriptor for usage discovery.
- `requirements.txt`: Python dependencies.
- `src/lobster_reports_monitor/`: unified CLI entrypoint.
- `src/reports_common/`: shared helpers (download and naming).
- `src/hkex_financial_monitor/`: HKEX implementation.
- `src/cninfo_financial_monitor/`: CNINFO implementation.
- `config.example.json`: HKEX config template.
- `config.cninfo.example.json`: CNINFO config template.

## Setup

```bash
pip install -r requirements.txt
```

## Unified Commands

HKEX pull:

```bash
python -m src.lobster_reports_monitor.cli hkex pull --pages 2 --download-dir downloads
```

CNINFO pull:

```bash
python -m src.lobster_reports_monitor.cli cninfo pull --stocks 600519 --market sse --lookback-days 365
```

CNINFO watch:

```bash
python -m src.lobster_reports_monitor.cli cninfo watch --stocks 600519 --market sse --interval 180
```

You can still call provider CLIs directly:

```bash
python -m src.hkex_financial_monitor.cli pull --pages 2
python -m src.cninfo_financial_monitor.cli pull --stocks 600519 --market sse
```

## HKEX Advanced Commands

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

For CNINFO, use `--config config.cninfo.example.json`.

## Caveats

- HKEX and CNINFO endpoint formats can change without notice.
- PDF extraction is heuristic and currently implemented for HKEX result documents.
- CNINFO filtering is title-based and may include occasional non-financial notices with similar wording.
- Resume behavior depends on local SQLite state and existing files.
- This project is for monitoring public disclosure records and should respect applicable terms of use.
