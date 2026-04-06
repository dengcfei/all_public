# Lobster HKEX Reports Skill

A starter project to download and monitor newly disclosed company reports from HKEXnews (Disclosure platform).

## Features

- Pull latest records from HKEX JSON data feed.
- Detect financial-report-like announcements.
- Filter by stock code.
- Deduplicate by `newsId` using SQLite state.
- Download new report files.
- One-shot mode and continuous watch mode.

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

Include non-financial documents too:

```bash
python -m src.hkex_financial_monitor.cli pull --include-non-financial
```

## Config File Example

You can also pass `--config config.example.json` and override values via CLI.

## Caveats

- HKEX endpoint format can change without notice.
- This project is for monitoring public disclosure records and should respect applicable terms of use.
