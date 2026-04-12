# Referential Provider (V1)

This module provides two core capabilities:

1. Import referential data into PostgreSQL.
2. Query referential mappings via CLI and HTTP API.

## CLI Commands

Health check:

```bash
python -m src.referential_provider.cli health
```

Lookup by ticker/name/alias:

```bash
python -m src.referential_provider.cli lookup --query 苹果 --limit 20
```

Fetch one entity with instruments and identifiers:

```bash
python -m src.referential_provider.cli entity --entity-id ENTITY_APPLE
```

Import JSON payload:

```bash
python -m src.referential_provider.cli import --input src/referential_provider/sample_import.json
```

Sync common production universes (dry-run + snapshot only):

```bash
python -m src.referential_provider.cli sync-common
```

Sync common production universes and apply into PostgreSQL:

```bash
python -m src.referential_provider.cli sync-common --apply
```

Start API server:

```bash
python -m src.referential_provider.cli serve-api --host 0.0.0.0 --port 8010
```

Minimal REST docs page:

```bash
http://127.0.0.1:8010/rest-docs
```

## API Endpoints

- `GET /health`
- `GET /lookup?q=<text>&limit=<int>`
- `GET /entity/{entity_id}`
- `POST /import`
- `GET /rest-docs` (minimal human-readable REST docs)

If `REFERENTIAL_ADMIN_TOKEN` is set, `POST /import` requires `X-Admin-Token` header.

## DB Configuration

Defaults are set to your remote PostgreSQL endpoint. You can override with CLI options or env vars:

- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSSLMODE`

## Common Universe Scope (V1)

- S&P 500 (Wikipedia constituents page)
- Nasdaq-100 (Wikipedia constituents table)
- Hang Seng Index (Wikipedia constituents table)
- Hang Seng Tech (official HSI dashboard constituents endpoint, current top holdings)
- CSI300 (CSIndex constituents via Akshare)
- CSI500 (CSIndex constituents via Akshare)

The sync process is fully codified in `common_universe.py` and reusable for scheduled updates.
