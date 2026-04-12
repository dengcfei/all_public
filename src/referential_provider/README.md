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
