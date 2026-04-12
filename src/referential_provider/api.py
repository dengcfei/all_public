from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse

from .config import DBConfig
from .repository import ReferentialRepository


def create_app(config: DBConfig | None = None) -> FastAPI:
    app = FastAPI(title="Referential Provider API", version="0.1.0")
    repo = ReferentialRepository(config or DBConfig.from_env())
    admin_token = os.getenv("REFERENTIAL_ADMIN_TOKEN", "")

    @app.get("/", response_class=HTMLResponse)
    def home() -> str:
        return (
            "<html><body><h2>Referential Provider API</h2>"
            "<p>Minimal REST docs: <a href='/rest-docs'>/rest-docs</a></p>"
            "<p>OpenAPI docs: <a href='/docs'>/docs</a></p>"
            "</body></html>"
        )

    @app.get("/rest-docs", response_class=HTMLResponse)
    def rest_docs() -> str:
        return """
<!doctype html>
<html lang="en">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Referential Provider - REST Docs</title>
        <style>
            body { font-family: -apple-system, Segoe UI, Arial, sans-serif; max-width: 960px; margin: 24px auto; padding: 0 16px; line-height: 1.5; }
            h1, h2 { margin-bottom: 8px; }
            .card { border: 1px solid #ddd; border-radius: 8px; padding: 14px; margin: 10px 0; }
            code, pre { background: #f6f8fa; border-radius: 6px; }
            pre { padding: 10px; overflow: auto; }
            .muted { color: #666; }
        </style>
    </head>
    <body>
        <h1>Referential Provider API</h1>
        <p class="muted">Minimal REST documentation page.</p>

        <div class="card">
            <h2>GET /health</h2>
            <p>Check database connectivity.</p>
            <pre>curl -s http://127.0.0.1:8010/health</pre>
        </div>

        <div class="card">
            <h2>GET /lookup</h2>
            <p>Query by ticker, English name, Chinese name, or alias.</p>
            <p>Parameters: <code>q</code> (required), <code>limit</code> (optional, 1-200)</p>
            <pre>curl -s "http://127.0.0.1:8010/lookup?q=苹果&limit=20"</pre>
        </div>

        <div class="card">
            <h2>GET /entity/{entity_id}</h2>
            <p>Fetch one entity with instruments and identifiers.</p>
            <pre>curl -s http://127.0.0.1:8010/entity/ENTITY_APPLE</pre>
        </div>

        <div class="card">
            <h2>POST /import</h2>
            <p>Import payload with upsert semantics.</p>
            <p>If <code>REFERENTIAL_ADMIN_TOKEN</code> is set, pass <code>X-Admin-Token</code>.</p>
            <pre>curl -s -X POST http://127.0.0.1:8010/import \\
    -H "Content-Type: application/json" \\
    -H "X-Admin-Token: YOUR_TOKEN" \\
    --data @src/referential_provider/sample_import.json</pre>
        </div>

        <p>OpenAPI interactive docs: <a href="/docs">/docs</a></p>
    </body>
</html>
"""

    @app.get("/health")
    def health() -> dict[str, Any]:
        return repo.healthcheck()

    @app.get("/lookup")
    def lookup(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=200)) -> dict[str, Any]:
        items = repo.lookup(q, limit=limit)
        return {"count": len(items), "items": items}

    @app.get("/entity/{entity_id}")
    def entity(entity_id: str) -> dict[str, Any]:
        item = repo.get_entity(entity_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Entity not found")
        return item

    @app.post("/import")
    def import_payload(payload: dict[str, Any], x_admin_token: str = Header(default="")) -> dict[str, Any]:
        if admin_token and x_admin_token != admin_token:
            raise HTTPException(status_code=401, detail="Unauthorized")
        result = repo.import_payload(payload)
        return {"ok": True, "imported": result}

    return app
