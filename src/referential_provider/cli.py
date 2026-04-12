from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import uvicorn

from .api import create_app
from .common_universe import CommonUniverseCollector, build_import_payload, write_snapshot
from .config import DBConfig
from .repository import ReferentialRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Referential provider: import data and query interfaces")

    parser.add_argument("--db-host", default="114.132.44.68")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", default="referential_db")
    parser.add_argument("--db-user", default="referential_user")
    parser.add_argument("--db-password", default="referential_pass123")
    parser.add_argument("--db-sslmode", default="prefer")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("health", help="Test database connectivity")

    lookup = sub.add_parser("lookup", help="Query by ticker/company/alias")
    lookup.add_argument("--query", required=True)
    lookup.add_argument("--limit", type=int, default=20)

    entity = sub.add_parser("entity", help="Fetch one entity with instruments and identifiers")
    entity.add_argument("--entity-id", required=True)

    do_import = sub.add_parser("import", help="Import payload JSON into database")
    do_import.add_argument("--input", required=True, help="JSON payload path")

    sync_common = sub.add_parser("sync-common", help="Sync common production universes into database")
    sync_common.add_argument("--apply", action="store_true", help="Apply import to database")
    sync_common.add_argument("--skip-us-enrich", action="store_true", help="Skip yfinance metadata enrichment")
    sync_common.add_argument("--us-enrich-limit", type=int, default=0, help="Optional cap on enriched US symbols")
    sync_common.add_argument(
        "--snapshot-dir",
        default="src/referential_provider/snapshots",
        help="Directory to store generated payload snapshots",
    )

    converge = sub.add_parser(
        "converge-legacy-entities",
        help="Map legacy entity IDs to canonical IDs and merge duplicate ticker/exchange entities",
    )
    converge.add_argument("--apply", action="store_true", help="Apply merge operations to database")

    serve = sub.add_parser("serve-api", help="Start HTTP API")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8010)

    return parser.parse_args()


def load_payload(path: str) -> dict:
    payload_path = Path(path)
    if not payload_path.exists():
        raise FileNotFoundError(f"Payload file not found: {payload_path}")
    return json.loads(payload_path.read_text(encoding="utf-8"))


def build_config(args: argparse.Namespace) -> DBConfig:
    return DBConfig.from_env(
        DBConfig(
            host=args.db_host,
            port=args.db_port,
            database=args.db_name,
            user=args.db_user,
            password=args.db_password,
            sslmode=args.db_sslmode,
        )
    )


def main() -> int:
    args = parse_args()
    config = build_config(args)
    repo = ReferentialRepository(config)

    try:
        if args.command == "health":
            print(json.dumps(repo.healthcheck(), ensure_ascii=False, default=str, indent=2))
            return 0

        if args.command == "lookup":
            items = repo.lookup(args.query, limit=args.limit)
            print(json.dumps({"count": len(items), "items": items}, ensure_ascii=False, default=str, indent=2))
            return 0

        if args.command == "entity":
            item = repo.get_entity(args.entity_id)
            if item is None:
                print("Entity not found", file=sys.stderr)
                return 1
            print(json.dumps(item, ensure_ascii=False, default=str, indent=2))
            return 0

        if args.command == "import":
            payload = load_payload(args.input)
            result = repo.import_payload(payload)
            print(json.dumps({"ok": True, "imported": result}, ensure_ascii=False, default=str, indent=2))
            return 0

        if args.command == "sync-common":
            collector = CommonUniverseCollector()
            records, source_stats = collector.collect_all(
                enrich_us=not args.skip_us_enrich,
                us_enrich_limit=max(0, int(args.us_enrich_limit)),
            )
            payload, payload_stats = build_import_payload(records)

            stats = {
                "source_stats": source_stats,
                "payload_stats": payload_stats,
                "apply": bool(args.apply),
                "skip_us_enrich": bool(args.skip_us_enrich),
                "us_enrich_limit": int(args.us_enrich_limit),
            }
            snapshot_path = write_snapshot(Path(args.snapshot_dir), stats=stats, payload=payload)

            output: dict[str, object] = {
                "ok": True,
                "stats": stats,
                "snapshot": str(snapshot_path),
            }
            if args.apply:
                imported = repo.import_payload(payload)
                output["imported"] = imported

            print(json.dumps(output, ensure_ascii=False, default=str, indent=2))
            return 0

        if args.command == "converge-legacy-entities":
            output = repo.converge_legacy_entities(apply=bool(args.apply))
            print(json.dumps({"ok": True, **output}, ensure_ascii=False, default=str, indent=2))
            return 0

        if args.command == "serve-api":
            app = create_app(config)
            uvicorn.run(app, host=args.host, port=args.port)
            return 0

        print(f"Unsupported command: {args.command}", file=sys.stderr)
        return 2

    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
