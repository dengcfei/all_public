from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DBConfig:
    host: str = "114.132.44.68"
    port: int = 5432
    database: str = "referential_db"
    user: str = "referential_user"
    password: str = "referential_pass123"
    sslmode: str = "prefer"

    @staticmethod
    def from_env(default: "DBConfig" | None = None) -> "DBConfig":
        base = default or DBConfig()
        return DBConfig(
            host=os.getenv("PGHOST", base.host),
            port=int(os.getenv("PGPORT", str(base.port))),
            database=os.getenv("PGDATABASE", base.database),
            user=os.getenv("PGUSER", base.user),
            password=os.getenv("PGPASSWORD", base.password),
            sslmode=os.getenv("PGSSLMODE", base.sslmode),
        )
