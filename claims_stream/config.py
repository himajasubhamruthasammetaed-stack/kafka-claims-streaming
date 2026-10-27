"""Settings from environment variables, with defaults that match docker-compose.yml."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Settings:
    bootstrap: str
    topic: str
    dlq_topic: str
    group_id: str
    pg_dsn: str
    partitions: int


def settings(**overrides) -> Settings:
    """Read the environment when called (not at import time) and apply overrides."""
    base = Settings(
        bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        topic=os.getenv("CLAIMS_TOPIC", "claims.events"),
        dlq_topic=os.getenv("DLQ_TOPIC", "claims.dlq"),
        group_id=os.getenv("GROUP_ID", "claims-loader"),
        pg_dsn=os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/claims"),
        partitions=int(os.getenv("CLAIMS_PARTITIONS", "3")),
    )
    return replace(base, **overrides)
