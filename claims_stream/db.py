"""PostgreSQL sink.

The whole point of this module is one SQL statement: an upsert that only
overwrites a row when the incoming event is newer than what's stored.

* Replaying the same event does nothing (same event_ts, so the WHERE is false).
* An older event that arrives late does nothing either.
* A newer event replaces the row.

That makes the sink idempotent, which is what lets the consumer use plain
at-least-once delivery: if it crashes after writing but before committing the
Kafka offset, the messages come again and the table ends up the same.
"""

from __future__ import annotations

from pathlib import Path

import psycopg

from .schema import ClaimEvent

SCHEMA_SQL = (Path(__file__).resolve().parent.parent / "sql" / "schema.sql").read_text()

UPSERT_SQL = """
INSERT INTO claims (
    claim_id, member_id, provider_npi, service_date, billed_amount, paid_amount,
    status, event_ts, last_event_id, source_partition, source_offset
)
VALUES (
    %(claim_id)s, %(member_id)s, %(provider_npi)s, %(service_date)s, %(billed_amount)s,
    %(paid_amount)s, %(status)s, %(event_ts)s, %(event_id)s, %(partition)s, %(offset)s
)
ON CONFLICT (claim_id) DO UPDATE SET
    member_id        = EXCLUDED.member_id,
    provider_npi     = EXCLUDED.provider_npi,
    service_date     = EXCLUDED.service_date,
    billed_amount    = EXCLUDED.billed_amount,
    paid_amount      = EXCLUDED.paid_amount,
    status           = EXCLUDED.status,
    event_ts         = EXCLUDED.event_ts,
    last_event_id    = EXCLUDED.last_event_id,
    source_partition = EXCLUDED.source_partition,
    source_offset    = EXCLUDED.source_offset,
    updated_at       = now()
WHERE claims.event_ts < EXCLUDED.event_ts
"""


def to_params(event: ClaimEvent, partition: int, offset: int) -> dict:
    return {
        "claim_id": event.claim_id,
        "member_id": event.member_id,
        "provider_npi": event.provider_npi,
        "service_date": event.service_date,
        "billed_amount": event.billed_amount,
        "paid_amount": event.paid_amount,
        "status": event.status,
        "event_ts": event.event_ts,
        "event_id": event.event_id,
        "partition": partition,
        "offset": offset,
    }


class PostgresStore:
    def __init__(self, dsn: str):
        # autocommit=True so every write sits inside an explicit transaction() block.
        self.conn = psycopg.connect(dsn, autocommit=True)

    def create_schema(self) -> None:
        with self.conn.transaction():
            self.conn.execute(SCHEMA_SQL)

    def upsert(self, rows: list[tuple[ClaimEvent, int, int]]) -> None:
        """Write a batch in ONE transaction: all rows land or none do."""
        if not rows:
            return
        with self.conn.transaction():
            with self.conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, [to_params(e, p, o) for e, p, o in rows])

    def close(self) -> None:
        self.conn.close()
