"""Offline demo: same processor and PostgreSQL upsert, but no Kafka broker.

Generated messages are fed straight to the processor in batches, and the
dead-letter "topic" is a list in memory. Handy for seeing the upsert and the
validation rules work before starting Kafka.

Run:  python -m claims_stream.simulate --claims 200
"""

from __future__ import annotations

import argparse
from collections import Counter

from .config import settings
from .db import PostgresStore
from .generator import generate
from .processor import ClaimProcessor, Record


class ListDLQ:
    def __init__(self):
        self.items: list[tuple[int, str]] = []

    def send(self, record: Record, reason: str, detail: str) -> None:
        self.items.append((record.offset, reason))

    def flush(self) -> None:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the loader without Kafka")
    parser.add_argument("--claims", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--replay", action="store_true", help="process everything twice")
    args = parser.parse_args()

    batch = generate(args.claims, seed=args.seed)
    records = [Record("simulated", 0, i, k.encode(), v) for i, (k, v) in enumerate(batch.messages)]

    store = PostgresStore(settings().pg_dsn)
    store.create_schema()
    store.conn.execute("TRUNCATE claims")
    dlq = ListDLQ()
    processor = ClaimProcessor(store, dlq)

    passes = 2 if args.replay else 1
    for _ in range(passes):
        for i in range(0, len(records), args.batch_size):
            processor.process(records[i : i + args.batch_size])

    print(f"messages: {len(records)} ({batch.duplicate_count} duplicates, {batch.bad_count} bad), passes: {passes}")
    print(f"dead-lettered: {len(dlq.items)}  {dict(Counter(r for _, r in dlq.items))}")
    for status, count in store.conn.execute("SELECT status, count(*) FROM claims GROUP BY 1 ORDER BY 1"):
        print(f"  {status:<12} {count}")
    total = store.conn.execute("SELECT count(*) FROM claims").fetchone()[0]
    print(f"claims in table: {total}")
    store.close()


if __name__ == "__main__":
    main()
