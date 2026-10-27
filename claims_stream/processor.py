"""Batch processing, with no Kafka or database code in it.

The consumer hands this a list of `Record`s. Each one is validated; good ones
are upserted together, bad ones go to the dead-letter sink with a reason.
Only after BOTH have succeeded does the consumer commit offsets. Keeping this
logic separate from Kafka is what makes it testable without a broker.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Protocol

from .schema import ClaimEvent, InvalidEvent, validate


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


class Store(Protocol):
    def upsert(self, rows: list[tuple[ClaimEvent, int, int]]) -> None: ...


class DeadLetterSink(Protocol):
    def send(self, record: Record, reason: str, detail: str) -> None: ...
    def flush(self) -> None: ...


@dataclass
class BatchResult:
    loaded: int = 0
    dead_lettered: int = 0
    reasons: Counter = field(default_factory=Counter)


class ClaimProcessor:
    def __init__(self, store: Store, dlq: DeadLetterSink):
        self.store = store
        self.dlq = dlq

    def process(self, records: list[Record]) -> BatchResult:
        result = BatchResult()
        rows: list[tuple[ClaimEvent, int, int]] = []
        for record in records:
            try:
                event = validate(record.value)
            except InvalidEvent as exc:
                self.dlq.send(record, exc.reason, exc.detail)
                result.dead_lettered += 1
                result.reasons[exc.reason] += 1
                continue
            rows.append((event, record.partition, record.offset))

        # If either call raises, the caller must NOT commit offsets.
        # The batch will be read again, and the upsert makes that safe.
        self.store.upsert(rows)
        self.dlq.flush()
        result.loaded = len(rows)
        return result
