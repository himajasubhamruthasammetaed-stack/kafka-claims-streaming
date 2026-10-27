"""Kafka consumer: read claim events, load them into PostgreSQL, dead-letter the bad ones.

Delivery guarantee: at-least-once.
  1. consume a batch
  2. upsert the good events in one DB transaction, send the bad ones to the DLQ
  3. only then commit the Kafka offsets (enable.auto.commit is off)

If the process dies between 2 and 3, the batch is read again on restart. The
upsert ignores events that aren't newer than the stored row, so the table ends
up the same. The DLQ topic can get a duplicate in that case, which is the
trade-off of at-least-once and is called out in the README.

Run:  python -m claims_stream.consumer --idle-exit 10
"""

from __future__ import annotations

import argparse
import logging
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from .config import Settings, settings
from .db import PostgresStore
from .processor import BatchResult, ClaimProcessor, Record

log = logging.getLogger("claims_stream.consumer")


class KafkaDeadLetterSink:
    """Sends the original bytes plus headers saying where it came from and why."""

    def __init__(self, producer: Producer, topic: str):
        self.producer = producer
        self.topic = topic
        self._errors: list[str] = []

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self._errors.append(str(err))

    def send(self, record: Record, reason: str, detail: str) -> None:
        self.producer.produce(
            self.topic,
            key=record.key,
            value=record.value,
            headers=[
                ("dlq.reason", reason.encode()),
                ("dlq.detail", detail[:500].encode()),
                ("source.topic", record.topic.encode()),
                ("source.partition", str(record.partition).encode()),
                ("source.offset", str(record.offset).encode()),
            ],
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)

    def flush(self) -> None:
        remaining = self.producer.flush(30)
        if remaining or self._errors:
            errors, self._errors = self._errors, []
            raise RuntimeError(f"DLQ delivery failed: {remaining} undelivered, errors={errors[:3]}")


def build_consumer(cfg: Settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": cfg.bootstrap,
            "group.id": cfg.group_id,
            "enable.auto.commit": False,  # we commit after the DB write, not on a timer
            "auto.offset.reset": "earliest",
        }
    )


def to_records(messages) -> list[Record]:
    records = []
    for msg in messages:
        err = msg.error()
        if err is not None:
            if err.code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(err)
        records.append(Record(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value()))
    return records


def run(cfg: Settings, batch_size: int = 500, idle_exit: float | None = None) -> BatchResult:
    """Consume until stopped. With idle_exit, stop after that many seconds with no messages."""
    store = PostgresStore(cfg.pg_dsn)
    store.create_schema()
    producer = Producer({"bootstrap.servers": cfg.bootstrap, "enable.idempotence": True})
    processor = ClaimProcessor(store, KafkaDeadLetterSink(producer, cfg.dlq_topic))
    consumer = build_consumer(cfg)
    consumer.subscribe([cfg.topic])

    totals = BatchResult()
    last_message_at = time.monotonic()
    try:
        while True:
            messages = consumer.consume(num_messages=batch_size, timeout=1.0)
            records = to_records(messages)
            if not records:
                if idle_exit is not None and time.monotonic() - last_message_at > idle_exit:
                    break
                continue
            last_message_at = time.monotonic()

            result = processor.process(records)
            consumer.commit(asynchronous=False)  # only reached if DB + DLQ succeeded

            totals.loaded += result.loaded
            totals.dead_lettered += result.dead_lettered
            totals.reasons.update(result.reasons)
            log.info("batch: %d loaded, %d dead-lettered %s", result.loaded, result.dead_lettered, dict(result.reasons))
    finally:
        consumer.close()
        store.close()
    log.info("done: %d loaded, %d dead-lettered %s", totals.loaded, totals.dead_lettered, dict(totals.reasons))
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--idle-exit", type=float, default=None, help="stop after N idle seconds")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(settings(), batch_size=args.batch_size, idle_exit=args.idle_exit)


if __name__ == "__main__":
    main()
