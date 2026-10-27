"""Consumer lag per partition, the same way kafka-consumer-groups.sh --describe does it:

    lag = log-end offset (high watermark) - group's committed offset

Two things this number does NOT tell you on its own:
* time: 5,000 messages behind can be seconds or hours, depending on throughput
* progress: a lag that stays flat while the committed offset also stays flat
  means nothing is being committed (stuck or crash-looping consumer)

So the report also gives an estimated drain time when you pass a rate.

Run:  python -m claims_stream.lag [--rate 250]
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from .config import Settings, settings


# Temporary errors while the group coordinator is being found or loaded.
COORDINATOR_ERRORS = {
    KafkaError.NOT_COORDINATOR,
    KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
    KafkaError.COORDINATOR_NOT_AVAILABLE,
}


@dataclass(frozen=True)
class PartitionLag:
    partition: int
    committed: int | None  # None = the group has never committed on this partition
    log_end: int
    lag: int


def compute_lag(partition: int, low: int, high: int, committed: int | None) -> PartitionLag:
    # With no commit yet, everything still retained (low..high) is unread.
    start = low if committed is None or committed < low else committed
    return PartitionLag(partition, committed, high, max(0, high - start))


def drain_seconds(total_lag: int, records_per_second: float) -> float | None:
    """Rough time to catch up at the current consume rate. None if not consuming."""
    if records_per_second <= 0:
        return None
    return total_lag / records_per_second


def _committed(client: Consumer, partitions: list[TopicPartition], timeout: float, attempts: int = 10):
    """client.committed(), retrying errors Kafka marks as retriable.

    On a fresh broker the first offset lookup can fail with NOT_COORDINATOR or
    COORDINATOR_LOAD_IN_PROGRESS while the internal __consumer_offsets topic is
    still being created. Those are temporary, so wait and ask again.
    """
    for attempt in range(1, attempts + 1):
        try:
            return client.committed(partitions, timeout=timeout)
        except KafkaException as exc:
            err = exc.args[0]
            if attempt == attempts or not (err.retriable() or err.code() in COORDINATOR_ERRORS):
                raise
            time.sleep(min(2.0, 0.25 * attempt))


def group_lag(cfg: Settings, timeout: float = 10.0) -> list[PartitionLag]:
    # A Consumer that never subscribes doesn't join the group, so this is read-only.
    client = Consumer({"bootstrap.servers": cfg.bootstrap, "group.id": cfg.group_id, "enable.auto.commit": False})
    try:
        metadata = client.list_topics(cfg.topic, timeout=timeout)
        partitions = sorted(metadata.topics[cfg.topic].partitions)
        committed = _committed(client, [TopicPartition(cfg.topic, p) for p in partitions], timeout)
        result = []
        for tp in committed:
            low, high = client.get_watermark_offsets(TopicPartition(cfg.topic, tp.partition), timeout=timeout)
            offset = tp.offset if tp.offset >= 0 else None  # negative = OFFSET_INVALID (no commit)
            result.append(compute_lag(tp.partition, low, high, offset))
        return result
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Show consumer lag for the claims loader")
    parser.add_argument("--rate", type=float, default=0.0, help="current records/second, for a drain estimate")
    args = parser.parse_args()
    cfg = settings()
    rows = group_lag(cfg)
    print(f"group={cfg.group_id} topic={cfg.topic}")
    print(f"{'PARTITION':>9} {'COMMITTED':>10} {'LOG-END':>8} {'LAG':>6}")
    for r in rows:
        committed = "-" if r.committed is None else r.committed
        print(f"{r.partition:>9} {committed!s:>10} {r.log_end:>8} {r.lag:>6}")
    total = sum(r.lag for r in rows)
    print(f"total lag: {total}")
    eta = drain_seconds(total, args.rate)
    if eta is not None:
        print(f"about {eta:.0f}s to drain at {args.rate:g} records/s")


if __name__ == "__main__":
    main()
