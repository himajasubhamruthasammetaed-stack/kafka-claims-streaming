"""Publish synthetic claim events to Kafka.

Messages are keyed by claim_id. Kafka's default partitioner hashes the key, so
every event for one claim lands on the same partition and keeps its order there.

Run:  python -m claims_stream.producer --claims 500
"""

from __future__ import annotations

import argparse

from confluent_kafka import Producer

from .config import Settings, settings
from .generator import GeneratedBatch, generate


def publish(cfg: Settings, batch: GeneratedBatch) -> int:
    errors: list[str] = []

    def on_delivery(err, msg):
        if err is not None:
            errors.append(str(err))

    producer = Producer(
        {
            "bootstrap.servers": cfg.bootstrap,
            # Broker de-duplicates producer retries; acks=all is implied.
            "enable.idempotence": True,
            "linger.ms": 20,
        }
    )
    for key, value in batch.messages:
        producer.produce(cfg.topic, key=key.encode(), value=value, on_delivery=on_delivery)
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"{remaining} messages not delivered, errors={errors[:3]}")
    return len(batch.messages)


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish synthetic claim events")
    parser.add_argument("--claims", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--bad-rate", type=float, default=0.05)
    parser.add_argument("--duplicate-rate", type=float, default=0.05)
    args = parser.parse_args()

    batch = generate(args.claims, seed=args.seed, bad_rate=args.bad_rate, duplicate_rate=args.duplicate_rate)
    sent = publish(settings(), batch)
    print(
        f"sent {sent} messages for {args.claims} claims "
        f"({batch.bad_count} bad, {batch.duplicate_count} duplicates)"
    )


if __name__ == "__main__":
    main()
