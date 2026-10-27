"""Create the topics. Safe to run more than once.

Run:  python -m claims_stream.admin
"""

from __future__ import annotations

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from .config import Settings, settings


def create_topics(cfg: Settings) -> list[str]:
    admin = AdminClient({"bootstrap.servers": cfg.bootstrap})
    topics = [
        NewTopic(cfg.topic, num_partitions=cfg.partitions, replication_factor=1),
        NewTopic(cfg.dlq_topic, num_partitions=1, replication_factor=1),
    ]
    created = []
    for name, future in admin.create_topics(topics).items():
        try:
            future.result()
            created.append(name)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    return created


if __name__ == "__main__":
    cfg = settings()
    print("created:", create_topics(cfg) or "nothing new", f"(topics: {cfg.topic}, {cfg.dlq_topic})")
