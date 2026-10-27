"""Producer -> Kafka -> consumer -> PostgreSQL + DLQ, against real services.

Needs KAFKA_BOOTSTRAP and PG_DSN (CI starts both as service containers).
Each run uses fresh topic and group names so reruns don't see old data.
"""

import os
import time
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    not (os.getenv("KAFKA_BOOTSTRAP") and os.getenv("PG_DSN")),
    reason="KAFKA_BOOTSTRAP and PG_DSN not set",
)


def wait_for_kafka(bootstrap, seconds=60):
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.time() + seconds
    while True:
        try:
            admin.list_topics(timeout=5)
            return
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(2)


def count_messages(bootstrap, topic):
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"counter-{uuid.uuid4().hex[:6]}"})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        total = 0
        for p in parts:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            total += high - low
        return total
    finally:
        c.close()


@pytest.fixture
def cfg():
    from claims_stream.config import settings

    run = uuid.uuid4().hex[:8]
    c = settings(topic=f"claims.events.{run}", dlq_topic=f"claims.dlq.{run}", group_id=f"loader-{run}")
    wait_for_kafka(c.bootstrap)
    return c


@pytest.fixture
def db(cfg):
    from claims_stream.db import PostgresStore

    s = PostgresStore(cfg.pg_dsn)
    s.create_schema()
    s.conn.execute("TRUNCATE claims")
    yield s
    s.close()


def test_stream_loads_good_events_dead_letters_bad_and_drains_lag(cfg, db):
    from dataclasses import replace

    from claims_stream.admin import create_topics
    from claims_stream.consumer import run
    from claims_stream.generator import expected_final_state, generate
    from claims_stream.lag import group_lag
    from claims_stream.producer import publish

    create_topics(cfg)
    batch = generate(150, seed=21, bad_rate=0.08, duplicate_rate=0.05, late_rate=0.2)
    sent = publish(cfg, batch)
    assert sent == len(batch.messages)

    # Before consuming: the group has no commits, so lag is everything.
    assert sum(p.lag for p in group_lag(cfg)) == sent

    totals = run(cfg, batch_size=100, idle_exit=15)
    assert totals.dead_lettered == batch.bad_count
    assert totals.loaded == sent - batch.bad_count

    expected = {k: v["status"] for k, v in expected_final_state(batch.valid_events).items()}
    assert dict(db.conn.execute("SELECT claim_id, status FROM claims").fetchall()) == expected
    assert count_messages(cfg.bootstrap, cfg.dlq_topic) == batch.bad_count
    assert sum(p.lag for p in group_lag(cfg)) == 0

    # Replay everything with a brand-new group: the table must not change.
    snapshot = db.conn.execute("SELECT * FROM claims ORDER BY claim_id").fetchall()
    replay = replace(cfg, group_id=cfg.group_id + "-replay")
    run(replay, batch_size=100, idle_exit=15)
    assert db.conn.execute("SELECT * FROM claims ORDER BY claim_id").fetchall() == snapshot
