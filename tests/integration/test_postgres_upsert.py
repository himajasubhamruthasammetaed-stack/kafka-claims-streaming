"""The upsert rule against a real PostgreSQL. Needs PG_DSN."""

import json
import os
from datetime import timedelta

import pytest

from claims_stream.generator import expected_final_state, generate
from claims_stream.schema import validate

pytestmark = pytest.mark.skipif(not os.getenv("PG_DSN"), reason="PG_DSN not set")


@pytest.fixture
def store():
    from claims_stream.db import PostgresStore

    s = PostgresStore(os.environ["PG_DSN"])
    s.create_schema()
    s.conn.execute("TRUNCATE claims")
    yield s
    s.close()


def rows_for(events):
    return [(validate(json.dumps(e)), 0, i) for i, e in enumerate(events)]


def table(store):
    return dict(store.conn.execute("SELECT claim_id, status FROM claims").fetchall())


def test_newest_event_wins_even_when_it_arrives_first(store):
    batch = generate(1, seed=1, bad_rate=0, duplicate_rate=0, late_rate=0)
    submitted, adjudicated, final = batch.valid_events
    store.upsert(rows_for([final]))
    store.upsert(rows_for([submitted, adjudicated]))  # late arrivals
    assert table(store) == {"CLM-000001": final["status"]}


def test_replay_is_a_no_op(store):
    batch = generate(60, seed=2, bad_rate=0, late_rate=0.3, duplicate_rate=0.2)
    store.upsert(rows_for(batch.valid_events))
    before = store.conn.execute("SELECT claim_id, status, event_ts, updated_at FROM claims ORDER BY 1").fetchall()
    store.upsert(rows_for(batch.valid_events))
    after = store.conn.execute("SELECT claim_id, status, event_ts, updated_at FROM claims ORDER BY 1").fetchall()
    assert before == after  # not even updated_at moved

    expected = {k: v["status"] for k, v in expected_final_state(batch.valid_events).items()}
    assert table(store) == expected


def test_batch_is_all_or_nothing(store):
    batch = generate(3, seed=3, bad_rate=0, duplicate_rate=0, late_rate=0)
    good = rows_for(batch.valid_events)
    event, p, o = good[-1]
    # Slip past the Python validator to hit the table's CHECK constraint.
    broken = (event.__class__(**{**event.__dict__, "paid_amount": event.billed_amount + 1,
                                  "event_ts": event.event_ts + timedelta(days=1)}), p, o)
    with pytest.raises(Exception):
        store.upsert(good + [broken])
    assert table(store) == {}
