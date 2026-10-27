import pytest

from claims_stream.generator import expected_final_state, generate
from claims_stream.processor import ClaimProcessor, Record


class FakeStore:
    """Mimics the upsert rule: keep the row only if the event is newer."""

    def __init__(self, fail=False):
        self.rows = {}
        self.fail = fail
        self.calls = 0

    def upsert(self, rows):
        self.calls += 1
        if self.fail:
            raise RuntimeError("database down")
        for event, _, _ in rows:
            current = self.rows.get(event.claim_id)
            if current is None or current.event_ts < event.event_ts:
                self.rows[event.claim_id] = event


class FakeDLQ:
    def __init__(self):
        self.sent = []
        self.flushed = 0

    def send(self, record, reason, detail):
        self.sent.append((record.offset, reason))

    def flush(self):
        self.flushed += 1


def records_from(batch, topic="claims.events"):
    return [Record(topic, 0, i, k.encode(), v) for i, (k, v) in enumerate(batch.messages)]


def test_good_rows_load_and_bad_rows_go_to_dlq():
    batch = generate(50, seed=11, bad_rate=0.1)
    store, dlq = FakeStore(), FakeDLQ()
    result = ClaimProcessor(store, dlq).process(records_from(batch))

    assert result.dead_lettered == batch.bad_count == len(dlq.sent)
    assert result.loaded == len(batch.messages) - batch.bad_count
    assert dlq.flushed == 1
    assert sum(result.reasons.values()) == batch.bad_count


def test_final_state_matches_newest_event_despite_late_and_duplicate_events():
    batch = generate(80, seed=12, late_rate=0.3, duplicate_rate=0.2)
    store = FakeStore()
    ClaimProcessor(store, FakeDLQ()).process(records_from(batch))

    expected = expected_final_state(batch.valid_events)
    assert {k: v.status for k, v in store.rows.items()} == {k: v["status"] for k, v in expected.items()}


def test_replaying_the_same_batch_changes_nothing():
    batch = generate(40, seed=13)
    store = FakeStore()
    processor = ClaimProcessor(store, FakeDLQ())
    processor.process(records_from(batch))
    snapshot = dict(store.rows)
    processor.process(records_from(batch))  # e.g. crash before the offset commit
    assert store.rows == snapshot


def test_store_failure_propagates_so_offsets_are_not_committed():
    batch = generate(5, seed=14)
    dlq = FakeDLQ()
    with pytest.raises(RuntimeError):
        ClaimProcessor(FakeStore(fail=True), dlq).process(records_from(batch))
    assert dlq.flushed == 0


def test_empty_value_is_dead_lettered_not_crashing():
    store, dlq = FakeStore(), FakeDLQ()
    result = ClaimProcessor(store, dlq).process([Record("t", 0, 0, None, None)])
    assert result.dead_lettered == 1 and dlq.sent == [(0, "empty_message")]
