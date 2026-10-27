import json

from claims_stream.generator import expected_final_state, generate
from claims_stream.schema import InvalidEvent, validate


def test_same_seed_same_stream():
    assert generate(20, seed=7).messages == generate(20, seed=7).messages


def test_every_claim_has_three_valid_events():
    batch = generate(30, seed=1, bad_rate=0, duplicate_rate=0)
    assert len(batch.valid_events) == 90
    for event in batch.valid_events:
        validate(json.dumps(event))  # raises if the generator made something invalid


def test_bad_events_are_all_rejected():
    batch = generate(200, seed=3, bad_rate=0.2, duplicate_rate=0)
    rejected = 0
    for _, value in batch.messages:
        try:
            validate(value)
        except InvalidEvent:
            rejected += 1
    assert batch.bad_count > 0
    assert rejected == batch.bad_count


def test_messages_are_keyed_by_claim():
    batch = generate(10, seed=2, bad_rate=0)
    for key, value in batch.messages:
        assert json.loads(value)["claim_id"] == key


def test_late_events_exist_and_final_state_uses_newest():
    batch = generate(100, seed=5, late_rate=0.3)
    seen = {}
    out_of_order = 0
    for event in batch.valid_events:
        prev = seen.get(event["claim_id"])
        if prev and event["event_ts"] < prev:
            out_of_order += 1
        seen[event["claim_id"]] = max(prev or "", event["event_ts"])
    assert out_of_order > 0

    final = expected_final_state(batch.valid_events)
    assert len(final) == 100
    assert all(e["status"] in ("PAID", "DENIED") for e in final.values())
