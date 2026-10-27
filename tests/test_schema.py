import json
from decimal import Decimal

import pytest

from claims_stream.schema import InvalidEvent, validate

GOOD = {
    "event_id": "e-1",
    "claim_id": "CLM-000001",
    "member_id": "M-00042",
    "provider_npi": "1234567890",
    "service_date": "2026-01-10",
    "billed_amount": "250.00",
    "paid_amount": "200.00",
    "status": "PAID",
    "event_ts": "2026-01-15T10:00:00+00:00",
}


def raw(**changes):
    event = {**GOOD, **changes}
    for k, v in list(event.items()):
        if v is None:
            del event[k]
    return json.dumps(event).encode()


def reason_for(value):
    with pytest.raises(InvalidEvent) as exc:
        validate(value)
    return exc.value.reason


def test_good_event_parses_with_exact_decimals():
    event = validate(raw())
    assert event.claim_id == "CLM-000001"
    assert event.billed_amount == Decimal("250.00")
    assert event.event_ts.utcoffset() is not None


def test_json_float_amounts_stay_exact():
    event = validate(b'{"event_id":"e","claim_id":"CLM-000001","member_id":"M-00001","provider_npi":"1234567890",'
                     b'"service_date":"2026-01-10","billed_amount":0.30,"paid_amount":0.10,"status":"PAID",'
                     b'"event_ts":"2026-01-15T10:00:00Z"}')
    assert event.billed_amount - event.paid_amount == Decimal("0.20")


@pytest.mark.parametrize(
    "value, reason",
    [
        (None, "empty_message"),
        (b"{not json", "invalid_json"),
        (b"[1, 2]", "not_an_object"),
        (raw(member_id=None), "missing_field"),
        (raw(member_id=""), "missing_field"),
        (raw(claim_id="12345"), "bad_claim_id"),
        (raw(member_id="M-1"), "bad_member_id"),
        (raw(provider_npi="12345"), "bad_npi"),
        (raw(status="APPROVED_MAYBE"), "bad_status"),
        (raw(service_date="2026-02-30"), "bad_date"),
        (raw(event_ts="yesterday"), "bad_timestamp"),
        (raw(event_ts="2026-01-15T10:00:00"), "bad_timestamp"),
        (raw(service_date="2026-03-01"), "future_service_date"),
        (raw(billed_amount="-1.00"), "negative_amount"),
        (raw(billed_amount="abc"), "bad_amount"),
        (raw(billed_amount=True), "bad_amount"),
        (raw(billed_amount="10.001"), "bad_amount"),
        (raw(billed_amount="NaN"), "bad_amount"),
        (raw(paid_amount="300.00"), "paid_exceeds_billed"),
        (raw(status=5), "bad_type"),
    ],
)
def test_bad_events_get_a_specific_reason(value, reason):
    assert reason_for(value) == reason
