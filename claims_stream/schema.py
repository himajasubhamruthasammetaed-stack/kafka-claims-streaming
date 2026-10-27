"""Validation for claim events.

Every message on the claims topic is JSON. `validate` turns the raw bytes into a
`ClaimEvent` or raises `InvalidEvent` with a short, machine-readable reason.
That reason travels with the message to the dead-letter topic, so whoever looks
at the DLQ can see why it was rejected without re-running anything.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

STATUSES = ("SUBMITTED", "ADJUDICATED", "PAID", "DENIED")
REQUIRED_FIELDS = (
    "event_id",
    "claim_id",
    "member_id",
    "provider_npi",
    "service_date",
    "billed_amount",
    "paid_amount",
    "status",
    "event_ts",
)

CLAIM_ID_RE = re.compile(r"^CLM-\d{6}$")
MEMBER_ID_RE = re.compile(r"^M-\d{5}$")
NPI_RE = re.compile(r"^\d{10}$")


class InvalidEvent(ValueError):
    """Raised when a message can't be loaded. `reason` is a short code."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class ClaimEvent:
    event_id: str
    claim_id: str
    member_id: str
    provider_npi: str
    service_date: date
    billed_amount: Decimal
    paid_amount: Decimal
    status: str
    event_ts: datetime


def _amount(payload: dict, field: str) -> Decimal:
    value = payload[field]
    # bool is a subclass of int in Python, so reject it explicitly.
    if isinstance(value, bool) or not isinstance(value, (int, Decimal, str)):
        raise InvalidEvent("bad_amount", f"{field} is not a number")
    try:
        amount = Decimal(str(value))
    except InvalidOperation:
        raise InvalidEvent("bad_amount", f"{field}={value!r}") from None
    if not amount.is_finite():
        raise InvalidEvent("bad_amount", f"{field} is not finite")
    if amount < 0:
        raise InvalidEvent("negative_amount", f"{field}={amount}")
    if amount.as_tuple().exponent < -2:
        raise InvalidEvent("bad_amount", f"{field} has more than 2 decimals")
    return amount


def validate(raw: bytes | str | None) -> ClaimEvent:
    if raw is None:
        raise InvalidEvent("empty_message")
    try:
        # parse_float=Decimal keeps money exact (no 0.1 + 0.2 surprises).
        payload = json.loads(raw, parse_float=Decimal)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidEvent("invalid_json", str(exc)) from None
    if not isinstance(payload, dict):
        raise InvalidEvent("not_an_object")

    missing = [f for f in REQUIRED_FIELDS if payload.get(f) in (None, "")]
    if missing:
        raise InvalidEvent("missing_field", ",".join(missing))

    for field in ("event_id", "claim_id", "member_id", "provider_npi", "status", "service_date", "event_ts"):
        if not isinstance(payload[field], str):
            raise InvalidEvent("bad_type", f"{field} must be a string")

    if not CLAIM_ID_RE.match(payload["claim_id"]):
        raise InvalidEvent("bad_claim_id", payload["claim_id"])
    if not MEMBER_ID_RE.match(payload["member_id"]):
        raise InvalidEvent("bad_member_id", payload["member_id"])
    if not NPI_RE.match(payload["provider_npi"]):
        raise InvalidEvent("bad_npi", payload["provider_npi"])
    if payload["status"] not in STATUSES:
        raise InvalidEvent("bad_status", payload["status"])

    try:
        service_date = date.fromisoformat(payload["service_date"])
    except ValueError:
        raise InvalidEvent("bad_date", payload["service_date"]) from None
    try:
        event_ts = datetime.fromisoformat(payload["event_ts"])
    except ValueError:
        raise InvalidEvent("bad_timestamp", payload["event_ts"]) from None
    if event_ts.tzinfo is None:
        # Without a timezone, "which event is newer" becomes a guess.
        raise InvalidEvent("bad_timestamp", "event_ts needs a UTC offset")
    if service_date > event_ts.date():
        raise InvalidEvent("future_service_date", payload["service_date"])

    billed = _amount(payload, "billed_amount")
    paid = _amount(payload, "paid_amount")
    if paid > billed:
        raise InvalidEvent("paid_exceeds_billed", f"paid={paid} billed={billed}")

    return ClaimEvent(
        event_id=payload["event_id"],
        claim_id=payload["claim_id"],
        member_id=payload["member_id"],
        provider_npi=payload["provider_npi"],
        service_date=service_date,
        billed_amount=billed,
        paid_amount=paid,
        status=payload["status"],
        event_ts=event_ts,
    )
