"""Synthetic claim events. No real people, members or providers.

Each claim goes through SUBMITTED -> ADJUDICATED -> PAID or DENIED, one event per
step. On top of the clean stream the generator mixes in the things a real feed
throws at you:

* duplicates: the exact same event sent twice (a producer retry, a replay)
* late events: some events are shuffled so an older status arrives after a newer one
* bad events: missing fields, negative amounts, paid > billed, broken JSON

`expected_final_state` tells tests what the table should look like afterwards.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

BAD_KINDS = ("missing_member", "negative_billed", "paid_exceeds_billed", "bad_status", "broken_json")


@dataclass
class GeneratedBatch:
    messages: list[tuple[str, bytes]] = field(default_factory=list)  # (key, value)
    valid_events: list[dict] = field(default_factory=list)
    bad_count: int = 0
    duplicate_count: int = 0


def _claim_lifecycle(rng: random.Random, n: int, start: datetime) -> list[dict]:
    claim_id = f"CLM-{n:06d}"
    member_id = f"M-{rng.randint(1, 99999):05d}"
    npi = f"{rng.randint(10**9, 10**10 - 1)}"
    service_date = (start - timedelta(days=rng.randint(1, 30))).date().isoformat()
    billed = Decimal(rng.randint(5_000, 500_000)) / 100
    t0 = start + timedelta(minutes=rng.randint(0, 600))
    final = "PAID" if rng.random() < 0.8 else "DENIED"
    paid = (billed * Decimal(rng.choice(["0.6", "0.75", "0.9", "1"]))).quantize(Decimal("0.01"))

    steps = [("SUBMITTED", Decimal("0")), ("ADJUDICATED", Decimal("0")), (final, paid if final == "PAID" else Decimal("0"))]
    events = []
    for i, (status, paid_amount) in enumerate(steps):
        events.append(
            {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "claim_id": claim_id,
                "member_id": member_id,
                "provider_npi": npi,
                "service_date": service_date,
                "billed_amount": str(billed),
                "paid_amount": str(paid_amount),
                "status": status,
                "event_ts": (t0 + timedelta(hours=i)).isoformat(),
            }
        )
    return events


def _corrupt(rng: random.Random, event: dict, kind: str) -> bytes:
    bad = dict(event, event_id=str(uuid.UUID(int=rng.getrandbits(128))))
    if kind == "missing_member":
        bad.pop("member_id")
    elif kind == "negative_billed":
        bad["billed_amount"] = "-125.00"
    elif kind == "paid_exceeds_billed":
        bad["paid_amount"] = str(Decimal(bad["billed_amount"]) + 1)
    elif kind == "bad_status":
        bad["status"] = "APPROVED_MAYBE"
    elif kind == "broken_json":
        return json.dumps(bad).encode()[:-5]
    return json.dumps(bad).encode()


def generate(
    claims: int = 100,
    seed: int = 42,
    bad_rate: float = 0.05,
    duplicate_rate: float = 0.05,
    late_rate: float = 0.1,
    start: datetime | None = None,
) -> GeneratedBatch:
    rng = random.Random(seed)
    start = start or datetime(2026, 1, 15, 8, 0, tzinfo=timezone.utc)
    batch = GeneratedBatch()

    events: list[dict] = []
    for n in range(1, claims + 1):
        events.extend(_claim_lifecycle(rng, n, start))
    events.sort(key=lambda e: e["event_ts"])

    # Late arrivals: swap some events with a later neighbour so an older status
    # shows up after a newer one for the same claim.
    for i in range(len(events) - 1):
        if rng.random() < late_rate:
            j = min(len(events) - 1, i + rng.randint(1, 5))
            events[i], events[j] = events[j], events[i]

    for event in events:
        value = json.dumps(event).encode()
        batch.messages.append((event["claim_id"], value))
        batch.valid_events.append(event)
        if rng.random() < duplicate_rate:
            batch.messages.append((event["claim_id"], value))
            batch.duplicate_count += 1
        if rng.random() < bad_rate:
            kind = rng.choice(BAD_KINDS)
            batch.messages.append((event["claim_id"], _corrupt(rng, event, kind)))
            batch.bad_count += 1
    return batch


def expected_final_state(valid_events: list[dict]) -> dict[str, dict]:
    """claim_id -> the event with the latest event_ts (what the table should hold)."""
    latest: dict[str, dict] = {}
    for event in valid_events:
        ts = datetime.fromisoformat(event["event_ts"])
        current = latest.get(event["claim_id"])
        if current is None or ts > datetime.fromisoformat(current["event_ts"]):
            latest[event["claim_id"]] = event
    return latest
