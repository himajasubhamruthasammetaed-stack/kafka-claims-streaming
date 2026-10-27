import pytest
from confluent_kafka import KafkaError, KafkaException

from claims_stream.lag import _committed, compute_lag, drain_seconds


def test_lag_is_log_end_minus_committed():
    assert compute_lag(0, low=0, high=1000, committed=850).lag == 150


def test_no_commit_counts_everything_retained():
    r = compute_lag(1, low=200, high=500, committed=None)
    assert r.lag == 300 and r.committed is None


def test_committed_below_retention_starts_from_low_watermark():
    # Old commit, data since deleted by retention: only what's still there counts.
    assert compute_lag(2, low=400, high=500, committed=100).lag == 100


def test_caught_up_is_zero_never_negative():
    assert compute_lag(0, low=0, high=10, committed=10).lag == 0
    assert compute_lag(0, low=0, high=10, committed=12).lag == 0


def test_drain_time():
    assert drain_seconds(20_000, 250) == 80
    assert drain_seconds(20_000, 0) is None


class FlakyClient:
    """Fails like a fresh broker would, then answers."""

    def __init__(self, failures, code):
        self.failures, self.code, self.calls = failures, code, 0

    def committed(self, partitions, timeout):
        self.calls += 1
        if self.calls <= self.failures:
            raise KafkaException(KafkaError(self.code))
        return partitions


def test_committed_retries_coordinator_errors(monkeypatch):
    monkeypatch.setattr("claims_stream.lag.time.sleep", lambda s: None)
    client = FlakyClient(failures=3, code=KafkaError.NOT_COORDINATOR)
    assert _committed(client, ["tp"], timeout=1) == ["tp"]
    assert client.calls == 4


def test_committed_does_not_retry_other_errors(monkeypatch):
    monkeypatch.setattr("claims_stream.lag.time.sleep", lambda s: None)
    client = FlakyClient(failures=1, code=KafkaError.GROUP_AUTHORIZATION_FAILED)
    with pytest.raises(KafkaException):
        _committed(client, ["tp"], timeout=1)
    assert client.calls == 1
