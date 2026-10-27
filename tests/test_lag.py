from claims_stream.lag import compute_lag, drain_seconds


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
