-- Current state of each claim. One row per claim_id, always the newest event.
CREATE TABLE IF NOT EXISTS claims (
    claim_id         TEXT PRIMARY KEY,
    member_id        TEXT          NOT NULL,
    provider_npi     CHAR(10)      NOT NULL,
    service_date     DATE          NOT NULL,
    billed_amount    NUMERIC(12,2) NOT NULL CHECK (billed_amount >= 0),
    paid_amount      NUMERIC(12,2) NOT NULL CHECK (paid_amount >= 0),
    status           TEXT          NOT NULL,
    event_ts         TIMESTAMPTZ   NOT NULL,
    last_event_id    TEXT          NOT NULL,
    source_partition INTEGER       NOT NULL,
    source_offset    BIGINT        NOT NULL,
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CHECK (paid_amount <= billed_amount)
);

CREATE INDEX IF NOT EXISTS claims_status_idx ON claims (status);
