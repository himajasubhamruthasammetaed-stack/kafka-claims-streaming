# Real-time Claims Streaming with Kafka

A personal project: stream synthetic healthcare claim events through Apache Kafka
into PostgreSQL, with validation, a dead-letter topic for bad events, idempotent
upserts and a consumer-lag report. All data is generated; there is no real
patient, member or provider data anywhere in this repo.

## Why

Claim systems send a status event each time a claim moves (submitted,
adjudicated, paid or denied). A downstream table should always show the
**current** status of each claim. Real feeds make that harder than it sounds:

- the same event arrives twice (producer retries, replays after a crash)
- events arrive late, so "SUBMITTED" can show up after "PAID"
- some events are just broken (missing member ID, negative amount, bad JSON)

A naive consumer either crashes on the bad event, or lets a late event roll a
paid claim back to "submitted". This project shows one way to handle all three
without losing data or double counting.

## Architecture

```
  producer.py                    Kafka                         consumer.py
 (synthetic claims)     +----------------------+     +-------------------------------+
  key = claim_id  ----> | claims.events (3 p.) | --> | validate (schema.py)          |
                        +----------------------+     |   ok  -> batch upsert -------+-----> PostgreSQL
                                                     |   bad -> dead-letter --+     |       claims table
                                                     | commit offsets AFTER   |     |       (newest event wins)
                                                     | both succeed           |     |
                        +----------------------+     +------------------------+-----+
                        | claims.dlq           | <------------------------- +
                        | (original bytes +    |
                        |  reason headers)     |          lag.py: log-end offset - committed offset
                        +----------------------+                  per partition, + drain estimate
```

## How it works

| Step | File | What it does |
|---|---|---|
| Topics | `claims_stream/admin.py` | Creates `claims.events` (3 partitions) and `claims.dlq`. Safe to rerun. |
| Generate | `claims_stream/generator.py` | Synthetic claim lifecycles plus duplicates, late events and broken events. Seeded, so runs are repeatable. |
| Produce | `claims_stream/producer.py` | Publishes keyed by `claim_id`, so one claim's events stay on one partition. Idempotent producer on. |
| Validate | `claims_stream/schema.py` | JSON to a typed `ClaimEvent`, or `InvalidEvent` with a short reason code. Money parsed as `Decimal`. |
| Process | `claims_stream/processor.py` | Splits a batch into good and bad, upserts the good ones, dead-letters the bad. No Kafka code, so it's unit-testable. |
| Load | `claims_stream/db.py`, `sql/schema.sql` | One upsert: `ON CONFLICT (claim_id) DO UPDATE ... WHERE claims.event_ts < EXCLUDED.event_ts`. |
| Consume | `claims_stream/consumer.py` | `enable.auto.commit=false`; commits offsets only after the DB transaction and DLQ sends succeed. |
| Lag | `claims_stream/lag.py` | Per-partition lag (log-end minus committed) and an optional time-to-drain estimate. |
| Offline demo | `claims_stream/simulate.py` | Same processor and upsert with no broker, for a quick look. |

### The three decisions that matter

1. **Newest event wins, in SQL.** The `WHERE claims.event_ts < EXCLUDED.event_ts`
   on the upsert means a duplicate does nothing (same timestamp) and a late,
   older event does nothing. Only a newer event changes the row.
2. **At-least-once, made safe.** Offsets are committed after the database
   commit. If the consumer dies in between, the batch is read again, and
   decision 1 makes that harmless. The trade-off: the DLQ topic can receive a
   bad event twice in that case. (The `--replay` demo below shows exactly that.)
3. **Bad events don't stop the stream.** They go to `claims.dlq` with the
   original bytes and headers: `dlq.reason`, `dlq.detail`, `source.topic`,
   `source.partition`, `source.offset`. Someone can fix and re-publish them later.

## Run it

Requirements: Python 3.11+, Docker.

```bash
docker compose up -d                        # Kafka (KRaft, no ZooKeeper) + PostgreSQL
pip install -r requirements.txt -r requirements-dev.txt

python -m claims_stream.admin               # create topics
python -m claims_stream.producer --claims 300
python -m claims_stream.lag                 # lag before consuming
python -m claims_stream.consumer --idle-exit 15
python -m claims_stream.lag --rate 500      # should be 0 now

psql postgresql://postgres:postgres@localhost:5432/claims \
  -c "SELECT status, count(*) FROM claims GROUP BY 1"
```

Without Kafka (PostgreSQL only):

```bash
python -m claims_stream.simulate --claims 200 --replay
```

Settings come from environment variables: `KAFKA_BOOTSTRAP`, `PG_DSN`,
`CLAIMS_TOPIC`, `DLQ_TOPIC`, `GROUP_ID`, `CLAIMS_PARTITIONS` (defaults match
`docker-compose.yml`).

## Tests

```bash
pytest                                       # unit tests; integration tests skip
PG_DSN=postgresql://postgres:postgres@localhost:5432/claims \
KAFKA_BOOTSTRAP=localhost:9092 pytest -v     # everything
```

- **Unit tests** (no services): validation rules, generator, processor with fake
  store and DLQ (late events, duplicates, replay, DB failure), lag math, message handling.
- **PostgreSQL tests**: the upsert rule on a real database, replay is a no-op
  (not even `updated_at` moves), a failing batch writes nothing.
- **End-to-end test**: produce, consume, check the table matches the newest valid
  event per claim, the DLQ holds exactly the bad events, lag drains to 0, and a
  full replay with a new consumer group leaves the table unchanged.

GitHub Actions runs all of it with PostgreSQL 16 and Kafka 3.9 as service containers.

## Example output

Offline demo, run twice over the same messages:

```
$ python -m claims_stream.simulate --claims 200 --replay
messages: 665 (32 duplicates, 33 bad), passes: 2
dead-lettered: 66  {'negative_amount': 12, 'missing_field': 12, 'paid_exceeds_billed': 20, 'invalid_json': 16, 'bad_status': 6}
  DENIED       47
  PAID         153
claims in table: 200
```

The table is identical after one pass or two (200 claims, all in a final
status). The DLQ count doubles, which is the at-least-once trade-off from
decision 2.

## Next steps

- Schema Registry with Avro or JSON Schema instead of hand-written validation
- A small "DLQ replayer" that re-publishes fixed events back to `claims.events`
- Export lag and DLQ counts as Prometheus metrics and alert on time lag, not raw lag
- Handle rebalances explicitly (commit on partition revoke)
