# Kafka consumer scaling benchmark

Benchmark for the post *"From 20 to 10K Messages/s: How We Scale Kafka Consumers"*.
It runs the sensor example from the post (temperature readings from a Kafka topic stored in PostgreSQL)
in two scenarios and measures how fast each one drains the same pre-filled topic.

## Scenarios

| | `baseline` (no recommendations) | `optimized` (all recommendations) |
|---|---|---|
| Partitions / consumers | 3 / 3 | 6 / 6 |
| Consumption | One record at a time | Batches of up to 2,000 records |
| Offset commit | Per record (`ackMode: RECORD`) | Per batch, after the DB commit |
| Decommissioned sensors | Read from the DB per message | Cached set, discarded before any DB work |
| Duplicates per key | Every message is written | Only the newest reading per key in the batch |
| Unchanged temperature | Written anyway | One `SELECT ... WHERE id IN (...)` per batch, unchanged readings skipped |
| Parallelism | None | Batch grouped by `floorMod(sensorId, 4)`, each chunk on a virtual thread |
| Transactions | One per message | One per chunk, retried up to 3 times on `TransientDataAccessException` |
| Hikari pool | 10 | 24 (= consumers x parallelism) |

## Data set

The producer (`app/src/main/kotlin/com/example/sensors/producer/Producer.kt`) is deterministic (fixed seed), so both scenarios consume exactly the same messages:

- 600,000 sensors, 5% of them decommissioned.
- 2,000,000 messages, key `sensor-<id>`, value `{"key":"sensor-<id>","payload":{"temperature":21.4}}`.
- 80% of the readings repeat the previous temperature of the sensor.
- The last temperature per sensor is stored in `expected_state` to verify correctness after each run.

## Running

Requirements: Docker and `docker-compose`. The application is built inside Docker, no local JDK or Gradle needed.

```bash
./scripts/compare.sh                    # builds, runs both scenarios and prints a comparison table
./scripts/run-benchmark.sh baseline     # runs a single scenario
./scripts/run-benchmark.sh optimized-3p # all recommendations except scaling partitions (3 partitions / 3 consumers)
SENSORS=20000 MESSAGES=100000 ./scripts/run-benchmark.sh optimized   # smaller data set
```

Each run starts from a clean environment (`docker-compose down -v`), creates the topic, produces the data set,
starts the consumer and waits until it has processed every message (or `TIMEOUT_SECONDS`, default 900).
Metrics are written to `results/<scenario>.env`:

- `throughput`: messages/s, measured by the consumer from the first processed batch to the last message.
- `db_commits`: delta of `pg_stat_database.xact_commit` during the run.
- `rows_updated`: `n_tup_upd` on the `sensor` table.
- `mismatches`: active sensors whose final temperature differs from the last produced one (must be 0).

Container limits are the same in both scenarios: PostgreSQL `1.5` CPUs / 2 GB, consumer `2` CPUs / 1 GB.

## Results

Run on a MacBook (Colima VM with 4 CPUs / 12 GB), 600,000 sensors, 2,000,000 messages:

| Metric | baseline | optimized |
|---|---:|---:|
| Duration | 232.7 s | 7.5 s |
| Throughput | 8,596 msg/s | 265,710 msg/s |
| DB commits | 2,000,179 | 5,157 |
| Rows updated | 1,900,629 | 377,781 |
| Mismatches | 0 | 0 |

**Speed-up: 30.9x**, with ~390x fewer commits and ~5x fewer row updates.

### Isolating the partition gain

`optimized-3p` applies every recommendation but keeps the baseline's 3 partitions / 3 consumers.
Average of 3 runs each:

| Scenario | Throughput (avg) | Range | vs baseline |
|---|---:|---:|---:|
| baseline (3 partitions) | 8,596 msg/s | single run | 1x |
| optimized-3p (3 partitions) | 211,559 msg/s | 192,492 - 236,742 | 24.6x |
| optimized (6 partitions) | 241,957 msg/s | 227,427 - 254,065 | 28.1x |

Once the consumer batches and discards work, doubling the partitions only adds ~14%: the bottleneck is no longer
the number of consumers but the CPU of the consumer and the database.

Note: everything runs on the same machine, so DB round trips cost tens of microseconds. With a real network
between consumer and database (around 1 ms per round trip), the baseline pays that latency several times per message,
so the gap is expected to be larger in production.
