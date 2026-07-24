# Real-Time Ingestion Fabric

A streaming pipeline over live London Underground arrival predictions. Ingests
the TfL Unified API into Kafka, processes it through bronze, silver and gold
layers with Spark Structured Streaming, and serves per-line service health from
Cassandra.

The interesting part is not the stack. It is that **45% of everything ingested
is a duplicate**, and finding that out is what the pipeline is for.

---

## The problem

TfL publishes arrival predictions per line. Poll `/Line/{ids}/Arrivals` twice a
minute and you get roughly 4,200 records each time - but the same train, at the
same station, with the same predicted arrival, appears in poll after poll. The
API sits behind a CDN with a cache lifetime close to the poll interval, so
consecutive requests frequently return byte-identical payloads.

Naive ingestion writes all of it. The table grows linearly with polling
frequency rather than with actual events, and every downstream aggregate is
wrong by whatever factor you happened to poll at.

## Measured results

Figures from a single continuous run, 11 tube lines, 30-second polling.

| Metric | Value |
| --- | ---: |
| Records ingested to bronze | 60,972 |
| Duplicates removed at silver | ~45% |
| Records quarantined | 0.37% |
| Distinct validation failures | 1 (`placeholder_vehicle_id`) |
| Records per poll | ~4,205 |
| Requests to TfL per minute | 2 |

The quarantine rate is low because the feed is clean apart from one known
quirk: TfL emits `vehicleId: "000"` for trains it cannot identify. Those
records are isolated rather than dropped, because the deduplication key is
`(vehicle_id, naptan_id, expected_arrival)` and dozens of unrelated trains
sharing a placeholder ID would collapse into one another.

---

## Architecture

```
TfL Unified API
      │  producer service, polls every 30s, keyed by lineId
      ▼
Kafka  tfl.arrivals.raw  (6 partitions)
      │
      ├─────────────► BRONZE   raw payload + Kafka coordinates, lossless
      │                        stateless append, never deduplicated
      │
      ├─────────────► QUARANTINE  validation failures + reason + raw payload
      │
      └─────────────► SILVER   parsed, typed, validated, deduplicated
                          │
                          ▼
                    Kafka  tfl.arrivals.clean
                          │
                          ▼
                        GOLD   5-minute windowed service health per line
```

Everything runs on Docker Compose. Cassandra holds all four layers plus a
per-micro-batch metrics table.

### Why bronze is a separate streaming query

Bronze must be lossless and replayable, which means it must never be
deduplicated or filtered. Silver is stateful - it holds a keyed state store for
the watermark window. Sharing one query would couple a cheap stateless append to
an expensive stateful operator, and bronze would fall behind for no reason. They
run as independent queries with independent checkpoints, so either can fail and
restart without the other.

### Why Kafka sits between silver and gold

Gold reads from `tfl.arrivals.clean` rather than from the silver Cassandra
table. Cassandra is not a streaming source, and more importantly this means the
gold job can be rewound, rebuilt or replaced without touching the ingest path.

### Why Airflow does not run the stream

A stream is a service, not a scheduled job. The producer runs continuously under
Docker; Airflow orchestrates the batch work that sits around the stream - a
daily data quality report that computes the quarantine rate by error type and
fails the DAG if it crosses a threshold. A failing DAG is the alert.

---

## Data modelling decisions

**Deduplication key.** `(vehicle_id, naptan_id, expected_arrival)` - one row per
"train X is predicted at station Y at time Z". Implemented with
`dropDuplicatesWithinWatermark` (Spark 3.5+), which bounds the state store to
the watermark window rather than remembering every key forever.

**Event time.** The watermark is on `prediction_ts`, when TfL generated the
prediction, not on ingestion time.

**Two direction columns, not one.** TfL exposes `direction` as
`inbound`/`outbound` (operational, relative to central London) and encodes a
compass bearing inside `platformName` (`"Southbound - Platform 5"`). The
`direction` field is populated on some records and absent on others. An early
version coalesced the two into a single column, which produced values from two
incompatible vocabularies depending on whether TfL happened to fill the field
in. The table queried cleanly and was meaningless. They are now separate
columns, each with one vocabulary, where `unknown` means the source did not say.

**Two latency metrics, not one.** `time_to_station` is service health - is the
tube running well. `source_lag_seconds` (TfL emit time to Kafka landing time) is
pipeline health - is the data fresh. Conflating them is a common mistake.

**Partition keys.** Every table is partitioned so no single partition grows
unbounded, and clustered so the common read is a sequential scan:

| Table | Partition key | Clustering |
| --- | --- | --- |
| `bronze_arrivals` | `(ingest_hour, kafka_partition)` | `kafka_offset` |
| `silver_arrivals` | `(line_id, arrival_date)` | `expected_arrival DESC` |
| `quarantine_arrivals` | `(ingest_hour)` | `error_type, ...` |
| `gold_line_health` | `(line_id)` | `window_start DESC` |

The quarantine key is the second version. The first put `error_type` in the
partition key, which made the obvious question - "what failed this hour?" - a
partial partition-key restriction that Cassandra cannot route, so it demanded
`ALLOW FILTERING`. In Cassandra you model for the query, and a query you did not
plan for is often awkward or impossible.

**Gold writes in update mode.** Append mode only emits a window once the
watermark passes it, so the served data would always be one watermark stale.
Update mode re-emits a window whenever it changes, and because a Cassandra
`INSERT` is an upsert, late data corrects the row in place with no merge logic.

---

## Running it

Requires Docker with at least 5 GB allocated.

```bash
cp .env.example .env

docker compose up -d cassandra_db
docker compose logs -f cassandra_db          # wait for "Startup complete"

docker compose up -d cassandra-init
docker compose up -d zookeeper broker kafka-init tfl-producer spark-stream
docker compose up -d spark-gold
```

Optional services sit behind profiles so they do not start by default:

```bash
docker compose --profile airflow up -d       # Airflow webserver + scheduler
docker compose --profile monitoring up -d    # Confluent Control Center
docker compose --profile registry up -d      # Schema Registry
```

### Inspecting the data

```sql
-- what got through
SELECT * FROM tfl.silver_arrivals
  WHERE line_id='victoria' AND arrival_date='2026-07-24' LIMIT 10;

-- what did not, and why
SELECT error_type, COUNT(*) FROM tfl.quarantine_arrivals
  WHERE ingest_hour='2026-07-24T18' GROUP BY error_type;

-- service health per line
SELECT line_id, window_start, active_vehicles, avg_time_to_station
  FROM tfl.gold_line_health WHERE line_id='victoria' LIMIT 5;
```

---

## Operational notes

**Memory.** Developed under a 5 GB Docker allocation on an 8 GB laptop. Every
service carries an explicit `mem_limit` and JVM heap cap. Cassandra was killed
repeatedly (exit 137) before tuning: the heap peaked at 217 MB of 1 GB, but the
chunk cache, memtable thresholds and mmap'd SSTable pages are all derived from
heap size and pushed the container past its cgroup limit. Reducing the heap
reduced total footprint by far more than the heap reduction itself.

**Schema changes.** `cassandra/init.cql` is idempotent and only applies to fresh
volumes. Existing clusters need `ALTER TABLE`. A production version would use
numbered migration files.

**Version pinning.** All images are pinned. Cassandra in particular does not
support downgrades - booting 4.1 against a data directory written by 5.0 fails
on commitlog replay and the node refuses to start.

**Known limitation.** `pipeline_metrics` records row counts inside
`foreachBatch`, which runs after deduplication, so `rows_in` and `rows_out` are
both post-dedup and the duplicate count reads as zero. The duplicate figure in
this README comes from comparing bronze, silver and quarantine counts directly.
Fixing this needs a separate pre-dedup counting query.

---

## Stack

Kafka 7.4 (Confluent), Spark 3.5 Structured Streaming, Cassandra 4.1,
Airflow 2.6, Docker Compose, Python 3.11.

## Not yet done

- Avro schemas on Schema Registry (currently JSON; the registry runs but is
  unused)
- Unit tests on the validation rules, and CI
- A serving layer over the gold table
- Backfilling `compass_direction` on rows written before that column existed,
  by replaying bronze