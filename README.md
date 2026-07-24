# Real-Time Ingestion Fabric

An end-to-end real-time data engineering pipeline that generates synthetic user events, streams them through Kafka at 10,000+ events/sec, processes them with Spark Structured Streaming, and persists them to Cassandra — fully orchestrated with Apache Airflow and containerised with Docker.

---

## Performance

| Metric | Result |
|---|---|
| Total events produced | 850,975 |
| Total rows written to Cassandra | 850,117 |
| Message delivery rate | 99.9% |
| Producer throughput | 10,000+ events/sec |
| DAG runs | 10 successful runs |
| Mean DAG run duration | 1 min 18 sec |
| Services on single Docker network | 7 |

---

## Architecture

```
Synthetic Data Generator (Airflow DAG)
       │
       ▼
 Apache Kafka            ← message broker (topic: users_created)
       │
       ▼
 Apache Spark            ← structured streaming consumer
       │
       ▼
 Apache Cassandra        ← persistent storage (spark_streams.created_users)
```

All services run on a shared Docker network (`confluent`), with Confluent Control Center for Kafka monitoring and Schema Registry for schema management.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.6.0 |
| Message Broker | Apache Kafka (Confluent 7.4.0) |
| Stream Processing | Apache Spark 3.5.3 (PySpark) |
| Storage | Apache Cassandra 5.0 |
| Monitoring | Confluent Control Center |
| Schema Management | Confluent Schema Registry |
| Containerisation | Docker + Docker Compose |
| Language | Python 3.9 / 3.11 |

---

## Pipeline Overview

### 1. Ingestion (Airflow DAG)
- `kafka_stream.py` defines a DAG (`user_automation`) scheduled to run daily
- On trigger, a synthetic data generator produces realistic user records (names, addresses, emails, phone numbers) at maximum throughput — no API rate limits
- Each record is serialised as JSON and published to the Kafka topic `users_created`
- Producer is configured with `batch_size=65536`, `linger_ms=10`, and `gzip` compression for high throughput
- Each DAG run streams for 60 seconds, producing ~85,000 events per run

### 2. Processing (Spark Structured Streaming)
- `spark_stream.py` reads from the `users_created` Kafka topic using Spark's `readStream`
- Deserialises the JSON payload against a defined schema
- Writes each micro-batch to Cassandra via the Spark-Cassandra connector
- Checkpoint location `/tmp/checkpoint` ensures exactly-once processing semantics

### 3. Storage (Cassandra)
- Keyspace: `spark_streams`
- Table: `created_users`
- Primary key: `id` (UUID)
- SSTable compression ratio: 0.63 (Cassandra compressed data to 63% of original size)

---

## Project Structure

```
.
├── dags/
│   └── kafka_stream.py          # Airflow DAG — synthetic data generator + Kafka producer
├── spark_stream.py              # Spark Structured Streaming consumer → Cassandra
├── docker-compose.yml           # Full stack: Zookeeper, Kafka, Spark, Airflow, Cassandra
├── script/
│   └── entrypoint.sh            # Airflow webserver entrypoint
├── requirements.txt             # Python dependencies
└── README.md
```

---

## Getting Started

### Prerequisites
- Docker + Docker Compose
- Python 3.9+

### 1. Clone the repo

```bash
git clone https://github.com/KushAgrawal1/Real-Time-Ingestion-Fabric.git
cd Real-Time-Ingestion-Fabric
```

### 2. Start all services

```bash
docker compose up -d
```

This starts:
- Zookeeper → `localhost:2181`
- Kafka Broker → `localhost:9092`
- Schema Registry → `localhost:8081`
- Confluent Control Center → `localhost:9021`
- Airflow Webserver → `localhost:8080`
- Spark Master → `localhost:9090`
- Cassandra → `localhost:9042`

### 3. Trigger the Airflow DAG

Open `http://localhost:8080` (user: `admin`, password: `admin`), find the `user_automation` DAG and trigger it manually. This starts the synthetic data generator and publishes messages to the `users_created` Kafka topic.

### 4. Run the Spark consumer

```bash
pip install -r requirements.txt
python spark_stream.py
```

Spark will connect to Kafka, consume messages, and write them to Cassandra. The process runs continuously — `awaitTermination()` keeps the stream alive.

### 5. Verify data in Cassandra

```bash
docker exec -it cassandra cqlsh -u cassandra -p cassandra
```

```sql
SELECT * FROM spark_streams.created_users LIMIT 10;
SELECT COUNT(*) FROM spark_streams.created_users;
```

---

## Kafka Topic Verification

To confirm messages are flowing into Kafka:

```bash
docker exec -it broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic users_created \
  --from-beginning \
  --max-messages 5
```

To check total messages produced:

```bash
docker exec -it broker kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic users_created
```

---

## Monitoring

- **Confluent Control Center**: `http://localhost:9021` — view topics, consumer groups, and message throughput
- **Airflow UI**: `http://localhost:8080` — monitor DAG runs and task logs
- **Spark Master UI**: `http://localhost:9090` — view active streaming jobs
- **Spark Streaming UI**: `http://localhost:4040` — view micro-batch stats and processing time

---

## Notes

- `spark_stream.py` runs on the host machine (not inside Docker), so it connects to services via `localhost` ports
- The Airflow DAG runs inside Docker, so it connects to Kafka via `broker:29092` (internal Docker network)
- Checkpoints are stored at `/tmp/checkpoint` — delete this folder if restarting from scratch
- Cassandra requires at least 256MB heap — configured via `MAX_HEAP_SIZE` in docker-compose.yml
- For high-volume runs (850K+ events), allow Cassandra 3-4 minutes to flush writes before running COUNT queries

```bash
docker exec -it broker kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic users_created
```