# Real-Time Ingestion Fabric

An end-to-end real-time data engineering pipeline that streams randomly generated user data from a public API through Kafka, processes it with Spark Structured Streaming, and persists it to Cassandra — fully orchestrated with Apache Airflow and containerised with Docker.

---

## Architecture

```
randomuser.me API
       │
       ▼
 Apache Airflow          ← orchestrates & schedules the producer DAG
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
| Stream Processing | Apache Spark 3.5.0 (PySpark) |
| Storage | Apache Cassandra 5.0 |
| Monitoring | Confluent Control Center |
| Schema Management | Confluent Schema Registry |
| Containerisation | Docker + Docker Compose |
| Language | Python 3.9 / 3.11 |

---

## Pipeline Overview

### 1. Ingestion (Airflow DAG)
- `kafka_stream.py` defines a DAG (`user_automation`) scheduled to run daily
- On trigger, it calls the [randomuser.me](https://randomuser.me) API every second for 60 seconds
- Each response is formatted and published to the Kafka topic `users_created` as a JSON message

### 2. Processing (Spark Structured Streaming)
- `spark_stream.py` reads from the `users_created` Kafka topic using Spark's `readStream`
- Deserialises the JSON payload against a defined schema
- Writes each micro-batch to Cassandra via the Spark-Cassandra connector

### 3. Storage (Cassandra)
- Keyspace: `spark_streams`
- Table: `created_users`
- Primary key: `id` (UUID)

---

## Project Structure

```
.
├── dags/
│   └── kafka_stream.py          # Airflow DAG — API ingestion + Kafka producer
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
git clone https://github.com/KushAgrawal1/real-time-ingestion-fabric.git
cd real-time-ingestion-fabric
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

Open `http://localhost:8080` (user: `admin`, password: `admin`), find the `user_automation` DAG and trigger it manually. This starts publishing messages to the `users_created` Kafka topic.

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

---

## Monitoring

- **Confluent Control Center**: `http://localhost:9021` — view topics, consumer groups, and message throughput
- **Airflow UI**: `http://localhost:8080` — monitor DAG runs and task logs
- **Spark Master UI**: `http://localhost:9090` — view active streaming jobs

---

## Notes

- `spark_stream.py` runs on the host machine (not inside Docker), so it connects to services via `localhost` ports
- The Airflow DAG runs inside Docker, so it connects to Kafka via `broker:29092` (internal Docker network)
- Checkpoints are stored at `/tmp/checkpoint` — delete this folder if restarting from scratch
- The Kafka topic `users_created` is auto-created on first message; to pre-create it manually run:

```bash
docker exec -it broker kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic users_created
```
