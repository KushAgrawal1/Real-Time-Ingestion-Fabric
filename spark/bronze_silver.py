
import sys
import time

from pyspark.sql import functions as F

from common import (
    CHECKPOINT_ROOT,
    CLEAN_SCHEMA,  # noqa: F401  (imported for symmetry / docs)
    CLEAN_TOPIC,
    KAFKA_BOOTSTRAP,
    KEYSPACE,
    RAW_TOPIC,
    STARTING_OFFSETS,
    TFL_PREDICTION_SCHEMA,
    WATERMARK_DELAY,
    build_spark,
    derive_compass_direction,
    normalise_direction,
    error_type_column,
    write_cassandra,
)

APP = "tfl-bronze-silver"


def read_raw(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        # Cap the batch so a restart after downtime does not try to swallow the
        # entire backlog in one micro-batch and OOM the executor.
        .option("maxOffsetsPerTrigger", "20000")
        .load()
    )


def with_envelope(raw_df):
    """Kafka coordinates + payload + parsed struct + validation verdict."""
    return (
        raw_df
        .select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_payload"),
        )
        .withColumn("ingest_hour", F.date_format(F.col("kafka_ts"), "yyyy-MM-dd'T'HH"))
        .withColumn("data", F.from_json(F.col("raw_payload"), TFL_PREDICTION_SCHEMA))
        .withColumn("error_type", error_type_column(F.col("data")))
    )


# ---------------------------------------------------------------------------
# Query 1: bronze
# ---------------------------------------------------------------------------
def start_bronze(envelope_df):
    bronze = envelope_df.select(
        "ingest_hour", "kafka_partition", "kafka_offset",
        "kafka_ts", "kafka_key", "raw_payload",
    )

    def sink(batch_df, batch_id):
        write_cassandra(batch_df, "bronze_arrivals")

    return (
        bronze.writeStream
        .foreachBatch(sink)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze")
        .outputMode("append")
        .queryName("bronze")
        .start()
    )


# ---------------------------------------------------------------------------
# Query 2: silver
# ---------------------------------------------------------------------------
def start_silver(envelope_df):
    typed = (
        envelope_df
        .filter(F.col("error_type").isNull())
        .select(
            F.col("data.lineId").alias("line_id"),
            F.col("data.lineName").alias("line_name"),
            F.col("data.vehicleId").alias("vehicle_id"),
            F.col("data.naptanId").alias("naptan_id"),
            F.col("data.stationName").alias("station_name"),
            F.col("data.platformName").alias("platform_name"),
            normalise_direction(F.col("data.direction")).alias("direction"),
            derive_compass_direction(F.col("data.platformName"))
                .alias("compass_direction"),
            F.col("data.destinationName").alias("destination_name"),
            F.col("data.towards").alias("towards"),
            F.col("data.currentLocation").alias("current_location"),
            F.col("data.timeToStation").alias("time_to_station"),
            F.col("data.modeName").alias("mode_name"),
            F.to_timestamp(F.col("data.expectedArrival")).alias("expected_arrival"),
            F.to_timestamp(F.col("data.timestamp")).alias("prediction_ts"),
            # TfL's own emit time, falling back to the top-level timestamp when
            # the timing block is absent.
            F.coalesce(
                F.to_timestamp(F.col("data.timing.sent")),
                F.to_timestamp(F.col("data.timestamp")),
            ).alias("source_sent_ts"),
            F.col("kafka_ts"),
        )
        # Seconds between TfL emitting the prediction and it landing in Kafka.
        # This is the honest ingestion latency number for the README - it
        # measures the whole path, not just how fast Spark runs.
        .withColumn(
            "source_lag_seconds",
            F.when(
                F.col("source_sent_ts").isNotNull(),
                F.unix_timestamp("kafka_ts") - F.unix_timestamp("source_sent_ts"),
            ).cast("int"),
        )
        .drop("kafka_ts")
    )

    # Event time is when TfL generated the prediction, not when we received it.
    # dropDuplicatesWithinWatermark (Spark 3.5+) bounds the state store: it only
    # remembers keys inside the watermark window instead of forever.
    deduped = (
        typed
        .withWatermark("prediction_ts", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["vehicle_id", "naptan_id", "expected_arrival"])
    )

    def sink(batch_df, batch_id):
        started = time.time()
        batch_df.persist()
        try:
            rows = batch_df.count()
            if rows == 0:
                return

            # -> Cassandra silver
            to_cassandra = (
                batch_df
                .withColumn("arrival_date", F.to_date(F.col("expected_arrival")))
                .withColumn("processed_at", F.current_timestamp())
                .select(
                    "line_id", "arrival_date", "expected_arrival", "vehicle_id",
                    "naptan_id", "line_name", "station_name", "platform_name",
                    "direction", "compass_direction", "destination_name",
                    "towards", "current_location",
                    "time_to_station", "prediction_ts", "mode_name",
                    "source_sent_ts", "source_lag_seconds", "processed_at",
                )
            )
            write_cassandra(to_cassandra, "silver_arrivals")

            # -> Kafka clean topic, which is the source for the gold job.
            # Kafka as the backbone between layers means gold can be rebuilt,
            # rewound or replaced without touching the silver job.
            payload = batch_df.select(
                "line_id", "line_name", "vehicle_id", "naptan_id", "station_name",
                "platform_name", "direction", "compass_direction",
                "destination_name", "towards",
                "current_location", "time_to_station", "mode_name",
                "source_lag_seconds",
                F.date_format("source_sent_ts", "yyyy-MM-dd'T'HH:mm:ss").alias("source_sent_ts"),
                F.date_format("expected_arrival", "yyyy-MM-dd'T'HH:mm:ss").alias("expected_arrival"),
                F.date_format("prediction_ts", "yyyy-MM-dd'T'HH:mm:ss").alias("prediction_ts"),
            )
            (
                payload
                .select(
                    F.col("line_id").cast("string").alias("key"),
                    F.to_json(F.struct("*")).alias("value"),
                )
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", CLEAN_TOPIC)
                .save()
            )

            _record_metrics(
                batch_df.sparkSession, "silver", batch_id,
                rows_in=rows, rows_out=rows, rows_quarantined=0,
                duration_ms=int((time.time() - started) * 1000),
            )
        finally:
            batch_df.unpersist()

    return (
        deduped.writeStream
        .foreachBatch(sink)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver")
        .outputMode("append")
        .queryName("silver")
        .start()
    )


# ---------------------------------------------------------------------------
# Query 3: quarantine
# ---------------------------------------------------------------------------
def start_quarantine(envelope_df):
    bad = (
        envelope_df
        .filter(F.col("error_type").isNotNull())
        .select(
            "ingest_hour", "error_type", "kafka_partition",
            "kafka_offset", "raw_payload",
        )
        .withColumn("quarantined_at", F.current_timestamp())
    )

    def sink(batch_df, batch_id):
        started = time.time()
        batch_df.persist()
        try:
            rows = batch_df.count()
            if rows == 0:
                return
            write_cassandra(batch_df, "quarantine_arrivals")
            _record_metrics(
                batch_df.sparkSession, "quarantine", batch_id,
                rows_in=rows, rows_out=0, rows_quarantined=rows,
                duration_ms=int((time.time() - started) * 1000),
            )
        finally:
            batch_df.unpersist()

    return (
        bad.writeStream
        .foreachBatch(sink)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/quarantine")
        .outputMode("append")
        .queryName("quarantine")
        .start()
    )


# ---------------------------------------------------------------------------
def _record_metrics(spark, job_name, batch_id, rows_in, rows_out,
                    rows_quarantined, duration_ms):
    """Persist per-micro-batch counts so throughput and quarantine rate are
    queryable facts rather than something you eyeball in the logs."""
    row = spark.createDataFrame(
        [(job_name, int(batch_id), int(rows_in), int(rows_out),
          int(rows_quarantined), int(duration_ms))],
        "job_name string, batch_id long, rows_in long, rows_out long, "
        "rows_quarantined long, duration_ms long",
    ).withColumn("batch_ts", F.current_timestamp()) \
     .withColumn("metric_date", F.current_date())
    write_cassandra(row, "pipeline_metrics")


def main() -> int:
    spark = build_spark(APP)
    envelope = with_envelope(read_raw(spark))

    start_bronze(envelope)
    start_silver(envelope)
    start_quarantine(envelope)

    print(f"[{APP}] three queries running against keyspace '{KEYSPACE}'", flush=True)
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
