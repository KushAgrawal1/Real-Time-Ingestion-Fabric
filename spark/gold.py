

import sys

from pyspark.sql import functions as F

from common import (
    CHECKPOINT_ROOT,
    CLEAN_SCHEMA,
    CLEAN_TOPIC,
    KAFKA_BOOTSTRAP,
    STARTING_OFFSETS,
    WATERMARK_DELAY,
    build_spark,
    write_cassandra,
)

APP = "tfl-gold"
WINDOW_SIZE = "5 minutes"


def read_clean(spark):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", CLEAN_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw
        .select(F.from_json(F.col("value").cast("string"), CLEAN_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("prediction_ts", F.to_timestamp("prediction_ts"))
        .withColumn("expected_arrival", F.to_timestamp("expected_arrival"))
        .filter(F.col("prediction_ts").isNotNull())
    )


def aggregate(clean_df):
    return (
        clean_df
        .withWatermark("prediction_ts", WATERMARK_DELAY)
        .groupBy(
            F.window(F.col("prediction_ts"), WINDOW_SIZE).alias("w"),
            F.col("line_id"),
        )
        .agg(
            F.max("line_name").alias("line_name"),
            F.count(F.lit(1)).alias("predictions"),
            # approx_count_distinct keeps state bounded. Exact distinct counts
            # in a stateful stream mean holding every value seen in the window.
            F.approx_count_distinct("vehicle_id").alias("active_vehicles"),
            F.approx_count_distinct("naptan_id").alias("stations_covered"),
            F.avg("time_to_station").alias("avg_time_to_station"),
            F.min("time_to_station").alias("min_time_to_station"),
            F.max("time_to_station").alias("max_time_to_station"),
            F.stddev("time_to_station").alias("stddev_time_to_station"),
            # Ingestion health, not service health: how stale was the data we
            # built this window from.
            F.avg("source_lag_seconds").alias("avg_source_lag_seconds"),
            F.max("source_lag_seconds").alias("max_source_lag_seconds"),
        )
        .select(
            F.col("line_id"),
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("line_name"),
            F.col("predictions").cast("long"),
            F.col("active_vehicles").cast("long"),
            F.col("stations_covered").cast("long"),
            F.col("avg_time_to_station").cast("double"),
            F.col("min_time_to_station").cast("int"),
            F.col("max_time_to_station").cast("int"),
            # stddev is NULL when a window has a single row - coalesce so the
            # Cassandra column is never an unexplained null.
            F.coalesce(F.col("stddev_time_to_station"), F.lit(0.0))
                .cast("double").alias("stddev_time_to_station"),
            F.coalesce(F.col("avg_source_lag_seconds"), F.lit(0.0))
                .cast("double").alias("avg_source_lag_seconds"),
            F.coalesce(F.col("max_source_lag_seconds"), F.lit(0))
                .cast("int").alias("max_source_lag_seconds"),
        )
    )


def main() -> int:
    spark = build_spark(APP)
    gold = aggregate(read_clean(spark))

    def sink(batch_df, batch_id):
        # The emptiness check forces evaluation of the whole aggregation.
        # Without persist, write_cassandra then recomputes it from scratch -
        # two full passes over every micro-batch. Same pattern as the silver
        # sink in bronze_silver.py.
        batch_df.persist()
        try:
            if batch_df.isEmpty():
                return
            write_cassandra(
                batch_df.withColumn("computed_at", F.current_timestamp()),
                "gold_line_health",
            )
        finally:
            batch_df.unpersist()
    query = (
        gold.writeStream
        .foreachBatch(sink)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/gold")
        .queryName("gold")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print(f"[{APP}] aggregating {WINDOW_SIZE} windows from '{CLEAN_TOPIC}'", flush=True)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
