

import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Config (env driven so the same code runs locally and in the cluster)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "tfl.arrivals.raw")
CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "tfl.arrivals.clean")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "tfl")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/tmp/checkpoints")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")

# How long we wait for stragglers before finalising state. TfL predictions have
# a short life, so 10 minutes is generous without holding much state.
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")

# A prediction more than an hour out is not a real prediction.
MAX_PLAUSIBLE_TIME_TO_STATION = 3600


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
# Raw shape of a TfL Prediction entity. Everything is nullable on purpose -
# we do not want from_json to fail, we want to catch the problem ourselves and
# route it to quarantine with a reason attached.
TFL_PREDICTION_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("operationType", IntegerType(), True),
    StructField("vehicleId", StringType(), True),
    StructField("naptanId", StringType(), True),
    StructField("stationName", StringType(), True),
    StructField("lineId", StringType(), True),
    StructField("lineName", StringType(), True),
    StructField("platformName", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("bearing", StringType(), True),
    StructField("destinationNaptanId", StringType(), True),
    StructField("destinationName", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("timeToStation", IntegerType(), True),
    StructField("currentLocation", StringType(), True),
    StructField("towards", StringType(), True),
    StructField("expectedArrival", StringType(), True),
    StructField("timeToLive", StringType(), True),
    StructField("modeName", StringType(), True),
    # TfL's own pipeline timestamps. timing.sent is when TfL emitted the
    # prediction, which lets us measure true source-to-gold latency rather than
    # only measuring our own processing time.
    StructField("timing", StructType([
        StructField("countdownServerAdjustment", StringType(), True),
        StructField("source", StringType(), True),
        StructField("insert", StringType(), True),
        StructField("read", StringType(), True),
        StructField("sent", StringType(), True),
        StructField("received", StringType(), True),
    ]), True),
])

# Shape of the silver records we republish to Kafka for the gold job.
# Timestamps travel as ISO strings and are re-cast on the way in.
CLEAN_SCHEMA = StructType([
    StructField("line_id", StringType(), True),
    StructField("line_name", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("naptan_id", StringType(), True),
    StructField("station_name", StringType(), True),
    StructField("platform_name", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("compass_direction", StringType(), True),
    StructField("destination_name", StringType(), True),
    StructField("towards", StringType(), True),
    StructField("current_location", StringType(), True),
    StructField("time_to_station", IntegerType(), True),
    StructField("mode_name", StringType(), True),
    StructField("expected_arrival", StringType(), True),
    StructField("prediction_ts", StringType(), True),
    StructField("source_sent_ts", StringType(), True),
    StructField("source_lag_seconds", IntegerType(), True),
])


# TfL exposes two different notions of direction, and they must not be mixed:
#
#   direction         "inbound" / "outbound" - operational, relative to central
#                     London. Present on some records, absent on others.
#   platformName      "Northbound - Platform 3" - compass bearing, present on
#                     essentially every record.
#
# Coalescing one into the other produces a column with two vocabularies, where
# the value you get depends on whether TfL happened to populate a field. That
# column cannot be grouped by. So we keep them separate: each column has
# exactly one vocabulary, and "unknown" means the source did not tell us.
DIRECTION_PATTERN = r"(north|south|east|west|inner|outer)bound"


def normalise_direction(direction_col: Column) -> Column:
    """TfL's operational direction, lowercased. Never derived from anything."""
    return F.when(
        (direction_col.isNotNull()) & (F.trim(direction_col) != F.lit("")),
        F.lower(F.trim(direction_col)),
    ).otherwise(F.lit("unknown"))


def derive_compass_direction(platform_col: Column) -> Column:
    """Compass bearing pulled out of platformName. Always derived, never taken
    from the direction field, so the vocabulary stays consistent."""
    extracted = F.regexp_extract(F.lower(F.coalesce(platform_col, F.lit(""))),
                                 DIRECTION_PATTERN, 0)
    return F.when(extracted != F.lit(""), extracted).otherwise(F.lit("unknown"))


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# The connector's DataSource V2 path resolves connection settings from the
# consolidated write options, not reliably from session-level config, so we
# pass them explicitly on every write. Without this the driver falls back to
# its default contact point of 127.0.0.1 and nothing can connect.
CASSANDRA_CONNECTION_OPTIONS = {
    "spark.cassandra.connection.host": CASSANDRA_HOST,
    "spark.cassandra.connection.port": str(CASSANDRA_PORT),
}


def write_cassandra(df: DataFrame, table: str) -> None:
    """Batch write into Cassandra. INSERT is an upsert, so this is idempotent
    as long as the DataFrame columns match the table's primary key."""
    (
        df.write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace=KEYSPACE, table=table, **CASSANDRA_CONNECTION_OPTIONS)
        .mode("append")
        .save()
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def _blank(col: Column) -> Column:
    return col.isNull() | (F.trim(col) == F.lit(""))


def error_type_column(data: Column) -> Column:
    """Returns the first validation failure for a parsed record, or NULL if the
    record is clean. Order matters: the most fundamental failure wins, so the
    quarantine table tells you the real cause rather than a downstream symptom.

    Every rule here came from actually looking at the live feed:
      - TfL emits vehicleId '000' for trains it cannot identify
      - timeToStation is occasionally negative when a train has already arrived
      - a handful of records arrive with no expectedArrival at all
    """
    return (
        F.when(data.isNull(), F.lit("unparseable_json"))
        .when(_blank(data.getField("lineId")), F.lit("missing_line_id"))
        .when(_blank(data.getField("naptanId")), F.lit("missing_naptan_id"))
        .when(_blank(data.getField("vehicleId")), F.lit("missing_vehicle_id"))
        .when(data.getField("vehicleId") == F.lit("000"), F.lit("placeholder_vehicle_id"))
        .when(_blank(data.getField("expectedArrival")), F.lit("missing_expected_arrival"))
        .when(F.to_timestamp(data.getField("expectedArrival")).isNull(),
              F.lit("unparseable_expected_arrival"))
        .when(F.to_timestamp(data.getField("timestamp")).isNull(),
              F.lit("unparseable_prediction_timestamp"))
        .when(data.getField("timeToStation").isNull(), F.lit("missing_time_to_station"))
        .when(data.getField("timeToStation") < 0, F.lit("negative_time_to_station"))
        .when(data.getField("timeToStation") > MAX_PLAUSIBLE_TIME_TO_STATION,
              F.lit("implausible_time_to_station"))
        .otherwise(F.lit(None).cast(StringType()))
    )
