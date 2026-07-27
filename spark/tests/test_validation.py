
import json

import pytest
from pyspark.sql import functions as F

from common import TFL_PREDICTION_SCHEMA, error_type_column


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .appName("validation-rules-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# A record that passes every rule. Individual tests mutate a copy of this.
VALID_PREDICTION = {
    "id": "abc123",
    "operationType": 1,
    "vehicleId": "217",
    "naptanId": "940GZZLUASL",
    "stationName": "Arsenal Underground Station",
    "lineId": "piccadilly",
    "lineName": "Piccadilly",
    "platformName": "Southbound - Platform 1",
    "direction": "inbound",
    "bearing": "180",
    "destinationNaptanId": "940GZZLUCVT",
    "destinationName": "Cockfosters",
    "timestamp": "2026-07-27T09:00:00Z",
    "timeToStation": 120,
    "currentLocation": "At Platform",
    "towards": "Cockfosters",
    "expectedArrival": "2026-07-27T09:02:00Z",
    "timeToLive": "2026-07-27T09:02:30Z",
    "modeName": "tube",
    "timing": {
        "countdownServerAdjustment": "0",
        "source": "live",
        "insert": "2026-07-27T09:00:01Z",
        "read": "2026-07-27T09:00:00Z",
        "sent": "2026-07-27T09:00:01Z",
        "received": "2026-07-27T09:00:02Z",
    },
}


def classify(spark, raw_payload):
    """Parse a raw JSON string (or None) through the same path bronze_silver
    uses, and return the resulting error_type value."""
    df = spark.createDataFrame([(raw_payload,)], "raw_payload string")
    result = (
        df
        .withColumn("data", F.from_json(F.col("raw_payload"), TFL_PREDICTION_SCHEMA))
        .withColumn("error_type", error_type_column(F.col("data")))
        .select("error_type")
        .collect()
    )
    return result[0]["error_type"]


def record(**overrides):
    """A valid prediction with the given fields overridden, as a JSON string."""
    payload = dict(VALID_PREDICTION)
    payload.update(overrides)
    return json.dumps(payload)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------
def test_valid_record_passes(spark):
    assert classify(spark, record()) is None


# ---------------------------------------------------------------------------
# One test per rule, in the order they appear in error_type_column
# ---------------------------------------------------------------------------
def test_unparseable_json(spark):
    """from_json's default PERMISSIVE mode nulls out individual fields on
    malformed text rather than failing the whole struct, so the only way
    data.isNull() actually fires is a NULL payload (e.g. a Kafka tombstone
    message or an empty value)."""
    assert classify(spark, None) == "unparseable_json"


def test_malformed_json_is_not_treated_as_unparseable_json(spark):
    """Documents the PERMISSIVE-mode behaviour above: garbled-but-non-null
    text parses into a struct with every field null, so it is reported as
    whichever field-level rule fires first, not 'unparseable_json'."""
    assert classify(spark, "{not valid json") == "missing_line_id"


@pytest.mark.parametrize("bad_value", [None, "", "   "])
def test_missing_line_id(spark, bad_value):
    assert classify(spark, record(lineId=bad_value)) == "missing_line_id"


@pytest.mark.parametrize("bad_value", [None, "", "  "])
def test_missing_naptan_id(spark, bad_value):
    assert classify(spark, record(naptanId=bad_value)) == "missing_naptan_id"


@pytest.mark.parametrize("bad_value", [None, "", "  "])
def test_missing_vehicle_id(spark, bad_value):
    assert classify(spark, record(vehicleId=bad_value)) == "missing_vehicle_id"


def test_placeholder_vehicle_id(spark):
    assert classify(spark, record(vehicleId="000")) == "placeholder_vehicle_id"


@pytest.mark.parametrize("bad_value", [None, "", "  "])
def test_missing_expected_arrival(spark, bad_value):
    assert classify(spark, record(expectedArrival=bad_value)) == "missing_expected_arrival"


def test_unparseable_expected_arrival(spark):
    assert classify(spark, record(expectedArrival="not-a-timestamp")) \
        == "unparseable_expected_arrival"


def test_unparseable_prediction_timestamp(spark):
    assert classify(spark, record(timestamp="not-a-timestamp")) \
        == "unparseable_prediction_timestamp"


def test_missing_time_to_station(spark):
    assert classify(spark, record(timeToStation=None)) == "missing_time_to_station"


def test_negative_time_to_station(spark):
    assert classify(spark, record(timeToStation=-5)) == "negative_time_to_station"


def test_time_to_station_zero_is_valid(spark):
    """A train already at the platform (0 seconds out) must not be flagged."""
    assert classify(spark, record(timeToStation=0)) is None


def test_implausible_time_to_station(spark):
    assert classify(spark, record(timeToStation=3601)) == "implausible_time_to_station"


def test_time_to_station_at_threshold_is_valid(spark):
    """3600 is the documented boundary and should still pass."""
    assert classify(spark, record(timeToStation=3600)) is None


# ---------------------------------------------------------------------------
# Precedence: the earliest rule in the chain should win when several fail
# ---------------------------------------------------------------------------
def test_missing_naptan_id_wins_over_missing_vehicle_id(spark):
    """missing_naptan_id is checked before missing_vehicle_id, so with both
    fields blank the naptan failure should be reported, not the vehicle one."""
    payload = record(naptanId=None, vehicleId=None)
    assert classify(spark, payload) == "missing_naptan_id"


def test_placeholder_vehicle_id_wins_over_missing_expected_arrival(spark):
    payload = record(vehicleId="000", expectedArrival=None)
    assert classify(spark, payload) == "placeholder_vehicle_id"


def test_negative_time_to_station_wins_over_implausible_check(spark):
    """A record can't be both negative and > MAX_PLAUSIBLE_TIME_TO_STATION,
    but this pins down that the negative check is the one that runs first
    for negative values, rather than falling through unexpectedly."""
    payload = record(timeToStation=-100)
    assert classify(spark, payload) == "negative_time_to_station"