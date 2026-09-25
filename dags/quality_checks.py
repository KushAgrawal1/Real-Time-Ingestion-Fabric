"""
Data-quality checks for the daily quarantine report, kept free of any Airflow
import so they can be unit tested without a running Airflow environment.

tfl_daily_quality_report.py wires these into PythonOperators; the logic that
decides what "quality" means lives here.
"""
from datetime import datetime

KAFKA_PARTITIONS = (0, 1, 2, 3, 4, 5)


def quarantine_rate(quarantined: int, bronze_total: int) -> float:
    """
    Fraction of everything ingested that ended up quarantined.

    bronze_total must be the pre-dedup count (bronze_arrivals), not the
    silver job's rows_in - dropDuplicatesWithinWatermark runs upstream of
    silver's foreachBatch sink, so anything counted inside that sink is
    already post-dedup. Using it as the denominator drops every duplicate
    from the count and inflates the rate by roughly the dedup factor - on
    the one full hour measured so far, silver kept about 55% of what
    bronze received, so the old denominator was missing nearly half the
    true total.
    """
    if bronze_total <= 0:
        return 0.0
    return quarantined / bronze_total


def fetch_quarantine_counts(session, ds: str) -> dict:
    """Quarantined record count per error_type for the whole day.

    quarantine_arrivals is partitioned on ingest_hour alone, so each hour
    is a single-partition read and no ALLOW FILTERING is needed.
    """
    counts = {}
    for hour in range(24):
        ingest_hour = f"{ds}T{hour:02d}"
        rows = session.execute(
            "SELECT error_type, COUNT(*) AS n FROM quarantine_arrivals "
            "WHERE ingest_hour = %s GROUP BY error_type",
            (ingest_hour,),
        )
        for row in rows:
            counts[row.error_type] = counts.get(row.error_type, 0) + row.n
    return counts


def fetch_bronze_volume(session, ds: str) -> int:
    """Total records ingested to bronze for the whole day.

    bronze_arrivals is partitioned on (ingest_hour, kafka_partition), so
    summing one hour across all 6 Kafka partitions needs an explicit
    kafka_partition IN (...) restriction - the same shape used to inspect
    a single hour in the README, just looped over the full day.
    """
    total = 0
    for hour in range(24):
        ingest_hour = f"{ds}T{hour:02d}"
        rows = session.execute(
            "SELECT COUNT(*) AS n FROM bronze_arrivals "
            "WHERE ingest_hour = %s AND kafka_partition IN %s",
            (ingest_hour, KAFKA_PARTITIONS),
        )
        total += rows.one().n
    return total


def fetch_throughput_stats(session, ds: str) -> dict:
    """Silver job's own batch stats - useful for latency/volume observability,
    but rows_in here is POST-dedup (see quarantine_rate's docstring) and
    must never be used as the quality-gate denominator.
    """
    rows = session.execute(
        "SELECT rows_in, rows_out, duration_ms FROM pipeline_metrics "
        "WHERE job_name = %s AND metric_date = %s",
        ("silver", datetime.strptime(ds, "%Y-%m-%d").date()),
    )

    batches = rows_in = rows_out = duration = 0
    for row in rows:
        batches += 1
        rows_in += row.rows_in or 0
        rows_out += row.rows_out or 0
        duration += row.duration_ms or 0

    avg_batch_ms = round(duration / batches, 1) if batches else 0.0
    return {
        "batches": batches,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "avg_batch_ms": avg_batch_ms,
    }
