

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

from quality_checks import (
    fetch_bronze_volume,
    fetch_quarantine_counts,
    fetch_throughput_stats,
    quarantine_rate,
)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = "tfl"

# Trip the alarm if more than 5% of everything ingested was quarantined.
QUARANTINE_RATE_THRESHOLD = 0.05

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

log = logging.getLogger(__name__)


def _session():
    from cassandra.cluster import Cluster
    return Cluster([CASSANDRA_HOST]).connect(KEYSPACE)


def summarise_quarantine(**context):
    """Break yesterday's quarantined records down by reason."""
    ds = context["ds"]
    counts = fetch_quarantine_counts(_session(), ds)
    total = sum(counts.values())
    log.info("quarantined on %s: %s (total %s)", ds, counts, total)
    context["ti"].xcom_push(key="quarantine_by_type", value=counts)
    context["ti"].xcom_push(key="quarantine_total", value=total)
    return counts


def summarise_bronze_volume(**context):
    """Total records ingested to bronze for the day - the correct
    denominator for the quarantine rate, since it is counted before
    deduplication runs. See quarantine_rate()'s docstring."""
    ds = context["ds"]
    total = fetch_bronze_volume(_session(), ds)
    log.info("bronze volume on %s: %s", ds, total)
    context["ti"].xcom_push(key="bronze_total", value=total)
    return total


def summarise_throughput(**context):
    """Silver job batch stats, for latency/volume observability only.

    rows_in here is counted inside silver's foreachBatch sink, which runs
    AFTER dropDuplicatesWithinWatermark - so it is the post-dedup row
    count, not a true input count. It must not be used as the quality
    gate's denominator (that was the original bug); use
    summarise_bronze_volume for that instead.
    """
    ds = context["ds"]
    stats = fetch_throughput_stats(_session(), ds)
    log.info(
        "throughput on %s: batches=%s rows_in(post-dedup)=%s avg_batch_ms=%s",
        ds, stats["batches"], stats["rows_in"], stats["avg_batch_ms"],
    )
    return stats


def enforce_quality_gate(**context):
    """Fail loudly if the quarantine rate crossed the threshold."""
    ti = context["ti"]
    quarantined = ti.xcom_pull(task_ids="summarise_quarantine", key="quarantine_total") or 0
    bronze_total = ti.xcom_pull(task_ids="summarise_bronze_volume", key="bronze_total") or 0

    if bronze_total == 0:
        log.warning("no bronze records at all for %s - the producer may have been down",
                    context["ds"])
        return

    rate = quarantine_rate(quarantined, bronze_total)
    log.info("quarantine rate: %.4f (%s of %s)", rate, quarantined, bronze_total)

    if rate > QUARANTINE_RATE_THRESHOLD:
        raise AirflowFailException(
            f"quarantine rate {rate:.2%} exceeds threshold "
            f"{QUARANTINE_RATE_THRESHOLD:.2%} on {context['ds']}"
        )


with DAG(
    dag_id="tfl_daily_quality_report",
    default_args=default_args,
    description="Daily quality and throughput report over the TfL streaming pipeline",
    start_date=datetime(2026, 7, 23),
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["tfl", "data-quality"],
) as dag:

    t_quarantine = PythonOperator(
        task_id="summarise_quarantine",
        python_callable=summarise_quarantine,
    )

    t_bronze_volume = PythonOperator(
        task_id="summarise_bronze_volume",
        python_callable=summarise_bronze_volume,
    )

    t_throughput = PythonOperator(
        task_id="summarise_throughput",
        python_callable=summarise_throughput,
    )

    t_gate = PythonOperator(
        task_id="enforce_quality_gate",
        python_callable=enforce_quality_gate,
    )

    [t_quarantine, t_bronze_volume, t_throughput] >> t_gate
