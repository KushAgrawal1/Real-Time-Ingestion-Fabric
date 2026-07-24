

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = "tfl"

# Trip the alarm if more than 5% of records were quarantined.
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
    session = _session()

    counts = {}
    for hour in range(24):
        ingest_hour = f"{ds}T{hour:02d}"
        rows = session.execute(
            "SELECT error_type, COUNT(*) AS n FROM quarantine_arrivals "
            "WHERE ingest_hour = %s GROUP BY error_type ALLOW FILTERING",
            (ingest_hour,),
        )
        for row in rows:
            counts[row.error_type] = counts.get(row.error_type, 0) + row.n

    total = sum(counts.values())
    log.info("quarantined on %s: %s (total %s)", ds, counts, total)
    context["ti"].xcom_push(key="quarantine_by_type", value=counts)
    context["ti"].xcom_push(key="quarantine_total", value=total)
    return counts


def summarise_throughput(**context):
    """Total rows in and out of the silver job for the day."""
    ds = context["ds"]
    session = _session()

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
    log.info(
        "throughput on %s: batches=%s rows_in=%s rows_out=%s avg_batch_ms=%s",
        ds, batches, rows_in, rows_out, avg_batch_ms,
    )
    context["ti"].xcom_push(key="rows_in", value=rows_in)
    return {"batches": batches, "rows_in": rows_in, "avg_batch_ms": avg_batch_ms}


def enforce_quality_gate(**context):
    """Fail loudly if the quarantine rate crossed the threshold."""
    ti = context["ti"]
    quarantined = ti.xcom_pull(task_ids="summarise_quarantine", key="quarantine_total") or 0
    processed = ti.xcom_pull(task_ids="summarise_throughput", key="rows_in") or 0

    total = quarantined + processed
    if total == 0:
        log.warning("no records at all for %s - the producer may have been down",
                    context["ds"])
        return

    rate = quarantined / total
    log.info("quarantine rate: %.4f (%s of %s)", rate, quarantined, total)

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

    t_throughput = PythonOperator(
        task_id="summarise_throughput",
        python_callable=summarise_throughput,
    )

    t_gate = PythonOperator(
        task_id="enforce_quality_gate",
        python_callable=enforce_quality_gate,
    )

    [t_quarantine, t_throughput] >> t_gate
