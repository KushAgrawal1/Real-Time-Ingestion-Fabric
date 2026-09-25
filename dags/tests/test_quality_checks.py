"""No Airflow import anywhere in this file - quality_checks.py has none,
so these run with nothing but the stdlib and pytest."""
import pytest

from quality_checks import (
    fetch_bronze_volume,
    fetch_quarantine_counts,
    fetch_throughput_stats,
    quarantine_rate,
)


# ---- quarantine_rate --------------------------------------------------------

def test_matches_the_readmes_worked_example_for_24_july():
    """README: 14,817 quarantined against 286,113 post-dedup rows_in gave
    the old code 4.92% (missing the removed duplicates entirely); against
    the true bronze total of 526,214 the honest rate is 2.82%."""
    quarantined, rows_in, bronze_total = 14_817, 286_113, 526_214

    old_buggy_rate = quarantined / (quarantined + rows_in)
    assert old_buggy_rate == pytest.approx(0.0492, abs=1e-4)

    assert quarantine_rate(quarantined, bronze_total) == pytest.approx(0.0282, abs=1e-4)


def test_old_formula_would_have_nearly_tripped_a_healthy_day():
    """The whole point of the bug: 4.92% sits just under the 5% threshold
    for a day that was actually healthy at 2.82%."""
    quarantined, rows_in = 14_817, 286_113
    old_buggy_rate = quarantined / (quarantined + rows_in)
    assert old_buggy_rate < 0.05
    assert 0.05 - old_buggy_rate < 0.001   # a hair's breadth from firing


def test_zero_bronze_volume_does_not_divide_by_zero():
    assert quarantine_rate(100, 0) == 0.0
    assert quarantine_rate(0, 0) == 0.0


def test_zero_quarantined_is_a_clean_zero_rate():
    assert quarantine_rate(0, 500_000) == 0.0


def test_rate_is_bounded_by_one():
    assert quarantine_rate(1000, 1000) == 1.0


# ---- fake Cassandra session --------------------------------------------------

class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class FakeSession:
    """Records every query/params pair and returns canned rows keyed by
    (query text, params) - close enough to exercise the calling code's
    query shape without a live Cassandra cluster."""

    def __init__(self, responses):
        self.responses = responses   # {(query, params): [rows]}
        self.calls = []

    def execute(self, query, params=None):
        self.calls.append((query, params))
        key = (query.strip(), params)
        if key not in self.responses:
            raise KeyError(f"FakeSession has no canned response for {key}")
        return self.responses[key]


class _OneResult(list):
    """A result set that also supports .one(), like the real driver's."""
    def one(self):
        return self[0]


# ---- fetch_quarantine_counts -------------------------------------------------

def test_fetch_quarantine_counts_sums_across_the_full_day():
    q = ("SELECT error_type, COUNT(*) AS n FROM quarantine_arrivals "
         "WHERE ingest_hour = %s GROUP BY error_type")
    responses = {(q, (f"2026-07-24T{h:02d}",)): [] for h in range(24)}
    responses[(q, ("2026-07-24T19",))] = [_Row(error_type="placeholder_vehicle_id", n=8846)]
    responses[(q, ("2026-07-24T20",))] = [_Row(error_type="placeholder_vehicle_id", n=5971)]
    session = FakeSession(responses)

    counts = fetch_quarantine_counts(session, "2026-07-24")

    assert counts == {"placeholder_vehicle_id": 8846 + 5971}
    assert len(session.calls) == 24   # one single-partition read per hour


def test_fetch_quarantine_counts_merges_multiple_error_types_same_hour():
    q = ("SELECT error_type, COUNT(*) AS n FROM quarantine_arrivals "
         "WHERE ingest_hour = %s GROUP BY error_type")
    responses = {(q, (f"2026-07-24T{h:02d}",)): [] for h in range(24)}
    responses[(q, ("2026-07-24T19",))] = [
        _Row(error_type="placeholder_vehicle_id", n=100),
        _Row(error_type="missing_naptan_id", n=5),
    ]
    counts = fetch_quarantine_counts(FakeSession(responses), "2026-07-24")
    assert counts == {"placeholder_vehicle_id": 100, "missing_naptan_id": 5}


# ---- fetch_bronze_volume ------------------------------------------------------

def test_fetch_bronze_volume_sums_across_all_partitions_and_hours():
    q = ("SELECT COUNT(*) AS n FROM bronze_arrivals "
         "WHERE ingest_hour = %s AND kafka_partition IN %s")
    parts = (0, 1, 2, 3, 4, 5)
    responses = {(q, (f"2026-07-24T{h:02d}", parts)): _OneResult([_Row(n=0)])
                for h in range(24)}
    responses[(q, ("2026-07-24T19", parts))] = _OneResult([_Row(n=413_445)])
    responses[(q, ("2026-07-24T20", parts))] = _OneResult([_Row(n=112_769)])
    session = FakeSession(responses)

    total = fetch_bronze_volume(session, "2026-07-24")

    assert total == 413_445 + 112_769
    assert len(session.calls) == 24


def test_fetch_bronze_volume_queries_every_kafka_partition_not_a_subset():
    """Regression-shaped: if this ever queried only partition 0, a
    partial-partition undercount would silently deflate the denominator
    and inflate the rate right back."""
    q = ("SELECT COUNT(*) AS n FROM bronze_arrivals "
         "WHERE ingest_hour = %s AND kafka_partition IN %s")
    responses = {(q, (f"2026-07-24T{h:02d}", (0, 1, 2, 3, 4, 5))): _OneResult([_Row(n=0)])
                for h in range(24)}
    session = FakeSession(responses)
    fetch_bronze_volume(session, "2026-07-24")
    for _, params in session.calls:
        assert params[1] == (0, 1, 2, 3, 4, 5)


# ---- fetch_throughput_stats ---------------------------------------------------

def test_fetch_throughput_stats_averages_batch_duration():
    q = ("SELECT rows_in, rows_out, duration_ms FROM pipeline_metrics "
         "WHERE job_name = %s AND metric_date = %s")
    import datetime
    key = (q, ("silver", datetime.date(2026, 7, 24)))
    responses = {
        key: [
            _Row(rows_in=1000, rows_out=1000, duration_ms=200),
            _Row(rows_in=2000, rows_out=2000, duration_ms=400),
        ]
    }
    stats = fetch_throughput_stats(FakeSession(responses), "2026-07-24")
    assert stats == {"batches": 2, "rows_in": 3000, "rows_out": 3000, "avg_batch_ms": 300.0}


def test_fetch_throughput_stats_handles_no_batches():
    import datetime
    q = ("SELECT rows_in, rows_out, duration_ms FROM pipeline_metrics "
         "WHERE job_name = %s AND metric_date = %s")
    key = (q, ("silver", datetime.date(2026, 7, 24)))
    stats = fetch_throughput_stats(FakeSession({key: []}), "2026-07-24")
    assert stats == {"batches": 0, "rows_in": 0, "rows_out": 0, "avg_batch_ms": 0.0}
