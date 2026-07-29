

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
TFL_BASE_URL = os.getenv("TFL_BASE_URL", "https://api.tfl.gov.uk").rstrip("/")
TFL_MODES = os.getenv("TFL_MODES", "tube")
TFL_APP_KEY = os.getenv("TFL_APP_KEY", "").strip()
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "tfl.arrivals.raw")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))

# Politeness delay between per-line requests when batching is unavailable.
PER_LINE_DELAY = float(os.getenv("PER_LINE_DELAY", "0.2"))

# Used only if /Line/Mode/tube discovery fails. Hardcoding the full list as
# the primary source would mean silently missing any new line TfL adds. This
# list is tube-specific and must not be used for any other mode.
FALLBACK_TUBE_LINES = [
    "bakerloo", "central", "circle", "district", "hammersmith-city",
    "jubilee", "metropolitan", "northern", "piccadilly", "victoria",
    "waterloo-city",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("tfl-producer")

_shutdown = False


def _handle_signal(signum, _frame):
    """Let Docker stop us cleanly instead of killing mid-flush."""
    global _shutdown
    log.info("received signal %s, shutting down after current poll", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# --------------------------------------------------------------------------
# HTTP / Kafka clients
# --------------------------------------------------------------------------
def build_http_session() -> requests.Session:
    """HTTP session with backoff. TfL returns 429 under load and 5xx at night."""
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        acks="all",             # do not consider a send done until replicated
        retries=5,
        linger_ms=20,           # small batching window; at 30s polls this is free
        batch_size=65536,
        compression_type="gzip",
        max_block_ms=10000,
    )


def _params() -> dict:
    return {"app_key": TFL_APP_KEY} if TFL_APP_KEY else {}


def _get_json(session: requests.Session, path: str):
    resp = session.get(f"{TFL_BASE_URL}{path}", params=_params(), timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# --------------------------------------------------------------------------
# Startup discovery
# --------------------------------------------------------------------------
def discover_line_ids(session: requests.Session) -> list:
    """Ask TfL which lines exist for the configured modes.

    The fallback list is tube-only, so it is a valid answer only when tube is
    the sole configured mode. For anything else, returning it would poll the
    wrong lines while appearing healthy - better to fail at startup.
    """
    try:
        payload = _get_json(session, f"/Line/Mode/{TFL_MODES}")
        ids = [line["id"] for line in payload if isinstance(line, dict) and line.get("id")]
        if ids:
            log.info("discovered %s lines for mode(s) '%s': %s", len(ids), TFL_MODES, ids)
            return ids
        raise ValueError("discovery returned no line IDs")
    except Exception as exc:
        if TFL_MODES.strip().lower() == "tube":
            log.warning("line discovery failed (%s), using tube fallback list", exc)
            return list(FALLBACK_TUBE_LINES)
        raise RuntimeError(
            f"line discovery failed for mode(s) '{TFL_MODES}' and no fallback "
            f"list exists for them: {exc}"
        ) from exc


def probe_batching(session: requests.Session, line_ids: list) -> bool:
    """Check whether /Line/{a,b,c}/Arrivals accepts a comma-separated list.

    If it does we make one request per poll instead of eleven, which keeps us
    comfortably inside the 50 requests/min unauthenticated rate limit.
    """
    if len(line_ids) < 2:
        return False
    try:
        payload = _get_json(session, f"/Line/{','.join(line_ids)}/Arrivals")
        if isinstance(payload, list) and payload:
            distinct = {r.get("lineId") for r in payload if isinstance(r, dict)}
            if len(distinct) > 1:
                log.info("batched requests supported (%s lines in one response)", len(distinct))
                return True
        log.info("batched request returned a single line, falling back to per-line")
    except Exception as exc:
        log.info("batched request not supported (%s), falling back to per-line", exc)
    return False


# --------------------------------------------------------------------------
# Polling
# --------------------------------------------------------------------------
def fetch_batched(session: requests.Session, line_ids: list) -> list:
    payload = _get_json(session, f"/Line/{','.join(line_ids)}/Arrivals")
    if not isinstance(payload, list):
        raise ValueError(f"expected a JSON array, got {type(payload).__name__}")
    return payload


def fetch_per_line(session: requests.Session, line_ids: list) -> list:
    """One request per line. A failure on one line must not lose the others."""
    records = []
    for index, line_id in enumerate(line_ids):
        if _shutdown:
            break
        try:
            payload = _get_json(session, f"/Line/{line_id}/Arrivals")
            if isinstance(payload, list):
                records.extend(payload)
            else:
                log.warning("line %s returned %s, skipping", line_id, type(payload).__name__)
        except Exception as exc:
            log.warning("line %s failed this poll: %s", line_id, exc)
        if PER_LINE_DELAY and index < len(line_ids) - 1:
            time.sleep(PER_LINE_DELAY)
    return records


def publish(producer: KafkaProducer, records: list) -> tuple:
    """Enqueue records and wait for delivery.

    send() is asynchronous - it returns a future and only raises locally when
    the buffer is full. Broker-side failures surface through the errback after
    the configured retries are exhausted, so counting send() calls counts
    enqueues, not deliveries. These callbacks all run on the producer's single
    sender thread and are read here only after flush(), so the counters need
    no locking.
    """
    stats = {"delivered": 0, "failed": 0}
    first_errors = []

    def _on_success(_metadata):
        stats["delivered"] += 1

    def _on_failure(exc):
        stats["failed"] += 1
        # A broker outage fails every record in the poll. Log a handful with
        # detail and let the count carry the rest.
        if len(first_errors) < 3:
            first_errors.append(exc)
            log.error("delivery failed: %s", exc)

    for record in records:
        # lineId is the natural partition key. Fall back to "unknown" rather
        # than None so malformed records still reach bronze and get quarantined
        # downstream instead of being silently dropped here.
        key = (record.get("lineId") or "unknown") if isinstance(record, dict) else "unknown"
        try:
            (producer.send(RAW_TOPIC, key=key, value=record)
                     .add_callback(_on_success)
                     .add_errback(_on_failure))
        except KafkaError as exc:
            stats["failed"] += 1
            log.error("enqueue failed: %s", exc)

    producer.flush(timeout=30)

    if stats["failed"] > len(first_errors):
        log.error("%s further delivery failures suppressed",
                  stats["failed"] - len(first_errors))

    return stats["delivered"], stats["failed"]


def main() -> int:
    log.info(
        "starting: base=%s modes=%s topic=%s bootstrap=%s interval=%ss app_key=%s",
        TFL_BASE_URL, TFL_MODES, RAW_TOPIC, KAFKA_BOOTSTRAP, POLL_SECONDS,
        "yes" if TFL_APP_KEY else "no",
    )

    session = build_http_session()
    line_ids = discover_line_ids(session)
    batched = probe_batching(session, line_ids)
    producer = build_producer()

    requests_per_poll = 1 if batched else len(line_ids)
    log.info(
        "polling %s lines every %ss in %s mode (~%.1f requests/min)",
        len(line_ids), POLL_SECONDS, "batched" if batched else "per-line",
        requests_per_poll * 60 / POLL_SECONDS,
    )

    polls = total_sent = total_failed = 0

    while not _shutdown:
        started = time.monotonic()
        poll_started_at = datetime.now(timezone.utc).isoformat()

        try:
            records = (fetch_batched(session, line_ids) if batched
                       else fetch_per_line(session, line_ids))
            fetch_ms = (time.monotonic() - started) * 1000
            sent, failed = publish(producer, records)
            total_sent += sent
            total_failed += failed
            polls += 1

            # One structured line per poll. This is what you screenshot for the
            # README and what you point at when asked about throughput.
            log.info(
                json.dumps({
                    "event": "poll_complete",
                    "poll_started_at": poll_started_at,
                    "poll_number": polls,
                    "mode": "batched" if batched else "per-line",
                    "lines": len(line_ids),
                    "records_fetched": len(records),
                    "records_sent": sent,
                    "records_failed": failed,
                    "fetch_ms": round(fetch_ms, 1),
                    "total_ms": round((time.monotonic() - started) * 1000, 1),
                    "cumulative_sent": total_sent,
                })
            )
        except Exception as exc:
            log.exception("poll failed, will retry next interval: %s", exc)

        # Sleep the remainder of the interval, in short slices so SIGTERM is fast.
        elapsed = time.monotonic() - started
        remaining = max(0.0, POLL_SECONDS - elapsed)
        while remaining > 0 and not _shutdown:
            nap = min(1.0, remaining)
            time.sleep(nap)
            remaining -= nap

    log.info("draining producer: polls=%s sent=%s failed=%s", polls, total_sent, total_failed)
    producer.flush(timeout=30)
    producer.close(timeout=30)
    return 0


if __name__ == "__main__":
    sys.exit(main())
