"""Results collection: dump a run's fl_benchmarks rows to a per-run CSV.

The Benchmarker streams long-format rows (node, training_index, round_number,
metric_name, metric_value, time) into benchmarkfl.fl_benchmarks on node1's operator
(BENCHMARK_REST_CONN). This module reads them back with an AnyLog SQL query over REST
and writes the raw rows to CSV — the harness's entire output responsibility this phase.
Plotting is deliberately left to a later layer that reads these CSVs.

Partial results are first-class: on a failed run the runner still calls collect_run_csv
so whatever rounds landed are preserved, and the manifest carries the FAILED status.
"""

import csv
import os

import requests

from platform_components.lib.logger.error_handling import get_logger

logger = get_logger(__name__)

ANYLOG_UA = {"User-Agent": "AnyLog/1.23"}

# Preferred column order for the CSV; any extra columns AnyLog adds (row_id,
# insert_timestamp, ...) are appended after these in sorted order.
PREFERRED_COLUMNS = [
    "node", "training_index", "round_number", "metric_name", "metric_value", "time",
]


def fetch_benchmark_rows(
    query_conn: str,
    run_id: str,
    db_name: str = "benchmarkfl",
    table_name: str = "fl_benchmarks",
) -> list[dict]:
    """Query all rows for run_id. query_conn is host:port of a query-capable EdgeLake
    node — node1's operator, the same node the Benchmarker writes to."""
    sql = f"select * from {table_name} where training_index = '{run_id}'"
    headers = {
        **ANYLOG_UA,
        "command": f'sql {db_name} format = json and stat = false "{sql}"',
        "destination": "network",
    }
    r = requests.get(f"http://{query_conn}", headers=headers, timeout=60)
    r.raise_for_status()
    payload = r.json()
    rows = payload.get("Query", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        logger.warning(f"unexpected sql response shape for {run_id}: {type(rows)}")
        return []
    return rows


def write_rows_csv(rows: list[dict], path: str) -> str:
    """Write rows to CSV at path (parents created). An empty result still writes a
    header-only file so a run always leaves an artifact."""
    columns = list(PREFERRED_COLUMNS)
    extra = sorted({k for row in rows for k in row} - set(columns))
    columns += extra

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def collect_run_csv(
    query_conn: str,
    run_id: str,
    path: str,
    settle_interval_s: float = 15.0,
    settle_max_s: float = 90.0,
) -> tuple[str, int]:
    """Fetch + write, waiting for the row count to stop growing first.

    Metric rows ride AnyLog's streaming buffer (60s / 100KB thresholds), so rows
    recorded near the end of a run land in the table up to a minute after completion.
    Poll until two consecutive fetches agree (or settle_max_s passes), then write.
    Returns (path, row_count).
    """
    import time as _time

    rows: list[dict] = []
    deadline = _time.time() + settle_max_s
    while True:
        try:
            fresh = fetch_benchmark_rows(query_conn, run_id)
        except requests.RequestException as e:
            logger.error(f"[{run_id}] could not fetch benchmark rows: {e}")
            fresh = rows
        if len(fresh) == len(rows) and rows:
            rows = fresh
            break
        rows = fresh
        if _time.time() >= deadline:
            logger.warning(f"[{run_id}] row count still growing at settle_max_s; collecting anyway")
            break
        _time.sleep(settle_interval_s)

    write_rows_csv(rows, path)
    logger.info(f"[{run_id}] wrote {len(rows)} benchmark rows to {path}")
    return path, len(rows)
