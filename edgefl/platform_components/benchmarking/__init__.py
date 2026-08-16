"""Benchmarking package: the Benchmarker metrics pipeline (see PLAN.md) plus an
env-driven singleton accessor used by the server instrumentation call-sites."""

import os
import threading

from platform_components.lib.logger.error_handling import get_logger

logger = get_logger(__name__)

_benchmarker = None
_lock = threading.Lock()


def get_benchmarker():
    """Process-wide Benchmarker, configured from env (BENCHMARK_ENABLED /
    BENCHMARK_REST_CONN). Built lazily on first metric call so construction happens
    after the harness's env is injected and the target operator is up. Never raises —
    a broken benchmarker must not take training down with it."""
    global _benchmarker
    if _benchmarker is None:
        with _lock:
            if _benchmarker is None:
                from platform_components.benchmarking.benchmarker import Benchmarker
                conn = os.getenv("BENCHMARK_REST_CONN", "")
                enabled = (
                    os.getenv("BENCHMARK_ENABLED", "false").lower() == "true"
                    and bool(conn)
                )
                try:
                    _benchmarker = Benchmarker(f"http://{conn}", enabled=enabled)
                except Exception as e:
                    logger.warning(f"Benchmarker init failed; metrics disabled: {e}")
                    _benchmarker = Benchmarker("", enabled=False)
    return _benchmarker
