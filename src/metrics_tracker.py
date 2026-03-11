"""
Real-time metrics tracker with Prometheus export + console dashboard.

Two metric layers:
  1. SDK-level  — built-in Prometheus endpoint (SS_ENABLE_METRICS=true,
                  default http://127.0.0.1:50000/metrics).  Zero-config.
  2. App-level  — custom counters/gauges exposed on a separate Prometheus
                  HTTP server (default port 9100) via prometheus_client.

Console dashboard prints to stdout every N cycles.
"""

import os
import time
import logging
import threading
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Try to import prometheus_client; gracefully degrade if missing
# ---------------------------------------------------------------------------
try:
    from prometheus_client import (
        Counter, Gauge, Histogram, Summary,
        start_http_server as _prom_start,
    )
    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False
    logger.info("prometheus_client not installed — app-level Prometheus metrics disabled.")


# ---------------------------------------------------------------------------
# Prometheus metric definitions (module-level singletons)
# ---------------------------------------------------------------------------
if _PROM_AVAILABLE:
    PROM_ROWS_SUBMITTED = Counter(
        "cdr_rows_submitted_total",
        "Total CDR rows submitted to append_row",
    )
    PROM_ROWS_COMMITTED = Gauge(
        "cdr_rows_committed",
        "Latest committed offset + 1 (≈ rows committed)",
    )
    PROM_BATCHES = Counter(
        "cdr_batches_sent_total",
        "Total batches sent",
    )
    PROM_ERRORS = Counter(
        "cdr_errors_total",
        "Total errors by category",
        ["category"],
    )
    PROM_RETRIES = Counter(
        "cdr_retries_total",
        "Total retry attempts",
    )
    PROM_CHANNEL_RECOVERIES = Counter(
        "cdr_channel_recoveries_total",
        "Number of channel recovery events",
    )
    PROM_DLQ = Counter(
        "cdr_dead_letter_total",
        "Records sent to dead-letter queue",
    )
    PROM_BATCH_LATENCY = Histogram(
        "cdr_batch_latency_seconds",
        "Time to send one batch (seconds)",
        buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    )
    PROM_COMMIT_LAG = Gauge(
        "cdr_commit_lag",
        "Rows submitted minus rows committed",
    )
    PROM_PII_ENCRYPTIONS = Counter(
        "cdr_pii_encryptions_total",
        "Number of fields encrypted via PII encryptor",
    )


@dataclass
class _Snapshot:
    ts: float
    rows: int


class MetricsTracker:
    """Thread-safe metrics collector with Prometheus export + rolling throughput."""

    def __init__(self, window_seconds: int = 30, prom_port: int = 9100):
        self._lock = threading.Lock()
        self._window = window_seconds

        # counters
        self.rows_submitted: int = 0
        self.rows_committed: int = 0
        self.batches_sent: int = 0
        self.errors: Dict[str, int] = {}
        self.retries: int = 0
        self.channel_recoveries: int = 0
        self.dead_letter_count: int = 0
        self.pii_encryptions: int = 0

        # throughput tracking
        self._snapshots: deque[_Snapshot] = deque()
        self._start_time: float = time.time()

        # Prometheus app-level server
        self._prom_started = False
        self._prom_port = prom_port

    # ---- Prometheus server lifecycle ----

    def start_prometheus(self) -> None:
        """Start the app-level Prometheus HTTP server (idempotent)."""
        if not _PROM_AVAILABLE or self._prom_started:
            return
        try:
            _prom_start(self._prom_port)
            self._prom_started = True
            logger.info(
                "App-level Prometheus metrics on http://127.0.0.1:%d/metrics",
                self._prom_port,
            )
        except OSError as exc:
            logger.warning("Could not start Prometheus server on port %d: %s",
                           self._prom_port, exc)

    # ---- recording methods ----

    def record_batch(self, row_count: int) -> None:
        with self._lock:
            self.rows_submitted += row_count
            self.batches_sent += 1
            self._snapshots.append(_Snapshot(time.time(), row_count))
        if _PROM_AVAILABLE:
            PROM_ROWS_SUBMITTED.inc(row_count)
            PROM_BATCHES.inc()

    def record_commit(self, committed_offset: int) -> None:
        with self._lock:
            self.rows_committed = committed_offset + 1
        if _PROM_AVAILABLE:
            PROM_ROWS_COMMITTED.set(committed_offset + 1)
            PROM_COMMIT_LAG.set(self.rows_submitted - (committed_offset + 1))

    def record_error(self, category: str) -> None:
        with self._lock:
            self.errors[category] = self.errors.get(category, 0) + 1
        if _PROM_AVAILABLE:
            PROM_ERRORS.labels(category=category).inc()

    def record_retry(self) -> None:
        with self._lock:
            self.retries += 1
        if _PROM_AVAILABLE:
            PROM_RETRIES.inc()

    def record_channel_recovery(self) -> None:
        with self._lock:
            self.channel_recoveries += 1
        if _PROM_AVAILABLE:
            PROM_CHANNEL_RECOVERIES.inc()

    def record_dead_letter(self, count: int = 1) -> None:
        with self._lock:
            self.dead_letter_count += count
        if _PROM_AVAILABLE:
            PROM_DLQ.inc(count)

    def record_pii_encryption(self, count: int = 1) -> None:
        with self._lock:
            self.pii_encryptions += count
        if _PROM_AVAILABLE:
            PROM_PII_ENCRYPTIONS.inc(count)

    def observe_batch_latency(self, seconds: float) -> None:
        if _PROM_AVAILABLE:
            PROM_BATCH_LATENCY.observe(seconds)

    # ---- derived metrics ----

    def throughput_rps(self) -> float:
        with self._lock:
            now = time.time()
            cutoff = now - self._window
            while self._snapshots and self._snapshots[0].ts < cutoff:
                self._snapshots.popleft()
            if not self._snapshots:
                return 0.0
            total_rows = sum(s.rows for s in self._snapshots)
            span = now - self._snapshots[0].ts
            return total_rows / span if span > 0 else 0.0

    def elapsed_seconds(self) -> float:
        return time.time() - self._start_time

    def commit_lag(self) -> int:
        with self._lock:
            return self.rows_submitted - self.rows_committed

    # ---- console dashboard ----

    def print_dashboard(
        self,
        profile_name: str = "",
        cycle: int = 0,
        dlq_file: str = "",
    ) -> None:
        elapsed = self.elapsed_seconds()
        rps = self.throughput_rps()
        lag = self.commit_lag()
        err_total = sum(self.errors.values())

        err_summary = ", ".join(
            f"{k}={v}" for k, v in sorted(self.errors.items())
        ) or "none"

        dlq_label = f"{self.dead_letter_count}"
        if dlq_file:
            dlq_label += f"  -> {dlq_file}"

        prom_sdk = "ON" if os.environ.get("SS_ENABLE_METRICS") == "true" else "OFF"
        prom_app = f":{self._prom_port}" if self._prom_started else "OFF"

        print(
            f"\n{'=' * 74}\n"
            f"  CYCLE {cycle:<6}  PROFILE: {profile_name:<15}  "
            f"ELAPSED: {elapsed:>7.1f}s\n"
            f"  rows submitted: {self.rows_submitted:>10,}    "
            f"committed: {self.rows_committed:>10,}    "
            f"lag: {lag:>6,}\n"
            f"  throughput:     {rps:>10,.1f} rows/s   "
            f"batches: {self.batches_sent:>8,}    "
            f"retries: {self.retries:>4}\n"
            f"  errors: {err_total:>4}  ({err_summary})\n"
            f"  channel recoveries: {self.channel_recoveries}    "
            f"dead-letter queue: {dlq_label}\n"
            f"  PII encryptions: {self.pii_encryptions:,}    "
            f"Prometheus SDK: {prom_sdk}  App: {prom_app}\n"
            f"{'=' * 74}"
        )
