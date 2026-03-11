"""
Production-grade Snowpipe Streaming client — demo_300.

Enhancements over demo_200:
  * Prometheus metrics (SDK built-in + app-level via prometheus_client)
  * Deterministic channel names (best practice for recovery)
  * Client-side metadata columns (channel_id, stream_offset, pipe_id)
  * PII encryption layer (caller/callee numbers)
  * Native Python types for semi-structured data (dict, list → VARIANT, ARRAY, OBJECT)
  * HTTP-status-aware retry (409, 429, 401/403, 500/503)
  * Full client rebuild on InvalidClientError
  * Channel health check via get_channel_status()
  * Batch-level circuit breaker
  * Dead-letter queue (Snowflake table + local file)
"""

import json
import logging
import os
import time
import uuid
from typing import Dict, List, Optional, Tuple

from snowflake.ingest.streaming import (
    StreamingIngestClient,
    StreamingIngestError,
    StreamingIngestErrorCode,
)

from metrics_tracker import MetricsTracker
from pii_encryptor import PIIEncryptor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Profile resolution: --profile CLI > SNOWFLAKE_PROFILE env var > "default"
# ---------------------------------------------------------------------------
_PROFILES_DIR = os.path.join(os.path.dirname(__file__), "..", "profiles")


def resolve_profile(profile_name: Optional[str] = None) -> str:
    """Return the absolute path to a profile JSON file.

    Resolution order:
      1. Explicit *profile_name* argument  (from --profile CLI flag)
      2. SNOWFLAKE_PROFILE environment variable
      3. Falls back to "default"

    Looks for ``profiles/profile_{name}.json`` relative to the project root.
    """
    name = (
        profile_name
        or os.environ.get("SNOWFLAKE_PROFILE")
        or "default"
    )
    path = os.path.join(_PROFILES_DIR, f"profile_{name}.json")
    if not os.path.isfile(path):
        available = [
            f.replace("profile_", "").replace(".json", "")
            for f in os.listdir(_PROFILES_DIR)
            if f.startswith("profile_") and f.endswith(".json")
            and not f.endswith(".example")
        ]
        raise FileNotFoundError(
            f"Profile '{name}' not found at {path}\n"
            f"Available profiles: {available}"
        )
    logger.info("Using profile: %s  (%s)", name, path)
    return path


# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------
_DEFAULTS = {
    "database": "TELCO_ANALYTICS",
    "schema": "CDR_STREAMING_300",
    "pipe_name": "CDR_STREAMING_PIPE_300",
    "max_retries": 3,
    "base_backoff_s": 1.0,
    "max_backoff_s": 30.0,
    "commit_poll_attempts": 60,
    "commit_poll_interval_s": 1.0,
}

# Validation: required fields AFTER PII transform (encrypted variants, not raw)
_REQUIRED_FIELDS = {
    "call_id", "caller_number_encrypted", "callee_number_encrypted",
    "call_start_time", "call_end_time", "duration_seconds",
    "call_type", "call_status", "cell_tower_id", "network_type",
    "data_usage_mb", "roaming", "country_code",
}

_FIELD_MAX_LENGTHS = {
    "call_id": 50,
    "caller_number_encrypted": 500,
    "callee_number_encrypted": 500,
    "caller_number_display": 20,
    "callee_number_display": 20,
    "call_type": 20,
    "call_status": 20,
    "cell_tower_id": 50,
    "network_type": 10,
    "country_code": 5,
}


# ===================================================================
# Dead-Letter Queue
# ===================================================================

class DeadLetterQueue:
    """Streams failed records to Snowflake CDR_DEAD_LETTER_300 + local file."""

    def __init__(
        self,
        max_size: int = 10_000,
        *,
        database: str = _DEFAULTS["database"],
        schema: str = _DEFAULTS["schema"],
        profile_json: Optional[str] = None,
        dlq_pipe: str = "CDR_DLQ_PIPE_300",
    ):
        self._records: List[Tuple[Dict, str, str]] = []
        self._max = max_size
        self._db = database
        self._schema = schema
        self._profile = profile_json
        self._dlq_pipe = dlq_pipe

        self._client: Optional[StreamingIngestClient] = None
        self._channel = None
        self._offset: int = 0
        self._client_name: str = ""
        self._channel_name: str = ""

    def connect(self) -> None:
        self._client_name = f"DLQ_CLIENT_{uuid.uuid4().hex[:8]}"
        logger.info("DLQ: creating client [%s] ...", self._client_name)
        self._client = StreamingIngestClient(
            client_name=self._client_name,
            db_name=self._db,
            schema_name=self._schema,
            pipe_name=self._dlq_pipe,
            profile_json=self._profile,
        )
        self._channel_name = f"dlq-{self._client_name}"
        self._channel, status = self._client.open_channel(self._channel_name)
        logger.info("DLQ: channel open [%s]  status=%s", self._channel_name, status)

    def close(self) -> None:
        if self._channel:
            try:
                self._channel.close()
            except Exception:
                pass
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
        logger.info("DLQ: client + channel closed.")

    def push(self, record: Dict, reason: str, category: str = "unknown") -> None:
        if len(self._records) < self._max:
            self._records.append((record, reason, category))

        if self._channel:
            try:
                dlq_row = {
                    "error_id": f"DLQ-{uuid.uuid4().hex[:12]}",
                    "error_reason": reason[:500],
                    "error_category": category,
                    "channel_name": self._channel_name,
                    "client_name": self._client_name,
                    "original_record": record,
                    "call_id": record.get("call_id", ""),
                    "call_type": record.get("call_type", ""),
                    "caller_number": str(record.get("caller_number_display",
                                         record.get("caller_number", "")))[:100],
                }
                self._channel.append_row(dlq_row, str(self._offset))
                self._offset += 1
            except Exception as exc:
                logger.warning("DLQ: failed to stream dead-letter row: %s", exc)

    def flush_to_file(self, path: str) -> int:
        if not self._records:
            return 0
        with open(path, "a") as f:
            for rec, reason, category in self._records:
                f.write(json.dumps({
                    "reason": reason, "category": category, "record": rec,
                }) + "\n")
        count = len(self._records)
        self._records.clear()
        return count

    def __len__(self) -> int:
        return len(self._records)


# ===================================================================
# Resilient Streaming Client
# ===================================================================

class ResilientStreamingClient:
    """Wraps StreamingIngestClient with all demo_300 features."""

    def __init__(
        self,
        metrics: MetricsTracker,
        *,
        database: str = _DEFAULTS["database"],
        schema: str = _DEFAULTS["schema"],
        pipe_name: str = _DEFAULTS["pipe_name"],
        profile_json: Optional[str] = None,
        max_retries: int = _DEFAULTS["max_retries"],
        base_backoff_s: float = _DEFAULTS["base_backoff_s"],
        max_backoff_s: float = _DEFAULTS["max_backoff_s"],
        commit_poll_attempts: int = _DEFAULTS["commit_poll_attempts"],
        commit_poll_interval_s: float = _DEFAULTS["commit_poll_interval_s"],
        enable_pii: bool = True,
        channel_prefix: str = "cdr",
    ):
        self.metrics = metrics
        self._db = database
        self._schema = schema
        self._pipe = pipe_name
        self._profile = profile_json
        self._max_retries = max_retries
        self._base_backoff = base_backoff_s
        self._max_backoff = max_backoff_s
        self._poll_attempts = commit_poll_attempts
        self._poll_interval = commit_poll_interval_s
        self._channel_prefix = channel_prefix

        self._client: Optional[StreamingIngestClient] = None
        self._channel = None
        self._client_name: str = ""
        self._channel_name: str = ""
        self._global_offset: int = 0

        # PII encryption
        self._pii: Optional[PIIEncryptor] = PIIEncryptor() if enable_pii else None

        self.dlq = DeadLetterQueue(
            database=database, schema=schema, profile_json=profile_json,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        self._client_name = f"CDR_CLIENT_{uuid.uuid4().hex[:8]}"
        logger.info("Creating client [%s] ...", self._client_name)
        self._client = StreamingIngestClient(
            client_name=self._client_name,
            db_name=self._db,
            schema_name=self._schema,
            pipe_name=self._pipe,
            profile_json=self._profile,
        )
        self._open_channel()
        try:
            self.dlq.connect()
        except Exception as exc:
            logger.warning("DLQ channel failed to open (local file still works): %s", exc)

    def _open_channel(self) -> None:
        # Deterministic naming (best practice): prefix-clientname
        self._channel_name = f"{self._channel_prefix}-{self._client_name}"
        logger.info("Opening channel [%s] ...", self._channel_name)
        self._channel, status = self._client.open_channel(self._channel_name)
        logger.info("Channel open.  Status: %s", status)

    def close(self) -> None:
        self.dlq.close()
        if self._channel:
            try:
                self._channel.close()
                logger.info("Channel closed.")
            except Exception as exc:
                logger.warning("Error closing channel: %s", exc)
        if self._client:
            try:
                self._client.close()
                logger.info("Client closed.")
            except Exception as exc:
                logger.warning("Error closing client: %s", exc)

    # ------------------------------------------------------------------
    # Channel / client recovery
    # ------------------------------------------------------------------

    def _recover_channel(self) -> bool:
        logger.warning("Attempting channel recovery ...")
        self.metrics.record_channel_recovery()
        try:
            if self._channel:
                try:
                    self._channel.close()
                except Exception:
                    pass
            self._open_channel()
            logger.info("Channel recovered successfully.")
            return True
        except Exception as exc:
            logger.error("Channel recovery failed: %s", exc)
            return False

    def _recover_client(self) -> bool:
        logger.warning("Attempting full client recovery ...")
        self.metrics.record_channel_recovery()
        try:
            if self._channel:
                try:
                    self._channel.close()
                except Exception:
                    pass
            if self._client:
                try:
                    self._client.close()
                except Exception:
                    pass

            self._client_name = f"CDR_CLIENT_{uuid.uuid4().hex[:8]}"
            logger.info("Recreating client [%s] ...", self._client_name)
            self._client = StreamingIngestClient(
                client_name=self._client_name,
                db_name=self._db,
                schema_name=self._schema,
                pipe_name=self._pipe,
                profile_json=self._profile,
            )
            self._open_channel()
            logger.info("Full client recovery succeeded.")
            return True
        except Exception as exc:
            logger.error("Full client recovery failed: %s", exc)
            return False

    def check_channel_health(self) -> Optional[str]:
        if not self._channel:
            return "no channel open"
        try:
            status = self._channel.get_channel_status()
            if status.status_code != "SUCCESS":
                msg = (
                    f"channel_status={status.status_code}  "
                    f"rows_error={status.rows_error_count}  "
                    f"last_error={status.last_error_message}"
                )
                logger.warning("Channel unhealthy: %s", msg)
                return msg
            return None
        except Exception as exc:
            logger.warning("Failed to check channel status: %s", exc)
            return f"status_check_error: {exc}"

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def validate_record(record: Dict) -> Optional[str]:
        missing = _REQUIRED_FIELDS - record.keys()
        if missing:
            return f"missing fields: {missing}"

        for field_name, max_len in _FIELD_MAX_LENGTHS.items():
            val = record.get(field_name)
            if isinstance(val, str) and len(val) > max_len:
                return f"{field_name} exceeds max length {max_len} (got {len(val)})"

        dur = record.get("duration_seconds")
        if not isinstance(dur, (int, float)):
            return f"duration_seconds has wrong type: {type(dur).__name__}"

        return None

    # ------------------------------------------------------------------
    # PII + metadata enrichment
    # ------------------------------------------------------------------

    def _enrich_record(self, record: Dict, offset: int) -> Dict:
        """Apply PII encryption and inject metadata columns."""
        # PII encryption (replaces caller_number/callee_number with encrypted + display)
        if self._pii and "caller_number" in record:
            record = self._pii.protect_record(record)
            self.metrics.record_pii_encryption(2)

        # Best-practice metadata columns
        record["channel_id"] = self._channel_name
        record["stream_offset"] = offset
        record["pipe_id"] = self._pipe

        return record

    # ------------------------------------------------------------------
    # Send with retry
    # ------------------------------------------------------------------

    def send_batch(self, records: List[Dict]) -> int:
        """Validate, encrypt PII, inject metadata, send, return count of successes."""
        batch_start = time.time()

        good_rows: List[Tuple[Dict, str]] = []
        for rec in records:
            offset = self._global_offset
            enriched = self._enrich_record(rec, offset)

            err = self.validate_record(enriched)
            if err:
                self.dlq.push(enriched, err, category="validation")
                self.metrics.record_error("validation")
                self.metrics.record_dead_letter()
                continue

            token = str(offset)
            good_rows.append((enriched, token))
            self._global_offset += 1

        sent = 0
        consecutive_failures = 0
        for idx, (row, token) in enumerate(good_rows):
            if not self._try_append_row(row, token):
                self.dlq.push(row, "send_failure_after_retries", category="send")
                self.metrics.record_dead_letter()
                consecutive_failures += 1
                if consecutive_failures >= 5:
                    health = self.check_channel_health()
                    if health:
                        logger.error(
                            "Aborting batch: %d consecutive failures, health: %s",
                            consecutive_failures, health,
                        )
                        for remaining_row, _ in good_rows[idx + 1:]:
                            self.dlq.push(remaining_row, f"batch_aborted: {health}", category="send")
                            self.metrics.record_dead_letter()
                        break
            else:
                sent += 1
                consecutive_failures = 0

        if sent:
            self.metrics.record_batch(sent)
        self.metrics.observe_batch_latency(time.time() - batch_start)
        return sent

    def _try_append_row(self, row: Dict, token: str) -> bool:
        """Append a single row with HTTP-status-aware retry logic."""
        backoff = self._base_backoff

        for attempt in range(1, self._max_retries + 1):
            try:
                self._channel.append_row(row, token)
                return True

            except StreamingIngestError as exc:
                http_code = exc.http_status_code
                error_code = exc.error_code
                self.metrics.record_error(f"send_http{http_code}")
                self.metrics.record_retry()

                logger.warning(
                    "append_row failed (attempt %d/%d) HTTP %d [%s]: %s",
                    attempt, self._max_retries, http_code,
                    error_code.value if error_code else "N/A", exc.message,
                )

                if http_code in (401, 403):
                    logger.error(
                        "Auth error (HTTP %d). Fix credentials/role/permissions. Aborting row.",
                        http_code,
                    )
                    self.metrics.record_error("auth")
                    return False

                if http_code == 409 or error_code in (
                    StreamingIngestErrorCode.INVALID_CHANNEL_ERROR,
                    StreamingIngestErrorCode.CLOSED_CHANNEL_ERROR,
                ):
                    logger.warning("Channel invalidated (HTTP %d). Recovering ...", http_code)
                    if self._recover_channel():
                        continue
                    return False

                if error_code == StreamingIngestErrorCode.INVALID_CLIENT_ERROR:
                    logger.error("Client invalidated. Full recovery ...")
                    if self._recover_client():
                        continue
                    return False

                if http_code == 429:
                    throttle_delay = min(backoff * 2, self._max_backoff)
                    logger.warning("Throttled (429). Backing off %.1fs ...", throttle_delay)
                    if attempt < self._max_retries:
                        time.sleep(throttle_delay)
                        backoff *= 2
                    continue

                if attempt < self._max_retries:
                    time.sleep(min(backoff, self._max_backoff))
                    backoff *= 2
                    if attempt == 2:
                        self._recover_channel()

            except (ValueError, TypeError) as exc:
                self.metrics.record_error("serialization")
                logger.error("Row serialization error (not retryable): %s", exc)
                return False

            except Exception as exc:
                self.metrics.record_error("send_unknown")
                self.metrics.record_retry()
                logger.warning(
                    "append_row unexpected error (attempt %d/%d): %s",
                    attempt, self._max_retries, exc,
                )
                if attempt < self._max_retries:
                    time.sleep(min(backoff, self._max_backoff))
                    backoff *= 2

        return False

    # ------------------------------------------------------------------
    # Commit polling
    # ------------------------------------------------------------------

    def wait_for_commit(self, target_offset: Optional[int] = None) -> bool:
        if target_offset is None:
            target_offset = self._global_offset - 1
        if target_offset < 0:
            return True
        target_int = target_offset

        committed = None
        for attempt in range(1, self._poll_attempts + 1):
            try:
                committed = self._channel.get_latest_committed_offset_token()
            except StreamingIngestError as exc:
                if exc.http_status_code == 409:
                    logger.warning("Channel invalidated during commit poll. Recovering ...")
                    self._recover_channel()
                elif exc.error_code == StreamingIngestErrorCode.INVALID_CLIENT_ERROR:
                    logger.error("Client invalidated during commit poll. Recovering ...")
                    self._recover_client()
                else:
                    logger.warning(
                        "Commit poll SDK error HTTP %d [%s]: %s",
                        exc.http_status_code,
                        exc.error_code.value if exc.error_code else "N/A",
                        exc.message,
                    )
                committed = None
            except Exception as exc:
                logger.warning("Commit poll error: %s", exc)
                committed = None

            if committed is not None:
                try:
                    committed_int = int(committed)
                except (ValueError, TypeError):
                    committed_int = -1
                self.metrics.record_commit(committed_int)

                if committed_int >= target_int:
                    logger.info("All data committed at offset %s (target was %s)",
                                committed, target_int)
                    return True

            logger.debug(
                "Commit poll %d/%d  committed=%s  target=%s",
                attempt, self._poll_attempts, committed, target_int,
            )
            time.sleep(self._poll_interval)

        logger.warning("Commit timeout: target=%s  last_committed=%s", target_int, committed)
        return False

    @property
    def current_offset(self) -> int:
        return self._global_offset
