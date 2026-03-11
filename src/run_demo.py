#!/usr/bin/env python3
"""
Continuous CDR Streaming Demo — demo_300 (Advanced)
=====================================================

Features beyond demo_200:
  * PII encryption in motion (AES-Fernet on phone numbers)
  * Prometheus metrics (SDK built-in + app-level on :9100)
  * GEOGRAPHY / GEOMETRY columns (cell tower points + coverage polygons)
  * Semi-structured: VARIANT, ARRAY, OBJECT (device_info, service_tags, etc.)
  * In-flight transformations (UPPER, TRIM, ROUND, TRY_TO_GEOGRAPHY/GEOMETRY)
  * Schema evolution pipe (MATCH_BY_COLUMN_NAME)
  * Client-side metadata columns (channel_id, stream_offset, pipe_id)
  * Offset-gap detection SQL

Usage:
  python run_demo.py                         # 200 cycles (~7 min)
  python run_demo.py --cycles 50             # shorter run
  python run_demo.py --cycles 0              # infinite until Ctrl+C
  python run_demo.py --bad-pct 0.05          # 5% bad records
  python run_demo.py --no-pii               # disable PII encryption
  python run_demo.py --prom-port 9200       # custom Prometheus port
"""

import argparse
import logging
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

# Enable SDK-level Prometheus metrics (built-in, port 50000 by default)
os.environ.setdefault("SS_ENABLE_METRICS", "true")
os.environ.setdefault("SS_LOG_LEVEL", "warn")

from cdr_generator import CDRGenerator, TrafficProfile
from metrics_tracker import MetricsTracker
from streaming_client import ResilientStreamingClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_shutdown_requested = False


def _handle_signal(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        logger.warning("Second interrupt — forcing exit.")
        sys.exit(1)
    _shutdown_requested = True
    print("\n>>> Ctrl+C received.  Finishing current batch, then shutting down ...")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
BANNER = r"""
 _____ ____  ____    _____ ___   ___
/ ___/|  _ \|  _ \  |___ // _ \ / _ \   demo_300
| |   | | | | |_) |   |_ \ | | | | | |  Advanced CDR Streaming
| |__ | |_| |  _ <   ___) | |_| | |_| | PII + Prometheus + Geo + Semi-structured
\____\|____/|_| \_\ |____/ \___/ \___/  snowpipe-streaming SDK
"""


def parse_args():
    p = argparse.ArgumentParser(description="demo_300 — advanced CDR streaming")
    p.add_argument("--cycles", type=int, default=200,
                   help="Total ingestion cycles (0 = infinite). Default 200.")
    p.add_argument("--batch-size", type=int, default=100,
                   help="Base batch size (scaled by traffic profile). Default 100.")
    p.add_argument("--pause", type=float, default=1.0,
                   help="Seconds between cycles. Default 1.0.")
    p.add_argument("--bad-pct", type=float, default=0.02,
                   help="Fraction of bad records (0.0-1.0). Default 0.02.")
    p.add_argument("--commit-every", type=int, default=10,
                   help="Poll for commit every N cycles. Default 10.")
    p.add_argument("--dlq-file", type=str, default="dead_letters.jsonl",
                   help="Local dead-letter file name.")
    p.add_argument("--no-pii", action="store_true",
                   help="Disable PII encryption (send raw phone numbers).")
    p.add_argument("--prom-port", type=int, default=9100,
                   help="App-level Prometheus metrics port. Default 9100.")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    print(BANNER)

    metrics = MetricsTracker(window_seconds=30, prom_port=args.prom_port)
    metrics.start_prometheus()

    client = ResilientStreamingClient(
        metrics,
        enable_pii=not args.no_pii,
    )

    logger.info("Connecting to Snowflake ...")
    client.connect()
    logger.info("Connected.  Starting ingestion loop.\n")

    if not args.no_pii:
        logger.info("PII encryption ENABLED — phone numbers encrypted with AES-Fernet")
    else:
        logger.info("PII encryption DISABLED — raw phone numbers will be sent")

    logger.info(
        "Prometheus endpoints:\n"
        "  SDK built-in : http://127.0.0.1:50000/metrics  (SS_ENABLE_METRICS=%s)\n"
        "  App-level    : http://127.0.0.1:%d/metrics",
        os.environ.get("SS_ENABLE_METRICS", "false"),
        args.prom_port,
    )

    cycle = 0
    infinite = args.cycles == 0
    total_cycles = args.cycles if not infinite else 200

    try:
        while not _shutdown_requested:
            cycle += 1
            if not infinite and cycle > args.cycles:
                break

            profile = CDRGenerator.pick_profile_for_cycle(cycle, total_cycles)
            records = CDRGenerator.generate_batch(
                base_size=args.batch_size,
                profile=profile,
                bad_record_pct=args.bad_pct,
            )

            sent = client.send_batch(records)

            if cycle % args.commit_every == 0 or _shutdown_requested:
                metrics.print_dashboard(
                    profile_name=profile.name,
                    cycle=cycle,
                    dlq_file=args.dlq_file,
                )
                client.wait_for_commit()

            time.sleep(args.pause)

    except Exception as exc:
        logger.error("Unexpected error in main loop: %s", exc, exc_info=True)

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------
    print("\n>>> Shutting down ...")

    logger.info("Waiting for remaining rows to commit ...")
    client.wait_for_commit()

    dlq_path = os.path.join(os.path.dirname(__file__), "..", args.dlq_file)
    flushed = client.dlq.flush_to_file(dlq_path)
    if flushed:
        logger.info("Flushed %d dead-letter records to %s", flushed, dlq_path)

    client.close()

    metrics.print_dashboard(profile_name="SHUTDOWN", cycle=cycle, dlq_file=args.dlq_file)

    print(
        f"""
    =====================================================
      DEMO 300 COMPLETE
      Total cycles:        {cycle}
      Rows submitted:      {metrics.rows_submitted:,}
      Rows committed:      {metrics.rows_committed:,}
      Dead-letter records: {metrics.dead_letter_count}
      PII encryptions:     {metrics.pii_encryptions:,}
      Errors:              {sum(metrics.errors.values())}
      Channel recoveries:  {metrics.channel_recoveries}
    =====================================================

    Verify in Snowflake:

    -- 1. Total good rows ingested (with advanced types)
    SELECT COUNT(*) AS total_rows,
           COUNT(tower_location) AS rows_with_geo,
           COUNT(coverage_area) AS rows_with_geometry,
           COUNT(device_info) AS rows_with_device
    FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300;

    -- 2. PII verification — encrypted columns (should be ciphertext)
    SELECT call_id,
           caller_number_encrypted,
           caller_number_display,
           callee_number_encrypted,
           callee_number_display
    FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300
    LIMIT 5;

    -- 3. GEOGRAPHY — tower locations + spatial query
    SELECT cell_tower_id,
           tower_location,
           ST_X(tower_location) AS longitude,
           ST_Y(tower_location) AS latitude,
           COUNT(*) AS call_count
    FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300
    WHERE tower_location IS NOT NULL
    GROUP BY 1, 2 ORDER BY call_count DESC LIMIT 10;

    -- 4. GEOMETRY — coverage area sizes
    SELECT cell_tower_id,
           ST_AREA(coverage_area) AS coverage_area_sq,
           COUNT(*) AS call_count
    FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300
    WHERE coverage_area IS NOT NULL
    GROUP BY 1, 2 ORDER BY call_count DESC LIMIT 10;

    -- 5. Semi-structured: device analytics (VARIANT)
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_DEVICE_ANALYTICS_300;

    -- 6. Semi-structured: service tag distribution (ARRAY flatten)
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_SERVICE_TAG_DIST_300;

    -- 7. Semi-structured: network measurements (OBJECT)
    SELECT call_id,
           network_measurements:signal_strength_dbm::FLOAT AS signal_dbm,
           network_measurements:latency_ms::FLOAT AS latency,
           network_measurements:throughput_mbps::FLOAT AS throughput
    FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_300
    ORDER BY ingestion_time DESC LIMIT 10;

    -- 8. Metadata columns — offset gaps (best practice)
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_OFFSET_GAPS_300;

    -- 9. Error rate monitoring
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_ERROR_RATE_300;

    -- 10. DLQ breakdown
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_DLQ_SUMMARY_300;

    -- 11. Tower heatmap (GEOGRAPHY)
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_TOWER_HEATMAP_300;

    -- 12. Schema evolution audit
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_SCHEMA_EVOLUTION_300;

    -- 13. Streaming latency
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_STREAMING_LATENCY_300;
    """
    )


if __name__ == "__main__":
    main()
