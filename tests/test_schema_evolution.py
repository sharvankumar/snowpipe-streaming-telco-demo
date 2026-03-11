#!/usr/bin/env python3
"""
Schema Evolution Test — demo_300
=================================
Demonstrates how MATCH_BY_COLUMN_NAME + ENABLE_SCHEMA_EVOLUTION lets
the producer add new columns on the fly without any DDL changes.

The target table CDR_RECORDS_EVOLVED_300 starts with ONLY:
  - call_id  VARCHAR(50)
  - ingestion_time  TIMESTAMP_NTZ

This script sends 3 phases of records, each adding new fields.
Snowflake auto-evolves the table schema after each phase.

Usage:
  python test_schema_evolution.py

After running, check the evolved schema in Snowflake:
  SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_SCHEMA_EVOLUTION_300;
  SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_EVOLVED_300;
"""

import logging
import os
import sys
import time
import uuid

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "src"))
os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest.streaming import StreamingIngestClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

PROFILE_JSON = os.path.join(_PROJECT_ROOT, "profile.json")
DATABASE = "TELCO_ANALYTICS"
SCHEMA = "CDR_STREAMING_300"
PIPE = "CDR_SCHEMA_EVOLVE_PIPE_300"


def main():
    print("""
    =====================================================
      SCHEMA EVOLUTION TEST  (demo_300)
    =====================================================
    Target table : CDR_RECORDS_EVOLVED_300
    Target pipe  : CDR_SCHEMA_EVOLVE_PIPE_300
    Strategy     : MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                   ENABLE_SCHEMA_EVOLUTION = TRUE
    =====================================================
    """)

    # Connect
    client_name = f"SCHEMA_EVO_CLIENT_{uuid.uuid4().hex[:8]}"
    logger.info("Creating client [%s] ...", client_name)
    client = StreamingIngestClient(
        client_name=client_name,
        db_name=DATABASE,
        schema_name=SCHEMA,
        pipe_name=PIPE,
        profile_json=PROFILE_JSON,
    )
    channel_name = f"schema-evo-{client_name}"
    channel, status = client.open_channel(channel_name)
    logger.info("Channel open [%s]. Status: %s", channel_name, status)

    offset = 0

    # ------------------------------------------------------------------
    # PHASE 1: Minimal columns (call_id + call_type)
    # Table already has call_id. call_type is NEW → should be auto-added.
    # ------------------------------------------------------------------
    print("\n>>> PHASE 1: Sending records with call_id + call_type only")
    print("    (call_type is a NEW column — expect Snowflake to add it)\n")

    for i in range(5):
        row = {
            "call_id": f"EVO-P1-{uuid.uuid4().hex[:8]}",
            "call_type": "VOICE",
        }
        channel.append_row(row, str(offset))
        offset += 1
        logger.info("  Phase 1 row %d sent: %s", i + 1, row)

    logger.info("Phase 1 done. Waiting for commit ...")
    time.sleep(5)
    committed = channel.get_latest_committed_offset_token()
    logger.info("Committed offset: %s", committed)

    print("""
    CHECK IN SNOWFLAKE NOW:
    -------------------------------------------------
    -- Should see call_id + call_type (auto-added!)
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_EVOLVED_300;

    -- Schema should show: call_id, ingestion_time, call_type
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_SCHEMA_EVOLUTION_300;
    -------------------------------------------------
    """)
    input("    Press ENTER to continue to Phase 2 ...")

    # ------------------------------------------------------------------
    # PHASE 2: Add more scalar columns
    # duration_seconds, network_type, call_status are all NEW.
    # ------------------------------------------------------------------
    print("\n>>> PHASE 2: Adding duration_seconds, network_type, call_status")
    print("    (3 more new columns — expect Snowflake to auto-add all 3)\n")

    for i in range(5):
        row = {
            "call_id": f"EVO-P2-{uuid.uuid4().hex[:8]}",
            "call_type": "VIDEO",
            "duration_seconds": 120 + i * 10,
            "network_type": "5G",
            "call_status": "COMPLETED",
        }
        channel.append_row(row, str(offset))
        offset += 1
        logger.info("  Phase 2 row %d sent: %s", i + 1, row)

    logger.info("Phase 2 done. Waiting for commit ...")
    time.sleep(5)
    committed = channel.get_latest_committed_offset_token()
    logger.info("Committed offset: %s", committed)

    print("""
    CHECK IN SNOWFLAKE NOW:
    -------------------------------------------------
    -- Phase 1 rows: call_type filled, new cols are NULL (they didn't exist yet)
    -- Phase 2 rows: all 5 columns populated
    SELECT call_id, call_type, duration_seconds, network_type, call_status
    FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_EVOLVED_300
    ORDER BY ingestion_time;

    -- Schema: should now have 6 columns total
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_SCHEMA_EVOLUTION_300;
    -------------------------------------------------
    """)
    input("    Press ENTER to continue to Phase 3 ...")

    # ------------------------------------------------------------------
    # PHASE 3: Add semi-structured columns
    # device_info (dict → VARIANT), service_tags (list → ARRAY),
    # tower_location (string → VARCHAR, since pipe is plain pass-through)
    # ------------------------------------------------------------------
    print("\n>>> PHASE 3: Adding semi-structured data (device_info, service_tags, tower_location)")
    print("    (dict → VARIANT, list → ARRAY — auto-added with correct types!)\n")

    for i in range(5):
        row = {
            "call_id": f"EVO-P3-{uuid.uuid4().hex[:8]}",
            "call_type": "DATA",
            "duration_seconds": 300 + i * 50,
            "network_type": "5G",
            "call_status": "COMPLETED",
            "device_info": {
                "make": "Apple",
                "model": "iPhone 16 Pro",
                "os": "iOS",
                "os_version": "18.3",
            },
            "service_tags": ["5G-SA", "VoNR", "HD_VOICE"],
            "tower_location": '{"type":"Point","coordinates":[-73.9855,40.7580]}',
        }
        channel.append_row(row, str(offset))
        offset += 1
        logger.info("  Phase 3 row %d sent", i + 1)

    logger.info("Phase 3 done. Waiting for commit ...")
    time.sleep(5)
    committed = channel.get_latest_committed_offset_token()
    logger.info("Committed offset: %s", committed)

    # Close
    channel.close()
    client.close()
    logger.info("Client closed.")

    print(f"""
    =====================================================
      SCHEMA EVOLUTION TEST COMPLETE
      Total rows sent: {offset}
    =====================================================

    FINAL VERIFICATION IN SNOWFLAKE:

    -- 1. See the fully evolved schema (started with 2 cols, now has ~9)
    SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'CDR_STREAMING_300'
      AND TABLE_NAME = 'CDR_RECORDS_EVOLVED_300'
    ORDER BY ORDINAL_POSITION;

    -- 2. See all rows — notice Phase 1 rows have NULLs for later columns
    SELECT *
    FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_EVOLVED_300
    ORDER BY ingestion_time;

    -- 3. Query the semi-structured data
    SELECT call_id,
           device_info:make::VARCHAR AS device_make,
           device_info:model::VARCHAR AS device_model,
           ARRAY_SIZE(service_tags) AS tag_count
    FROM TELCO_ANALYTICS.CDR_STREAMING_300.CDR_RECORDS_EVOLVED_300
    WHERE device_info IS NOT NULL;

    -- 4. Use the monitoring view
    SELECT * FROM TELCO_ANALYTICS.CDR_STREAMING_300.V_SCHEMA_EVOLUTION_300;

    KEY TAKEAWAY:
    The table started with 2 columns and grew to ~9 columns automatically.
    No ALTER TABLE was needed. The producer just sent new fields and
    Snowflake added the columns on the fly.
    """)


if __name__ == "__main__":
    main()
