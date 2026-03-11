-- =====================================================================
-- demo_300: Advanced Snowpipe Streaming  — Snowflake Object Setup
-- =====================================================================
-- Features:
--   * In-flight transformations in PIPE (UPPER, TRIM, ROUND, TRY_TO_*)
--   * PII columns (encrypted + masked display)
--   * GEOGRAPHY (cell tower GeoJSON points)
--   * GEOMETRY  (cell tower coverage polygons via WKT)
--   * Semi-structured: VARIANT (device_info), ARRAY (service_tags),
--                      OBJECT (network_measurements)
--   * Schema evolution via MATCH_BY_COLUMN_NAME pipe
--   * Client-side metadata columns (channel_id, stream_offset, pipe_id)
--   * Dead-letter queue table + pipe
--   * Monitoring views with offset-gap detection
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- =====================================================================
-- 1. Warehouse
-- =====================================================================
CREATE WAREHOUSE IF NOT EXISTS CDR_STREAMING_WH_300
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'demo_300 — advanced streaming features';

-- =====================================================================
-- 2. Database + Schema
-- =====================================================================
CREATE DATABASE IF NOT EXISTS TELCO_ANALYTICS;
USE DATABASE TELCO_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS CDR_STREAMING_300
  COMMENT = 'demo_300 — GEOGRAPHY, GEOMETRY, PII, semi-structured, transforms';

USE SCHEMA CDR_STREAMING_300;

-- =====================================================================
-- 3. Main CDR Table — with advanced data types
-- =====================================================================
CREATE OR REPLACE TABLE CDR_RECORDS_300 (

    -- Core CDR --------------------------------------------------------
    call_id                 VARCHAR(50)     NOT NULL,
    call_type               VARCHAR(20),
    call_status             VARCHAR(20),
    call_start_time         TIMESTAMP_NTZ,
    call_end_time           TIMESTAMP_NTZ,
    duration_seconds        NUMBER(10,2),

    -- PII — encrypted client-side (AES-Fernet) -----------------------
    caller_number_encrypted VARCHAR(500),
    callee_number_encrypted VARCHAR(500),

    -- PII — masked for display (in-flight transform) -----------------
    caller_number_display   VARCHAR(20),
    callee_number_display   VARCHAR(20),

    -- Network & tower -------------------------------------------------
    cell_tower_id           VARCHAR(50),
    network_type            VARCHAR(10),
    data_usage_mb           FLOAT,
    roaming                 BOOLEAN,
    country_code            VARCHAR(5),

    -- GEOGRAPHY — cell tower lat/lon as GeoJSON Point -----------------
    tower_location          GEOGRAPHY,

    -- GEOMETRY — coverage area as WKT polygon -------------------------
    coverage_area           GEOMETRY,

    -- Semi-structured: VARIANT (nested object) ------------------------
    device_info             VARIANT,

    -- Semi-structured: ARRAY ------------------------------------------
    service_tags            ARRAY,

    -- Semi-structured: OBJECT -----------------------------------------
    network_measurements    OBJECT,

    -- Best-practice metadata columns ----------------------------------
    channel_id              VARCHAR(100),
    stream_offset           BIGINT,
    pipe_id                 VARCHAR(100),

    -- System ----------------------------------------------------------
    ingestion_time          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================================
-- 4. Main PIPE — with in-flight transformations
-- =====================================================================
-- Demonstrates: UPPER, TRIM, ROUND, TRY_TO_GEOGRAPHY, TRY_TO_GEOMETRY,
--               native VARIANT/ARRAY/OBJECT pass-through
-- =====================================================================
CREATE OR REPLACE PIPE CDR_STREAMING_PIPE_300
  COMMENT = 'Main CDR pipe with in-flight transformations and advanced types'
  AS COPY INTO CDR_RECORDS_300
  FROM (
    SELECT
      -- Core fields — cleansed in-flight
      $1:call_id::VARCHAR                                       AS call_id,
      UPPER(TRIM($1:call_type::VARCHAR))                        AS call_type,
      UPPER(TRIM($1:call_status::VARCHAR))                      AS call_status,
      $1:call_start_time::TIMESTAMP_NTZ                         AS call_start_time,
      $1:call_end_time::TIMESTAMP_NTZ                           AS call_end_time,
      ROUND($1:duration_seconds::FLOAT, 2)                      AS duration_seconds,

      -- PII: already encrypted client-side, pass through as-is
      $1:caller_number_encrypted::VARCHAR                       AS caller_number_encrypted,
      $1:callee_number_encrypted::VARCHAR                       AS callee_number_encrypted,

      -- PII: masked display values (client provides these)
      $1:caller_number_display::VARCHAR                         AS caller_number_display,
      $1:callee_number_display::VARCHAR                         AS callee_number_display,

      -- Network — normalised
      UPPER(TRIM($1:cell_tower_id::VARCHAR))                    AS cell_tower_id,
      UPPER($1:network_type::VARCHAR)                           AS network_type,
      ROUND($1:data_usage_mb::FLOAT, 3)                         AS data_usage_mb,
      $1:roaming::BOOLEAN                                       AS roaming,
      UPPER($1:country_code::VARCHAR)                           AS country_code,

      -- GEOGRAPHY: parse GeoJSON string → native GEOGRAPHY
      TRY_TO_GEOGRAPHY($1:tower_location::VARCHAR)              AS tower_location,

      -- GEOMETRY: parse WKT string → native GEOMETRY
      TRY_TO_GEOMETRY($1:coverage_area::VARCHAR)                AS coverage_area,

      -- Semi-structured: pass as native types (best practice)
      $1:device_info                                            AS device_info,
      $1:service_tags                                           AS service_tags,
      $1:network_measurements                                   AS network_measurements,

      -- Metadata columns (best practice)
      $1:channel_id::VARCHAR                                    AS channel_id,
      $1:stream_offset::BIGINT                                  AS stream_offset,
      $1:pipe_id::VARCHAR                                       AS pipe_id,

      CURRENT_TIMESTAMP()                                       AS ingestion_time
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
  )
  CLUSTER_AT_INGEST_TIME = TRUE;

-- =====================================================================
-- 5. Schema-evolution PIPE (alternative — MATCH_BY_COLUMN_NAME)
-- =====================================================================
-- When the producer adds new columns, Snowflake auto-evolves the table.
-- Use this pipe when you want automatic schema evolution instead of
-- explicit column transformations.
-- =====================================================================
CREATE OR REPLACE TABLE CDR_RECORDS_EVOLVED_300 (
    call_id                 VARCHAR(50) NOT NULL,
    ingestion_time          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE CDR_RECORDS_EVOLVED_300 SET
  ENABLE_SCHEMA_EVOLUTION = TRUE;

CREATE OR REPLACE PIPE CDR_SCHEMA_EVOLVE_PIPE_300
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  COMMENT = 'Schema-evolution pipe — auto-adds columns when producer sends new fields'
  AS COPY INTO CDR_RECORDS_EVOLVED_300
  FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'));

-- =====================================================================
-- 6. Dead-Letter Queue table + pipe
-- =====================================================================
CREATE OR REPLACE TABLE CDR_DEAD_LETTER_300 (
    error_id            VARCHAR(50)     NOT NULL,
    error_reason        VARCHAR(500),
    error_category      VARCHAR(50),
    channel_name        VARCHAR(100),
    client_name         VARCHAR(100),
    original_record     VARIANT,
    call_id             VARCHAR(50),
    call_type           VARCHAR(20),
    caller_number       VARCHAR(100),
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PIPE CDR_DLQ_PIPE_300
  COMMENT = 'Dead-letter queue pipe for failed CDR records'
  AS COPY INTO CDR_DEAD_LETTER_300
  FROM (
    SELECT
      $1:error_id::VARCHAR          AS error_id,
      $1:error_reason::VARCHAR      AS error_reason,
      $1:error_category::VARCHAR    AS error_category,
      $1:channel_name::VARCHAR      AS channel_name,
      $1:client_name::VARCHAR       AS client_name,
      $1:original_record            AS original_record,
      $1:call_id::VARCHAR           AS call_id,
      $1:call_type::VARCHAR         AS call_type,
      $1:caller_number::VARCHAR     AS caller_number,
      CURRENT_TIMESTAMP()           AS created_at
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
  );

-- =====================================================================
-- 7. Monitoring views
-- =====================================================================

-- 7a. Streaming latency
CREATE OR REPLACE VIEW V_STREAMING_LATENCY_300 AS
SELECT
    DATE_TRUNC('minute', ingestion_time)        AS minute,
    COUNT(*)                                    AS rows_ingested,
    MIN(ingestion_time)                         AS first_row,
    MAX(ingestion_time)                         AS last_row,
    DATEDIFF('second', MIN(call_start_time), MIN(ingestion_time)) AS approx_latency_s
FROM CDR_RECORDS_300
WHERE ingestion_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

-- 7b. DLQ summary
CREATE OR REPLACE VIEW V_DLQ_SUMMARY_300 AS
SELECT
    error_category,
    COUNT(*)                                    AS error_count,
    MIN(created_at)                             AS first_error,
    MAX(created_at)                             AS last_error,
    COUNT(DISTINCT channel_name)                AS channels_affected
FROM CDR_DEAD_LETTER_300
GROUP BY 1
ORDER BY error_count DESC;

-- 7c. Error rate (good vs bad per minute)
CREATE OR REPLACE VIEW V_ERROR_RATE_300 AS
SELECT
    m.minute,
    COALESCE(m.good_rows, 0)                    AS good_rows,
    COALESCE(d.bad_rows, 0)                     AS bad_rows,
    ROUND(100.0 * COALESCE(d.bad_rows, 0)
        / NULLIF(COALESCE(m.good_rows, 0) + COALESCE(d.bad_rows, 0), 0), 2)
                                                AS error_rate_pct
FROM (
    SELECT DATE_TRUNC('minute', ingestion_time) AS minute,
           COUNT(*)                             AS good_rows
    FROM   CDR_RECORDS_300
    WHERE  ingestion_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
    GROUP BY 1
) m
FULL OUTER JOIN (
    SELECT DATE_TRUNC('minute', created_at) AS minute,
           COUNT(*)                         AS bad_rows
    FROM   CDR_DEAD_LETTER_300
    WHERE  created_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
    GROUP BY 1
) d ON m.minute = d.minute
ORDER BY COALESCE(m.minute, d.minute) DESC;

-- 7d. Errors by channel
CREATE OR REPLACE VIEW V_ERROR_BY_CHANNEL_300 AS
SELECT
    channel_name,
    error_category,
    COUNT(*)                                    AS cnt,
    MIN(created_at)                             AS first_seen,
    MAX(created_at)                             AS last_seen
FROM CDR_DEAD_LETTER_300
GROUP BY 1, 2
ORDER BY cnt DESC;

-- 7e. Offset-gap detection (best practice from Snowflake docs)
CREATE OR REPLACE VIEW V_OFFSET_GAPS_300 AS
SELECT
    pipe_id,
    channel_id,
    stream_offset,
    LAG(stream_offset) OVER (
        PARTITION BY pipe_id, channel_id
        ORDER BY stream_offset
    ) AS previous_offset,
    (LAG(stream_offset) OVER (
        PARTITION BY pipe_id, channel_id
        ORDER BY stream_offset
    ) + 1) AS expected_next
FROM CDR_RECORDS_300
QUALIFY stream_offset != previous_offset + 1;

-- 7f. GEOGRAPHY analytics — tower heatmap
CREATE OR REPLACE VIEW V_TOWER_HEATMAP_300 AS
SELECT
    cell_tower_id,
    tower_location,
    COUNT(*)                                    AS call_count,
    AVG(duration_seconds)                       AS avg_duration,
    SUM(data_usage_mb)                          AS total_data_mb,
    SUM(CASE WHEN call_status = 'DROPPED' THEN 1 ELSE 0 END) AS dropped_calls
FROM CDR_RECORDS_300
WHERE ingestion_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  AND tower_location IS NOT NULL
GROUP BY 1, 2
ORDER BY call_count DESC;

-- 7g. Device analytics (semi-structured queries)
CREATE OR REPLACE VIEW V_DEVICE_ANALYTICS_300 AS
SELECT
    device_info:make::VARCHAR                   AS device_make,
    device_info:model::VARCHAR                  AS device_model,
    device_info:os::VARCHAR                     AS device_os,
    COUNT(*)                                    AS call_count,
    AVG(duration_seconds)                       AS avg_duration,
    AVG(network_measurements:signal_strength_dbm::FLOAT)
                                                AS avg_signal_strength
FROM CDR_RECORDS_300
WHERE ingestion_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY call_count DESC;

-- 7h. Service tag distribution (ARRAY flattening)
CREATE OR REPLACE VIEW V_SERVICE_TAG_DIST_300 AS
SELECT
    tag.VALUE::VARCHAR                          AS service_tag,
    COUNT(*)                                    AS usage_count
FROM CDR_RECORDS_300,
     LATERAL FLATTEN(input => service_tags) tag
WHERE ingestion_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY usage_count DESC;

-- 7i. Schema evolution audit
CREATE OR REPLACE VIEW V_SCHEMA_EVOLUTION_300 AS
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    COMMENT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'CDR_STREAMING_300'
  AND TABLE_NAME = 'CDR_RECORDS_EVOLVED_300'
ORDER BY ORDINAL_POSITION;

-- =====================================================================
-- 8. Role + grants
-- =====================================================================
CREATE ROLE IF NOT EXISTS CDR_STREAMING_ROLE_300;

GRANT USAGE  ON WAREHOUSE CDR_STREAMING_WH_300      TO ROLE CDR_STREAMING_ROLE_300;
GRANT USAGE  ON DATABASE  TELCO_ANALYTICS            TO ROLE CDR_STREAMING_ROLE_300;
GRANT USAGE  ON SCHEMA    CDR_STREAMING_300          TO ROLE CDR_STREAMING_ROLE_300;

-- Tables
GRANT SELECT, INSERT ON TABLE CDR_RECORDS_300         TO ROLE CDR_STREAMING_ROLE_300;
GRANT SELECT, INSERT ON TABLE CDR_RECORDS_EVOLVED_300 TO ROLE CDR_STREAMING_ROLE_300;
GRANT SELECT, INSERT ON TABLE CDR_DEAD_LETTER_300     TO ROLE CDR_STREAMING_ROLE_300;

-- Pipes
GRANT OPERATE, MONITOR ON PIPE CDR_STREAMING_PIPE_300       TO ROLE CDR_STREAMING_ROLE_300;
GRANT OPERATE, MONITOR ON PIPE CDR_SCHEMA_EVOLVE_PIPE_300   TO ROLE CDR_STREAMING_ROLE_300;
GRANT OPERATE, MONITOR ON PIPE CDR_DLQ_PIPE_300             TO ROLE CDR_STREAMING_ROLE_300;

-- Views
GRANT SELECT ON VIEW V_STREAMING_LATENCY_300    TO ROLE CDR_STREAMING_ROLE_300;
GRANT SELECT ON VIEW V_DLQ_SUMMARY_300          TO ROLE CDR_STREAMING_ROLE_300;
GRANT SELECT ON VIEW V_ERROR_RATE_300           TO ROLE CDR_STREAMING_ROLE_300;
GRANT SELECT ON VIEW V_ERROR_BY_CHANNEL_300     TO ROLE CDR_STREAMING_ROLE_300;
GRANT SELECT ON VIEW V_OFFSET_GAPS_300          TO ROLE CDR_STREAMING_ROLE_300;
GRANT SELECT ON VIEW V_TOWER_HEATMAP_300        TO ROLE CDR_STREAMING_ROLE_300;
GRANT SELECT ON VIEW V_DEVICE_ANALYTICS_300     TO ROLE CDR_STREAMING_ROLE_300;
GRANT SELECT ON VIEW V_SERVICE_TAG_DIST_300     TO ROLE CDR_STREAMING_ROLE_300;
GRANT SELECT ON VIEW V_SCHEMA_EVOLUTION_300     TO ROLE CDR_STREAMING_ROLE_300;

-- Assign role to user
GRANT ROLE CDR_STREAMING_ROLE_300 TO USER CDR_STREAMING_USER;

-- =====================================================================
-- Done.  Run this before starting the demo_300 producer.
-- =====================================================================
