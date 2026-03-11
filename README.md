# demo_300 — Advanced Snowpipe Streaming

Production-grade CDR streaming demo showcasing **every** advanced feature
of the Snowpipe Streaming high-performance architecture SDK.

## New in demo_300 (vs demo_200)

| Feature | Description |
|---------|-------------|
| **PII Encryption** | AES-Fernet encryption of phone numbers client-side before data leaves the producer. Snowflake stores only ciphertext. Masked display columns for safe querying. |
| **Prometheus Metrics** | Dual-layer: SDK built-in (`:50000`) + app-level custom counters/gauges (`:9100`). |
| **GEOGRAPHY** | Cell tower lat/lon as GeoJSON points → native `GEOGRAPHY` type via `TRY_TO_GEOGRAPHY` in-flight. |
| **GEOMETRY** | Hexagonal coverage polygons as WKT → native `GEOMETRY` type via `TRY_TO_GEOMETRY` in-flight. |
| **Semi-structured** | `device_info` (VARIANT), `service_tags` (ARRAY), `network_measurements` (OBJECT) — all passed as native Python types per best practices. |
| **In-flight Transforms** | PIPE applies `UPPER`, `TRIM`, `ROUND`, `TRY_TO_GEOGRAPHY`, `TRY_TO_GEOMETRY` during ingestion. |
| **Schema Evolution** | Second pipe (`CDR_SCHEMA_EVOLVE_PIPE_300`) with `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` and `ENABLE_SCHEMA_EVOLUTION = TRUE`. |
| **Metadata Columns** | `channel_id`, `stream_offset`, `pipe_id` per Snowflake best practices for offset-gap detection. |
| **Deterministic Channels** | Channel names follow `prefix-clientname` convention for easier debugging. |
| **Offset-Gap Detection** | SQL view `V_OFFSET_GAPS_300` identifies missing records (Snowflake best practice). |

## Architecture

```
  CDR Generator
       │
       ▼
  PII Encryptor (AES-Fernet)
       │  encrypt phone numbers
       │  add masked display columns
       ▼
  Metadata Enrichment
       │  channel_id, stream_offset, pipe_id
       ▼
  Client-side Validation
       │
  ┌────┴────┐
  │ PASS    │ FAIL → DLQ (validation)
  ▼         │
  append_row() ──── HTTP-status-aware retry
  │                 409 → channel reopen
  │                 429 → throttle backoff
  │                 401/403 → abort
  │                 500/503 → exponential backoff
  ▼
  Snowflake PIPE (in-flight transforms)
  │  UPPER(TRIM(call_type))
  │  TRY_TO_GEOGRAPHY(tower_location)
  │  TRY_TO_GEOMETRY(coverage_area)
  │  ROUND(duration_seconds, 2)
  ▼
  CDR_RECORDS_300 table
       │
  Prometheus ◄── SDK metrics (:50000)
  Prometheus ◄── App metrics (:9100)
```

## Prerequisites

1. **Snowflake objects** — run `01_snowflake_setup.sql` in Snowflake
2. **Python environment** — use the `snowpipe-streaming-v2` virtualenv
3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
4. **profile.json** — update with your account credentials and private key

## How to Run

### Basic run (200 cycles)

```bash
cd demo_300
python run_demo.py
```

### CLI options

```bash
python run_demo.py --cycles 50          # shorter run
python run_demo.py --cycles 0           # infinite (Ctrl+C to stop)
python run_demo.py --bad-pct 0.10       # 10% bad records
python run_demo.py --no-pii            # disable PII encryption
python run_demo.py --prom-port 9200    # custom Prometheus port
python run_demo.py --batch-size 200    # larger batches
```

### Prometheus endpoints

After starting, two Prometheus endpoints are available:

| Endpoint | Source | Metrics |
|----------|--------|---------|
| `http://127.0.0.1:50000/metrics` | SDK built-in | Flush latency, buffer size, HTTP request stats |
| `http://127.0.0.1:9100/metrics` | App-level | `cdr_rows_submitted_total`, `cdr_errors_total`, `cdr_batch_latency_seconds`, etc. |

Verify with:
```bash
curl http://127.0.0.1:50000/metrics    # SDK metrics
curl http://127.0.0.1:9100/metrics     # App metrics
```

### Prometheus scrape config

```yaml
scrape_configs:
  - job_name: snowpipe_streaming_sdk
    static_configs:
      - targets: ['127.0.0.1:50000']
  - job_name: snowpipe_streaming_app
    static_configs:
      - targets: ['127.0.0.1:9100']
```

## Snowflake Objects (all suffixed `_300`)

| Object | Type | Purpose |
|--------|------|---------|
| `CDR_STREAMING_WH_300` | Warehouse | Compute |
| `CDR_STREAMING_300` | Schema | All demo_300 objects |
| `CDR_RECORDS_300` | Table | Main CDR table with GEOGRAPHY, GEOMETRY, VARIANT, ARRAY, OBJECT |
| `CDR_RECORDS_EVOLVED_300` | Table | Schema evolution target (ENABLE_SCHEMA_EVOLUTION=TRUE) |
| `CDR_DEAD_LETTER_300` | Table | Dead-letter queue |
| `CDR_STREAMING_PIPE_300` | Pipe | Main pipe with in-flight transforms |
| `CDR_SCHEMA_EVOLVE_PIPE_300` | Pipe | Schema evolution pipe (MATCH_BY_COLUMN_NAME) |
| `CDR_DLQ_PIPE_300` | Pipe | DLQ pipe |
| `CDR_STREAMING_ROLE_300` | Role | Least-privilege role |

## Monitoring Views

| View | What it shows |
|------|---------------|
| `V_STREAMING_LATENCY_300` | Rows ingested per minute + approx latency |
| `V_DLQ_SUMMARY_300` | DLQ breakdown by error category |
| `V_ERROR_RATE_300` | Good vs bad rows per minute |
| `V_ERROR_BY_CHANNEL_300` | Errors grouped by channel |
| `V_OFFSET_GAPS_300` | Missing records (offset gaps) |
| `V_TOWER_HEATMAP_300` | GEOGRAPHY: tower call counts + dropped calls |
| `V_DEVICE_ANALYTICS_300` | VARIANT: device make/model breakdown |
| `V_SERVICE_TAG_DIST_300` | ARRAY: flattened service tag usage |
| `V_SCHEMA_EVOLUTION_300` | Schema evolution audit trail |

## App-level Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `cdr_rows_submitted_total` | Counter | Total rows submitted |
| `cdr_rows_committed` | Gauge | Latest committed count |
| `cdr_batches_sent_total` | Counter | Total batches |
| `cdr_errors_total{category}` | Counter | Errors by category (validation, send_http409, auth, ...) |
| `cdr_retries_total` | Counter | Retry attempts |
| `cdr_channel_recoveries_total` | Counter | Channel recovery events |
| `cdr_dead_letter_total` | Counter | DLQ records |
| `cdr_batch_latency_seconds` | Histogram | Per-batch send time |
| `cdr_commit_lag` | Gauge | Submitted - committed |
| `cdr_pii_encryptions_total` | Counter | Fields encrypted |

## File Reference

| File | Purpose |
|------|---------|
| `01_snowflake_setup.sql` | DDL: tables, pipes (with transforms), views, role, grants |
| `profile.json` | Snowflake connection credentials |
| `requirements.txt` | Python dependencies |
| `pii_encryptor.py` | AES-Fernet PII encryption + phone masking |
| `cdr_generator.py` | CDR data generator with GeoJSON, WKT, device_info, service_tags |
| `metrics_tracker.py` | Prometheus + console dashboard metrics |
| `streaming_client.py` | Resilient client with PII, metadata, HTTP-aware retry |
| `run_demo.py` | Main entry point |

## Best Practices Applied (per Snowflake docs)

- [x] Long-lived channels (open once, keep active)
- [x] Deterministic channel names (`prefix-clientname`)
- [x] Client-side validation before append_row
- [x] Client-side metadata columns (channel_id, stream_offset, pipe_id)
- [x] Offset-gap detection SQL
- [x] MATCH_BY_COLUMN_NAME for schema evolution
- [x] Native Python types for semi-structured data (no string serialisation)
- [x] Prometheus metrics (SDK + app-level)
- [x] Try-catch with HTTP status code interpretation (409, 429, 401, 500)
- [x] Exponential backoff for retryable errors
- [x] Verify progress with offset tokens
- [x] Monitor channel status via get_channel_status()
