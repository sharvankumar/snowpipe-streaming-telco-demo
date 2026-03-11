# Snowpipe Streaming Telco Demo

Production-grade demo that streams telecom **Call Detail Records (CDR)** into
Snowflake using the high-performance [Snowpipe Streaming SDK](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview) (Rust-based).

Includes a **Streamlit in Snowflake** dashboard for real-time analytics.

---

## Architecture

```mermaid
flowchart TD
    A[CDR Generator] --> B["PII Encryptor\n(AES-Fernet)"]
    B --> C["Metadata Enrichment\n(channel_id, stream_offset, pipe_id)"]
    C --> D{Validation}

    D -- PASS --> E["append_row()"]
    D -- FAIL --> DLQ[(Dead Letter Queue)]

    E --> R{"HTTP-aware Retry"}
    R -- "409" --> R1["Channel Reopen"]
    R -- "429" --> R2["Throttle Backoff"]
    R -- "401 / 403" --> R3["Abort"]
    R -- "500 / 503" --> R4["Exponential Backoff"]
    R1 --> E
    R2 --> E
    R4 --> E

    E -- Success --> F["Snowflake PIPE\n(in-flight transforms:\nUPPER, TRIM, ROUND,\nTRY_TO_GEOGRAPHY,\nTRY_TO_GEOMETRY)"]
    F --> G[(CDR_RECORDS_300)]

    G --> H["Prometheus\n(:50000 SDK · :9100 App)"]
    G --> I["Streamlit Dashboard\n(Streamlit in Snowflake)"]

    style A fill:#4a9eff,color:#fff
    style B fill:#ff6b6b,color:#fff
    style C fill:#ffa94d,color:#fff
    style D fill:#845ef7,color:#fff
    style DLQ fill:#e03131,color:#fff
    style E fill:#51cf66,color:#fff
    style F fill:#339af0,color:#fff
    style G fill:#20c997,color:#fff
    style H fill:#fab005,color:#000
    style I fill:#fab005,color:#000
    style R fill:#845ef7,color:#fff
    style R3 fill:#e03131,color:#fff
```

---

## Features

| Feature | Description |
|---------|-------------|
| **PII Encryption** | AES-Fernet encryption of phone numbers client-side before data leaves the producer |
| **Prometheus Metrics** | Dual-layer: SDK built-in (`:50000`) + app-level custom counters (`:9100`) |
| **GEOGRAPHY** | Cell tower lat/lon as GeoJSON points via `TRY_TO_GEOGRAPHY` |
| **GEOMETRY** | Hexagonal coverage polygons as WKT via `TRY_TO_GEOMETRY` |
| **Semi-structured** | `device_info` (VARIANT), `service_tags` (ARRAY), `network_measurements` (OBJECT) |
| **In-flight Transforms** | PIPE applies `UPPER`, `TRIM`, `ROUND`, `TRY_TO_GEOGRAPHY`, `TRY_TO_GEOMETRY` |
| **Schema Evolution** | `MATCH_BY_COLUMN_NAME` + `ENABLE_SCHEMA_EVOLUTION = TRUE` |
| **HTTP-aware Retry** | Status-code-specific handling: 409 (channel reopen), 429 (throttle), 401/403 (abort), 500/503 (backoff) |
| **Offset-Gap Detection** | SQL view to identify missing records |
| **Dead-Letter Queue** | Failed records streamed to Snowflake + local JSONL file |
| **Streamlit Dashboard** | 5-page analytics dashboard running in Snowflake |

---

## Project Structure

```
snowpipe-streaming-telco-demo/
├── README.md
├── requirements.txt
│
├── profiles/                     # Snowflake connection profiles
│   ├── profile_default.json.example
│   ├── profile_afe.json.example
│   ├── profile_default.json      # ← your credentials (gitignored)
│   └── profile_afe.json          # ← your credentials (gitignored)
│
├── sql/
│   └── 01_snowflake_setup.sql  # DDL: tables, pipes, views, roles, grants
│
├── src/
│   ├── run_demo.py             # Main entry point
│   ├── streaming_client.py     # Resilient client with retry + DLQ
│   ├── cdr_generator.py        # CDR data generator (geo, device, network)
│   ├── pii_encryptor.py        # AES-Fernet PII encryption
│   └── metrics_tracker.py      # Prometheus + console dashboard metrics
│
├── streamlit/
│   ├── streamlit_app.py        # Streamlit in Snowflake analytics dashboard
│   └── environment.yml         # Streamlit app dependencies (pydeck)
│
└── tests/
    └── test_schema_evolution.py  # Interactive schema evolution test
```

---

## Quick Start

### 1. Set up Snowflake objects

Open `sql/01_snowflake_setup.sql` in Snowflake and run it. This creates:

- Warehouse, database, schema
- CDR table with GEOGRAPHY, GEOMETRY, VARIANT, ARRAY, OBJECT columns
- Pipes with in-flight transformations
- Schema evolution table + pipe
- Dead-letter queue table + pipe
- Monitoring views
- Role with least-privilege grants

### 2. Configure credentials

Copy an example profile and fill in your credentials:

```bash
cp profiles/profile_default.json.example profiles/profile_default.json
```

Edit `profiles/profile_default.json` with your Snowflake account details:

```json
{
  "account": "<your_snowflake_account>",
  "user": "<your_snowflake_user>",
  "url": "https://<your_account>.snowflakecomputing.com:443",
  "role": "CDR_STREAMING_ROLE_300",
  "private_key": "<private_key_content>"
}
```

> To get the `private_key` value, extract the key content from your `.p8` PEM
> file into a single line:
>
> ```bash
> grep -v '^\-\-' rsa_key.p8 | tr -d '\n'
> ```
>
> This strips the `-----BEGIN PRIVATE KEY-----` / `-----END PRIVATE KEY-----`
> lines and removes all newlines. Paste the output into the `private_key` field.
> (Ignore the `%` at the end if you see one — that's just zsh indicating no trailing newline.)

You can create multiple profiles for different accounts (e.g. `profile_afe.json`,
`profile_dev.json`). All `profile_*.json` files are gitignored; only `.example`
files are tracked.

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the streaming demo

```bash
python src/run_demo.py
```

That's it! The demo will stream ~200 cycles of CDR data into Snowflake.

---

## CLI Options

```bash
python src/run_demo.py --cycles 50          # shorter run
python src/run_demo.py --cycles 0           # infinite (Ctrl+C to stop)
python src/run_demo.py --bad-pct 0.10       # 10% bad records
python src/run_demo.py --no-pii             # disable PII encryption
python src/run_demo.py --prom-port 9200     # custom Prometheus port
python src/run_demo.py --batch-size 200     # larger batches
python src/run_demo.py --profile afe        # use profiles/profile_afe.json
```

---

## Switching Snowflake Accounts

Profile resolution follows this priority order:

| Priority | Method | Example |
|----------|--------|---------|
| 1 (highest) | `--profile` CLI flag | `python src/run_demo.py --profile afe` |
| 2 | `SNOWFLAKE_PROFILE` env var | `export SNOWFLAKE_PROFILE=afe` |
| 3 (fallback) | Default | Uses `profiles/profile_default.json` |

### Available profiles

| Profile name | File | Account |
|-------------|------|---------|
| `default` | `profiles/profile_default.json` | YOURACCOUNT |
| `afe` | `profiles/profile_afe.json` | YOURACCOUNT-xxxx_xxxx |

### Adding a new profile

```bash
cp profiles/profile_default.json.example profiles/profile_myaccount.json
# Edit with your credentials, then:
python src/run_demo.py --profile myaccount
```

---

## Streamlit Dashboard

The `streamlit/streamlit_app.py` is a **Streamlit in Snowflake (SiS)** app. It
provides 5 analytics views over the streaming data:

| Page | What it shows |
|------|---------------|
| **Overview** | Total records, call type / status / network distribution |
| **Streaming Health** | Ingestion latency, error rate trends, offset gap detection |
| **Error Analysis** | Dead-letter queue breakdown, errors by channel, schema evolution audit |
| **Tower Analytics** | Interactive tower heatmap (pydeck), call counts, dropped calls |
| **Device Analytics** | Device make/model breakdown, signal strength, service tag distribution |

### How to deploy

1. In Snowflake, go to **Streamlit** and create a new app
2. Upload `streamlit/streamlit_app.py` as the main file
3. Upload `streamlit/environment.yml` for the `pydeck` dependency
4. Set the app warehouse and select the `CDR_STREAMING_300` schema
5. Run the app — it queries the views created by `sql/01_snowflake_setup.sql`

> The dashboard auto-refreshes every 60 seconds. Run it alongside the streaming
> demo to see data flowing in real time.

---

## Monitoring

### Prometheus endpoints

After starting the demo, two Prometheus endpoints are available:

| Endpoint | Source | Metrics |
|----------|--------|---------|
| `http://127.0.0.1:50000/metrics` | SDK built-in | Flush latency, buffer size, HTTP stats |
| `http://127.0.0.1:9100/metrics` | App-level | `cdr_rows_submitted_total`, `cdr_errors_total`, etc. |

```bash
curl http://127.0.0.1:50000/metrics    # SDK metrics
curl http://127.0.0.1:9100/metrics     # App metrics
```

To visualize these metrics, connect the endpoints to your Prometheus server. You
can run Prometheus + Grafana in containers for a quick local setup:

```bash
docker run -d --name prometheus --net=host -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
docker run -d --name grafana --net=host grafana/grafana
```

### Prometheus scrape config

Add the following to your `prometheus.yml` (or create one in the project root):

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: snowpipe_streaming_sdk
    static_configs:
      - targets: ['127.0.0.1:50000']
  - job_name: snowpipe_streaming_app
    static_configs:
      - targets: ['127.0.0.1:9100']
```

Once running, access Prometheus at `http://localhost:9090` and Grafana at
`http://localhost:3000` (default login: admin/admin).

### App-level metrics reference

| Metric | Type | Description |
|--------|------|-------------|
| `cdr_rows_submitted_total` | Counter | Total rows submitted |
| `cdr_rows_committed` | Gauge | Latest committed count |
| `cdr_batches_sent_total` | Counter | Total batches |
| `cdr_errors_total{category}` | Counter | Errors by category |
| `cdr_retries_total` | Counter | Retry attempts |
| `cdr_channel_recoveries_total` | Counter | Channel recovery events |
| `cdr_dead_letter_total` | Counter | DLQ records |
| `cdr_batch_latency_seconds` | Histogram | Per-batch send time |
| `cdr_commit_lag` | Gauge | Submitted minus committed |
| `cdr_pii_encryptions_total` | Counter | Fields encrypted |

---

## Snowflake Objects

### Tables

| Object | Purpose |
|--------|---------|
| `CDR_RECORDS_300` | Main CDR table with GEOGRAPHY, GEOMETRY, VARIANT, ARRAY, OBJECT |
| `CDR_RECORDS_EVOLVED_300` | Schema evolution target |
| `CDR_DEAD_LETTER_300` | Dead-letter queue |

### Pipes

| Object | Purpose |
|--------|---------|
| `CDR_STREAMING_PIPE_300` | Main pipe with in-flight transforms |
| `CDR_SCHEMA_EVOLVE_PIPE_300` | Schema evolution pipe (`MATCH_BY_COLUMN_NAME`) |
| `CDR_DLQ_PIPE_300` | DLQ pipe |

### Monitoring Views

| View | What it shows |
|------|---------------|
| `V_STREAMING_LATENCY_300` | Rows ingested per minute + approx latency |
| `V_DLQ_SUMMARY_300` | DLQ breakdown by error category |
| `V_ERROR_RATE_300` | Good vs bad rows per minute |
| `V_ERROR_BY_CHANNEL_300` | Errors grouped by channel |
| `V_OFFSET_GAPS_300` | Missing records (offset gaps) |
| `V_TOWER_HEATMAP_300` | Tower call counts + dropped calls |
| `V_DEVICE_ANALYTICS_300` | Device make/model breakdown |
| `V_SERVICE_TAG_DIST_300` | Service tag usage |
| `V_SCHEMA_EVOLUTION_300` | Schema evolution audit trail |

---

## Testing Schema Evolution

Run the interactive schema evolution test to see Snowflake auto-add columns:

```bash
python tests/test_schema_evolution.py
```

The test sends 3 phases of records, each adding new fields. Snowflake
automatically evolves the `CDR_RECORDS_EVOLVED_300` table schema.

---

## Best Practices Applied

- [x] Long-lived channels (open once, keep active)
- [x] Deterministic channel names (`prefix-clientname`)
- [x] Client-side validation before `append_row`
- [x] Client-side metadata columns for offset-gap detection
- [x] `MATCH_BY_COLUMN_NAME` for schema evolution
- [x] Native Python types for semi-structured data (no string serialization)
- [x] Prometheus metrics (SDK + app-level)
- [x] HTTP status code interpretation with retry logic
- [x] Exponential backoff for retryable errors
- [x] Commit progress verification with offset tokens
- [x] Channel health monitoring via `get_channel_status()`
