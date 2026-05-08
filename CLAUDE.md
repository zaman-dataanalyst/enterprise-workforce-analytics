# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Enterprise workforce analytics platform built on Google BigQuery using a **Medallion architecture** (Bronze → Silver → Gold). The pipeline ingests **synthetic** HRIS timesheet data (generated via Faker/OpenExchangeRates), transforms it through dbt, and surfaces a star schema for BI consumption.

- **~3.7M rows** of timesheet data spanning ~1,098 days
- **dbt 1.11.1** with BigQuery adapter
- **Python 3.10/3.11** for ingestion and audit scripts
- **GitHub Actions** for CI/CD

## Commands

### dbt (run from `workforce_analytics/`)

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run a single model
dbt run --select stg_hris__timesheets
dbt run --select fct_timesheets

# Run all tests
dbt test

# Test a single model
dbt test --select stg_hris__timesheets

# Seed reference data (seeds to silver_layer schema)
dbt seed

# Compile without running
dbt compile

# Generate docs
dbt docs generate && dbt docs serve
```

### Local dbt authentication

dbt requires a `~/.dbt/profiles.yml`. The CI pipeline generates this dynamically; for local development create it manually:

```yaml
workforce_analytics:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: oauth
      project: enterprise-workforce-analytics
      dataset: gold_layer
      threads: 4
      location: US
```

Authentication requires `GOOGLE_APPLICATION_CREDENTIALS` pointing to a GCP service account key, or run `gcloud auth application-default login`.

### Python ingestion (`ingestion/`)

```bash
pip install pandas faker google-cloud-bigquery google-cloud-storage requests "pandas-gbq>=0.26.1" pyarrow db-dtypes

OXR_APP_ID=<your_key> python ingest_workforce_bronze.py
```

`OXR_APP_ID` is required — it fetches monthly FX rates from OpenExchangeRates. Falls back to hardcoded defaults if the API call fails (rows get `fx_source = 'FALLBACK'`).

### Data quality audit (`data_quality_audits/`)

```bash
python validate_stg_timesheets.py
```

## Architecture

### Medallion Layers

| Layer | Schema | Materialization | Location |
|-------|--------|-----------------|----------|
| Bronze | `bronze_layer` | Raw BigQuery table | `ingestion/` Python ETL |
| Silver | `silver_layer` | View* | `models/staging/hris/` |
| Gold | `gold_layer` | Tables | `models/marts/hris/` |

\* `stg_hris__timesheets` has model-level `materialized = 'view'` which overrides the project-level `+materialized: table` in `dbt_project.yml`. All gold mart models are tables.

### dbt Project Structure

```
workforce_analytics/
├── models/
│   ├── staging/hris/
│   │   ├── stg_hris__timesheets.sql   # Primary 713-line Silver transform (4-CTE pipeline)
│   │   ├── stg_hris__timesheets.yml   # dbt tests + source freshness
│   │   └── sources.yml                # bronze_layer.raw_hris_timesheets_v1
│   └── marts/hris/
│       ├── dim_employees.sql
│       ├── dim_date.sql
│       ├── dim_clients.sql
│       ├── dim_projects.sql
│       ├── dim_locations.sql
│       ├── dim_activity_metadata.sql
│       ├── dim_skills_bridge.sql
│       ├── fct_timesheets.sql         # Central fact table with FX calculations
│       └── _marts_hris.yml            # Tests for gold layer
├── seeds/
│   ├── market_mapping.csv             # Geographic/currency mappings
│   └── skill_categories.csv          # Skill dimension lookup
├── macros/
│   └── generate_schema_name.sql      # Prevents double-prefixing of schema names
├── analyses/
│   └── eda_hris_timesheets.sql       # 50+ profiling queries (reference only)
├── dbt_project.yml
└── packages.yml                       # codegen 0.14.0, dbt_utils 1.3.3
```

### Key Design Decisions

**Schema name macro:** `macros/generate_schema_name.sql` overrides dbt's default schema naming to prevent `dataset_silver_layer` double-prefixing. When adding new models, target schemas are set in `dbt_project.yml` under `+schema:`.

**Seeds go to silver_layer:** The `dbt seed` step is configured in `dbt_project.yml` with `+schema: silver_layer`. The GitHub Actions pipeline seeds before `dbt run`. Both seeds are actively consumed:
- `market_mapping` — joined in `dim_locations` to resolve `market_segment`; uses full ISO country names to match Silver layer output
- `skill_categories` — joined in `dim_skills_bridge` to populate `skill_categories` (STRING_AGG of capability labels per skill)

**Alphanumeric corruption:** The raw HRIS data contains OCR-like corruption (e.g., `"1"` → `"I"`, `"0"` → `"O"`). The Silver model cleans this with a 4-CTE pipeline: `trim_nullify` → `fix_text_corruption` → `standardize` → final SELECT. The Python audit in `data_quality_audits/validate_stg_timesheets.py` independently validates this.

**Correction records:** ~50,908 rows have negative hours (correction entries). These are preserved with an `is_correction` flag in Silver and properly handled in `fct_timesheets`.

**Surrogate keys in fct_timesheets:** Use `FARM_FINGERPRINT` (native BigQuery), not `dbt_utils.generate_surrogate_key`. Dimensions use `dbt_utils.generate_surrogate_key`.

**Silver model strict business rules — do NOT change these:**
- `original_timesheet_id` → never imputed; NULL is a valid factual state
- `project_name` → no digit-to-letter regex (contains valid values like `'24/7'`, `'3rdgeneration'`)
- Negative hours → keep as-is (valid correction records for Gold netting)
- `utilization_pct` → stored as 0–1 decimal scale; do NOT multiply by 100

### CI/CD Pipelines (`.github/workflows/`)

**`dbt_pipeline.yml`** — triggers on push to `main`, PRs, or manual dispatch:
1. Authenticates to GCP via `GCP_CREDENTIALS` secret
2. `dbt seed` → `dbt run` → `dbt test`

**`bronze_ingestion.yml`** — runs daily at 00:00 UTC or manual dispatch:
1. Authenticates to GCP
2. Runs `ingestion/ingest_workforce_bronze.py`
3. Posts Slack webhook alert (via `SLACK_WEBHOOK_URL` secret)

Required GitHub secrets: `GCP_CREDENTIALS`, `GCP_PROJECT_ID`, `OXR_APP_ID`, `SLACK_WEBHOOK_URL`.

## dbt Packages

- **`dbt-labs/codegen`** — used for generating model YAML boilerplate (dev-time utility)
- **`dbt-labs/dbt_utils`** — used for surrogate keys and generic test macros in mart models

## Data Quality

dbt tests are defined in `.yml` files alongside models:
- `unique` + `not_null` on all primary/surrogate keys
- `accepted_values` for all categorical enums (employment_type, project_type, client_segment, currency, etc.)
- Custom business-rule tests for financial consistency

The standalone Python audit (`data_quality_audits/validate_stg_timesheets.py`) validates a sample of the Silver layer independently of dbt and is not part of the CI pipeline — run manually against local samples (excluded from version control via `.gitignore`).
