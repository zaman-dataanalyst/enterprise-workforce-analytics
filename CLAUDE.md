# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Enterprise workforce analytics platform built on Google BigQuery using a **Medallion architecture** (Bronze → Silver → Gold). The pipeline ingests HRIS timesheet data, transforms it through dbt, and surfaces a star schema for BI consumption.

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

### Python ingestion (`ingestion/`)

```bash
pip install -r requirements.txt  # if present, else install manually
python ingest_workforce_bronze.py
```

### Data quality audit (`data_quality_audits/`)

```bash
python validate_stg_timesheets.py
```

Authentication requires `GOOGLE_APPLICATION_CREDENTIALS` or `GOOGLE_CLOUD_KEYFILE_JSON` env var pointing to a GCP service account key.

## Architecture

### Medallion Layers

| Layer | Schema | Materialization | Location |
|-------|--------|-----------------|----------|
| Bronze | `bronze_layer` | Raw BigQuery table | `ingestion/` Python ETL |
| Silver | `silver_layer` | Table | `models/staging/hris/` |
| Gold | `gold_layer` | Tables | `models/marts/hris/` |

### dbt Project Structure

```
workforce_analytics/
├── models/
│   ├── staging/hris/
│   │   ├── stg_hris__timesheets.sql   # Primary 713-line Silver transform
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

**Seeds go to silver_layer:** The `dbt seed` step is configured in `dbt_project.yml` with `+schema: silver_layer`. The GitHub Actions pipeline seeds before `dbt run`.

**Silver layer materialization:** `stg_hris__timesheets` is materialized as a `table` (not view) because it processes 3.7M rows with 22+ string corrections. All other staging models default to views.

**Alphanumeric corruption:** The raw HRIS data contains OCR-like corruption (e.g., `"1"` → `"I"`, `"0"` → `"O"`). The Silver model has explicit CASE WHEN blocks to fix these in categorical fields. The Python audit in `data_quality_audits/validate_stg_timesheets.py` independently validates this.

**Correction records:** ~50,908 rows have negative hours (correction entries). These are preserved with a `is_correction` flag in Silver and properly handled in `fct_timesheets`.

### CI/CD Pipelines (`.github/workflows/`)

**`dbt_pipeline.yml`** — triggers on push to `main`, PRs, or manual dispatch:
1. Authenticates to GCP via `GOOGLE_CLOUD_KEYFILE_JSON` secret
2. `dbt seed` → `dbt run` → `dbt test`

**`bronze_ingestion.yml`** — runs daily at 00:00 UTC or manual dispatch:
1. Authenticates to GCP
2. Runs `ingestion/ingest_workforce_bronze.py`
3. Posts Slack webhook alert (via `SLACK_WEBHOOK_URL` secret)

Required GitHub secrets: `GOOGLE_CLOUD_KEYFILE_JSON`, `SLACK_WEBHOOK_URL`, `DBT_PROJECT_ID`, `DBT_DATASET`.

## dbt Packages

- **`dbt-labs/codegen`** — used for generating model YAML boilerplate (dev-time utility)
- **`dbt-labs/dbt_utils`** — used for surrogate keys and generic test macros in mart models

## Data Quality

dbt tests are defined in `.yml` files alongside models:
- `unique` + `not_null` on all primary/surrogate keys
- `accepted_values` for all categorical enums (employment_type, project_type, client_segment, currency, etc.)
- Custom business-rule tests for financial consistency

The standalone Python audit (`data_quality_audits/validate_stg_timesheets.py`) validates a sample of the Silver layer independently of dbt and is not part of the CI pipeline — it's run manually against local samples (excluded from version control via `.gitignore`).
