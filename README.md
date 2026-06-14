<div align="center">

# Enterprise Workforce Analytics
### Production-Grade Data Pipeline for IT Staff Augmentation Margin Intelligence

**Modern Data Stack** · Python → BigQuery → dbt → Power BI · 3.75M rows · End-to-end ETL with live FX API

[![Pipeline](https://img.shields.io/badge/Pipeline-Automated_CI%2FCD-4ECB71?style=flat-square)](#architecture)
[![dbt](https://img.shields.io/badge/dbt-50_tests_passing-F5A623?style=flat-square)](#data-quality--provable-not-promised)
[![BigQuery](https://img.shields.io/badge/BigQuery-Medallion_Architecture-45B7D1?style=flat-square)](#architecture)
[![Power BI](https://img.shields.io/badge/Power_BI-Import_Mode-F5A623?style=flat-square)](#the-dashboard)
[![Modeling](https://img.shields.io/badge/Modeling-Kimball_Star_Schema-A8C4D8?style=flat-square)](#data-model--kimball-star-schema)

</div>

---

## 30-Second Summary (for recruiters)

I built a **fully automated analytics platform** that solves a real margin-leakage problem for IT staff-augmentation firms. It ingests **3.75 million timesheet records** through a nightly **GitHub Actions** pipeline that calls a **live currency API**, transforms them through a **Medallion architecture (Bronze → Silver → Gold)** on **BigQuery** using **dbt** (50 tests + an independent audit suite proving zero data loss), and serves a **Kimball star schema** to **Power BI** — including a custom **scikit-learn Logistic Regression** model that predicts which roles drive bench-cost risk.

This is not a CSV-and-a-pie-chart project. It is an **engineering-grade data product**: version-controlled, tested, CI/CD-automated, and documented like a system that has to survive in production.

| | |
|---|---|
| **Scale** | 3,750,002 rows · 63 raw columns · 5,000 employees · 9 countries · 3-year span |
| **Stack** | Python · OpenExchangeRates API · GitHub Actions · BigQuery · dbt 1.11 · Power BI |
| **Architecture** | Medallion (Bronze/Silver/Gold) + Kimball Star Schema (7 dims + 1 fact) |
| **Quality** | 50 dbt tests · independent Python audit (row / hour / revenue diff = 0) |
| **ML** | scikit-learn Logistic Regression — bench-risk drivers, AUC-validated |
| **Status** | Pipeline ✅ Complete · Dashboard 🚧 In Progress (Pages 1–2 done, Page 3 partial) |

---

## The Business Problem

IT Staff Augmentation firms bill international clients in **USD** but pay engineers in **local currency (PKR)**. Three silent forces erode margin — and traditional Excel reporting surfaces them 30 days too late, after the money is already lost.

| # | Problem | Why it hurts |
|---|---------|--------------|
| **1** | **Bench Cost Leakage** | When an engineer isn't on a client project, the firm pays their salary for **zero revenue**. Across 5,000 employees, finding *who* is benched and *for how long* in real time was impossible. |
| **2** | **FX Volatility** | Revenue is in USD, salaries in PKR. Every PKR depreciation (repeatedly in 2023–24) silently shrinks margin **with no operational change**. This risk was invisible in existing reports. |
| **3** | **30-Day Reporting Lag** | Excel pipelines delivered insight a month late. Decisions ran on stale data; by the time a bench spike was visible, the loss had already occurred. |

**The mandate:** turn a 30-day, manual, blind process into a **daily, automated, predictive** one.

---

## The Solution — A System, Not a Spreadsheet

```
┌──────────────┐   nightly    ┌───────────────────────────────┐   dbt    ┌──────────────┐
│   Python     │   CI/CD      │          BigQuery             │  build   │   Power BI   │
│  ingestion   │ ───────────▶ │  Bronze → Silver → Gold       │ ───────▶ │  Import Mode │
│ + OXR FX API │              │  (Medallion + Star Schema)    │          │  Dashboards  │
└──────────────┘              └───────────────────────────────┘          └──────────────┘
       │                                   │                                     │
   live FX rate                     50 dbt tests +                       Logistic Regression
   per record                    independent audit suite                 bench-risk model
```

Every night, **GitHub Actions** runs the ingestion script, which fetches the **actual historical USD/PKR rate for each day** from the **OpenExchangeRates API** — so currency conversion reflects reality, not an assumption. A chained `workflow_run` then triggers the **dbt build** automatically. Zero manual intervention, end to end.

---

## Architecture

### Bronze — Raw, Immutable Landing
Raw timesheet records land untouched in `bronze_layer.raw_hris_timesheets_v1`. **Nothing is cleaned here** — raw data stays immutable for audit and lineage (a core principle real data teams enforce).

### Silver — Cleansing & Standardization &nbsp;`dbt · view`
The raw data is intentionally *messy* (simulating a real HRIS export): 310K+ whitespace rows, 187K+ corrupt names, 205K+ corrupt designations, 291K non-standard currency codes, 277K city typos. A disciplined **4-CTE pipeline** repairs it:

1. `trim_nullify` — strip whitespace; convert `'null'` / `''` artifacts to true `NULL`
2. `fix_text_corruption` — targeted `REGEXP_REPLACE` / `REPLACE` for known corruption patterns
3. `standardize_categoricals` — `CASE WHEN` normalization (employment type, region, currency)
4. final `SELECT` — derived flags + computed columns

**Engineering judgment encoded as strict rules** — this is what separates an engineer from a query-writer:
- `original_timesheet_id` → **never imputed** — a `NULL` is a factual state (self-referencing FK), not missing data
- `project_name` → **no digit-to-letter regex** — values like `'24/7'` and `'3rdGeneration'` are valid
- Negative hours → **kept** — valid correction records for Gold-layer netting
- `utilization_pct` → confirmed 0–1 decimal scale; **not** multiplied by 100

### Gold — Kimball Star Schema &nbsp;`dbt · tables`
Business-ready dimensional model: **7 dimensions + 1 fact table**. Engineering highlights:
- `FARM_FINGERPRINT` surrogate keys for stable, deterministic joins
- `is_bench_entry` defined as a **working-day idle event only** — weekends and holidays are explicitly excluded, because a benched engineer is an *unbilled working day*, not a weekend. This single business-logic decision is what makes the bench metric trustworthy.
- `stable_cost_usd` computed against a **monthly-average FX rate** (window function), isolating operational cost from daily currency noise
- `productivity_segment` derived (Burnout / Under-Utilized / Correction / Optimal)

---

## Data Model — Kimball Star Schema

![Star Schema — Power BI Model View](docs/assets/model_view_erd.png)

A clean **star schema**: `fct_timesheets` at the center, surrounded by 7 conformed dimensions with enforced **single-direction filtering** and correct cardinality. This is the part most self-taught analysts skip — and the part that makes a model performant and trustworthy at 3.75M rows.

---

## Data Quality — Provable, Not Promised

Quality is **enforced on every run**, not assumed:

- **50 dbt tests** (uniqueness, not-null, referential integrity, accepted values) run on every build
- An **independent Python audit suite** (`data_quality_audits/`) reconciles Bronze → Gold and proves:

```
row_diff      = 0
hour_diff     = 0
revenue_diff  = 0      across $1,999,685,164 total revenue
```

Zero data loss through the entire transformation — verified by a second system that doesn't trust the first.

---

## The Dashboard

Three-page Power BI report following the **Descriptive → Diagnostic → Predictive** analytics maturity arc.

### Page 1 — CEO Executive View &nbsp;*(Descriptive)*
![Page 1 — CEO Executive View](docs/assets/page1_ceo.png)

Real-time KPIs (Total Revenue, Bench Cost, Utilization %, Active Headcount) each with **MoM and YoY variance** and RAG conditional formatting; revenue-vs-bench 36-month trend; revenue by region and department.

### Page 2 — Operations Root Cause Analysis &nbsp;*(Diagnostic)*
![Page 2 — Operations](docs/assets/page2_operations.png)

**AI Decomposition Tree** drilling bench cost from Market Tier → Department → Designation → Seniority; **FX volatility** chart correlating the PKR/USD drop directly to revenue loss; employee performance matrix with RAG formatting; revenue **anomaly detection** flagging the modeled 2024 devaluation crash.

### Page 3 — Predictive Risk & Forecast &nbsp;*(Predictive)* 🚧
![Page 3 — Predictive](docs/assets/page3_predictive.png)

- **Bench-Risk Drivers — Logistic Regression:** a custom **scikit-learn** model (frequency-weighted, **AUC-validated**) embedded as a Power BI Python visual. It quantifies, in log-odds, which roles and seniorities *increase* vs *decrease* bench risk — QA Engineers and Juniors raise risk; Data Scientists and Leads lower it. A genuine classification model, not a `GROUP BY` relabeled as "ML".
- **90-Day Revenue Forecast:** native ETS time-series forecast with a 95% confidence band and a dynamic "forecast start" marker.
- **In development:** cost-vs-revenue risk scatter, at-risk employee table, utilization ribbon, and an sklearn revenue-regression visual.

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| **Ingestion** | Python · OpenExchangeRates API · GitHub Actions (nightly CI/CD) |
| **Warehouse** | Google BigQuery — Medallion: Bronze / Silver / Gold |
| **Transformation** | dbt 1.11 · `dbt_utils` · `codegen` · 50 tests · seeds |
| **Modeling** | Kimball star schema · `FARM_FINGERPRINT` surrogate keys |
| **Visualization** | Power BI (Import Mode) · DAX · Python visuals (scikit-learn, matplotlib) |
| **Quality** | dbt tests + independent Python reconciliation audit |

---

## Repository Structure

```
enterprise-workforce-analytics/
├── ingestion/
│   └── ingest_workforce_bronze.py      # Python + live OXR FX API → Bronze
├── workforce_analytics/                # dbt project
│   ├── models/
│   │   ├── staging/hris/               # Silver: 4-CTE cleansing pipeline
│   │   └── marts/hris/                 # Gold: 7 dims + fct_timesheets
│   ├── seeds/                          # market_mapping, skill_categories
│   └── ...
├── data_quality_audits/
│   └── validate_stg_timesheets.py      # independent Bronze→Gold reconciliation
├── docs/
│   ├── eda_hris_timesheets.md          # exploratory data analysis
│   └── assets/                         # dashboard + ERD screenshots
└── README.md
```

---

## What This Project Demonstrates

Built to prove I can own a data product end-to-end, the way a data / analytics engineer is expected to:

- **Data Engineering** — automated ingestion, live-API integration, idempotent pipeline design
- **Modern Data Stack fluency** — BigQuery + dbt + Medallion + CI/CD, the stack used at scale today
- **Dimensional modeling** — a correct Kimball star schema with surrogate keys and conformed dimensions
- **Data quality discipline** — tested transformations *and* an independent audit that proves zero loss
- **Business translation** — every technical choice ties back to a margin-leakage dollar
- **Applied ML** — a real, validated classification model answering a business question, presented honestly

> **Project status:** Pipeline complete and running daily. Dashboard build in progress — Pages 1–2 complete, Page 3 partially built (Logistic Regression + Forecast live; remaining predictive visuals in development).

---

<div align="center">

**Hafiz Zaman Yaseen** — Data Analyst / Analytics Engineer
*Modern Data Stack · SQL · dbt · BigQuery · Power BI · Kimball & Medallion Architectures*

</div>
