# 🏢 Enterprise Workforce Analytics Pipeline

Welcome to the **Enterprise Workforce Analytics** dbt project — the transformation engine powering a 3.75M-row HR analytics warehouse on Google BigQuery.

---

## 🏗️ Architecture & Tech Stack

- **Data Warehouse:** Google BigQuery
- **Transformation:** dbt (Data Build Tool)
- **Quality & Audit:** Python (Pandas), Regex, dbt tests
- **Architecture:** Medallion Architecture (Bronze → Silver → Gold)
- **CI/CD:** GitHub Actions (automated daily pipeline)
- **Linting:** SQLFluff

---

## 🥉 Phase 1: Bronze Layer (Ingestion)

Raw HR and timesheet data is ingested into the Bronze layer via an automated GitHub Actions pipeline running on a daily CRON schedule. This layer acts as the raw data lake where historical truth is preserved before any transformations are applied.

**Scale:** 3,750,002 rows | 5,000 employees | 401 projects | 174 clients | 9 countries | 2023-05-08 → 2026-05-08

---

## 🔍 Phase 2: Pre-Silver EDA (Exploratory Data Analysis)

Before writing a single transformation, a comprehensive EDA was conducted against the raw Bronze table to profile every data quality issue deterministically.

**Key findings across 3,750,002 rows:**

- **Alphanumeric corruption:** 188,698 employee names infected with digit substitution (e.g., `M1chael Sm1th`)
- **Whitespace infection:** 311k–328k rows per column contain hidden leading/trailing spaces
- **Categorical fragmentation:** 20+ columns with severe casing and aliasing issues
- **Geolocation typos:** `Lahora`, `Bangalora`, `Duba1`, `Naw York`
- **Currency variants:** 23+ formats including symbols (`₹`, `£`, `€`, `Rs`)
- **Correction records:** 51,534 valid negative-hour financial offsets confirmed
- **Math violations:** 0 across all financial columns

Full EDA documented in [`docs/eda_hris_timesheets.md`](../docs/eda_hris_timesheets.md)

---

## 🥈 Phase 3: Silver Layer (Staging) & Quality Audit

The raw data is cleaned, standardized, and modeled using **dbt** — based entirely on EDA findings. After the SQL transformations, an independent Python-based audit is conducted to ensure enterprise-grade data integrity.

### 🔍 Independent Quality Audit (Cross-Engine Validation)

To achieve 100% data reliability, a standalone Python suite (`data_quality_audits/validate_stg_timesheets.py`) performs a deterministic scan against the staging layer.

**Audit Results (`stg_hris__timesheets`):**

- **Total Records Validated:** 3,750,002
- **Alphanumeric Corruption:** 0 Found (Regex Verified)
- **Business Logic Mapping:** 100% Accuracy (Negative Hours vs Corrections)
- **Math Violations:** 0 (`billable + non_billable = hours_worked` across all rows)
- **PII Protection:** Data files are air-gapped and excluded from version control via `.gitignore`

**Terminal Output:**
> [SUCCESS] Independent audit passed. Data integrity verified.
> AUDIT COMPLETE: STAGING LAYER IS PRODUCTION-READY.

---

## 🥇 Phase 4: Gold Layer (Marts & Star Schema)

The Gold layer implements a **Kimball-style star schema** optimised for Power BI DirectQuery and Import modes.

### Dimension Tables

| Model | Grain | Key |
|-------|-------|-----|
| `dim_employees` | One row per employee | `employee_key` |
| `dim_clients` | One row per client | `client_key` |
| `dim_projects` | One row per project | `project_key` |
| `dim_locations` | One row per country–city pair | `location_key` |
| `dim_date` | One row per calendar date | `date_key` |
| `dim_activity_metadata` | One row per activity-type combination | `activity_key` |
| `dim_skills_bridge` | One row per employee–skill pair | `bridge_key` |

### Fact Table

`fct_timesheets` — central grain: one row per timesheet entry. Holds financial metrics (`revenue_usd`, `cost_usd`, `profit_usd`, `stable_cost_usd`, `stable_profit_usd`), utilisation KPIs, surrogate foreign keys to all dimensions, and a `productivity_segment` derived column.

Surrogate keys use `FARM_FINGERPRINT` (native BigQuery) for deterministic, collision-resistant hashing without an external dependency.

### 🧪 Data Tests

49 dbt tests run on every pipeline execution — covering uniqueness, not_null, accepted_values, and referential integrity across all Gold layer dimensions and facts.

**Result: PASS=49 | WARN=0 | ERROR=0**

---

## ⚖️ Phase 5: Enterprise Data Reconciliation Auditing

Data integrity is mathematically proven post-transformation through two independent SQL audit scripts.

### Bronze-to-Gold Financial Reconciliation

[`analyses/audit_reconciliation_bronze_gold.sql`](analyses/audit_reconciliation_bronze_gold.sql) confirms zero data loss across the full pipeline:

| Metric | Result |
|--------|--------|
| row_diff | 0 |
| hour_diff | 0.00 |
| revenue_diff | 0.00 |
| Total Revenue | USD 1,999,685,164.40 |
| Total Rows | 3,755,002 |

### Dimension Completeness Audit

[`analyses/audit_dimension_completeness.sql`](analyses/audit_dimension_completeness.sql) verifies exact entity counts after SCD Type-1 deduplication:

| Dimension | Count |
|-----------|-------|
| Employees | 5,000 |
| Projects | 401 |
| Clients | 174 |
| Locations | 22 |
| Date | 1,461 |
| Activity Metadata | 11 |
| Skills Bridge | 7,006 |

---

## 🌱 Seeds (Reference Data)

| File | Purpose |
|------|---------|
| `market_mapping.csv` | Country → Market Tier mapping (TIER 1/2/3) |
| `skill_categories.csv` | Role → Skill category mapping for `dim_skills_bridge` |

---

## ⚙️ How to Run

```bash
# Load reference seeds
dbt seed

# Build Silver + Gold layers
dbt run

# Validate all 49 tests
dbt test
```