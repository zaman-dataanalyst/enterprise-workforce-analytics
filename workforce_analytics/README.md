# 🏢 Enterprise Workforce Analytics Pipeline

Welcome to the **Enterprise Workforce Analytics** project. This repository demonstrates a complete, end-to-end Data Engineering and Analytics pipeline built to FAANG-level enterprise standards. The project ingests, transforms, and validates HR and Timesheet data to enable scalable Business Intelligence.

---

## 🏗️ Architecture & Tech Stack
- **Data Warehouse:** Google BigQuery
- **Transformation:** dbt (Data Build Tool)
- **Quality & Audit:** Python (Pandas), Regex, dbt expectations
- **Architecture:** Medallion Architecture (Bronze -> Silver -> Gold)

---

## 🥉 Phase 1: Bronze Layer (Ingestion)
Raw HR and timesheet data is ingested into the Bronze layer. This acts as the raw data lake where historical truth is preserved before any transformations are applied.

---

## 🥈 Phase 2: Silver Layer (Staging) & Quality Audit
The raw data is cleaned, standardized, and modeled using **dbt**. After the SQL transformations, an independent Python-based audit is conducted to ensure enterprise-grade data integrity.

### 🔍 Independent Quality Audit (Cross-Engine Validation)
To achieve 100% data reliability, a standalone Python suite (`scripts/validate_stg_timesheets.py`) performs a deterministic scan on a representative sample of the staging data.

**Audit Results (`stg_hris__timesheets`):**
- **Total Records Scanned:** 12,046
- **Alphanumeric Corruption:** 0 Found (Regex Verified)
- **Business Logic Mapping:** 100% Accuracy (Negative Hours vs Corrections)
- **PII Protection:** Data files are air-gapped and excluded from version control via `.gitignore`.

**Terminal Output:**
> [SUCCESS] Independent audit passed. Data integrity verified.
> AUDIT COMPLETE: STAGING LAYER IS PRODUCTION-READY.

---

## 🥇 Phase 3: Gold Layer (Marts & Star Schema)
*(Work in Progress - Building Dimension and Fact tables for PowerBI/Tableau integration...)*