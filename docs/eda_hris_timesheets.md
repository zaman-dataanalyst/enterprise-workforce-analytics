# Exploratory Data Analysis (EDA) – Workforce Timesheets

*Note: In a standard enterprise environment, this documentation is maintained on internal platforms such as Confluence. A snapshot is included below for reference, followed by a complete searchable version for repository indexing.*

---

### 🏢 Enterprise Documentation (Confluence Snapshot)
![Confluence EDA Snapshot](./assets/confluence_eda_snapshot.PNG)

---

## Executive Objective
The objective of this analysis is to assess the data quality of the Bronze layer table (`raw_hris_timesheets_v1`) prior to transformation into the Silver layer using dbt.

This assessment focuses on validating structural integrity, completeness, consistency, and business rule alignment to ensure the dataset is reliable for downstream analytics and reporting.

---

## Data Profile

| Dimension | Value |
|-----------|-------|
| Total Records | 3,750,002 |
| Total Columns | 63 |
| Date Range | 2023-05-08 → 2026-05-08 |
| Distinct Employees | 5,000 |
| Distinct Projects | 401 |
| Distinct Clients | 174 |
| Countries | 9 |
| Load Batches | 1 |
| Duplicate Primary Keys | 0 |
| NULL in Critical Fields | 0 |

---

## Data Quality Scorecard

| Category | Status | Finding |
|----------|--------|---------|
| Primary Key Integrity | ✅ PASS | Zero duplicate timesheet_id values across 3.75M rows |
| Critical Field NULLs | ✅ PASS | emp_id, work_date, hours_worked, entry_type — fully populated |
| Hours Math | ✅ PASS | billable + non_billable = hours_worked — 0 violations |
| Profit Math | ✅ PASS | profit = revenue - cost — 0 violations |
| Correction Integrity | ✅ PASS | All 51,534 correction rows reference a valid original record |
| Flag Consistency | ✅ PASS | entry_type and is_correction align perfectly — 0 mismatches |
| Utilization Scale | ✅ PASS | Confirmed 0.0–1.0 decimal scale — no *100 required |
| String Whitespace | ❌ FAIL | 311k–328k rows per column contain hidden leading/trailing spaces |
| Alphanumeric Corruption | ❌ FAIL | Digit 1 replacing letters — 188,698 names, 296,716 departments affected |
| Categorical Fragmentation | ❌ FAIL | 20+ columns have severe casing and aliasing issues |
| Geolocation Typos | ❌ FAIL | Lahora (110k), Bangalora (78k), Duba1 (33k), Naw York (23k) |
| Currency Variants | ❌ FAIL | 23+ variants including symbols |
| Dimension NULLs | ⚠️ WARN | ~187k NULLs per soft dimension column — COALESCE required |
| Skill/Role Mismatch | ⚠️ WARN | 82.66% of rows show designation vs skill_primary conflict — is_skill_mismatch flag created |

---

## Key Findings

### Finding 1 — Systemic Alphanumeric Corruption
The digit '1' has systematically replaced letters across string columns — a known OCR/transcription bug in the source HRIS system.

- Employee names affected: 188,698 rows (e.g., `M1chael Sm1th`)
- Departments affected: 296,716 rows (e.g., `Data Eng1naer1ng`)
- Designations affected: 185,033 rows (e.g., `QA Eng1naer`)
- Cities affected: 69,332 rows (e.g., `Duba1`, `S1ngapora`)

**Decision:** Apply `REGEXP_REPLACE(col, r'1', 'i')` globally. `project_name` is exempt — it contains valid numeric strings (e.g., `24/7 Implementation`).

---

### Finding 2 — Correction Records (Negative Values)
51,534 records (1.37%) contain negative hours and negative financials.

- All 51,534 map strictly to `entry_type = 'correction'` and `data_quality_flag = 'CORRECTED'`
- Every correction references a valid original `timesheet_id` — zero orphan corrections
- Hours range: -8.0 to -0.5

**Decision:** Retain all correction records. Required for accurate P&L netting in Gold layer.

---

### Finding 3 — Zero-Hour Entries
1,137,573 rows (30.3%) contain 0.0 working hours.

- These represent weekends, public holidays, and bench placeholders
- All math validations pass — billable + non-billable = 0 = hours_worked

**Decision:** Retain in Silver. Apply conditional exclusion logic in Gold layer utilization calculations.

---

### Finding 4 — Categorical Fragmentation
20+ columns contain severe casing, aliasing, and variant issues requiring standardization.

| Column | Variants | Example |
|--------|----------|---------|
| `salary_currency` | 23+ | PKR, Rs, pkr, ₹ |
| `employment_type` | 7 | Full-time, FTE, ft, CWR |
| `project_status` | 10 | Active, active, ACT, On Hold, Paused |
| `project_priority` | 12 | Critical, P0, High, P1 |
| `project_type` | 9 | T&M, t&m, Time & Material, F.P |

**Decision:** Apply exhaustive `CASE WHEN` mapping in Silver layer for all affected columns.

---

### Finding 5 — Skill/Designation Mismatch
82.66% of valid rows (2,945,020 of 3,562,761) show a conflict between `designation` and `skill_primary`.

- Example: Employee with `designation = 'Data Engineer'` has `skill_primary = 'QA Engineer'`
- Pattern is systemic across thousands of employees

**Decision:** Pass through as-is. `is_skill_mismatch` boolean flag created in Silver for downstream workforce analysis.

---

## Transformation Decisions (Silver Layer Blueprint)

| Priority | Issue | Action Taken |
|----------|-------|-------------|
| P1 | Alphanumeric corruption | `REGEXP_REPLACE(col, r'1', 'i')` on all string columns except `project_name` |
| P1 | Whitespace infection | Universal `TRIM()` on all string columns |
| P1 | Categorical fragmentation | Exhaustive `CASE WHEN` mapping — UPPER/LOWER normalization |
| P1 | Geolocation typos | Hardcoded `CASE WHEN` overrides for known city variants |
| P1 | Currency symbols | Map to 3-letter ISO codes via `CASE WHEN` |
| P2 | Dimension NULLs | `COALESCE(col, 'UNKNOWN')` on all soft dimension columns |
| P2 | Correction records | Retained as-is — required for P&L netting |
| P2 | Zero-hour entries | Retained in Silver — conditional exclusion in Gold layer |
| P3 | Skill/role mismatch | `is_skill_mismatch` flag added — HR escalation in progress |

---

## Verdict

The Bronze layer dataset is approved for Silver transformation.

All P1 deterministic fixes have been implemented in `stg_hris__timesheets.sql` via dbt. Mathematical integrity is confirmed across all 3,750,002 rows with zero violations. The GitHub Actions pipeline runs on a daily CRON schedule, ensuring continuous Bronze layer refresh.
