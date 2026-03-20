/* ====================================================================================================
   Project    : Enterprise Workforce & Revenue Analytics — IT Services Model Simulation
   Author     : Hafiz Zaman Yaseen (Business Data Analyst)
   Script     : 01_schema_setup.sql
   Stack      : Python → BigQuery → Power BI
   Repository : github.com/hafizzamanyaseen/enterprise-workforce-analytics

   Tables
   ──────────────────────────────────────────────────────────────────────────────────────
   dim_employees_source   — Stable source system (generated once, reused on every run)
   dim_projects_source    — Stable source system (generated once, reused on every run)
   dim_employees          — Analytics dimension (loaded from source on each pipeline run)
   dim_projects           — Analytics dimension (loaded from source on each pipeline run)
   dim_date               — Calendar dimension Jan 2023 → Today
   dim_exchange_rates     — Monthly FX snapshots from Open Exchange Rates API
   fact_timesheets        — Bronze raw fact | UUID PK | Partitioned by work_date

   Architecture
   ──────────────────────────────────────────────────────────────────────────────────────
   Kimball Star Schema + Medallion Architecture (Bronze → Silver → Gold)
   Bronze stores all raw rows including zero-hour bench records.
   Gold view applies: WHERE hours_worked != 0 AND is_correction = FALSE

   Usage
   ──────────────────────────────────────────────────────────────────────────────────────
   Step 1 : Run this file (01_schema_setup.sql)
   Step 2 : Run the Python pipeline (python_etl/data_pipeline.py)
   Step 3 : Run 03_silver_cleaning_views.sql
   Step 4 : Run 04_gold_transformation_views.sql

   To reset: uncomment the DROP statements in Section 5 and run first.
==================================================================================================== */


-- ============================================================
-- SECTION 1 — SOURCE TABLES
-- These two tables act as the source system for employee and
-- project master data. The Python pipeline generates them on
-- the first run and reads from them on every subsequent run,
-- so the records stay consistent across all pipeline executions.
-- ============================================================

CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_employees_source`
(
    emp_id        INT64    NOT NULL,
    name          STRING   NOT NULL,
    department    STRING,
    designation   STRING,
    local_salary  FLOAT64,
    currency      STRING,
    region        STRING,
    join_date     DATE
)
OPTIONS(
    description = "Source system for employee master data. Generated once on first pipeline run and reused on all subsequent runs."
);


CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_projects_source`
(
    project_id      INT64   NOT NULL,
    project_name    STRING  NOT NULL,
    client_name     STRING,
    client_country  STRING
)
OPTIONS(
    description = "Source system for project master data. Generated once on first pipeline run and reused on all subsequent runs."
);


-- ============================================================
-- SECTION 2 — DIMENSION TABLES
-- ============================================================


-- dim_employees
-- Loaded fresh from dim_employees_source on every pipeline run.
-- Bronze layer intentionally includes dirty-data anomalies:
--   15% rows: region in UPPERCASE
--   20% rows: name has leading/trailing spaces
--    7% rows: department is NULL
-- These are cleaned in the Silver layer.
CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_employees`
(
    emp_id        INT64    NOT NULL  OPTIONS(description="Unique employee identifier"),
    name          STRING   NOT NULL  OPTIONS(description="Full name — 20% rows have extra whitespace (Bronze anomaly)"),
    department    STRING            OPTIONS(description="Business unit — 7% rows intentionally NULL (Bronze anomaly)"),
    designation   STRING            OPTIONS(description="Job level: consultant | Analyst | MANAGER | SENIOR ANALYST"),
    local_salary  FLOAT64           OPTIONS(description="Monthly salary in employee local currency"),
    currency      STRING            OPTIONS(description="PKR | USD | GBP | AED | EUR | SAR | SGD"),
    region        STRING            OPTIONS(description="Work location — 15% rows in UPPERCASE (Bronze anomaly)"),
    join_date     DATE              OPTIONS(description="Employment start date")
)
OPTIONS(
    description = "Employee dimension. 5,000 employees across 7 regions. Dirty data cleaned in Silver layer."
);


-- dim_projects
-- project_id = -1 is reserved for the Bench (Internal) row.
-- hourly_rate_usd is not stored here — it lives in fact_timesheets
-- so the rate at the time of billing is always the single source of truth.
CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_projects`
(
    project_id      INT64   NOT NULL  OPTIONS(description="Unique project identifier. -1 = Bench (Internal)"),
    project_name    STRING  NOT NULL  OPTIONS(description="Project code — Bronze has leading/trailing spaces (anomaly)"),
    client_name     STRING            OPTIONS(description="Client company name"),
    client_country  STRING            OPTIONS(description="Client billing market. Internal for Bench row")
)
OPTIONS(
    description = "Project dimension. 100 client projects + Bench row (project_id = -1)."
);


-- dim_date
CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_date`
(
    date_key    DATE    NOT NULL  OPTIONS(description="Calendar date — primary join key"),
    year        INT64             OPTIONS(description="Calendar year"),
    quarter     INT64             OPTIONS(description="Quarter: 1 | 2 | 3 | 4"),
    month       INT64             OPTIONS(description="Month number: 1–12"),
    month_name  STRING            OPTIONS(description="January | February | ..."),
    day_of_week STRING            OPTIONS(description="Monday | Tuesday | ..."),
    is_weekend  BOOL              OPTIONS(description="True for Saturday and Sunday")
)
OPTIONS(
    description = "Calendar dimension. Jan 2023 → Today. Refreshed daily via GitHub Actions."
);


-- dim_exchange_rates
-- Monthly FX snapshots sourced from the Open Exchange Rates API.
-- Formula: API returns 1 USD = X local → we store 1 local = 1/X USD
-- Example: 1 USD = 278.5 PKR → rate_to_usd = 0.003591
-- Join key: fact_timesheets.fx_month → dim_exchange_rates.fx_month
-- Loaded incrementally — new months only, never overwritten.
-- LOGICAL PRIMARY KEY: (fx_month, currency_code)
--   Uniqueness guaranteed in the pipeline via drop_duplicates() before load.
CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_exchange_rates`
(
    fx_month       DATE    NOT NULL  OPTIONS(description="First day of month — join key to fact.fx_month"),
    base_currency  STRING  NOT NULL  OPTIONS(description="Always USD"),
    currency_code  STRING  NOT NULL  OPTIONS(description="PKR | GBP | EUR | AED | SAR | SGD | USD"),
    rate_to_usd    FLOAT64           OPTIONS(description="1 unit of currency_code = ? USD")
)
OPTIONS(
    description = "Monthly FX rates. Append-only incremental load. Natural key: (fx_month, currency_code)."
);


-- ============================================================
-- SECTION 3 — FACT TABLE
-- ============================================================


-- fact_timesheets
-- Central Bronze fact table. All rows stored including zero-hour bench records.
-- Gold view handles filtering: WHERE hours_worked != 0 AND is_correction = FALSE
--
-- Grain     : One event per employee per day per entry_type
--             (one original + at most one correction per employee per day)
-- Partition : work_date (DATE column)
-- Cluster   : emp_id, project_id
-- PK        : timesheet_id — UUID generated in the pipeline (uuid.uuid4())
--
-- BigQuery does not enforce PRIMARY KEY or FOREIGN KEY constraints.
-- Logical relationships:
--   emp_id      → dim_employees.emp_id
--   project_id  → dim_projects.project_id
--   work_date   → dim_date.date_key
--   fx_month    → dim_exchange_rates.fx_month
--
-- Business rules enforced upstream in Python:
--   allocation_pct  IN (0, 1]
--   hours_worked    <= 8 per day
--   revenue_usd     = 0 when is_billable = FALSE
CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`
(
    timesheet_id          STRING    NOT NULL  OPTIONS(description="UUID primary key"),
    emp_id                INT64     NOT NULL  OPTIONS(description="FK → dim_employees.emp_id"),
    project_id            INT64     NOT NULL  OPTIONS(description="FK → dim_projects.project_id. -1 = Bench"),
    work_date             DATE      NOT NULL  OPTIONS(description="FK → dim_date.date_key. Partition column."),
    fx_month              DATE      NOT NULL  OPTIONS(description="FK → dim_exchange_rates.fx_month. First day of work_date month."),
    created_at            TIMESTAMP NOT NULL  OPTIONS(description="When the work occurred"),
    hours_worked          FLOAT64   NOT NULL  OPTIONS(description="Includes 0.0 and negative values in Bronze"),
    is_billable           BOOL      NOT NULL  OPTIONS(description="True when billed to client"),
    hourly_rate_usd       FLOAT64   NOT NULL  OPTIONS(description="Rate at time of billing — single source of truth"),
    revenue_usd           FLOAT64   NOT NULL  OPTIONS(description="hours_worked x hourly_rate_usd. 0 if non-billable."),
    allocation_pct        FLOAT64   NOT NULL  OPTIONS(description="0.60 | 0.80 | 1.00"),
    is_anomaly            BOOL      NOT NULL  OPTIONS(description="True when hours_worked < 0"),
    is_weekend_work       BOOL      NOT NULL  OPTIONS(description="True for Saturday and Sunday"),
    is_correction         BOOL      NOT NULL  OPTIONS(description="True for reversal entries"),
    entry_type            STRING    NOT NULL  OPTIONS(description="original | correction"),
    original_timesheet_id STRING             OPTIONS(description="UUID of the row being reversed — NULL for original rows"),
    load_timestamp        TIMESTAMP NOT NULL  OPTIONS(description="Load time — used for Silver deduplication"),
    batch_id              STRING    NOT NULL  OPTIONS(description="Pipeline run ID: RUN-YYYYMMDD-HHMMSS-xxxx")
)
PARTITION BY work_date
CLUSTER BY emp_id, project_id
OPTIONS(
    description = "Bronze raw fact table. All rows including zero-hour. Gold view: WHERE hours_worked != 0 AND is_correction = FALSE."
);


-- ============================================================
-- SECTION 4 — SILVER + GOLD REFERENCE PATTERNS
-- ============================================================

-- Silver deduplication (03_silver_cleaning_views.sql)
-- ─────────────────────────────────────────────────────────
-- CREATE OR REPLACE VIEW silver_timesheets AS
-- SELECT * EXCEPT(row_num)
-- FROM (
--     SELECT *,
--         ROW_NUMBER() OVER (
--             PARTITION BY timesheet_id
--             ORDER BY load_timestamp DESC
--         ) AS row_num
--     FROM fact_timesheets
-- )
-- WHERE row_num = 1;

-- Gold analytics view (04_gold_transformation_views.sql)
-- ─────────────────────────────────────────────────────────
-- CREATE OR REPLACE VIEW gold_timesheets_enriched AS
-- SELECT f.*, e.department, e.region, e.currency,
--        e.local_salary * fx.rate_to_usd AS salary_usd,
--        p.client_country, p.project_name
-- FROM silver_timesheets f
-- JOIN dim_employees      e  ON f.emp_id     = e.emp_id
-- JOIN dim_projects       p  ON f.project_id = p.project_id
-- JOIN dim_exchange_rates fx ON f.fx_month   = fx.fx_month
--                           AND e.currency   = fx.currency_code
-- WHERE f.hours_worked != 0
--   AND f.is_correction = FALSE;


-- ============================================================
-- SECTION 5 — RESET (uncomment and run before re-initializing)
-- ============================================================
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_employees`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_employees_source`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_projects`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_projects_source`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_date`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_exchange_rates`;


-- ============================================================
-- SECTION 6 — VERIFICATION
-- ============================================================
SELECT 'Schema initialized successfully' AS status, CURRENT_TIMESTAMP() AS at;

-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.dim_employees`        LIMIT 0;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.dim_projects`         LIMIT 0;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.dim_date`             LIMIT 0;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.dim_exchange_rates`   LIMIT 0;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`      LIMIT 0;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.dim_employees_source` LIMIT 0;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.dim_projects_source`  LIMIT 0;
