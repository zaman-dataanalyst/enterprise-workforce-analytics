/* 
   Project : Enterprise Workforce & Revenue Analytics
   Author  : Hafiz Zaman Yaseen
   Script  : 01_schema_setup.sql
   Stack   : Python → BigQuery → Power BI
 */


-- Source tables: generated once on first pipeline run, reused on every subsequent run

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
);

CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_projects_source`
(
    project_id      INT64   NOT NULL,
    project_name    STRING  NOT NULL,
    client_name     STRING,
    client_country  STRING
);


-- Dimension tables

CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_employees`
(
    emp_id        INT64    NOT NULL,
    name          STRING   NOT NULL,
    department    STRING,               -- 7% rows intentionally NULL (Bronze anomaly)
    designation   STRING,
    local_salary  FLOAT64,
    currency      STRING,               -- PKR | USD | GBP | AED | EUR | SAR | SGD
    region        STRING,               -- 15% rows in UPPERCASE (Bronze anomaly)
    join_date     DATE
);

CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_projects`
(
    project_id      INT64   NOT NULL,   -- -1 reserved for Bench (Internal)
    project_name    STRING  NOT NULL,
    client_name     STRING,
    client_country  STRING
);

CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_date`
(
    date_key    DATE    NOT NULL,
    year        INT64,
    quarter     INT64,
    month       INT64,
    month_name  STRING,
    day_of_week STRING,
    is_weekend  BOOL
);

CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.dim_exchange_rates`
(
    fx_month       DATE    NOT NULL,    -- First day of month, e.g. 2024-01-01
    base_currency  STRING  NOT NULL,    -- Always USD
    currency_code  STRING  NOT NULL,    -- PKR | GBP | EUR | AED | SAR | SGD | USD
    rate_to_usd    FLOAT64             -- 1 unit of currency_code = ? USD
);


-- Fact table

CREATE OR REPLACE TABLE `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`
(
    timesheet_id          STRING    NOT NULL,   -- UUID primary key
    emp_id                INT64     NOT NULL,
    project_id            INT64     NOT NULL,   -- -1 = Bench
    work_date             DATE      NOT NULL,   -- Partition column
    fx_month              DATE      NOT NULL,   -- Join key to dim_exchange_rates
    created_at            TIMESTAMP NOT NULL,
    hours_worked          FLOAT64   NOT NULL,
    is_billable           BOOL      NOT NULL,
    hourly_rate_usd       FLOAT64   NOT NULL,
    revenue_usd           FLOAT64   NOT NULL,
    allocation_pct        FLOAT64   NOT NULL,
    is_anomaly            BOOL      NOT NULL,
    is_weekend_work       BOOL      NOT NULL,
    is_correction         BOOL      NOT NULL,
    entry_type            STRING    NOT NULL,   -- original | correction
    original_timesheet_id STRING,
    load_timestamp        TIMESTAMP NOT NULL,
    batch_id              STRING    NOT NULL
)
PARTITION BY work_date
CLUSTER BY emp_id, project_id;


-- To reset, uncomment and run:
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_employees`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_employees_source`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_projects`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_projects_source`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_date`;
-- DROP TABLE IF EXISTS `enterprise-workforce-analytics.it_services_simulation.dim_exchange_rates`;
