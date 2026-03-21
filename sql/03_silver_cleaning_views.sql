/*
   Project : Enterprise Workforce & Revenue Analytics
   Author  : Hafiz Zaman Yaseen
   Script  : 03_silver_cleaning_views.sql
   Stack   : Python → BigQuery → Power BI

   Silver layer — structural cleaning and deduplication only.
   No business logic, no metric calculation, no filtering.
   Business rules live exclusively in the Gold layer.
*/


-- silver_employees
-- Fixes: whitespace in names, UPPERCASE regions, UPPERCASE designations, NULL departments
CREATE OR REPLACE VIEW `enterprise-workforce-analytics.it_services_simulation.silver_employees` AS
SELECT
    emp_id,
    TRIM(INITCAP(name))                AS full_name,
    COALESCE(department, 'Unassigned') AS department,
    INITCAP(TRIM(LOWER(designation)))  AS designation_level,
    local_salary,
    currency,
    INITCAP(TRIM(LOWER(region)))       AS region,
    join_date
FROM `enterprise-workforce-analytics.it_services_simulation.dim_employees`;


-- silver_projects
-- Fixes: leading/trailing spaces in project_name and client_name
CREATE OR REPLACE VIEW `enterprise-workforce-analytics.it_services_simulation.silver_projects` AS
SELECT
    project_id,
    TRIM(project_name) AS project_name,
    TRIM(client_name)  AS client_name,
    client_country
FROM `enterprise-workforce-analytics.it_services_simulation.dim_projects`;


-- silver_timesheets
-- Deduplicates fact rows on timesheet_id using load_timestamp.
-- All columns passed through as-is — no values are modified.
-- Negative hours on correction rows are intentional and preserved.
-- Zero-hour rows are retained here — Gold applies: WHERE hours_worked != 0
CREATE OR REPLACE VIEW `enterprise-workforce-analytics.it_services_simulation.silver_timesheets` AS
SELECT * EXCEPT(row_num)
FROM (
    SELECT
        timesheet_id,
        emp_id,
        project_id,
        work_date,
        fx_month,
        created_at,
        hours_worked,
        is_billable,
        hourly_rate_usd,
        revenue_usd,
        allocation_pct,
        is_anomaly,
        is_weekend_work,
        is_correction,
        entry_type,
        original_timesheet_id,
        load_timestamp,
        batch_id,
        ROW_NUMBER() OVER (
            PARTITION BY timesheet_id
            ORDER BY load_timestamp DESC
        ) AS row_num
    FROM `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`
)
WHERE row_num = 1;


-- silver_exchange_rates
-- Pass-through view — FX data is already validated and deduplicated in the pipeline
CREATE OR REPLACE VIEW `enterprise-workforce-analytics.it_services_simulation.silver_exchange_rates` AS
SELECT
    fx_month,
    base_currency,
    currency_code,
    rate_to_usd
FROM `enterprise-workforce-analytics.it_services_simulation.dim_exchange_rates`;


-- Sanity checks (uncomment one at a time):
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.silver_employees`      LIMIT 10;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.silver_projects`       LIMIT 10;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.silver_timesheets`     LIMIT 10;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.silver_exchange_rates` LIMIT 10;
