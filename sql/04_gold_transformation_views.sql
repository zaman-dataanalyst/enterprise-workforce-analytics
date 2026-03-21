/*
   Project : Enterprise Workforce & Revenue Analytics
   Author  : Hafiz Zaman Yaseen
   Script  : 04_gold_transformation_views.sql
   Stack   : Python → BigQuery → Power BI

   Gold layer — business logic on top of Silver.
   Reads only from Silver views, never from Bronze or dim tables directly.
   revenue_usd and hourly_rate_usd are pre-materialized in the pipeline
   and sourced directly from the fact table — not recalculated here.
*/


-- gold_employees
-- Converts local salary to USD using the most recent monthly FX snapshot.
-- Formula: local_salary × rate_to_usd = salary_usd
CREATE OR REPLACE VIEW `enterprise-workforce-analytics.it_services_simulation.gold_employees` AS
SELECT
    e.emp_id,
    e.full_name,
    e.department,
    e.designation_level,
    e.local_salary,
    e.currency,
    e.region,
    e.join_date,
    ROUND(e.local_salary * fx.rate_to_usd, 2) AS salary_usd
FROM `enterprise-workforce-analytics.it_services_simulation.silver_employees` e
LEFT JOIN (
    SELECT currency_code, rate_to_usd
    FROM `enterprise-workforce-analytics.it_services_simulation.silver_exchange_rates`
    WHERE fx_month = (
        SELECT MAX(fx_month)
        FROM `enterprise-workforce-analytics.it_services_simulation.silver_exchange_rates`
    )
) fx ON e.currency = fx.currency_code;


-- gold_timesheets_enriched
-- Analytics-ready grain: one row per employee per working day.
-- Filtered to exclude zero-hour rows and correction entries.
--
-- bench_cost_usd: salary cost incurred when an employee logs non-billable hours.
-- Formula: (monthly_salary_usd / 30 / 8) * hours_worked
-- Applies to all is_billable = FALSE rows (bench and internal non-billable).
--
-- revenue_usd and hourly_rate_usd come directly from the fact table.
-- fx_month is pre-computed in the pipeline as the first day of work_date month.
CREATE OR REPLACE VIEW `enterprise-workforce-analytics.it_services_simulation.gold_timesheets_enriched` AS
SELECT
    t.timesheet_id,
    t.emp_id,
    t.project_id,
    t.work_date,
    t.fx_month,

    e.full_name,
    e.department,
    e.designation_level,
    e.region,
    e.currency,
    e.local_salary,
    ROUND(e.local_salary * fx.rate_to_usd, 2) AS salary_usd,

    p.project_name,
    p.client_country,

    t.hours_worked,
    t.is_billable,
    t.hourly_rate_usd,
    t.revenue_usd,
    t.allocation_pct,

    CASE
        WHEN t.is_billable = FALSE
        THEN ROUND((e.local_salary * fx.rate_to_usd / 30.0 / 8.0) * t.hours_worked, 2)
        ELSE 0.0
    END AS bench_cost_usd,

    t.is_anomaly,
    t.is_weekend_work,
    t.entry_type,
    t.batch_id

FROM `enterprise-workforce-analytics.it_services_simulation.silver_timesheets` t

JOIN `enterprise-workforce-analytics.it_services_simulation.silver_employees` e
    ON t.emp_id = e.emp_id

JOIN `enterprise-workforce-analytics.it_services_simulation.silver_projects` p
    ON t.project_id = p.project_id

JOIN `enterprise-workforce-analytics.it_services_simulation.silver_exchange_rates` fx
    ON  e.currency = fx.currency_code
    AND t.fx_month = fx.fx_month

WHERE t.hours_worked  != 0
  AND t.is_correction = FALSE;

-- Sanity checks (uncomment one at a time to verify):
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.gold_employees`           LIMIT 10;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.gold_timesheets_enriched` LIMIT 10;
-- SELECT SUM(revenue_usd) AS total_revenue, SUM(bench_cost_usd) AS total_bench_cost FROM `enterprise-workforce-analytics.it_services_simulation.gold_timesheets_enriched`;
