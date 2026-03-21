/*
   Project : Enterprise Workforce & Revenue Analytics
   Author  : Hafiz Zaman Yaseen
   Script  : 02_data_profiling_validation.sql
   Stack   : Python → BigQuery → Power BI
*/


-- Preview (uncomment one at a time)
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.dim_employees`      LIMIT 10;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.dim_projects`       LIMIT 10;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.dim_date`           LIMIT 10;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.dim_exchange_rates` LIMIT 10;
-- SELECT * FROM `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`    LIMIT 10;


-- 1. Date range — expect 2023-01-01 to today
SELECT
    MIN(work_date)                                 AS start_date,
    MAX(work_date)                                 AS end_date,
    DATE_DIFF(MAX(work_date), MIN(work_date), DAY) AS total_days
FROM `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`;


-- 2. Department NULL check — expect ~7% NULL
SELECT
    COALESCE(department, 'MISSING') AS department,
    COUNT(*)                        AS employee_count
FROM `enterprise-workforce-analytics.it_services_simulation.dim_employees`
GROUP BY department
ORDER BY employee_count DESC;


-- 3. Hours validation — expect ~2% negative (correction rows)
SELECT
    COUNT(*)                                            AS total_rows,
    SUM(CASE WHEN hours_worked < 0 THEN 1 ELSE 0 END)  AS negative_count,
    SUM(CASE WHEN hours_worked = 0 THEN 1 ELSE 0 END)  AS zero_count,
    SUM(CASE WHEN hours_worked > 0 THEN 1 ELSE 0 END)  AS positive_count,
    ROUND(MIN(hours_worked), 2)                         AS min_hours,
    ROUND(MAX(hours_worked), 2)                         AS max_hours
FROM `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`;


-- 4. Messy names — expect ~20% rows with whitespace
SELECT
    name,
    LENGTH(name) AS name_length
FROM `enterprise-workforce-analytics.it_services_simulation.dim_employees`
WHERE name LIKE ' %' OR name LIKE '% '
LIMIT 10;


-- 5. Region casing — expect mix of 'Pakistan' and 'PAKISTAN'
SELECT
    region,
    COUNT(*) AS employee_count
FROM `enterprise-workforce-analytics.it_services_simulation.dim_employees`
GROUP BY region
ORDER BY region ASC;


-- 6. Currency distribution — expect ~75% PKR
SELECT
    currency,
    COUNT(*)                                           AS employee_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct,
    ROUND(MIN(local_salary), 2)                        AS min_salary,
    ROUND(MAX(local_salary), 2)                        AS max_salary,
    ROUND(AVG(local_salary), 2)                        AS avg_salary
FROM `enterprise-workforce-analytics.it_services_simulation.dim_employees`
GROUP BY currency
ORDER BY employee_count DESC;


-- 7. FX rates — expect ~39 months × 7 currencies = ~273 rows
SELECT
    currency_code,
    base_currency,
    COUNT(*)               AS monthly_snapshots,
    MIN(fx_month)          AS earliest,
    MAX(fx_month)          AS latest,
    ROUND(MIN(rate_to_usd), 6) AS min_rate,
    ROUND(MAX(rate_to_usd), 6) AS max_rate
FROM `enterprise-workforce-analytics.it_services_simulation.dim_exchange_rates`
GROUP BY currency_code, base_currency
ORDER BY currency_code;


-- 8. Designation casing — expect mix of consultant/MANAGER/Analyst/SENIOR ANALYST
SELECT
    designation,
    COUNT(*) AS employee_count
FROM `enterprise-workforce-analytics.it_services_simulation.dim_employees`
GROUP BY designation
ORDER BY employee_count DESC;


-- 9. Client market distribution — 100 client projects, Bench excluded
SELECT
    client_country,
    COUNT(*)                          AS project_count,
    ROUND(COUNT(*) * 100.0 / 100, 1) AS pct
FROM `enterprise-workforce-analytics.it_services_simulation.dim_projects`
WHERE project_id != -1
GROUP BY client_country
ORDER BY project_count DESC;


-- 10. Bench row — expect exactly 1 row with project_id = -1
SELECT project_id, project_name, client_name, client_country
FROM `enterprise-workforce-analytics.it_services_simulation.dim_projects`
WHERE project_id = -1;


-- 11. Entry type distribution — expect ~2% correction rows
SELECT
    entry_type,
    is_correction,
    COUNT(*)                                           AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct,
    ROUND(SUM(revenue_usd), 2)                        AS total_revenue_usd,
    ROUND(AVG(hours_worked), 2)                       AS avg_hours
FROM `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`
GROUP BY entry_type, is_correction
ORDER BY entry_type;


-- 12. UUID uniqueness — expect duplicate_count = 0
SELECT
    COUNT(*)                             AS total_rows,
    COUNT(DISTINCT timesheet_id)         AS unique_ids,
    COUNT(*) - COUNT(DISTINCT timesheet_id) AS duplicate_count
FROM `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`;


-- 13. Batch traceability — last 10 pipeline runs
SELECT
    batch_id,
    COUNT(*)            AS rows_in_batch,
    MIN(load_timestamp) AS batch_start,
    MAX(load_timestamp) AS batch_end,
    MIN(work_date)      AS work_date_from,
    MAX(work_date)      AS work_date_to
FROM `enterprise-workforce-analytics.it_services_simulation.fact_timesheets`
GROUP BY batch_id
ORDER BY batch_start DESC
LIMIT 10;
