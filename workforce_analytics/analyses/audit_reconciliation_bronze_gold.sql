/*
================================================================================
AUDIT    : Bronze-to-Gold Financial Reconciliation
LAYER    : Gold — Post-Load Validation
PROJECT  : Enterprise Workforce Analytics
AUTHOR   : Hafiz Zaman Yaseen
PURPOSE  : Verify zero data loss during Bronze → Silver → Gold transformation.
           Confirms row count, total hours, and total revenue match exactly.
================================================================================
*/

-- WHY: Any row dropped in transformation = missing employee records or revenue.
-- IMPACT: 1 missing row = incorrect P&L, headcount, or utilization reporting.

WITH bronze AS (
    SELECT 
        COUNT(*) AS total_rows,
        ROUND(SUM(CAST(hours_worked AS FLOAT64)), 2) AS total_hours,
        ROUND(SUM(CAST(revenue_usd AS FLOAT64)), 2) AS total_revenue
    FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
),
gold AS (
    SELECT 
        COUNT(*) AS total_rows,
        ROUND(SUM(hours_worked), 2) AS total_hours,
        ROUND(SUM(revenue_usd), 2) AS total_revenue
    FROM `enterprise-workforce-analytics.gold_layer.fct_timesheets`
)
SELECT 
    b.total_rows AS bronze_rows,
    g.total_rows AS gold_rows,
    (b.total_rows - g.total_rows) AS row_diff,
    b.total_hours AS bronze_hours,
    g.total_hours AS gold_hours,
    ROUND(b.total_hours - g.total_hours,2) AS hour_diff,
    b.total_revenue AS bronze_revenue,
    g.total_revenue AS gold_revenue,
    ROUND(b.total_revenue - g.total_revenue, 2) AS revenue_diff
FROM bronze b CROSS JOIN gold g;
-- RESULT: row_diff=0 | hour_diff=0.00 | revenue_diff=0.00 | Bronze=3,755,002 rows | Total hours=17,602,139.5 | Total revenue=USD 1,999,685,164.40
-- ACTION: If any diff != 0, trace the Silver CTE that caused the drop.