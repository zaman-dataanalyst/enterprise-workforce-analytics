{{ config(materialized='table') }}

WITH monthly_rates AS (
    SELECT
        *,
        AVG(fx_rate) OVER (
            PARTITION BY
                salary_currency,
                EXTRACT(YEAR FROM work_date),
                EXTRACT(MONTH FROM work_date)
        ) AS monthly_avg_cost_fx_rate
    FROM {{ ref('stg_hris__timesheets') }}
)

SELECT
    timesheet_id,
    employee_id AS employee_key,
    project_id AS project_key,
    client_id AS client_key,
    hours_worked,
    billable_hours,

    -- MUST EXACTLY MATCH dim_activity_metadata
    capacity_hours,

    utilization_pct,
    revenue_usd,
    fx_rate AS daily_fx_rate_audit,
    monthly_avg_cost_fx_rate,

    -- FIXED: Safe Burnout Logic
    work_date,

    CAST(FORMAT_DATE('%Y%m%d', work_date) AS INT64) AS date_key,
    FARM_FINGERPRINT(
        CONCAT(location_country, COALESCE(location_city, 'UNKNOWN'))
    ) AS location_key,
    FARM_FINGERPRINT(CONCAT(
        COALESCE(task_category, 'UNKNOWN'),
        COALESCE(revenue_type, 'UNKNOWN'),
        COALESCE(entry_type, 'UNKNOWN'),
        COALESCE(data_quality_flag, 'UNKNOWN'),
        CAST(is_billable AS STRING),
        CAST(is_anomaly AS STRING),     -- ADDED
        CAST(is_correction AS STRING),  -- ADDED
        CAST(is_holiday AS STRING)      -- ADDED
    )) AS activity_key,

    CASE
        -- Protects negative values
        WHEN hours_worked < 0 THEN 'Correction Record'
        WHEN hours_worked > 9 THEN 'Burnout Risk'
        WHEN hours_worked < 4 AND project_id != 'BENCH' THEN 'Under-Utilized'
        ELSE 'Optimal'
    END AS productivity_segment,
    SAFE_DIVIDE(cost_local, NULLIF(monthly_avg_cost_fx_rate, 0))
        AS stable_cost_usd,

    (
        revenue_usd
        - SAFE_DIVIDE(cost_local, NULLIF(monthly_avg_cost_fx_rate, 0))
    ) AS stable_profit_usd,
    COALESCE(
        project_id = 'BENCH'
        AND EXTRACT(DAYOFWEEK FROM work_date) NOT IN (1, 7)
        AND COALESCE(is_holiday, FALSE) = FALSE,
        FALSE
    ) AS is_bench_entry,
    CAST(
        COALESCE(
            project_id = 'BENCH'
            AND EXTRACT(DAYOFWEEK FROM work_date) NOT IN (1, 7)
            AND COALESCE(is_holiday, FALSE) = FALSE,
            FALSE
        ) AS INT64
    ) AS bench_int
FROM monthly_rates
