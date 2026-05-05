{{ config(materialized='table') }}

SELECT DISTINCT
    task_category,
    revenue_type,
    entry_type,
    data_quality_flag,
    is_billable,
    is_anomaly,
    is_correction,
    is_holiday,
    FARM_FINGERPRINT(CONCAT(
        COALESCE(task_category, 'UNKNOWN'),
        COALESCE(revenue_type, 'UNKNOWN'),
        COALESCE(entry_type, 'UNKNOWN'),
        COALESCE(data_quality_flag, 'UNKNOWN'),
        CAST(is_billable AS STRING),
        CAST(is_anomaly AS STRING),     -- ADDED
        CAST(is_correction AS STRING),  -- ADDED
        CAST(is_holiday AS STRING)      -- ADDED
    )) AS activity_key
FROM {{ ref('stg_hris__timesheets') }}
