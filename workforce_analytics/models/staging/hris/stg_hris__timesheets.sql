/*
===============================================================================
Layer: Silver (Staging)
Model: stg_hris__timesheets
Purpose: 
  - Standardize column names
  - Cast data types
  - Introduce data quality flags based on EDA findings
===============================================================================
*/

WITH source AS (
    SELECT * 
    FROM {{ source('bronze_layer', 'raw_hris_timesheets_v1') }}
),

renamed_and_casted AS (
    SELECT
        -- Identifiers
        CAST(timesheet_id AS STRING) AS timesheet_id,
        CAST(emp_id AS STRING) AS employee_id,

        -- Dates
        CAST(work_date AS DATE) AS work_date,

        -- Categorical
        CAST(LOWER(TRIM(entry_type)) AS STRING) AS entry_type,

        -- Metrics
        CAST(hours_worked AS FLOAT64) AS hours_worked,
        CAST(revenue_usd AS FLOAT64) AS revenue_usd,
        CAST(profit_usd AS FLOAT64) AS profit_usd,

        -- ================================
        -- DATA QUALITY FLAGS (CRITICAL)
        -- ================================

        CASE 
            WHEN hours_worked < 0 THEN TRUE 
            ELSE FALSE 
        END AS is_correction_record,

        CASE 
            WHEN hours_worked = 0 THEN TRUE 
            ELSE FALSE 
        END AS is_zero_hour_record,

        CASE 
            WHEN hours_worked > 0 THEN TRUE 
            ELSE FALSE 
        END AS is_valid_working_day

    FROM source
)

SELECT * FROM renamed_and_casted