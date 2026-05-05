{{ config(materialized='table') }}

WITH latest_records AS (
    SELECT
        employee_id,
        employee_name,
        manager_id,
        cost_center,
        department,
        designation,
        seniority_level,
        skill_primary,
        employment_status,
        employment_type,
        experience_years,
        location_country,
        location_city,
        salary_local,
        salary_currency AS payroll_currency,
        ROW_NUMBER()
            OVER (
                PARTITION BY employee_id
                ORDER BY work_date DESC, load_timestamp DESC
            )
            AS latest_rank
    FROM {{ ref('stg_hris__timesheets') }}
    WHERE employee_id IS NOT NULL -- FAANG Defensive check
)

SELECT
    employee_id AS employee_key,
    employee_name,
    manager_id,
    cost_center,
    department,
    designation,
    seniority_level,
    skill_primary,
    employment_type,
    location_country,
    location_city,
    salary_local,
    payroll_currency,
    CASE WHEN employment_status = 'ACTIVE' THEN 1 ELSE 0 END AS is_active_flag,
    CASE
        WHEN experience_years < 2 THEN 'Early Career (0-2Y)'
        WHEN experience_years BETWEEN 2 AND 5 THEN 'Mid-Career (2-5Y)'
        ELSE 'Senior (5Y+)'
    END AS experience_bracket
FROM latest_records
WHERE latest_rank = 1
