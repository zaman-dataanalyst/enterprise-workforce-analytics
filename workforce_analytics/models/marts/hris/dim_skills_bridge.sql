{{ config(materialized='table') }}

WITH unique_skills AS (
    SELECT
        employee_id,
        skill_primary,
        designation,
        ROW_NUMBER()
            OVER (
                PARTITION BY employee_id, skill_primary ORDER BY work_date DESC
            )
            AS latest_rank
    FROM {{ ref('stg_hris__timesheets') }}
    WHERE employee_id IS NOT NULL AND skill_primary IS NOT NULL
)

SELECT
    employee_id AS employee_key,
    skill_primary,
    FARM_FINGERPRINT(CONCAT(employee_id, skill_primary)) AS bridge_key,
    CASE
        WHEN
            LOWER(TRIM(skill_primary)) != LOWER(TRIM(designation))
            THEN 'Skill Mismatch'
        ELSE 'Aligned'
    END AS skill_alignment_status
FROM unique_skills
WHERE latest_rank = 1
