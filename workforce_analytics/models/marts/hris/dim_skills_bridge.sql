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
    s.employee_id AS employee_key,
    s.skill_primary,
    FARM_FINGERPRINT(CONCAT(s.employee_id, s.skill_primary)) AS bridge_key,
    CASE
        WHEN
            LOWER(TRIM(s.skill_primary)) != LOWER(TRIM(s.designation))
            THEN 'Skill Mismatch'
        ELSE 'Aligned'
    END AS skill_alignment_status,
    STRING_AGG(sc.skill_category, ' | ' ORDER BY sc.skill_category) AS skill_categories
FROM unique_skills AS s
LEFT JOIN {{ ref('skill_categories') }} AS sc
    ON sc.skill_name = s.skill_primary
WHERE s.latest_rank = 1
GROUP BY
    s.employee_id,
    s.skill_primary,
    s.designation
