{{ config(materialized='table') }}

WITH unique_projects AS (
    SELECT
        project_id,
        project_name,
        project_type,
        project_status,
        project_phase,
        project_priority,
        project_manager_id,
        ROW_NUMBER()
            OVER (PARTITION BY project_id ORDER BY work_date DESC)
            AS latest_rank
    FROM {{ ref('stg_hris__timesheets') }}
    WHERE project_id IS NOT NULL
)

SELECT
    project_id AS project_key,
    project_name,
    project_type,
    project_status,
    project_phase,
    project_priority,
    project_manager_id
FROM unique_projects
WHERE latest_rank = 1
