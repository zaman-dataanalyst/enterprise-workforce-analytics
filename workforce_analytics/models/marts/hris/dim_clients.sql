{{ config(materialized='table') }}

WITH unique_clients AS (
    SELECT
        client_id,
        client_name,
        client_industry,
        client_segment,
        client_size,
        client_country,
        client_region,
        ROW_NUMBER()
            OVER (PARTITION BY client_id ORDER BY work_date DESC)
            AS latest_rank
    FROM {{ ref('stg_hris__timesheets') }}
    WHERE client_id IS NOT NULL
)

SELECT
    client_id AS client_key,
    client_name,
    client_industry,
    client_segment,
    client_size,
    client_country,
    client_region
FROM unique_clients
WHERE latest_rank = 1
