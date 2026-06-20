{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT *
    FROM
        UNNEST(
            GENERATE_DATE_ARRAY('2023-01-01', '2026-12-31', INTERVAL 1 DAY)
        ) AS date
)

SELECT
    `date` AS work_date,
    CAST(FORMAT_DATE('%Y%m%d', `date`) AS INT64) AS date_key,
    EXTRACT(YEAR FROM `date`) AS year,
    EXTRACT(QUARTER FROM `date`) AS quarter,
    EXTRACT(MONTH FROM `date`) AS month_num,
    FORMAT_DATE('%B', `date`) AS month_name,
    CAST(FORMAT_DATE('%Y%m', `date`) AS INT64) AS month_year_sort,
    DATE_TRUNC(`date`, MONTH) AS month_start_date,
    DATE_TRUNC(`date`, QUARTER) AS quarter_start_date,
    COALESCE(EXTRACT(DAYOFWEEK FROM `date`) IN (1, 7), FALSE)
        AS is_weekend
FROM date_spine
