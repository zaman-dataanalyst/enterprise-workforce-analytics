{{ config(materialized='table') }}

SELECT DISTINCT
    location_country,
    location_city,
    FARM_FINGERPRINT(
        CONCAT(location_country, COALESCE(location_city, 'UNKNOWN'))
    ) AS location_key,
    CASE
        WHEN
            location_country IN (
                'United States',
                'United Kingdom',
                'Canada',
                'Germany',
                'Australia',
                'Sweden',
                'Netherlands',
                'Singapore'
            )
            THEN 'TIER 1'
        WHEN
            location_country IN (
                'United Arab Emirates', 'Saudi Arabia', 'Qatar'
            )
            THEN 'TIER 2'
        ELSE 'TIER 3' -- Pakistan, India will correctly map here
    END AS market_segment
FROM {{ ref('stg_hris__timesheets') }}
