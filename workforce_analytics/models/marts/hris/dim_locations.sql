{{ config(materialized='table') }}

SELECT DISTINCT
    t.location_country,
    t.location_city,
    FARM_FINGERPRINT(
        CONCAT(t.location_country, COALESCE(t.location_city, 'UNKNOWN'))
    ) AS location_key,
    COALESCE(m.market_tier, 'TIER 3') AS market_segment,
    CASE COALESCE(m.market_tier, 'TIER 3')
        WHEN 'TIER 1' THEN 1
        WHEN 'TIER 2' THEN 2
        WHEN 'TIER 3' THEN 3
        ELSE 99
    END AS market_segment_sort,
    CASE t.location_country
        WHEN 'United States'        THEN 'AMER'
        WHEN 'Canada'               THEN 'AMER'
        WHEN 'United Kingdom'       THEN 'EMEA'
        WHEN 'Germany'              THEN 'EMEA'
        WHEN 'Netherlands'          THEN 'EMEA'
        WHEN 'Sweden'               THEN 'EMEA'
        WHEN 'Pakistan'             THEN 'APAC'
        WHEN 'India'                THEN 'APAC'
        WHEN 'Singapore'            THEN 'APAC'
        WHEN 'Australia'            THEN 'APAC'
        WHEN 'United Arab Emirates' THEN 'MEA'
        WHEN 'Saudi Arabia'         THEN 'MEA'
        WHEN 'Qatar'                THEN 'MEA'
        ELSE 'UNKNOWN'
    END AS macro_region
FROM {{ ref('stg_hris__timesheets') }} AS t
LEFT JOIN {{ ref('market_mapping') }} AS m
    ON m.country = t.location_country
