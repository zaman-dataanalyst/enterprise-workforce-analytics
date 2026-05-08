{{ config(materialized='table') }}

SELECT DISTINCT
    t.location_country,
    t.location_city,
    FARM_FINGERPRINT(
        CONCAT(t.location_country, COALESCE(t.location_city, 'UNKNOWN'))
    ) AS location_key,
    COALESCE(m.market_tier, 'TIER 3') AS market_segment
FROM {{ ref('stg_hris__timesheets') }} AS t
LEFT JOIN {{ ref('market_mapping') }} AS m
    ON m.country = t.location_country
