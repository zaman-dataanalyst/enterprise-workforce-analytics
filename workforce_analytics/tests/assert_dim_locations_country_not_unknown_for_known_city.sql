-- Singular test: fails when location_country = 'UNKNOWN' but location_city
-- carries a real value. This combination means Silver's country CASE missed a
-- source variant — the city is identifiable but the country couldn't be resolved.
-- A row with both fields = 'UNKNOWN' is acceptable (genuinely unresolvable source).
SELECT
    location_country,
    location_city,
    COUNT(*) AS row_count
FROM {{ ref('dim_locations') }}
WHERE location_country = 'UNKNOWN'
  AND location_city    != 'UNKNOWN'
  AND location_city    IS NOT NULL
GROUP BY 1, 2
