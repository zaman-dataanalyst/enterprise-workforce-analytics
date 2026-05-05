/*
================================================================================
AUDIT    : Gold Dimension Entity Completeness
LAYER    : Gold — Post-Load Validation
PROJECT  : Enterprise Workforce Analytics
AUTHOR   : Hafiz Zaman Yaseen
PURPOSE  : Verify all 7 Gold dimensions loaded the correct number of unique
           entities after SCD Type-1 deduplication via ROW_NUMBER().
================================================================================
*/

-- WHY: Duplicate keys in any dimension cause Power BI relationships to fail.
-- IMPACT: Fan-out duplicates inflate revenue/hours by 2x or 3x silently.
-- RESULT: Employees=5000, Projects=401, Clients=196, confirmed post-run.
-- ACTION: If count > expected, check ROW_NUMBER() partition logic in that dim.

SELECT 'Employees' AS dimension, COUNT(*) AS row_count FROM `enterprise-workforce-analytics.gold_layer.dim_employees`
UNION ALL
SELECT 'Projects', COUNT(*) FROM `enterprise-workforce-analytics.gold_layer.dim_projects`
UNION ALL
SELECT 'Clients', COUNT(*) FROM `enterprise-workforce-analytics.gold_layer.dim_clients`
UNION ALL
SELECT 'Locations', COUNT(*) FROM `enterprise-workforce-analytics.gold_layer.dim_locations`
UNION ALL
SELECT 'Date', COUNT(*) FROM `enterprise-workforce-analytics.gold_layer.dim_date`
UNION ALL
SELECT 'Activity Metadata', COUNT(*) FROM `enterprise-workforce-analytics.gold_layer.dim_activity_metadata`
UNION ALL
SELECT 'Skills Bridge', COUNT(*) FROM `enterprise-workforce-analytics.gold_layer.dim_skills_bridge`;