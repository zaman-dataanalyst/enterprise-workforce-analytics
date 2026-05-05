/*
================================================================================
PROJECT  : Enterprise Workforce Analytics
TABLE    : enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1
LAYER    : Pre-Silver — Complete Data Quality EDA
AUTHOR   : Hafiz Zaman Yaseen
PURPOSE  : Identify every category of data problem BEFORE writing stg.sql.
           Structure per query: 
           [TOP]    WHY and IMPACT (The Hypothesis)
           [QUERY]  Execution
           [BOTTOM] RESULT and ACTION (The Evidence & Next Steps)
TOTAL COLUMNS : 63 | TOTAL ROWS (full table): ~3.7M
================================================================================
*/


-- ============================================================================
-- SECTION 0: TABLE SNAPSHOT (Run First — Baseline Numbers)
-- ============================================================================

-- 0.1 Row count & table size
-- WHY: Establish a baseline of total volume and distinct entities to ensure no data loss during Staging.
-- IMPACT: If rows are silently dropped in Staging, downstream P&L metrics will be under-reported.
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT timesheet_id) AS distinct_timesheet_ids,
    COUNT(*) - COUNT(DISTINCT timesheet_id) AS duplicate_timesheet_ids,
    COUNT(DISTINCT emp_id) AS distinct_employees,
    COUNT(DISTINCT project_id) AS distinct_projects,
    COUNT(DISTINCT client_id) AS distinct_clients,
    MIN(work_date) AS earliest_work_date,
    MAX(work_date) AS latest_work_date,
    COUNT(DISTINCT batch_id) AS distinct_batches,
    CURRENT_TIMESTAMP() AS eda_run_at
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: Total rows: 3,727,481. Unique timesheet_ids: 3,727,481 (0 duplicates). Employees: 5,000.
-- ACTION: Baseline confirmed. Proceed with 1:1 row mapping in Staging.

-- 0.2 Entry type split (original vs correction records)
-- WHY: Corrections contain negative hours and financials that offset original errors.
-- IMPACT: If corrections are filtered out, the company's total revenue will be artificially inflated.
SELECT
    entry_type,
    is_correction,
    COUNT(*) AS row_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY entry_type, is_correction
ORDER BY row_count DESC;
-- RESULT: 3,676,573 (98.63%) Original rows | 50,908 (1.37%) Correction rows.
-- ACTION: Keep both entry types in Staging. Ensure boolean flags are preserved for Gold layer netting.


-- ============================================================================
-- SECTION 1: NULL & "NULL STRING" ANALYSIS
-- ============================================================================

-- 1.1 True NULL count across ALL columns
-- WHY: Missing dimensional data breaks JOINs and creates orphan records in the Star Schema.
-- IMPACT: Power BI slicers will show "(Blank)" for over 1.8 Lakh rows, ruining the user experience.
SELECT
    'timesheet_id'AS col , COUNTIF(timesheet_id IS NULL) AS true_nulls FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'emp_id', COUNTIF(emp_id IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'manager_id', COUNTIF(manager_id IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'project_id', COUNTIF(project_id IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'client_id', COUNTIF(client_id IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'work_date', COUNTIF(work_date IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'employee_name', COUNTIF(employee_name IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'designation', COUNTIF(designation IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'seniority_level', COUNTIF(seniority_level IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'employment_type', COUNTIF(employment_type IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'employment_status', COUNTIF(employment_status     IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'department', COUNTIF(department IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'skill_primary', COUNTIF(skill_primary IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'location_country', COUNTIF(location_country IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'location_city', COUNTIF(location_city IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'salary_currency', COUNTIF(salary_currency IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'project_name', COUNTIF(project_name IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'project_type', COUNTIF(project_type IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'project_priority', COUNTIF(project_priority IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'project_status', COUNTIF(project_status IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'client_name', COUNTIF(client_name IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'client_industry', COUNTIF(client_industry IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'client_segment', COUNTIF(client_segment IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'client_country', COUNTIF(client_country IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'task_category', COUNTIF(task_category IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` UNION ALL
SELECT 'original_timesheet_id', COUNTIF(original_timesheet_id IS NULL) FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
ORDER BY true_nulls DESC;
-- RESULT: Heavy True NULLs found: client_name (186,941), client_industry (186,913), designation (186,760), location_city (186,462).
-- ACTION: Use COALESCE(col, 'UNKNOWN') for dimension columns in Staging to ensure referential integrity.

-- 1.2 "null" STRING injection — columns where literal text "null" appears
-- WHY: Source systems sometimes export literal text 'null' instead of SQL NULLs.
-- IMPACT: 'null' as text bypasses standard IS NULL checks, causing silent failures in aggregations.
SELECT
    COUNTIF(LOWER(TRIM(employee_name)) = 'null') AS employee_name_nullstr,
    COUNTIF(LOWER(TRIM(designation)) = 'null') AS designation_nullstr,
    COUNTIF(LOWER(TRIM(client_name)) = 'null') AS client_name_nullstr,
    COUNTIF(LOWER(TRIM(client_industry)) = 'null') AS client_industry_nullstr,
    COUNTIF(LOWER(TRIM(project_name)) = 'null') AS project_name_nullstr,
    COUNTIF(LOWER(TRIM(location_city)) = 'null') AS location_city_nullstr
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: 0 rows found. No literal 'null' string injection occurred in this batch.
-- ACTION: Apply NULLIF(LOWER(TRIM(col)), 'null') anyway as a defensive architecture practice for future loads.

-- 1.3 Empty-string injection
-- WHY: Empty spaces (' ') pass IS NOT NULL checks but are functionally missing data.
-- IMPACT: Creates blank lines in BI tool dropdowns.
SELECT
    COUNTIF(TRIM(employee_name) = '') AS employee_name_empty,
    COUNTIF(TRIM(designation) = '') AS designation_empty,
    COUNTIF(TRIM(location_city) = '') AS location_city_empty,
    COUNTIF(TRIM(client_name) = '') AS client_name_empty,
    COUNTIF(TRIM(client_industry) = '') AS client_industry_empty,
    COUNTIF(TRIM(project_name) = '') AS project_name_empty
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: 0 rows found.
-- ACTION: Apply NULLIF(TRIM(col), '') as a standard safeguard in Staging.


-- ============================================================================
-- SECTION 2: WHITESPACE CONTAMINATION
-- ============================================================================

-- 2.1 Count rows with whitespace in each text column
-- WHY: Leading/trailing spaces create invisible duplicates.
-- IMPACT: ' Lahore ' and 'Lahore' will group as two completely different cities in BI.
SELECT
    COUNTIF(employee_name != TRIM(employee_name)) AS employee_name_ws,
    COUNTIF(designation != TRIM(designation)) AS designation_ws,
    COUNTIF(skill_primary != TRIM(skill_primary)) AS skill_primary_ws,
    COUNTIF(department != TRIM(department)) AS department_ws,
    COUNTIF(location_city != TRIM(location_city)) AS location_city_ws,
    COUNTIF(project_name != TRIM(project_name)) AS project_name_ws,
    COUNTIF(client_name != TRIM(client_name)) AS client_name_ws,
    COUNTIF(client_industry != TRIM(client_industry)) AS client_industry_ws,
    COUNTIF(client_segment != TRIM(client_segment)) AS client_segment_ws,
    COUNTIF(task_category != TRIM(task_category)) AS task_category_ws,
    COUNTIF(employment_type != TRIM(employment_type)) AS employment_type_ws,
    COUNTIF(salary_currency != TRIM(salary_currency)) AS salary_currency_ws
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: Massive contamination. employee_name (310,033), designation (309,086), city (309,640), task_category (325,616) have hidden whitespace.
-- ACTION: Wrap every single string column in TRIM() without exception in Staging.


-- ============================================================================
-- SECTION 3: CASE INCONSISTENCY (Same value, different cases)
-- ============================================================================

-- 3.1 employment_type
-- WHY: Verify consistency of employment types.
-- IMPACT: Incorrect headcount allocation for contractors vs FTEs.
SELECT
    employment_type,
    COUNT(*) AS row_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY employment_type
ORDER BY row_count DESC;
-- RESULT: 7 variants found. 'Full-time' (41%), 'Contractor' (38%), plus fragments like 'CWR', 'contract', 'ft', 'FTE'.
-- ACTION: Use CASE WHEN to force mapping to strictly 'FULL_TIME' and 'CONTRACTOR'.

-- 3.2 employment_status 
-- WHY: Active vs Inactive tracking.
-- IMPACT: Miscounts in currently active workforce.
SELECT
    employment_status,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY employment_status
ORDER BY row_count DESC;
-- RESULT: 3 variants found → Active, active, ACT.
-- ACTION: Map entirely to 'ACTIVE' / 'INACTIVE'.

-- 3.3 project_status
-- WHY: Distinguish open vs closed projects.
-- IMPACT: Billing hours attributed to closed projects if status is fragmented.
SELECT
    project_status,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY project_status
ORDER BY row_count DESC;
-- RESULT: 3 variants found → Active, active, ACT.
-- ACTION: Map entirely to 'ACTIVE' / 'COMPLETED'.

-- 3.4 salary_currency 
-- WHY: Identify currency anomalies before financial calculations.
-- IMPACT: Multi-currency aggregations fail if symbols and codes are mixed.
SELECT
    salary_currency,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY salary_currency
ORDER BY row_count DESC;
-- RESULT: 23 severe variants including symbols (₹, £, €, $) and aliases (Rs, pkr).
-- ACTION: Apply UPPER(TRIM()) and explicitly map symbols/aliases to 3-letter ISO codes (PKR, INR, USD, etc.).

-- 3.5 project_type 
-- WHY: Ensure billing models are standardized.
-- IMPACT: Revenue recognition formulas break if 'Time & Material' is read differently than 'T&M'.
SELECT
    project_type,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY project_type
ORDER BY row_count DESC;
-- RESULT: 9 variants found. T&M = t&m = Time & Material | Fixed = fixed price = F.P.
-- ACTION: Standardize to 'T_M', 'FIXED_PRICE', 'INTERNAL'.

-- 3.6 project_priority 
-- WHY: Check if SLA priorities are unified.
-- IMPACT: Two completely different scales mixed together will break priority dashboard filters.
SELECT
    project_priority,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY project_priority
ORDER BY row_count DESC;
-- RESULT: Text scale (Critical, High, Medium, Low) mixed with Numeric scale (P0, P1, P2, P3).
-- ACTION: Map entirely to Text scale (CRITICAL, HIGH, MEDIUM, LOW).

-- 3.7 client_segment 
-- WHY: Validate business segmentation.
-- IMPACT: P&L by segment reports will be fragmented.
SELECT
    client_segment,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY client_segment
ORDER BY row_count DESC;
-- RESULT: Aliases present (B2B/b2b/Business to Business | B2G/Government/Govt).
-- ACTION: Standardize via UPPER() and CASE mapping.

-- 3.8 department
-- WHY: Validate organizational hierarchy.
-- IMPACT: Headcount reporting by department will splinter.
SELECT
    department,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY department
ORDER BY row_count DESC;
-- RESULT: Case variants plus digit corruption (e.g., 'Eng1naering', 'Analyt1cs').
-- ACTION: INITCAP(TRIM(REGEXP_REPLACE(department, r'1', 'i'))) then map clean categories.

-- 3.9 designation 
-- WHY: Validate role taxonomy.
-- IMPACT: Resource utilization by role will be inaccurate.
SELECT
    designation,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY designation
ORDER BY row_count DESC;
-- RESULT: Base roles exist but fragmented by corruption (Data Eng1naer) and case.
-- ACTION: Clean digits via REGEXP first, then INITCAP().

-- 3.10 task_category 
-- WHY: Validate work activity groupings.
-- IMPACT: Cannot accurately track R&D vs Support costs if categories are messy.
SELECT
    task_category,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY task_category
ORDER BY row_count DESC;
-- RESULT: 8 categories deeply fragmented by typos (Intarnal Tra1ning, Maet1ng, Daployment).
-- ACTION: Use REGEXP_CONTAINS inside CASE statement to catch and standardise all typo variants.


-- ============================================================================
-- SECTION 4: ALPHANUMERIC CORRUPTION ("1" replacing "i" / "e")
-- ============================================================================

-- 4.1 Corruption scorecard across all affected columns
-- WHY: Source system has a systemic OCR/transcription bug where the digit "1" replaces letters.
-- IMPACT: Employee profiles, roles, and client names become unreadable and fail dimension joins.
SELECT
    COUNTIF(REGEXP_CONTAINS(employee_name,    r'[0-9]'))                               AS corrupt_employee_names,
    COUNTIF(REGEXP_CONTAINS(designation,      r'[0-9]'))                               AS corrupt_designations,
    COUNTIF(REGEXP_CONTAINS(department,       r'[0-9]'))                               AS corrupt_departments,
    COUNTIF(REGEXP_CONTAINS(skill_primary,    r'[0-9]'))                               AS corrupt_skill_primary,
    COUNTIF(REGEXP_CONTAINS(task_category,    r'[0-9]'))                               AS corrupt_task_categories,
    COUNTIF(REGEXP_CONTAINS(location_city,    r'[0-9]'))                               AS corrupt_cities,
    COUNTIF(REGEXP_CONTAINS(client_name,      r'[0-9]'))                               AS corrupt_client_names,
    COUNTIF(REGEXP_CONTAINS(client_industry,  r'[0-9]'))                               AS corrupt_client_industries,
    COUNTIF(REGEXP_CONTAINS(project_name,     r'[0-9]'))                               AS project_name_with_numbers_DO_NOT_CLEAN
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: Massive systemic corruption found. Names (187,927), depts (244,714), designations (205,135). Project names have 365,082 valid numbers.
-- ACTION: Use REGEXP_REPLACE(col, r'1', 'i') on all string columns EXCEPT project_name.
-- WARNING: DO NOT APPLY regex cleaning to project_name to protect valid numeric strings.

-- 4.2 All unique corrupt employee_name values 
-- WHY: Build a corpus of dirty values for regex testing.
-- IMPACT: Ensures we don't accidentally replace valid numbers in names if any exist.
SELECT
    employee_name AS dirty_value,
    COUNT(*) AS frequency
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE REGEXP_CONTAINS(employee_name, r'[0-9]')
  AND LOWER(TRIM(employee_name)) != 'null'
GROUP BY employee_name
ORDER BY frequency DESC;
-- RESULT: 'M1chaal Smith', 'Chr1stophar' etc. confirmed.
-- ACTION: Regex replace logic is safe to apply globally to employee names.

-- 4.3 All unique corrupt designation values
-- WHY: Verify the specific roles affected by the '1' bug.
-- IMPACT: Helps QA the final standardized roles.
SELECT
    designation AS dirty_value,
    COUNT(*) AS frequency
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE REGEXP_CONTAINS(designation, r'[0-9]')
GROUP BY designation
ORDER BY frequency DESC;
-- RESULT: 'Data Eng1naer' (102,953 rows) and 'QA Eng1naer' (102,182 rows).
-- ACTION: Regex replace will fully cure this column.

-- 4.4 All unique corrupt location_city values
-- WHY: Identify cities suffering from both digit corruption and phonetic typos.
-- IMPACT: Location-based revenue maps will have blank spots.
SELECT
    location_city AS dirty_value,
    COUNT(*) AS frequency
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE REGEXP_CONTAINS(location_city, r'[0-9]')
   OR LOWER(TRIM(location_city)) IN ('lahora','bangalora','naw york','s1ngapora','duba1','r1yadh','barl1n')
GROUP BY location_city
ORDER BY frequency DESC;
-- RESULT: Lahora (109k), Bangalora (80k), Duba1 (30k), Naw York (22k), Barl1n (7.9k).
-- ACTION: Needs explicit HARDCODED CASE overrides in Staging (regex won't fix 'Lahora' to 'Lahore').

-- 4.5 All unique corrupt client_industry values
-- WHY: Profile industry fragmentation.
-- IMPACT: Client portfolio analysis will be skewed.
SELECT
    client_industry AS dirty_value,
    COUNT(*) AS frequency
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE REGEXP_CONTAINS(client_industry, r'[0-9]')
   OR LOWER(TRIM(client_industry)) IN ('intarnal','rata1l','edtach','haalthcare','f1ntach')
GROUP BY client_industry
ORDER BY frequency DESC;
-- RESULT: Intarnal (94k), F1nTach (62k), EdTach (55k), Rata1l (52k), Haalthcare (44k).
-- ACTION: Regex replace + CASE mapping to standard industry names.

-- 4.6 Validate which project_name numbers are VALID
-- WHY: Ensure we don't destroy valid business data while cleaning corruption.
-- IMPACT: Overzealous cleaning removes actual project version numbers.
SELECT
    project_name,
    COUNT(*) AS frequency
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE REGEXP_CONTAINS(project_name, r'[0-9]')
  AND LOWER(TRIM(project_name)) != 'null'
GROUP BY project_name
ORDER BY frequency DESC
LIMIT 50;
-- RESULT: '24/7', '3rdgeneration', '6thgeneration' etc. are valid business terms.
-- ACTION: Exempt project_name from numeric Regex cleaning entirely.


-- ============================================================================
-- SECTION 5: LOCATION STANDARDIZATION PROBLEMS
-- ============================================================================

-- 5.1 location_country cardinality and all variants
-- WHY: Check geographic mapping formats.
-- IMPACT: The same country appearing as 'PK', 'pk', 'Pakistan' causes 3 filter options in BI for one country.
SELECT
    location_country,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY location_country
ORDER BY row_count DESC;
-- RESULT: 30 variants for 10 countries (PK/pk/Pakistan, USA/U.S.A/us, etc.)
-- ACTION: Remove dots, apply UPPER(), and explicitly map everything to standard ISO-2 codes (PK, US, AE).

-- 5.2 client_country cardinality 
-- WHY: Verify if client countries suffer from the same formatting issues as employee locations.
-- IMPACT: Client revenue mapping will fail.
SELECT
    client_country,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY client_country
ORDER BY row_count DESC;
-- RESULT: 39 variants found. Same fragmentation issue as location_country.
-- ACTION: Apply the exact same ISO-2 location mapping logic used for location_country.

-- 5.3 location_city — typos AND case issues
-- WHY: Final check on city canonical forms.
-- IMPACT: Double counting in city-level aggregations.
SELECT
    location_city,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY location_city
ORDER BY row_count DESC;
-- RESULT: Mixed casing (LONDON, London, london) combined with the typos discovered earlier.
-- ACTION: Fix typos via CASE, then enforce INITCAP(LOWER(TRIM())) across the board.


-- ============================================================================
-- SECTION 6: CURRENCY NORMALIZATION AUDIT
-- ============================================================================

-- 6.1 Currency variants with fx_rate confirmation
-- WHY: 'PKR' and 'Rs' are the same currency but stored differently. Need mathematical proof before merging.
-- IMPACT: If we falsely merge different currencies, the total USD revenue conversion will be corrupted.
SELECT
    salary_currency,
    COUNT(*) AS row_count,
    ROUND(MIN(fx_rate), 4) AS min_fx_rate,
    ROUND(MAX(fx_rate), 4) AS max_fx_rate,
    ROUND(AVG(fx_rate), 4) AS avg_fx_rate
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY salary_currency
ORDER BY avg_fx_rate, salary_currency;
-- RESULT: Confirmed! 'Rs' and 'PKR' share identical fx_rate ranges (~262-306). '₹' and 'INR' share identical ranges (~81-91).
-- ACTION: It is mathematically safe to merge these symbols/aliases into 3-letter ISO codes in Staging.

-- 6.2 fx_rate variability for same currency 
-- WHY: Check if multiple FX rates per currency is an error or daily API fluctuation.
-- IMPACT: Decides if we need to average the rates or leave them at row-level.
SELECT
    salary_currency,
    COUNT(DISTINCT ROUND(fx_rate, 3)) AS distinct_rate_snapshots,
    MIN(ROUND(fx_rate, 4)) AS lowest_rate,
    MAX(ROUND(fx_rate, 4)) AS highest_rate
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY salary_currency
ORDER BY distinct_rate_snapshots DESC;
-- RESULT: PKR has 37 distinct rates over 3 years. This reflects normal daily/weekly API fluctuations.
-- ACTION: Do not alter fx_rate column. Row-level temporal rates are correct.


-- ============================================================================
-- SECTION 7: NUMERIC COLUMN PROFILING
-- ============================================================================

-- 7.1 Hours columns — full statistical profile
-- WHY: Detect impossible values (e.g., working 30 hours a day) and zero-fill errors.
-- IMPACT: Skews average utilization and total capacity metrics.
SELECT
    -- hours_worked
    MIN(hours_worked) AS hw_min,
    MAX(hours_worked) AS hw_max,
    ROUND(AVG(hours_worked), 4) AS hw_avg,
    COUNTIF(hours_worked < 0) AS hw_negative,
    COUNTIF(hours_worked > 24) AS hw_over_24,
    COUNTIF(hours_worked = 0) AS hw_zero,
    -- billable_hours
    MIN(billable_hours) AS bh_min,
    MAX(billable_hours) AS bh_max,
    COUNTIF(billable_hours < 0) AS bh_negative,
    -- capacity_hours
    MIN(capacity_hours) AS cap_min,
    MAX(capacity_hours) AS cap_max,
    COUNTIF(capacity_hours < 0) AS cap_negative,
    -- utilization_pct
    MAX(utilization_pct) AS util_max,
    COUNTIF(utilization_pct > 1) AS util_over_1,
    -- allocation_pct
    MAX(allocation_pct) AS alloc_max,
    COUNTIF(allocation_pct > 1) AS alloc_over_1
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: Hours range from -8 to 8. Exactly 50,908 rows have negative hours. Utilization is confirmed on a 0-1 scale. No hours > 24 found.
-- ACTION: Pass through as-is. Do not apply *100 to utilization.

-- 7.2 Financial columns — profile
-- WHY: Detect outliers or impossible negative costs.
-- IMPACT: Ensures P&L sums are not distorted by garbage data.
SELECT
    MIN(revenue_usd) AS rev_min,
    MAX(revenue_usd) AS rev_max,
    ROUND(AVG(revenue_usd), 2) AS rev_avg,
    COUNTIF(revenue_usd < 0) AS rev_negative,
    MIN(cost_usd) AS cost_min,
    MAX(cost_usd) AS cost_max,
    COUNTIF(cost_usd < 0) AS cost_negative,
    MIN(profit_usd) AS profit_min,
    MAX(profit_usd) AS profit_max,
    COUNTIF(profit_usd < 0) AS profit_negative,
    MIN(hourly_rate_usd) AS rate_min,
    MAX(hourly_rate_usd) AS rate_max,
    COUNTIF(hourly_rate_usd < 0) AS rate_negative,
    COUNTIF(hourly_rate_usd = 0 AND is_billable = true) AS billable_zero_rate
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: 50,908 rows contain negative revenue and cost. No impossible outliers found. No billable rows with 0 rate.
-- ACTION: Data is structurally sound. Proceed without applying capping rules.

-- 7.3 Negative hours breakdown 
-- WHY: Investigate the 50,908 rows with negative hours to see if they are valid.
-- IMPACT: If deleted, original erroneous timesheets will remain uncorrected in the warehouse.
SELECT
    entry_type,
    is_correction,
    data_quality_flag,
    COUNT(*) AS count,
    MIN(hours_worked) AS min_hours,
    MAX(hours_worked) AS max_hours
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE hours_worked < 0
GROUP BY entry_type, is_correction, data_quality_flag;
-- RESULT: 100% of negative hours (50,908 rows) belong strictly to entry_type='correction' and data_quality_flag='CORRECTED'.
-- ACTION: Do NOT delete. They are valid financial offsets for P&L reporting. Ensure logic allows negative values.


-- ============================================================================
-- SECTION 8: CROSS-COLUMN MATH VALIDATION
-- ============================================================================

-- 8.1 Hours reconciliation: billable + non_billable = hours_worked
-- WHY: Ensure base arithmetic holds true from source system.
-- IMPACT: Conflicting totals in BI if components do not sum to total hours.
SELECT
    COUNT(*) AS total_rows,
    COUNTIF(ABS((billable_hours + non_billable_hours) - hours_worked) > 0.01) AS hours_math_violations
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: 0 violations found across 3.7M rows.
-- ACTION: Arithmetic is trustworthy.

-- 8.2 Utilization scale confirmation: utilization_pct = billable/capacity
-- WHY: Verify if utilization is a percentage out of 1 or 100.
-- IMPACT: Prevents multiplying by 100 twice in BI reports.
SELECT
    ROUND(AVG(ABS(utilization_pct - SAFE_DIVIDE(billable_hours, capacity_hours))),6) AS delta_01_scale,
    ROUND(AVG(ABS(utilization_pct - SAFE_DIVIDE(billable_hours, capacity_hours)*100)), 6) AS delta_100_scale
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE capacity_hours > 0;
-- RESULT: delta_01_scale approaches 0.
-- ACTION: Treat utilization as a decimal (0.0 to 1.0).

-- 8.3 Profit math: profit_usd = revenue_usd - cost_usd
-- WHY: Ensure P&L math holds.
-- IMPACT: Incorrect margins reported to finance.
SELECT
    COUNTIF(ABS(profit_usd - (revenue_usd - cost_usd)) > 0.05) AS profit_math_violations
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: 0 violations found.
-- ACTION: Math is verified.

-- 8.4 Revenue math: revenue_usd ≈ billable_hours × hourly_rate_usd
-- WHY: Ensure base revenue generation formula holds.
-- IMPACT: Detects missing FX conversions.
SELECT
    COUNTIF(
        billable_hours > 0
        AND hourly_rate_usd > 0
        AND ABS(revenue_usd - (billable_hours * hourly_rate_usd)) > 1.0) AS revenue_calc_violations
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: 0 violations found.
-- ACTION: Revenue calculation is solid.

-- 8.5 is_billable vs billable_hours consistency check
-- WHY: Ensure boolean flag matches numeric reality.
-- IMPACT: Slicing by "Is Billable" will show zero-hour rows if broken.
SELECT
    is_billable,
    COUNTIF(billable_hours = 0) AS zero_billable_hours,
    COUNTIF(billable_hours > 0) AS positive_billable_hours,
    COUNT(*) AS total
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY is_billable;
-- RESULT: Clean. 0 billable hours when is_billable = False.
-- ACTION: Flag is trustworthy for BI slicers.

-- 8.6 entry_type vs is_correction consistency check
-- WHY: Ensure string flag matches boolean flag.
-- IMPACT: Filters on correction type will fail.
SELECT
    entry_type,
    is_correction,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY entry_type, is_correction
HAVING (entry_type = 'correction' AND is_correction = false)
    OR (entry_type = 'original' AND is_correction = true);
-- RESULT: 0 rows returned. Flags align perfectly.
-- ACTION: Safe to use `is_correction` boolean for filtering in Gold layer.


-- ============================================================================
-- SECTION 9: SKILL_PRIMARY vs DESIGNATION MISMATCH
-- ============================================================================

-- 9.1 Count of rows where skill and designation do not match
-- WHY: Evaluate if employees are working outside their primary skill.
-- IMPACT: Breaks resource allocation matching algorithms and role-level security in Power BI.
SELECT
    COUNT(*) AS total_rows,
    COUNTIF(
        LOWER(TRIM(REGEXP_REPLACE(skill_primary, r'1', 'i')))
        !=
        LOWER(TRIM(REGEXP_REPLACE(designation, r'1', 'i')))
    ) AS skill_designation_mismatch_rows,
    ROUND(100.0 * COUNTIF(
        LOWER(TRIM(REGEXP_REPLACE(skill_primary, r'1', 'i')))
        !=
        LOWER(TRIM(REGEXP_REPLACE(designation, r'1', 'i')))
    ) / COUNT(*), 2) AS mismatch_pct
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE designation IS NOT NULL AND TRIM(designation) NOT IN ('', 'null')
  AND skill_primary IS NOT NULL AND TRIM(skill_primary) NOT IN ('', 'null');
-- RESULT: 2,509,490 rows (70.88%) have conflicting designation and primary skill.
-- ACTION: Pass through as-is, but create a boolean `is_skill_mismatch` flag for downstream analysis.

-- 9.2 Mismatch breakdown by employee 
-- WHY: Identify if the mismatch is systemic across the workforce or isolated to a few power-users.
-- IMPACT: Helps HR determine if ingestion column mapping was swapped.
SELECT
    emp_id,
    MAX(TRIM(employee_name))                    AS employee_name,
    MAX(TRIM(designation))                      AS designation_used,
    MAX(TRIM(skill_primary))                    AS skill_primary_used,
    COUNT(*)                                    AS affected_rows
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE LOWER(TRIM(REGEXP_REPLACE(skill_primary, r'1', 'i')))
    != LOWER(TRIM(REGEXP_REPLACE(designation, r'1', 'i')))
  AND TRIM(designation) NOT IN ('', 'null')
GROUP BY emp_id
ORDER BY affected_rows DESC
LIMIT 50;
-- RESULT: Systemic issue across thousands of employees. (e.g., emp has designation='Data Engineer' but skill_primary='QA Engineer').
-- WARNING: Escalated to Business Owner. This is likely an upstream HR system column swap.


-- ============================================================================
-- SECTION 10: CLIENT DATA INTEGRITY
-- ============================================================================

-- 10.1 client_industry 
-- WHY: Check client industry fragmentation.
-- IMPACT: B2B segment analysis fails.
SELECT
    client_industry,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY client_industry
ORDER BY row_count DESC;
-- RESULT: Confirmed dirty: F1nTach, EdTach, Rata1l, Haalthcare, Intarnal.
-- ACTION: Regex replace + map explicitly.

-- 10.2 client_name with digit corruption
-- WHY: Check extent of corruption on Client names.
-- IMPACT: Invoices and client-facing dashboards will look unprofessional.
SELECT
    client_name,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE REGEXP_CONTAINS(client_name, r'[0-9]')
  AND LOWER(TRIM(client_name)) != 'null'
GROUP BY client_name
ORDER BY row_count DESC
LIMIT 50;
-- RESULT: Extensive corruption ('Robarts, Munoz and Wh1taker', 'H1ll Ltd').
-- ACTION: Regex replace (1 to i) is required on this column.

-- 10.3 NULL + null-string client_name combined
-- WHY: Check for completely unmapped clients.
-- IMPACT: Unmapped revenue.
SELECT
    COUNTIF(client_name IS NULL) AS true_null,
    COUNTIF(LOWER(TRIM(client_name)) = 'null') AS null_string,
    COUNTIF(TRIM(client_name) = '') AS empty_string,
    COUNTIF(client_name IS NULL
         OR LOWER(TRIM(client_name)) IN ('null','')) AS total_missing
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: 186,941 True Nulls. No null strings found.
-- ACTION: Apply COALESCE(client_name, 'UNKNOWN') in staging.


-- ============================================================================
-- SECTION 11: TEMPORAL ANALYSIS
-- ============================================================================

-- 11.1 Date boundary check
-- WHY: Detect future dates, testing data, or historical dumps.
-- IMPACT: Skews Time Intelligence (YTD/MTD) calculations in DAX/SQL.
SELECT
    MIN(work_date) AS earliest_work_date,
    MAX(work_date) AS latest_work_date,
    COUNTIF(work_date > CURRENT_DATE()) AS future_dates,
    COUNTIF(work_date < DATE('2020-01-01')) AS very_old_dates,
    DATE_DIFF(MAX(work_date), MIN(work_date), DAY) AS date_span_days
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`;
-- RESULT: Data spans 2023-03-26 to 2026-03-27. No future dates. No historical junk.
-- ACTION: Date boundaries are clean. No filtering needed.

-- 11.2 Monthly row volume
-- WHY: Detect pipeline gaps or sudden volume spikes.
-- IMPACT: Identifies months where data ingestion failed.
SELECT
    FORMAT_DATE('%Y-%m', work_date) AS year_month,
    COUNT(*) AS row_count,
    SUM(CASE WHEN hours_worked < 0 THEN 1 ELSE 0 END) AS correction_rows,
    SUM(CASE WHEN hours_worked = 0 THEN 1 ELSE 0 END) AS bench_rows,
    SUM(CASE WHEN billable_hours > 0 THEN 1 ELSE 0 END) AS billable_rows
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY year_month
ORDER BY year_month;
-- RESULT: Gradual volume increase month-over-month. No missing months.
-- ACTION: Temporal data is healthy.

-- 11.3 Load timestamp anomalies
-- WHY: Check if data was loaded out of order.
-- IMPACT: Determines if we need strict deduplication based on load_time.
SELECT
    DATE(load_timestamp) AS load_date,
    COUNT(*) AS rows_loaded,
    COUNT(DISTINCT batch_id) AS batches
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY load_date
ORDER BY load_date;
-- RESULT: 2 distinct load batches found. 
-- ACTION: Proceed normally.


-- ============================================================================
-- SECTION 12: DUPLICATE ANALYSIS (Beyond Primary Key)
-- ============================================================================

-- 12.1 timesheet_id duplicates (primary key check)
-- WHY: Ensure strict uniqueness of the primary key.
-- IMPACT: Duplicates will cause Cartesian explosions in Fact tables.
SELECT
    timesheet_id,
    COUNT(*) AS occurrences
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY timesheet_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;
-- RESULT: 0 occurrences. Primary key is strictly unique.
-- ACTION: Implement `unique` dbt test on timesheet_id.

-- 12.2 Business-key duplicates (same emp+date+project, different timesheet_id)
-- WHY: Check if an employee logged time to the same project twice on the same day.
-- IMPACT: Could be accidental double-entry OR legitimate correction workflows.
SELECT
    emp_id,
    work_date,
    project_id,
    COUNT(*) AS occurrences,
    STRING_AGG(DISTINCT entry_type, ' | ')  AS entry_types,
    STRING_AGG(DISTINCT timesheet_id, ' | ' LIMIT 3) AS sample_ids
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY emp_id, work_date, project_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 50;
-- RESULT: Duplicates exist but entry_types show strict 'correction | original' pairs. 
-- ACTION: This is expected behavior for corrected timesheets. No deduplication required.

-- 12.3 Correction record integrity — every correction must reference an original
-- WHY: Ensure foreign key integrity within the self-referencing hierarchy.
-- IMPACT: Orphan corrections will deduct revenue without an original baseline.
SELECT
    c.timesheet_id AS correction_id,
    c.original_timesheet_id AS references_original,
    o.timesheet_id AS original_exists
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` c
LEFT JOIN `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1` o
    ON c.original_timesheet_id = o.timesheet_id
WHERE c.entry_type = 'correction'
  AND o.timesheet_id IS NULL
LIMIT 50;
-- RESULT: 0 Orphan corrections found. Foreign key constraint holds perfectly.
-- ACTION: Relationship is intact.


-- ============================================================================
-- SECTION 13: EMPLOYEE CONSISTENCY CHECK
-- ============================================================================

-- 13.1 emp_id with multiple distinct employee_names
-- WHY: Same emp_id should ideally have consistent profile data across all records.
-- IMPACT: Dimension tables will duplicate the same employee if names are not aggregated.
SELECT
    emp_id,
    COUNT(DISTINCT TRIM(LOWER(employee_name))) AS distinct_name_variants,
    STRING_AGG(DISTINCT TRIM(employee_name), ' | ' ORDER BY TRIM(employee_name) LIMIT 5) AS name_variants,
    COUNT(*) AS total_rows
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE LOWER(TRIM(employee_name)) NOT IN ('null', '')
GROUP BY emp_id
HAVING COUNT(DISTINCT TRIM(LOWER(employee_name))) > 1
ORDER BY distinct_name_variants DESC
LIMIT 30;
-- RESULT: Severe Slowly Changing Dimension (SCD) issues. IDs have up to 4 name variants (due to Typos + Clean versions).
-- ACTION: Staging will clean strings. Gold Layer (dim_employees) MUST group by emp_id to select a single unique name.

-- 13.2 emp_id with multiple departments 
-- WHY: Check if employees move departments or if data is just noisy.
-- IMPACT: Headcount by department will be inaccurate.
SELECT
    emp_id,
    COUNT(DISTINCT TRIM(LOWER(department))) AS distinct_departments,
    STRING_AGG(DISTINCT TRIM(department), ' | ') AS dept_variants,
    COUNT(*) AS total_rows
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY emp_id
HAVING COUNT(DISTINCT TRIM(LOWER(department))) > 1
ORDER BY distinct_departments DESC
LIMIT 20;
-- RESULT: High fragmentation due to case/digit corruption (e.g., Engineering | Eng1naering).
-- ACTION: Handled by universal department cleanup in Staging.

-- 13.3 emp_id with multiple cost_centers
-- WHY: Cost centers define financial allocation.
-- IMPACT: Cost misallocation for employees splitting time.
SELECT
    emp_id,
    COUNT(DISTINCT cost_center) AS distinct_cost_centers,
    STRING_AGG(DISTINCT cost_center, ' | ') AS cost_center_variants
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY emp_id
HAVING COUNT(DISTINCT cost_center) > 1
ORDER BY distinct_cost_centers DESC
LIMIT 20;
-- RESULT: Some employees naturally have 2 cost centers (valid business scenario for matrix organizations).
-- ACTION: Pass through as-is.


-- ============================================================================
-- SECTION 14: PROJECT & CLIENT CONSISTENCY CHECK
-- ============================================================================

-- 14.1 project_id with multiple project_names 
-- WHY: Ensure project ID is the source of truth.
-- IMPACT: Project dimension table will fail uniqueness tests.
SELECT
    project_id,
    COUNT(DISTINCT TRIM(LOWER(project_name))) AS distinct_name_variants,
    STRING_AGG(DISTINCT TRIM(project_name), ' | ' LIMIT 3) AS project_name_variants,
    COUNT(*) AS total_rows
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE LOWER(TRIM(project_name)) NOT IN ('null', '')
GROUP BY project_id
HAVING COUNT(DISTINCT TRIM(LOWER(project_name))) > 1
ORDER BY distinct_name_variants DESC
LIMIT 30;
-- RESULT: Multiple string variants per project_id.
-- ACTION: Gold Layer (dim_projects) will handle grouping and deduplication.

-- 14.2 client_id with multiple client_names 
-- WHY: Ensure client ID is the source of truth.
-- IMPACT: Client dimension table will fail uniqueness tests.
SELECT
    client_id,
    COUNT(DISTINCT TRIM(LOWER(client_name))) AS distinct_name_variants,
    STRING_AGG(DISTINCT TRIM(client_name), ' | ' LIMIT 3) AS client_name_variants
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE LOWER(TRIM(client_name)) NOT IN ('null', '')
GROUP BY client_id
HAVING COUNT(DISTINCT TRIM(LOWER(client_name))) > 1
ORDER BY distinct_name_variants DESC
LIMIT 30;
-- RESULT: Multiple string variants per client_id.
-- ACTION: Gold Layer (dim_clients) will handle grouping and deduplication.

-- 14.3 project_id with multiple project_types 
-- WHY: Ensure billing methodology is fixed per project.
-- IMPACT: Cannot mix T&M and Fixed Price on the same contract ID.
SELECT
    project_id,
    COUNT(DISTINCT LOWER(TRIM(project_type))) AS distinct_type_variants,
    STRING_AGG(DISTINCT TRIM(project_type), ' | ') AS type_variants
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
WHERE project_id NOT IN ('BENCH')
GROUP BY project_id
HAVING COUNT(DISTINCT LOWER(TRIM(project_type))) > 1
ORDER BY distinct_type_variants DESC
LIMIT 20;
-- RESULT: Variants exist purely due to case/typo issues ('T&M' vs 'Time & Material').
-- ACTION: Staging standardization will naturally resolve this.


-- ============================================================================
-- SECTION 15: DATA QUALITY FLAG AUDIT
-- ============================================================================

-- WHY: data_quality_flag column ('VALID','CORRECTED') must align perfectly with entry_type.
-- IMPACT: Any mismatch = pipeline logic failure in the Bronze ingestion layer.
SELECT
    data_quality_flag,
    entry_type,
    is_correction,
    is_anomaly,
    COUNT(*) AS row_count
FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
GROUP BY data_quality_flag, entry_type, is_correction, is_anomaly
ORDER BY row_count DESC;
-- RESULT: Exact Match. 3,676,573 VALID and 50,908 CORRECTED rows aligned perfectly.
-- ACTION: Flags are safe to use in downstream models.


-- ============================================================================
-- SECTION 16: FULL EDA SUMMARY DASHBOARD
-- Run LAST — use this as your one-page sign-off before writing stg.sql
-- ============================================================================

-- WHY: Compile all findings into a single executive view.
-- IMPACT: Provides the exact blueprint and validation targets for the Staging layer.
WITH base AS (
    SELECT
        COUNT(*) AS total_rows,
        -- Null/string injection
        COUNTIF(LOWER(TRIM(employee_name)) IN ('null','')) AS missing_employee_names,
        COUNTIF(LOWER(TRIM(client_name)) IN ('null','')) AS missing_client_names,
        COUNTIF(LOWER(TRIM(project_name)) IN ('null','')) AS missing_project_names,
        COUNTIF(LOWER(TRIM(designation)) IN ('null','')) AS missing_designations,
        COUNTIF(LOWER(TRIM(location_city)) IN ('null','')) AS missing_cities,
        -- Whitespace
        COUNTIF(employee_name != TRIM(employee_name)) AS names_with_whitespace,
        COUNTIF(location_city != TRIM(location_city)) AS cities_with_whitespace,
        -- Digit corruption
        COUNTIF(REGEXP_CONTAINS(employee_name, r'[0-9]')
                AND LOWER(TRIM(employee_name)) != 'null') AS corrupt_employee_names,
        COUNTIF(REGEXP_CONTAINS(designation, r'[0-9]')) AS corrupt_designations,
        COUNTIF(REGEXP_CONTAINS(department, r'[0-9]')) AS corrupt_departments,
        COUNTIF(REGEXP_CONTAINS(task_category, r'[0-9]')
                OR LOWER(TRIM(task_category)) IN ('intarnal tra1ning','maet1ng','daployment','davelopment')) AS corrupt_task_categories,
        COUNTIF(REGEXP_CONTAINS(client_name, r'[0-9]')
                AND LOWER(TRIM(client_name)) != 'null') AS corrupt_client_names,
        COUNTIF(REGEXP_CONTAINS(client_industry, r'[0-9]')) AS corrupt_client_industries,
        -- Negative hours
        COUNTIF(hours_worked < 0) AS negative_hours_rows,
        -- Hours math check
        COUNTIF(ABS((billable_hours + non_billable_hours) - hours_worked) > 0.01) AS hours_math_violations,
        -- Priority scale mix
        COUNTIF(TRIM(project_priority) IN ('P0','P1','P2','P3')) AS numeric_priority_rows,
        -- Currency mess
        COUNTIF(TRIM(salary_currency) IN ('Rs','₹','£','€','$')) AS non_standard_currency_codes,
        -- City typos
        COUNTIF(LOWER(TRIM(location_city)) IN ('lahora','bangalora','duba1','naw york','s1ngapora','r1yadh','barl1n')) AS city_typos,
        -- Correction integrity
        COUNTIF(entry_type = 'correction' AND is_correction = false) AS broken_correction_flags,
        CURRENT_TIMESTAMP() AS report_generated_at
    FROM `enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1`
)
SELECT * FROM base;
-- RESULT: Dashboard outputs 3,727,481 rows, confirming exactly 187k+ corrupt names, 310k+ whitespace issues, and 0 math violations.
-- ACTION: Proceed to write `stg_hris__timesheets.sql` using the blueprint below.

/*
================================================================================
REAL-WORLD DATA QUALITY FINDINGS (The Blueprint for stg_hris__timesheets.sql)
================================================================================

PRIORITY 1 — DETERMINISTIC FIXES (Must resolve in Staging):
  ✗ Alphanumeric Corruption: Digit '1' has infected names, departments, cities, and designations (187k+ affected rows).
    → ACTION: REGEXP_REPLACE(col, r'1', 'i'). Exempt project_name to protect valid numbers.
  ✗ Whitespace Infection: Over 300k+ rows per string column have hidden spaces.
    → ACTION: Universal TRIM() application.
  ✗ Categorical Chaos: 20+ columns suffer from severe casing and aliasing issues (e.g., 23 variants for Currency, 7 for Employment Type).
    → ACTION: Standardize via UPPER()/LOWER() and exhaustive CASE WHEN mapping.
  ✗ Geolocation Typos: 'Lahora', 'Bangalora', 'Duba1'.
    → ACTION: Hardcoded CASE WHEN overrides for known city typos.

PRIORITY 2 — BUSINESS LOGIC (Validated, Do Not Alter):
  ✓ Negative Hours & Revenue: Confirmed exactly 50,908 rows. All map strictly to 'correction' entry types.
    → ACTION: Pass through as-is. Required for P&L netting.
  ✓ Financial Math: Billable + Non-Billable exactly matches Hours Worked. Profit math holds perfectly.
    → ACTION: No recalculation required.
  ✓ Literal 'null' Strings: Checked and found 0 occurrences. 
    → ACTION: Standard NULLIF handling is sufficient.

PRIORITY 3 — ARCHITECTURAL WARNINGS (Handled in Gold Layer):
  ! Skill/Designation Mismatch: ~71% of rows show mismatch. Escalating to HR.
  ! Dimensional Inconsistency: Same emp_id has multiple department/name spellings due to historical typos.
    → ACTION: Staging will clean strings; Dimensions (dim_employees) will aggregate to unique records.
================================================================================
*/