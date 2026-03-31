/*
===============================================================================
Project: Enterprise Workforce Analytics
Layer: Pre-Silver (Data Profiling / DQ Assessment)
Purpose:
  - Assess data quality of Bronze layer
  - Identify anomalies, NULLs, duplicates, inconsistencies
  - Validate data before transformation into Silver layer (dbt staging)
Table: bronze_layer.raw_hris_timesheets_v1
Author: Hafiz Zaman Yaseen
Date: 29-Mar-2026
===============================================================================
*/

-- ============================================================================
-- PART 1: SIZE & STRUCTURE (Data Volume & Schema)
-- ============================================================================

-- 1. Total Rows Check
-- WHY: Reconcile source vs destination counts.
-- IMPACT: Missing rows lead to incorrect KPIs and 'Blank' records in Power BI.
SELECT COUNT(*) AS Total_Rows 
FROM bronze_layer.raw_hris_timesheets_v1;
-- RESULT: 3,727,481 rows confirmed. Ingestion is successful.
-- ACTION (Silver Layer): Proceed with standard table/view materialization in dbt.

-- 2. Total Columns Check
-- WHY: Detect Schema Drift (unexpected column changes).
-- IMPACT: Missing/extra columns will crash dbt models and Power BI refreshes.
SELECT COUNT(*) AS Total_Columns
FROM bronze_layer.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'raw_hris_timesheets_v1';
-- RESULT: 63 columns confirmed. Initial schema baseline established.
-- ACTION (Silver Layer): Proceed to column data type validation and null checks.

-- 3. Detailed Schema Inspection
-- WHY: Verify column names and data types (Blueprint validation).
-- IMPACT: Wrong types (e.g. Date as String) break Power BI Time Intelligence and DAX.Abbreviations violate strict data modeling standards.
SELECT 
    column_name, 
    data_type, 
    is_nullable
FROM bronze_layer.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'raw_hris_timesheets_v1'
ORDER BY ordinal_position;
-- RESULT: Core data types verified (DATE, FLOAT64, BOOL correctly assigned). Column naming is consistent but includes minor abbreviations (e.g., emp_id, pct, fx).
-- WARNING: 100% of columns have is_nullable = YES, indicating absence of source-level constraints.
-- ACTION (Silver Layer): Apply strict null-handling and standardize column names where required.


-- ============================================================================
-- PART 2: DATA QUALITY & INTEGRITY (Nulls & Duplicates)
-- ============================================================================

-- 4. Duplicate Check on Primary Key (timesheet_id)
-- WHY: Detect multiple submissions of the same timesheet due to source system glitches.
-- IMPACT: Uncaught duplicates inflate key metrics (e.g., billable hours, cost, revenue) and break One-to-Many joins in the Power BI Star Schema.
SELECT 
    timesheet_id, 
    COUNT(*) AS Duplicate_Count
FROM bronze_layer.raw_hris_timesheets_v1
GROUP BY timesheet_id
HAVING COUNT(*) > 1;
-- RESULT: 0 duplicates found. Primary Key (timesheet_id) is fully unique across the dataset.
-- ACTION (Silver Layer): No de-duplication required. Proceed with downstream transformations.

-- 5. NULL / Missing Values Check on Critical Columns
-- WHY: Ensure completeness of critical fields required for joins, time analysis, and accurate metric calculations.
-- IMPACT: Missing values create orphaned records, distort key metrics (e.g., hours, cost, revenue), and break filtering and time-based analysis in Power BI.
SELECT 
    SUM(CASE WHEN emp_id IS NULL THEN 1 ELSE 0 END) AS Null_Employees,
    SUM(CASE WHEN work_date IS NULL THEN 1 ELSE 0 END) AS Null_Dates,
    SUM(CASE WHEN hours_worked IS NULL THEN 1 ELSE 0 END) AS Null_Hours,
    SUM(CASE WHEN entry_type IS NULL THEN 1 ELSE 0 END) AS Null_EntryType
FROM bronze_layer.raw_hris_timesheets_v1;
-- RESULT: 0 NULLs found across all critical columns (emp_id, work_date, hours_worked, entry_type).
-- ACTION (Silver Layer): No NULL handling required; proceed with downstream validation.


-- ============================================================================  
-- PART 3: BUSINESS RULES & STATS (Distributions)  
-- ============================================================================  

-- 6. Distinct Values Check (Categorical Data)
-- WHY: Identify inconsistent, unexpected, or malformed categorical values.
-- IMPACT: Fragmented categories lead to incorrect grouping, inconsistent slicers, and unreliable segmentation in Power BI.
SELECT entry_type, COUNT(*) AS Category_Count
FROM bronze_layer.raw_hris_timesheets_v1
GROUP BY entry_type
ORDER BY Category_Count DESC;
-- RESULT: Only 2 valid categories found ('original', 'correction'); no malformed values detected.
-- ACTION (Silver Layer): Implement logic to correctly handle 'correction' records to avoid double-counting in downstream metrics.


-- 7. Basic Statistics (Hours Worked)
-- WHY: Validate numeric boundaries and overall distribution of working hours.
-- IMPACT: Invalid ranges (e.g., negative or excessive hours) skew aggregates and produce misleading KPI values.
SELECT 
    MIN(hours_worked) AS Min_Hours,
    MAX(hours_worked) AS Max_Hours,
    AVG(hours_worked) AS Avg_Hours,
    SUM(hours_worked) AS Total_Hours_Logged
FROM bronze_layer.raw_hris_timesheets_v1;
-- RESULT: Max hours (8.0) and average hours (~4.7) fall within expected operational ranges.
-- WARNING: Negative hours (-8.0) detected, indicating the presence of reversal or adjustment entries in the dataset.
-- ACTION (Silver Layer): Implement netting or adjustment logic to correctly aggregate positive and negative entries, ensuring accurate reporting of total working hours and utilization metrics.

-- 8. Outlier Detection (Hours Distribution)
-- WHY: Detect abnormal frequency patterns indicating system defaults or anomalous entries.
-- IMPACT: Outliers distort distributions, hide genuine trends, and reduce reliability of utilization analysis.
SELECT hours_worked, COUNT(*) AS Frequency
FROM bronze_layer.raw_hris_timesheets_v1
GROUP BY hours_worked
ORDER BY hours_worked;
-- RESULT: Working hour distribution is centered around standard values (8.0, 6.5, 5.0), with a smaller proportion of negative entries representing correction records. A substantial volume of 0.0-hour entries is also present.
-- WARNING: The high concentration of 0.0-hour records (~30%) indicates a mix of non-working days and system-generated placeholders, which can materially distort average-based performance metrics.
-- ACTION (Silver Layer): Retain all records and apply conditional logic in downstream models to appropriately handle 0.0-hour entries, ensuring accurate calculation of productivity and utilization metrics while preserving them for attendance-related analysis.

-- 9. Financial / Revenue Validation
-- WHY: Validate revenue and profit ranges for consistency and plausibility.
-- IMPACT: Extreme or invalid financial values distort aggregates and lead to inaccurate profitability insights.
SELECT 
    MIN(revenue_usd) AS Min_Revenue_USD,
    MAX(revenue_usd) AS Max_Revenue_USD,
    MIN(profit_usd) AS Min_Profit_USD,
    MAX(profit_usd) AS Max_Profit_USD
FROM bronze_layer.raw_hris_timesheets_v1;
-- RESULT: Revenue and profit values exhibit symmetric positive and negative ranges, indicating that financial amounts are consistently reversed in alignment with correction entries. No extreme out-of-bound values detected beyond expected operational limits.
-- WARNING: Presence of negative revenue and profit values may be misinterpreted as losses if viewed without context, although they represent valid reversal transactions.
-- ACTION (Silver Layer): Ensure aggregation logic applies netting of positive and negative financial entries. Downstream models must maintain consistency between hours, revenue, and profit calculations to preserve accurate financial reporting.


-- ============================================================================
-- PART 4: ZERO TRUST POLICY & TIME-SERIES PIPELINE
-- ============================================================================

-- 10. Logic Anomalies (Negative Hours or > 24 hours in a day)
-- WHY: Detect invalid time entries that violate business rules or indicate failed upstream validations.
-- IMPACT: Such values compromise utilization calculations, distort labor metrics, and introduce risk in compliance reporting.
SELECT 
    SUM(CASE WHEN hours_worked < 0 THEN 1 ELSE 0 END) AS Negative_Hours_Count,
    SUM(CASE WHEN hours_worked > 24 THEN 1 ELSE 0 END) AS Impossible_Daily_Hours
FROM bronze_layer.raw_hris_timesheets_v1;
-- RESULT: 50,908 negative hour records detected (confirming the volume of corrections). 0 records found with impossible daily hours (>24). Upstream system validation for daily limits is functioning correctly.
-- WARNING: Negative hour entries represent correction records and may distort utilization metrics if not handled correctly during aggregation.
-- ACTION (Silver Layer): No physical anomalies to drop. Implement financial netting logic for the 50,908 negative records.

-- 11. String Anomalies & Hidden Whitespaces
-- WHY: Identify inconsistencies caused by hidden or trailing characters in categorical fields.
-- IMPACT: Leads to fragmented groupings, duplicated categories, and unreliable aggregations in reporting layers.
SELECT entry_type, COUNT(*) AS Issue_Count
FROM bronze_layer.raw_hris_timesheets_v1
WHERE LENGTH(entry_type) != LENGTH(TRIM(entry_type))
GROUP BY entry_type;
-- RESULT: 0 rows returned. No hidden whitespaces or trailing characters detected in categorical fields.
-- ACTION (Silver Layer): No string manipulation or TRIM() functions required for entry_type. Data is clean.

-- 12. Data Freshness Check
-- WHY: Validate that the ingestion pipeline is delivering up-to-date transactional data.
-- IMPACT: Outdated data results in stale dashboards, delayed insights, and reduced trust in reporting systems.
SELECT MAX(work_date) AS Latest_Data_Date
FROM bronze_layer.raw_hris_timesheets_v1;
-- RESULT: Latest available data is 2026-03-27, corresponding to the initial manual ingestion. No recent data has been loaded, indicating the absence of an automated pipeline schedule.
-- ACTION (Pipeline): Current behavior is expected for a non-scheduled ingestion phase. Implement a scheduled ingestion process (e.g., CRON/GitHub Actions) and perform a one-time backfill to restore data continuity before production deployment.

-- 13. Date Boundaries (Future Dates)
-- WHY: Detect invalid future-dated records introduced by data entry errors or system issues.
-- IMPACT: Expands calendar ranges incorrectly, breaks time intelligence calculations, and skews period-based reporting.
SELECT 
    SUM(CASE WHEN work_date > CURRENT_DATE() THEN 1 ELSE 0 END) AS Future_Dates_Count
FROM bronze_layer.raw_hris_timesheets_v1;
-- RESULT: 0 future-dated records detected. 
-- ACTION (Silver Layer): No upper-bound date filtering required. Time intelligence DAX measures will function safely.

-- 14. Missing Dates Check (Time-Series Continuity)
-- WHY: Ensure continuity of daily data to confirm no ingestion gaps or missing batches.
-- IMPACT: Gaps in time-series data disrupt trend analysis, distort rolling metrics, and lead to incomplete reporting.
SELECT work_date, COUNT(*) AS Records
FROM bronze_layer.raw_hris_timesheets_v1
GROUP BY work_date
ORDER BY work_date;
-- RESULT: Time-series continuity is intact. The dataset spans 1098 consecutive days (2023-03-26 to 2026-03-27), with no missing dates detected.
-- ACTION (Silver Layer): No gap-handling or interpolation logic required. Proceed with standard date dimension joins and time-based aggregations.