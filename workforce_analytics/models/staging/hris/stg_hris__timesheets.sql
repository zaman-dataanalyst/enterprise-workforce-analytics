/*
================================================================================
Model   : stg_hris__timesheets
Layer   : Silver (Staging)
Project : Enterprise Workforce Analytics
Source  : enterprise-workforce-analytics.bronze_layer.raw_hris_timesheets_v1

EDA Summary (3,727,481 rows | 63 columns | 2023-03-26 → 2026-03-27):
  ✓ 0 duplicate timesheet_ids            ✓ 0 orphan correction records
  ✓ 0 broken is_correction flags         ✓ 0 future-dated entries
  ✓ 0 hours math violations              ✓ 0 profit math violations
  ✗ 310K+ rows with whitespace           ✗ 187K+ corrupt employee names
  ✗ 205K+ corrupt designations           ✗ 326K+ corrupt task categories
  ✗ 372K rows with P0–P3 priority scale  ✗ 291K non-standard currency codes
  ✗ 277K city typos / corruptions        ✗ 50,908 correction records (VALID)
  ⚠ 70.88% skill_designation_mismatch   → Systemic column swap; flagged only

Cleaning Logic (4 CTE pipeline):
  1. trim_nullify        → TRIM + NULLIF('null') + NULLIF('')
  2. fix_text_corruption → REGEXP_REPLACE digits→'I' + targeted REPLACE typos
  3. standardize         → CASE WHEN for all categorical dimensions
  4. Final SELECT        → Adds derived/computed columns

STRICT BUSINESS RULES:
  - original_timesheet_id → NEVER imputed (self-referencing FK; NULL = factual state)
  - project_name          → NO digit-to-letter regex (contains valid '24/7', '3rdgeneration')
  - Negative hours        → KEEP (valid correction records for Gold layer netting)
  - utilization_pct       → 0–1 decimal scale confirmed; do NOT multiply by 100
================================================================================
*/

{{ config(
    materialized = 'view',
    tags         = ['hris', 'timesheets', 'silver'],
    description  = 'Silver staging: cleanses text corruption, standardizes categoricals, and adds DQ flags for the HRIS timesheet bronze table.'
) }}

WITH source AS (

    SELECT * FROM {{ source('bronze_layer', 'raw_hris_timesheets_v1') }}

),

-- ─────────────────────────────────────────────────────────────────────────────
-- CTE 1 | trim_nullify
-- Goal : Strip whitespace and convert both string 'null' and empty-string ''
--        artifacts to TRUE NULL, enabling consistent downstream IS NULL logic.
-- Note : IDs, numerics, booleans, and timestamps are passed through unchanged
--        (EDA confirmed these columns are structurally clean).
-- ─────────────────────────────────────────────────────────────────────────────
trim_nullify AS (

    SELECT
        -- ── Identity ──────────────────────────────────────────────────────────
        CAST(timesheet_id  AS STRING) AS timesheet_id,
        CAST(emp_id AS STRING) AS employee_id,
        CAST(manager_id    AS STRING) AS manager_id,
        CAST(project_id    AS STRING) AS project_id,
        CAST(client_id     AS STRING) AS client_id,
        CAST(work_date     AS DATE)   AS work_date,

        -- ── Employee Dimensions ───────────────────────────────────────────────
        NULLIF(NULLIF(TRIM(employee_name),    ''), 'null') AS employee_name,
        NULLIF(NULLIF(TRIM(designation),      ''), 'null') AS designation,
        NULLIF(NULLIF(TRIM(seniority_level),  ''), 'null') AS seniority_level,
        NULLIF(NULLIF(TRIM(employment_type),  ''), 'null') AS employment_type,
        NULLIF(NULLIF(TRIM(employment_status),''), 'null') AS employment_status,
        NULLIF(NULLIF(TRIM(department),       ''), 'null') AS department,
        NULLIF(NULLIF(TRIM(cost_center),      ''), 'null') AS cost_center,
        NULLIF(NULLIF(TRIM(skill_primary),    ''), 'null') AS skill_primary,
        CAST(experience_years AS INT64)                    AS experience_years,

        -- ── Location ─────────────────────────────────────────────────────────
        NULLIF(NULLIF(TRIM(location_country),  ''), 'null') AS location_country,
        NULLIF(NULLIF(TRIM(location_city),     ''), 'null') AS location_city,
        NULLIF(NULLIF(TRIM(location_region),   ''), 'null') AS location_region,
        NULLIF(NULLIF(TRIM(location_timezone), ''), 'null') AS location_timezone,

        -- ── Compensation ──────────────────────────────────────────────────────
        CAST(salary_local AS FLOAT64)                      AS salary_local,
        NULLIF(NULLIF(TRIM(salary_currency), ''), 'null')  AS salary_currency,

        -- ── Project Dimensions ────────────────────────────────────────────────
        NULLIF(NULLIF(TRIM(project_name),       ''), 'null') AS project_name,
        NULLIF(NULLIF(TRIM(project_type),       ''), 'null') AS project_type,
        NULLIF(NULLIF(TRIM(project_status),     ''), 'null') AS project_status,
        NULLIF(NULLIF(TRIM(project_phase),      ''), 'null') AS project_phase,
        NULLIF(NULLIF(TRIM(project_priority),   ''), 'null') AS project_priority,
        NULLIF(NULLIF(TRIM(project_manager_id), ''), 'null') AS project_manager_id,

        -- ── Client Dimensions ─────────────────────────────────────────────────
        NULLIF(NULLIF(TRIM(client_name),     ''), 'null') AS client_name,
        NULLIF(NULLIF(TRIM(client_industry), ''), 'null') AS client_industry,
        NULLIF(NULLIF(TRIM(client_segment),  ''), 'null') AS client_segment,
        NULLIF(NULLIF(TRIM(client_size),     ''), 'null') AS client_size,
        NULLIF(NULLIF(TRIM(client_country),  ''), 'null') AS client_country,
        NULLIF(NULLIF(TRIM(client_region),   ''), 'null') AS client_region,

        -- ── Activity Dimensions ───────────────────────────────────────────────
        NULLIF(NULLIF(TRIM(task_category), ''), 'null') AS task_category,
        NULLIF(NULLIF(TRIM(revenue_type),  ''), 'null') AS revenue_type,

        -- ── Numeric Metrics (EDA confirmed structurally clean) ────────────────
        CAST(hours_worked       AS FLOAT64) AS hours_worked,       -- Negatives = valid corrections
        CAST(capacity_hours     AS FLOAT64) AS capacity_hours,
        CAST(billable_hours     AS FLOAT64) AS billable_hours,
        CAST(non_billable_hours AS FLOAT64) AS non_billable_hours,
        CAST(available_hours    AS FLOAT64) AS available_hours,
        CAST(utilization_pct    AS FLOAT64) AS utilization_pct,    -- Confirmed 0–1 scale; no /100 needed
        NULLIF(NULLIF(TRIM(utilization_category), ''), 'null')     AS utilization_category,
        CAST(allocation_pct     AS FLOAT64) AS allocation_pct,     -- Confirmed 0–1 scale

        -- ── Financials ────────────────────────────────────────────────────────
        CAST(hourly_rate_usd AS FLOAT64) AS hourly_rate_usd,
        CAST(cost_rate_local AS FLOAT64) AS cost_rate_local,
        CAST(revenue_usd     AS FLOAT64) AS revenue_usd,
        CAST(cost_usd        AS FLOAT64) AS cost_usd,
        CAST(profit_usd      AS FLOAT64) AS profit_usd,    -- Confirmed: profit = revenue − cost ✓
        CAST(revenue_local   AS FLOAT64) AS revenue_local,
        CAST(cost_local      AS FLOAT64) AS cost_local,
        CAST(fx_rate         AS FLOAT64) AS fx_rate,       -- Daily rate snapshots; variance is expected
        NULLIF(NULLIF(TRIM(fx_source), ''), 'null')        AS fx_source,

        -- ── Quality & Control Flags ───────────────────────────────────────────
        NULLIF(NULLIF(TRIM(data_quality_flag), ''), 'null') AS data_quality_flag,
        CAST(is_billable   AS BOOL) AS is_billable,
        CAST(is_holiday    AS BOOL) AS is_holiday,
        CAST(is_anomaly    AS BOOL) AS is_anomaly,
        CAST(is_correction AS BOOL) AS is_correction,
        NULLIF(NULLIF(TRIM(entry_type), ''), 'null')        AS entry_type,

        -- ── Correction Lineage (STRICT: DO NOT IMPUTE) ───────────────────────
        -- NULL here = factual absence of a parent (all 'original' records).
        -- Imputing this would cause a Cartesian explosion in Gold self-joins.
        original_timesheet_id,

        -- ── Audit Timestamps ──────────────────────────────────────────────────
        record_created_at,
        record_updated_at,
        load_timestamp,
        NULLIF(NULLIF(TRIM(batch_id), ''), 'null') AS batch_id

    FROM source

),

-- ─────────────────────────────────────────────────────────────────────────────
-- CTE 2 | fix_text_corruption
-- Goal : Repair the systemic digit-to-letter corruption where '1' was
--        substituted for 'i' or 'e' in the source HRIS system.
--
-- Pattern: UPPER(col) → REGEXP_REPLACE(r'[0-9]', 'I') → REPLACE(known_typos)
--          → COALESCE(result, fallback_placeholder)
--
-- CRITICAL EXCEPTION: project_name is excluded from REGEXP_REPLACE because it
-- contains valid business numbers ('24/7', '3rdgeneration', '6thgeneration').
-- Only non-digit typos are corrected in project_name.
-- ─────────────────────────────────────────────────────────────────────────────
fix_text_corruption AS (

    SELECT
        -- ── Pass-through: IDs / Numerics / Booleans / Timestamps / Clean cols ─
        timesheet_id,
        employee_id,
        manager_id,
        project_id,
        client_id,
        work_date,
        seniority_level,
        cost_center,
        experience_years,
        location_region,
        location_timezone,
        project_phase,
        project_manager_id,
        client_size,
        client_region,
        revenue_type,
        salary_local,
        hours_worked,
        capacity_hours,
        billable_hours,
        non_billable_hours,
        available_hours,
        utilization_pct,
        utilization_category,
        allocation_pct,
        hourly_rate_usd,
        cost_rate_local,
        revenue_usd,
        cost_usd,
        profit_usd,
        revenue_local,
        cost_local,
        fx_rate,
        fx_source,
        data_quality_flag,
        is_billable,
        is_holiday,
        is_anomaly,
        is_correction,
        entry_type,
        original_timesheet_id,   -- NEVER touched
        record_created_at,
        record_updated_at,
        load_timestamp,
        batch_id,

        -- ── Categoricals standardized in CTE 3 (pass raw for now) ────────────
        employment_type,
        employment_status,
        project_type,
        project_status,
        project_priority,
        salary_currency,
        location_country,
        client_country,
        client_segment,

        -- ══════════════════════════════════════════════════════════════════════
        -- TEXT CORRUPTION REPAIR
        -- ══════════════════════════════════════════════════════════════════════

        -- ── employee_name ─────────────────────────────────────────────────────
        COALESCE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                UPPER(REGEXP_REPLACE(employee_name, r'[0-9]', 'I')),
            'MICHAAL',     'MICHAEL'),
            'CHRISTOPHAR', 'CHRISTOPHER'),
            'ANGALA',      'ANGELA'),
            'ROBART',      'ROBERT'),
            'JANNIFER',    'JENNIFER'),
            'JOSAPH',      'JOSEPH'),
            'DANIAL',      'DANIEL'),
            'JASSICA',     'JESSICA'),
            'KANNETH',     'KENNETH'),
            'MALISSA',     'MELISSA'),
            'HAATHER',     'HEATHER'),
            'KATHLAEN',    'KATHLEEN'),
            'ANDARSON',    'ANDERSON'),
            'RACHAAL',     'RACHAEL'),
            'DANISE',      'DENISE'),
            'RODRIGUAZ',   'RODRIGUEZ'),
            'BA1LAY',      'BAILEY'),      -- pre-regex form just in case
            'WHITA',       'WHITE'),
            'FISCHAR',     'FISCHER'),
            'FARGUSON',    'FERGUSON')
        , 'ANONYMOUS') AS employee_name,

        -- ── designation ───────────────────────────────────────────────────────
        COALESCE(
            REPLACE(
                UPPER(REGEXP_REPLACE(designation, r'[0-9]', 'I')),
            'ENGINAER', 'ENGINEER')
        , 'UNKNOWN') AS designation,

        -- ── skill_primary ─────────────────────────────────────────────────────
        COALESCE(
            REPLACE(
                UPPER(REGEXP_REPLACE(skill_primary, r'[0-9]', 'I')),
            'ENGINAER', 'ENGINEER')
        , 'UNKNOWN') AS skill_primary,

        -- ── department ────────────────────────────────────────────────────────
        COALESCE(
            REPLACE(REPLACE(REPLACE(
                UPPER(REGEXP_REPLACE(department, r'[0-9]', 'I')),
            'ENGINAERING', 'ENGINEERING'),
            'SCIANCE',     'SCIENCE'),
            'ANALYTACS',   'ANALYTICS')
        , 'UNKNOWN') AS department,

        -- ── task_category ─────────────────────────────────────────────────────
        COALESCE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                UPPER(REGEXP_REPLACE(task_category, r'[0-9]', 'I')),
            'INTARNAL',    'INTERNAL'),
            'MAETING',     'MEETING'),
            'DAPLOYMENT',  'DEPLOYMENT'),
            'DAVELOPMENT', 'DEVELOPMENT'),
            'LAAVE',       'LEAVE')
        , 'UNKNOWN') AS task_category,

        -- ── location_city ─────────────────────────────────────────────────────
        COALESCE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                UPPER(REGEXP_REPLACE(location_city, r'[0-9]', 'I')),
            'LAHORA',    'LAHORE'),
            'BANGALORA', 'BANGALORE'),
            'NAW YORK',  'NEW YORK'),
            'SINGAPORA', 'SINGAPORE'),
            'BARLIN',    'BERLIN')
        , 'UNKNOWN') AS location_city,

        -- ── client_name ───────────────────────────────────────────────────────
        COALESCE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(
                UPPER(REGEXP_REPLACE(client_name, r'[0-9]', 'I')),
            'RIVARA',      'RIVERA'),
            'MARTINAZ',    'MARTINEZ'),
            'SANCHAZ',     'SANCHEZ'),
            'FITZGARALD',  'FITZGERALD'),
            'GILBART',     'GILBERT'),
            'HANDERSON',   'HENDERSON'),
            'FISHAR',      'FISHER'),
            'FOWLAR',      'FOWLER'),
            'COOPAR',      'COOPER'),
            'NALSON',      'NELSON'),
            'MICHAAL',     'MICHAEL'),
            'PRICA',       'PRICE')
        , 'INTERNAL') AS client_name,

        -- ── client_industry ───────────────────────────────────────────────────
        COALESCE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                UPPER(REGEXP_REPLACE(client_industry, r'[0-9]', 'I')),
            'INTARNAL',   'INTERNAL'),
            'FINTACH',    'FINTECH'),
            'EDTACH',     'EDTECH'),
            'RATAIL',     'RETAIL'),
            'HAALTHCARE', 'HEALTHCARE')
        , 'UNKNOWN') AS client_industry,

        -- ── project_name (NO digit regex — preserves '24/7', '3rdgeneration') ─
        COALESCE(
            REPLACE(REPLACE(
                UPPER(TRIM(project_name)),
            'BANCH',          'BENCH'),
            'IMPLAMENTATION', 'IMPLEMENTATION')
        , 'BENCH') AS project_name

    FROM trim_nullify

),

-- ─────────────────────────────────────────────────────────────────────────────
-- CTE 3 | standardize_categoricals
-- Goal : Apply CASE WHEN mappings to unify all dimensions that had multiple
--        surface representations of the same logical value.
--
-- All categorical outputs use consistent UPPER CASE for reliable Power BI
-- filtering and dbt accepted_values tests.
-- ─────────────────────────────────────────────────────────────────────────────
standardize_categoricals AS (

    SELECT
        -- ── Pass-through all cleaned text + numeric columns ───────────────────
        timesheet_id,
        employee_id,
        manager_id,
        project_id,
        client_id,
        work_date,
        employee_name,
        seniority_level,
        department,
        cost_center,
        skill_primary,
        experience_years,
        location_timezone,
        salary_local,
        project_name,
        project_manager_id,
        client_name,
        client_industry,
        client_size,
        task_category,
        revenue_type,
        hours_worked,
        capacity_hours,
        billable_hours,
        non_billable_hours,
        available_hours,
        utilization_pct,
        utilization_category,
        allocation_pct,
        hourly_rate_usd,
        cost_rate_local,
        revenue_usd,
        cost_usd,
        profit_usd,
        revenue_local,
        cost_local,
        fx_rate,
        fx_source,
        data_quality_flag,
        is_billable,
        is_holiday,
        is_anomaly,
        is_correction,
        entry_type,
        original_timesheet_id,
        record_created_at,
        record_updated_at,
        load_timestamp,
        batch_id,
        location_city,

        -- ── Employment Type (7 variants → 2 canonical values) ─────────────────
        CASE UPPER(TRIM(employment_type))
            WHEN 'CONTRACTOR' THEN 'CONTRACTOR'
            WHEN 'CONTRACT'   THEN 'CONTRACTOR'
            WHEN 'CWR'        THEN 'CONTRACTOR'
            WHEN 'FULL-TIME'  THEN 'FULL-TIME'
            WHEN 'FULL TIME'  THEN 'FULL-TIME'
            WHEN 'FT'         THEN 'FULL-TIME'
            WHEN 'FTE'        THEN 'FULL-TIME'
            ELSE COALESCE(UPPER(TRIM(employment_type)), 'UNKNOWN')
        END AS employment_type,

        -- ── Employment Status (3 variants → 1 active value) ──────────────────
        CASE UPPER(TRIM(employment_status))
            WHEN 'ACTIVE'     THEN 'ACTIVE'
            WHEN 'ACT'        THEN 'ACTIVE'
            WHEN 'INACTIVE'   THEN 'INACTIVE'
            WHEN 'TERM'       THEN 'INACTIVE'
            WHEN 'TERMINATED' THEN 'INACTIVE'
            ELSE COALESCE(UPPER(TRIM(employment_status)), 'UNKNOWN')
        END AS employment_status,

        -- ── Designation (5 canonical roles) ──────────────────────────────────
        CASE
            WHEN REGEXP_CONTAINS(LOWER(TRIM(REGEXP_REPLACE(designation, r'[0-9]', 'i'))), r'data.*engineer|engineer.*data') THEN 'DATA ENGINEER'
            WHEN REGEXP_CONTAINS(LOWER(TRIM(REGEXP_REPLACE(designation, r'[0-9]', 'i'))), r'data.*analyst|analyst')          THEN 'DATA ANALYST'
            WHEN REGEXP_CONTAINS(LOWER(TRIM(REGEXP_REPLACE(designation, r'[0-9]', 'i'))), r'qa.*engineer|quality')            THEN 'QA ENGINEER'
            WHEN REGEXP_CONTAINS(LOWER(TRIM(REGEXP_REPLACE(designation, r'[0-9]', 'i'))), r'bi.*d[ae]v|business.*intel')         THEN 'BI DEVELOPER'
            WHEN REGEXP_CONTAINS(LOWER(TRIM(REGEXP_REPLACE(designation, r'[0-9]', 'i'))), r'data.*sci|scientist')             THEN 'DATA SCIENTIST'
            ELSE COALESCE(UPPER(TRIM(designation)), 'UNKNOWN')
        END AS designation,

        -- ── Project Type (9 variants → 3 canonical values) ───────────────────
        CASE UPPER(TRIM(project_type))
            WHEN 'T&M'               THEN 'T&M'
            WHEN 'TIME & MATERIAL'   THEN 'T&M'
            WHEN 'TIME AND MATERIAL' THEN 'T&M'
            WHEN 'FIXED'             THEN 'FIXED PRICE'
            WHEN 'FIXED PRICE'       THEN 'FIXED PRICE'
            WHEN 'F.P'               THEN 'FIXED PRICE'
            WHEN 'FP'                THEN 'FIXED PRICE'
            WHEN 'INTERNAL'          THEN 'INTERNAL'
            ELSE COALESCE(UPPER(TRIM(project_type)), 'UNKNOWN')
        END AS project_type,

        -- ── Project Status (9 variants → 3 canonical values) ────────────────
        CASE UPPER(TRIM(project_status))
            WHEN 'ACTIVE'    THEN 'ACTIVE'
            WHEN 'ACT'       THEN 'ACTIVE'
            WHEN 'COMPLETED' THEN 'COMPLETED'
            WHEN 'DONE'      THEN 'COMPLETED'
            WHEN 'CLOSED'    THEN 'COMPLETED'
            WHEN 'ON HOLD'   THEN 'ON HOLD'
            WHEN 'ON-HOLD'   THEN 'ON HOLD'
            WHEN 'ON_HOLD'   THEN 'ON HOLD'
            WHEN 'PAUSED'    THEN 'ON HOLD'
            ELSE COALESCE(UPPER(TRIM(project_status)), 'UNKNOWN')
        END AS project_status,

        -- ── Project Priority (CRITICAL: unifies two separate scales) ──────────
        CASE UPPER(TRIM(project_priority))
            WHEN 'CRITICAL' THEN 'CRITICAL'
            WHEN 'P0'       THEN 'CRITICAL'
            WHEN 'HIGH'     THEN 'HIGH'
            WHEN 'P1'       THEN 'HIGH'
            WHEN 'MEDIUM'   THEN 'MEDIUM'
            WHEN 'MED'      THEN 'MEDIUM'
            WHEN 'P2'       THEN 'MEDIUM'
            WHEN 'LOW'      THEN 'LOW'
            WHEN 'P3'       THEN 'LOW'
            ELSE COALESCE(UPPER(TRIM(project_priority)), 'UNKNOWN')
        END AS project_priority,

        -- ── Salary Currency (22 variants → 9 ISO codes) ───────────────────────
        CASE UPPER(TRIM(salary_currency))
            WHEN 'PKR' THEN 'PKR'
            WHEN 'RS'  THEN 'PKR'
            WHEN 'RS.' THEN 'PKR'
            WHEN 'INR' THEN 'INR'
            WHEN '₹'   THEN 'INR'
            WHEN 'GBP' THEN 'GBP'
            WHEN '£'   THEN 'GBP'
            WHEN 'EUR' THEN 'EUR'
            WHEN '€'   THEN 'EUR'
            WHEN 'USD' THEN 'USD'
            WHEN '$'   THEN 'USD'
            WHEN 'AED' THEN 'AED'
            WHEN 'CAD' THEN 'CAD'
            WHEN 'SGD' THEN 'SGD'
            WHEN 'SAR' THEN 'SAR'
            ELSE COALESCE(UPPER(TRIM(salary_currency)), 'UNKNOWN')
        END AS salary_currency,

        -- ── Location Country (Strict ISO-3166 Full Names) ────────────────────
        -- WHY: Full standard names are required for optimal Geocoding API rendering in BI tools.
        CASE REGEXP_REPLACE(UPPER(TRIM(location_country)), r'\.', '')
            WHEN 'PK'            THEN 'Pakistan'
            WHEN 'PAK'           THEN 'Pakistan'
            WHEN 'PAKISTAN'      THEN 'Pakistan'
            WHEN 'IN'            THEN 'India'
            WHEN 'IND'           THEN 'India'
            WHEN 'INDIA'         THEN 'India'
            WHEN 'UK'            THEN 'United Kingdom'
            WHEN 'GB'            THEN 'United Kingdom'
            WHEN 'UNITEDKINGDOM' THEN 'United Kingdom'
            WHEN 'UAE'           THEN 'United Arab Emirates'
            WHEN 'UAR'           THEN 'United Arab Emirates'
            WHEN 'USA'           THEN 'United States'
            WHEN 'US'            THEN 'United States'
            WHEN 'UNITEDSTATES'  THEN 'United States'
            WHEN 'SA'            THEN 'Saudi Arabia'
            WHEN 'KSA'           THEN 'Saudi Arabia'
            WHEN 'SAUDIARABIA'   THEN 'Saudi Arabia'
            WHEN 'CA'            THEN 'Canada'
            WHEN 'CAN'           THEN 'Canada'
            WHEN 'CANADA'        THEN 'Canada'
            WHEN 'DE'            THEN 'Germany'
            WHEN 'DEU'           THEN 'Germany'
            WHEN 'GERMANY'       THEN 'Germany'
            WHEN 'SG'            THEN 'Singapore'
            WHEN 'SGP'           THEN 'Singapore'
            WHEN 'SINGAPORE'     THEN 'Singapore'
            WHEN 'AU'            THEN 'Australia'
            WHEN 'AUS'           THEN 'Australia'
            WHEN 'AUSTRALIA'     THEN 'Australia'
            WHEN 'SE'            THEN 'Sweden'
            WHEN 'SWE'           THEN 'Sweden'
            WHEN 'SWEDEN'        THEN 'Sweden'
            WHEN 'NL'            THEN 'Netherlands'
            WHEN 'NLD'           THEN 'Netherlands'
            WHEN 'NETHERLANDS'   THEN 'Netherlands'
            WHEN 'QA'            THEN 'Qatar'
            WHEN 'QAT'           THEN 'Qatar'
            WHEN 'QATAR'         THEN 'Qatar'
            ELSE 'UNKNOWN'  -- STRICT DQ: Explicit trap for unmapped anomalies
        END AS location_country,

        -- ── Client Country (Strict ISO-3166 Full Names) ──────────────────────
        CASE REGEXP_REPLACE(UPPER(TRIM(client_country)), r'\.', '')
            WHEN 'PK'            THEN 'Pakistan'
            WHEN 'PAK'           THEN 'Pakistan'
            WHEN 'PAKISTAN'      THEN 'Pakistan'
            WHEN 'IN'            THEN 'India'
            WHEN 'IND'           THEN 'India'
            WHEN 'INDIA'         THEN 'India'
            WHEN 'UK'            THEN 'United Kingdom'
            WHEN 'GB'            THEN 'United Kingdom'
            WHEN 'UNITEDKINGDOM' THEN 'United Kingdom'
            WHEN 'UAE'           THEN 'United Arab Emirates'
            WHEN 'UAR'           THEN 'United Arab Emirates'
            WHEN 'USA'           THEN 'United States'
            WHEN 'US'            THEN 'United States'
            WHEN 'UNITEDSTATES'  THEN 'United States'
            WHEN 'SA'            THEN 'Saudi Arabia'
            WHEN 'KSA'           THEN 'Saudi Arabia'
            WHEN 'SAUDIARABIA'   THEN 'Saudi Arabia'
            WHEN 'CA'            THEN 'Canada'
            WHEN 'CAN'           THEN 'Canada'
            WHEN 'CANADA'        THEN 'Canada'
            WHEN 'DE'            THEN 'Germany'
            WHEN 'DEU'           THEN 'Germany'
            WHEN 'GERMANY'       THEN 'Germany'
            WHEN 'SG'            THEN 'Singapore'
            WHEN 'SGP'           THEN 'Singapore'
            WHEN 'SINGAPORE'     THEN 'Singapore'
            WHEN 'AU'            THEN 'Australia'
            WHEN 'AUS'           THEN 'Australia'
            WHEN 'AUSTRALIA'     THEN 'Australia'
            WHEN 'SE'            THEN 'Sweden'
            WHEN 'SWE'           THEN 'Sweden'
            WHEN 'SWEDEN'        THEN 'Sweden'
            WHEN 'NL'            THEN 'Netherlands'
            WHEN 'NLD'           THEN 'Netherlands'
            WHEN 'NETHERLANDS'   THEN 'Netherlands'
            WHEN 'QA'            THEN 'Qatar'
            WHEN 'QAT'           THEN 'Qatar'
            WHEN 'QATAR'         THEN 'Qatar'
            ELSE 'UNKNOWN'  -- STRICT DQ: Explicit trap for unmapped anomalies
        END AS client_country,

        -- ── Location Region (Corporate Canonical Abbreviations) ────────────────
        -- WHY: Standardizing to global corporate acronyms. 'NA' is explicitly mapped 
        --      to 'AMER' to prevent Null-parsing bugs in downstream Python/BI layers.
        CASE UPPER(TRIM(location_region))
            WHEN 'NA'            THEN 'AMER'
            WHEN 'NAMER'         THEN 'AMER'
            WHEN 'AMER'          THEN 'AMER'
            WHEN 'NORTH AMERICA' THEN 'AMER'
            WHEN 'EMEA'          THEN 'EMEA'
            WHEN 'EU'            THEN 'EMEA'
            WHEN 'APAC'          THEN 'APAC'
            WHEN 'LATAM'         THEN 'LATAM'
            WHEN 'MEA'           THEN 'MEA'
            ELSE 'UNKNOWN'  -- STRICT DQ: Explicit trap for unmapped anomalies
        END AS location_region,

        -- ── Client Region (Corporate Canonical Abbreviations) ──────────────────
        CASE UPPER(TRIM(client_region))
            WHEN 'NA'            THEN 'AMER'
            WHEN 'NAMER'         THEN 'AMER'
            WHEN 'AMER'          THEN 'AMER'
            WHEN 'NORTH AMERICA' THEN 'AMER'
            WHEN 'EMEA'          THEN 'EMEA'
            WHEN 'EU'            THEN 'EMEA'
            WHEN 'APAC'          THEN 'APAC'
            WHEN 'LATAM'         THEN 'LATAM'
            WHEN 'MEA'           THEN 'MEA'
            ELSE 'UNKNOWN'  -- STRICT DQ: Explicit trap for unmapped anomalies
        END AS client_region,

        -- ── Client Segment (13 variants → 5 canonical values) ─────────────────
        CASE UPPER(TRIM(client_segment))
            WHEN 'B2B'                  THEN 'B2B'
            WHEN 'BUSINESS TO BUSINESS' THEN 'B2B'
            WHEN 'B2C'                  THEN 'B2C'
            WHEN 'CONSUMER'             THEN 'B2C'
            WHEN 'B2G'                  THEN 'B2G'
            WHEN 'GOVERNMENT'           THEN 'B2G'
            WHEN 'GOVT'                 THEN 'B2G'
            WHEN 'CORPORATE'            THEN 'CORPORATE'
            WHEN 'INTERNAL'             THEN 'INTERNAL'
            ELSE COALESCE(UPPER(TRIM(client_segment)), 'UNKNOWN')
        END AS client_segment,

-- ── Project Phase (Strict ISO Full Names & DQ Fix) ───────────────────
        CASE UPPER(TRIM(project_phase))
            WHEN 'N/A' THEN 'UNKNOWN'
            ELSE COALESCE(INITCAP(TRIM(project_phase)), 'UNKNOWN')
        END AS project_phase

    FROM fix_text_corruption
)

-- ─────────────────────────────────────────────────────────────────────────────
-- Final SELECT | Derived / Computed Columns
-- Adds business-intelligence helper flags that support Gold layer logic and
-- Power BI report filtering without requiring additional SQL.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT

    -- ── All standardized columns ──────────────────────────────────────────────
    timesheet_id,
    employee_id,
    manager_id,
    project_id,
    client_id,
    work_date,
    employee_name,
    designation,
    seniority_level,
    employment_type,
    employment_status,
    department,
    cost_center,
    skill_primary,
    experience_years,
    location_country,
    location_city,
    location_region,
    location_timezone,
    salary_local,
    salary_currency,
    project_name,
    project_type,
    project_status,
    project_phase,
    project_priority,
    project_manager_id,
    client_name,
    client_industry,
    client_segment,
    client_size,
    client_country,
    client_region,
    task_category,
    revenue_type,
    hours_worked,
    capacity_hours,
    billable_hours,
    non_billable_hours,
    available_hours,
    utilization_pct,
    utilization_category,
    allocation_pct,
    hourly_rate_usd,
    cost_rate_local,
    revenue_usd,
    cost_usd,
    profit_usd,
    revenue_local,
    cost_local,
    fx_rate,
    fx_source,
    data_quality_flag,
    is_billable,
    is_holiday,
    is_anomaly,
    is_correction,
    entry_type,
    original_timesheet_id,
    record_created_at,
    record_updated_at,
    load_timestamp,
    batch_id,

    -- ── Derived: Bench record indicator ──────────────────────────────────────
    -- Simplifies bench vs billable filtering in all downstream models.
    (project_id = 'BENCH') AS is_bench_record,

    -- ── Derived: Skill ↔ Designation mismatch flag ────────────────────────────
    -- EDA FINDING: 70.88% of rows (2.5M+) have mismatched skill_primary and
    -- designation. This is a SYSTEMIC source-system column swap, NOT per-row
    -- data entry errors. Auto-correction is NOT safe without HR sign-off.
    -- ACTION: Gold layer dim_employees should use emp_id → HRIS master record
    --         to establish canonical designation and skill mappings.
    (
        skill_primary IS NOT NULL
        AND designation   IS NOT NULL
        AND skill_primary != designation
    ) AS is_skill_designation_mismatch,

    -- ── Derived: Valid negative-hour correction flag ──────────────────────────
    -- EDA confirmed: all 50,908 correction records are properly flagged.
    -- These are valid financial netting entries, NOT data errors.
    -- Gold layer aggregation MUST net these against their original entries.
    (LOWER(entry_type) = 'correction' AND hours_worked < 0) AS is_negative_hour_correction

FROM standardize_categoricals