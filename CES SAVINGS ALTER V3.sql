-- ============================================================
-- CES SAVINGS DATA WAREHOUSE
-- ALTER SCRIPTS V3
-- Oracle 21c
-- Date     : 2026-06-04
-- Based on actual tables in database
-- Safe approach: no constraint/index name dependencies
-- ============================================================
-- Tables in database:
-- CES_SAVINGS_DIM_PERIOD
-- CES_SAVINGS_DIM_VENDOR             (kept as is)
-- CES_SAVINGS_ETL_BATCH_LOG
-- CES_SAVINGS_ETL_FILE_LOG
-- CES_SAVINGS_ETL_ROW_LOG
-- CES_SAVINGS_FACT_ACTUALS
-- CES_SAVINGS_FACT_ACTUALS_ARCH
-- CES_SAVINGS_FACT_FORECAST
-- CES_SAVINGS_FACT_FORECAST_ARCH
-- CES_SAVINGS_STG_ACTUALS
-- CES_SAVINGS_STG_FORECAST
-- ============================================================
-- Changes:
-- 1. CES_SAVINGS_DIM_PERIOD
--    DROP   : DATA_DATE, TRIENNIUM, PROGRAM_YEAR
--             DATA_YEAR_MONTH, UPDATED_DATE, UPDATED_BY
--    MODIFY : PERIOD_SK convention YYYYMMDD
--
-- 2. CES_SAVINGS_DIM_VENDOR
--    DROP   : PROGRAM_YEAR
--    ADD    : VENDOR_NAME (if not exists)
--
-- 3. CES_SAVINGS_FACT_ACTUALS
--    ADD    : DATA_DATE DATE
--
-- 4. CES_SAVINGS_FACT_FORECAST
--    ADD    : DATA_DATE DATE
--
-- 5. CES_SAVINGS_FACT_ACTUALS_ARCH
--    ADD    : DATA_DATE DATE
--
-- 6. CES_SAVINGS_FACT_FORECAST_ARCH
--    ADD    : DATA_DATE DATE
--
-- 7. Recreate all views using CES_SAVINGS_DIM_VENDOR
-- ============================================================
-- IMPORTANT: Run verification queries at bottom first
--            to check current state before altering
-- ============================================================


-- ============================================================
-- STEP 0: VERIFY CURRENT STATE
-- Run these first before any alterations
-- Comment out after verification
-- ============================================================

-- Check what columns exist currently
-- SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
-- FROM   USER_TAB_COLUMNS
-- WHERE  TABLE_NAME = 'CES_SAVINGS_DIM_PERIOD'
-- ORDER BY COLUMN_ID;

-- SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
-- FROM   USER_TAB_COLUMNS
-- WHERE  TABLE_NAME = 'CES_SAVINGS_DIM_VENDOR'
-- ORDER BY COLUMN_ID;

-- SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
-- FROM   USER_TAB_COLUMNS
-- WHERE  TABLE_NAME = 'CES_SAVINGS_FACT_ACTUALS'
-- ORDER BY COLUMN_ID;

-- Check if tables have data
-- SELECT 'DIM_PERIOD'    , COUNT(*) FROM CES_SAVINGS_DIM_PERIOD    UNION ALL
-- SELECT 'DIM_VENDOR'    , COUNT(*) FROM CES_SAVINGS_DIM_VENDOR    UNION ALL
-- SELECT 'FACT_ACTUALS'  , COUNT(*) FROM CES_SAVINGS_FACT_ACTUALS  UNION ALL
-- SELECT 'FACT_FORECAST' , COUNT(*) FROM CES_SAVINGS_FACT_FORECAST;


-- ============================================================
-- STEP 1: CES_SAVINGS_DIM_PERIOD
-- Drop columns that do not belong in a pure time dimension
-- Safe: Oracle handles dependent constraints automatically
-- ============================================================

-- Drop DATA_DATE
-- Reason: DATA_DATE belongs in FACT not DIM
--         FACT derives it from filename YYYY_MM
--         Power BI reads it from FACT directly
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN DATA_DATE;

-- Drop TRIENNIUM
-- Reason: Program concept not time concept
--         Belongs in DIM_VENDOR with other program attributes
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN TRIENNIUM;

-- Drop PROGRAM_YEAR
-- Reason: Derivable from PERIOD_YEAR
--         No need to store separately
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN PROGRAM_YEAR;

-- Drop DATA_YEAR_MONTH
-- Reason: PERIOD_SK in YYYYMMDD format replaces this
--         Redundant storage
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN DATA_YEAR_MONTH;

-- Drop UPDATED_DATE
-- Reason: Period rows are immutable
--         Once created a period never changes
--         No update tracking needed
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN UPDATED_DATE;

-- Drop UPDATED_BY
-- Same reason as UPDATED_DATE
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN UPDATED_BY;

-- Update table comment
COMMENT ON TABLE CES_SAVINGS_DIM_PERIOD IS
    'V3: Pure time dimension. Strictly date attributes only. TRIENNIUM and PROGRAM_YEAR moved to DIM_VENDOR. DATA_DATE moved to FACT tables. PERIOD_SK format YYYYMMDD day always 01 for monthly grain e.g. 20260401 = April 2026.';

COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.PERIOD_SK IS
    'Smart natural key YYYYMMDD format. Day always 01 monthly grain convention. e.g. 20260401=April 2026, 20260101=January 2026. Derived by ETL from vendor filename pattern YYYY_MM.';

COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.REPORTING_PERIOD IS
    'Natural key from source e.g. Apr 2026. Cross validated against vendor filename during ETL. Filename is authority if mismatch.';


-- ============================================================
-- STEP 2: CES_SAVINGS_DIM_VENDOR
-- Drop PROGRAM_YEAR - derived from period not vendor attribute
-- Add VENDOR_NAME if not already present
-- ============================================================

-- Drop PROGRAM_YEAR
-- Reason: Derived from PERIOD_YEAR in DIM_PERIOD
--         Not an attribute of vendor subprogram
--         Varies by period not by vendor
ALTER TABLE CES_SAVINGS_DIM_VENDOR
    DROP COLUMN PROGRAM_YEAR;

-- Add VENDOR_NAME
-- Reason: Enables Power BI rollup across subprograms
--         ICF may have multiple subprogram rows
--         VENDOR_NAME groups them for drill down
--         VENDOR_NAME > SUBPROGRAM > VENDOR_SUBPROGRAM_KEY
-- NOTE: Only run if VENDOR_NAME does not already exist
--       Check first:
--       SELECT COUNT(*) FROM USER_TAB_COLUMNS
--       WHERE TABLE_NAME='CES_SAVINGS_DIM_VENDOR'
--       AND COLUMN_NAME='VENDOR_NAME';
ALTER TABLE CES_SAVINGS_DIM_VENDOR
    ADD VENDOR_NAME VARCHAR2(200);

COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR.VENDOR_NAME IS
    'Vendor company name. Enables Power BI rollup across all subprograms for one vendor. Derive from VENDOR_SUBPROGRAM_KEY or client provided master list. e.g. ICF appears on multiple subprogram rows.';

COMMENT ON TABLE CES_SAVINGS_DIM_VENDOR IS
    'V3: Vendor dimension. PROGRAM_YEAR removed - use DIM_PERIOD.PERIOD_YEAR. VENDOR_NAME added for Power BI rollup. Kimball denormalized design - low cardinality attributes stored directly. SCD Type 2 ready via config flag.';


-- ============================================================
-- STEP 3: CES_SAVINGS_FACT_ACTUALS
-- Add DATA_DATE
-- Reason: Proper Oracle DATE for Power BI time intelligence
--         Derived from vendor filename YYYY_MM pattern
--         e.g. Vendor_A_2026_04.xlsx -> 01-APR-2026
--         Cannot do date math on PERIOD_SK integer
-- ============================================================

ALTER TABLE CES_SAVINGS_FACT_ACTUALS
    ADD DATA_DATE DATE;

-- Backfill from PERIOD_SK if rows exist
-- PERIOD_SK is YYYYMMDD so direct conversion
-- Run only if table has data
UPDATE CES_SAVINGS_FACT_ACTUALS
SET    DATA_DATE = TO_DATE(TO_CHAR(PERIOD_SK), 'YYYYMMDD')
WHERE  DATA_DATE IS NULL
AND    PERIOD_SK IS NOT NULL;

COMMIT;

COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.DATA_DATE IS
    'Proper Oracle DATE for Power BI time intelligence. Always first of month e.g. 01-APR-2026. Derived from vendor filename YYYY_MM during ETL. Added V3. Enables date range queries and time intelligence functions.';

-- Add index for date range queries
CREATE INDEX CES_SAVINGS_FA_DATA_DATE_IX
    ON CES_SAVINGS_FACT_ACTUALS (DATA_DATE);


-- ============================================================
-- STEP 4: CES_SAVINGS_FACT_FORECAST
-- Add DATA_DATE - same reasoning as FACT_ACTUALS
-- ============================================================

ALTER TABLE CES_SAVINGS_FACT_FORECAST
    ADD DATA_DATE DATE;

-- Backfill from PERIOD_SK if rows exist
UPDATE CES_SAVINGS_FACT_FORECAST
SET    DATA_DATE = TO_DATE(TO_CHAR(PERIOD_SK), 'YYYYMMDD')
WHERE  DATA_DATE IS NULL
AND    PERIOD_SK IS NOT NULL;

COMMIT;

COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST.DATA_DATE IS
    'Proper Oracle DATE for Power BI time intelligence. Always first of month. Derived from vendor filename YYYY_MM during ETL. Added V3.';

CREATE INDEX CES_SAVINGS_FF_DATA_DATE_IX
    ON CES_SAVINGS_FACT_FORECAST (DATA_DATE);


-- ============================================================
-- STEP 5: CES_SAVINGS_FACT_ACTUALS_ARCH
-- Mirror DATA_DATE addition from FACT_ACTUALS
-- ARCH tables always mirror FACT structure
-- ============================================================

ALTER TABLE CES_SAVINGS_FACT_ACTUALS_ARCH
    ADD DATA_DATE DATE;

COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS_ARCH.DATA_DATE IS
    'Mirrored from FACT_ACTUALS. Added V3.';


-- ============================================================
-- STEP 6: CES_SAVINGS_FACT_FORECAST_ARCH
-- Mirror DATA_DATE addition from FACT_FORECAST
-- ============================================================

ALTER TABLE CES_SAVINGS_FACT_FORECAST_ARCH
    ADD DATA_DATE DATE;

COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST_ARCH.DATA_DATE IS
    'Mirrored from FACT_FORECAST. Added V3.';


-- ============================================================
-- STEP 7: RECREATE ALL VIEWS
-- Using CES_SAVINGS_DIM_VENDOR (actual table name)
-- DATA_DATE now sourced from FACT not DIM
-- All views rebuilt clean
-- ============================================================

-- ------------------------------------------------------------
-- 7a. VW_ACTUALS
-- Primary Power BI source for actuals
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW CES_SAVINGS_VW_ACTUALS AS
SELECT
    -- Fact key
    a.FACT_SK,
    -- DATA_DATE from FACT (V3)
    a.DATA_DATE,
    -- Period attributes from DIM
    p.PERIOD_SK,
    p.REPORTING_PERIOD,
    p.PERIOD_YEAR,
    p.PERIOD_MONTH,
    p.PERIOD_MONTH_NAME,
    p.PERIOD_QUARTER,
    p.FISCAL_YEAR,
    p.FISCAL_QUARTER,
    -- Vendor attributes from DIM
    v.VENDOR_SUBPROGRAM_KEY,
    v.VENDOR_NAME,
    v.SECTOR,
    v.PROGRAM,
    v.SUBPROGRAM,
    v.TRIENNIUM,
    -- Financials
    a.INVEST_COST_REBATE,
    a.INVEST_COST_OBR,
    a.INVEST_COST_OTHER,
    a.TOTAL_INVEST_COST,
    -- Participants
    a.PARTICIPANTS_TOTAL,
    a.PARTICIPANTS_RES_LMI_OBC,
    a.PARTICIPANTS_RES_LMI_ONLY,
    a.PARTICIPANTS_OBC_ONLY,
    a.PARTICIPANTS_SMALL_BIZ,
    -- Gross Site Savings Electric
    a.GROSS_SITE_ELEC_ANNUAL_KWH,
    a.GROSS_SITE_ELEC_DEMAND_KW,
    a.GROSS_SITE_ELEC_LIFETIME_KWH,
    -- Gross Site Savings Gas
    a.GROSS_SITE_GAS_ANNUAL_THERMS,
    a.GROSS_SITE_GAS_DAILY_PEAK_THERMS,
    a.GROSS_SITE_GAS_LIFETIME_THERMS,
    -- Net Site ISR Electric
    a.NET_SITE_ISR_ELEC_ANNUAL_KWH,
    a.NET_SITE_ISR_ELEC_DEMAND_KW,
    a.NET_SITE_ISR_ELEC_LIFETIME_KWH,
    -- Net Site ISR Gas
    a.NET_SITE_ISR_GAS_ANNUAL_THERMS,
    a.NET_SITE_ISR_GAS_DAILY_PEAK_THERMS,
    a.NET_SITE_ISR_GAS_LIFETIME_THERMS,
    -- Net Site RR NTG Electric
    a.NET_SITE_RR_NTG_ELEC_ANNUAL_KWH,
    a.NET_SITE_RR_NTG_ELEC_DEMAND_KW,
    a.NET_SITE_RR_NTG_ELEC_LIFETIME_KWH,
    -- Net Site RR NTG Gas
    a.NET_SITE_RR_NTG_GAS_ANNUAL_THERMS,
    a.NET_SITE_RR_NTG_GAS_DAILY_PEAK_THERMS,
    a.NET_SITE_RR_NTG_GAS_LIFETIME_THERMS,
    -- Negative IEs
    a.NEG_IE_ELEC_ANNUAL_KWH,
    a.NEG_IE_ELEC_LIFETIME_KWH,
    a.NEG_IE_GAS_ANNUAL_THERMS,
    a.NEG_IE_GAS_LIFETIME_THERMS,
    -- Total Net Site
    a.TOTAL_NET_SITE_ELEC_ANNUAL_KWH,
    a.TOTAL_NET_SITE_ELEC_LIFETIME_KWH,
    a.TOTAL_NET_SITE_GAS_ANNUAL_THERMS,
    a.TOTAL_NET_SITE_GAS_LIFETIME_THERMS,
    -- Net Source Savings (key management metrics)
    a.NET_SRC_ELEC_ANNUAL_MMBTU,
    a.NET_SRC_ELEC_LIFETIME_MMBTU,
    a.NET_SRC_GAS_ANNUAL_MMBTU,
    a.NET_SRC_GAS_LIFETIME_MMBTU,
    a.NET_SRC_ANNUAL_MMBTU,
    a.NET_SRC_LIFETIME_MMBTU,
    -- Target Segments Source Savings
    a.SEG_LMI_OBC_ANNUAL_MMBTU,
    a.SEG_LMI_OBC_LIFETIME_MMBTU,
    a.SEG_LMI_ANNUAL_MMBTU,
    a.SEG_LMI_LIFETIME_MMBTU,
    a.SEG_OBC_ANNUAL_MMBTU,
    a.SEG_OBC_LIFETIME_MMBTU,
    a.SEG_SMALL_BIZ_ANNUAL_MMBTU,
    a.SEG_SMALL_BIZ_LIFETIME_MMBTU,
    a.SEG_MULTIFAMILY_ANNUAL_MMBTU,
    a.SEG_MULTIFAMILY_LIFETIME_MMBTU,
    -- Target Segments Site Savings
    a.SEG_RES_LMI_OBC_ELEC_ANNUAL_KWH,
    a.SEG_RES_LMI_OBC_ELEC_LIFETIME_KWH,
    a.SEG_RES_LMI_OBC_GAS_ANNUAL_THERMS,
    a.SEG_RES_LMI_OBC_GAS_LIFETIME_THERMS,
    a.SEG_RES_LMI_ELEC_ANNUAL_KWH,
    a.SEG_RES_LMI_ELEC_LIFETIME_KWH,
    a.SEG_RES_LMI_GAS_ANNUAL_THERMS,
    a.SEG_RES_LMI_GAS_LIFETIME_THERMS,
    a.SEG_OBC_ELEC_ANNUAL_KWH,
    a.SEG_OBC_ELEC_LIFETIME_KWH,
    a.SEG_OBC_GAS_ANNUAL_THERMS,
    a.SEG_OBC_GAS_LIFETIME_THERMS,
    a.SEG_SMALL_BIZ_ELEC_ANNUAL_KWH,
    a.SEG_SMALL_BIZ_ELEC_LIFETIME_KWH,
    a.SEG_SMALL_BIZ_GAS_ANNUAL_THERMS,
    a.SEG_SMALL_BIZ_GAS_LIFETIME_THERMS,
    a.SEG_MULTIFAMILY_ELEC_ANNUAL_KWH,
    a.SEG_MULTIFAMILY_ELEC_LIFETIME_KWH,
    a.SEG_MULTIFAMILY_GAS_ANNUAL_THERMS,
    a.SEG_MULTIFAMILY_GAS_LIFETIME_THERMS,
    -- Metadata
    a.SOURCE_FILE_NAME,
    a.LOAD_TIMESTAMP,
    a.VERSION_NUMBER
FROM      CES_SAVINGS_FACT_ACTUALS       a
JOIN      CES_SAVINGS_DIM_VENDOR         v
    ON    a.VENDOR_SUBPROGRAM_SK         = v.VENDOR_SUBPROGRAM_SK
    AND   v.IS_CURRENT                   = 'Y'
JOIN      CES_SAVINGS_DIM_PERIOD         p
    ON    a.PERIOD_SK                    = p.PERIOD_SK
WHERE     a.IS_CURRENT_VERSION           = 'Y';

COMMENT ON TABLE CES_SAVINGS_VW_ACTUALS IS
    'V3: Primary Power BI view for actuals. DATA_DATE from FACT. Pre-joined to DIM_VENDOR and DIM_PERIOD. Current versions only.';


-- ------------------------------------------------------------
-- 7b. VW_FORECAST
-- Primary Power BI source for forecasts
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW CES_SAVINGS_VW_FORECAST AS
SELECT
    f.FACT_SK,
    -- DATA_DATE from FACT (V3)
    f.DATA_DATE,
    -- Period attributes
    p.PERIOD_SK,
    f.FORECAST_PERIOD,
    p.PERIOD_YEAR,
    p.PERIOD_MONTH,
    p.PERIOD_MONTH_NAME,
    p.PERIOD_QUARTER,
    p.FISCAL_YEAR,
    p.FISCAL_QUARTER,
    -- Vendor attributes
    v.VENDOR_SUBPROGRAM_KEY,
    v.VENDOR_NAME,
    v.SECTOR,
    v.PROGRAM,
    v.SUBPROGRAM,
    v.TRIENNIUM,
    -- Forecast specific
    f.CONFIDENCE_LEVEL,
    -- Financials
    f.INVEST_COST_REBATE,
    f.INVEST_COST_OBR,
    f.INVEST_COST_OTHER,
    f.TOTAL_INVEST_COST,
    -- Participants
    f.PARTICIPANTS_TOTAL,
    f.PARTICIPANTS_RES_LMI_OBC,
    f.PARTICIPANTS_RES_LMI_ONLY,
    f.PARTICIPANTS_OBC_ONLY,
    f.PARTICIPANTS_SMALL_BIZ,
    -- Gross Site Savings Electric
    f.GROSS_SITE_ELEC_ANNUAL_KWH,
    f.GROSS_SITE_ELEC_DEMAND_KW,
    f.GROSS_SITE_ELEC_LIFETIME_KWH,
    -- Gross Site Savings Gas
    f.GROSS_SITE_GAS_ANNUAL_THERMS,
    f.GROSS_SITE_GAS_DAILY_PEAK_THERMS,
    f.GROSS_SITE_GAS_LIFETIME_THERMS,
    -- Net Site ISR Electric
    f.NET_SITE_ISR_ELEC_ANNUAL_KWH,
    f.NET_SITE_ISR_ELEC_DEMAND_KW,
    f.NET_SITE_ISR_ELEC_LIFETIME_KWH,
    -- Net Site ISR Gas
    f.NET_SITE_ISR_GAS_ANNUAL_THERMS,
    f.NET_SITE_ISR_GAS_DAILY_PEAK_THERMS,
    f.NET_SITE_ISR_GAS_LIFETIME_THERMS,
    -- Net Site RR NTG Electric
    f.NET_SITE_RR_NTG_ELEC_ANNUAL_KWH,
    f.NET_SITE_RR_NTG_ELEC_DEMAND_KW,
    f.NET_SITE_RR_NTG_ELEC_LIFETIME_KWH,
    -- Net Site RR NTG Gas
    f.NET_SITE_RR_NTG_GAS_ANNUAL_THERMS,
    f.NET_SITE_RR_NTG_GAS_DAILY_PEAK_THERMS,
    f.NET_SITE_RR_NTG_GAS_LIFETIME_THERMS,
    -- Negative IEs
    f.NEG_IE_ELEC_ANNUAL_KWH,
    f.NEG_IE_ELEC_LIFETIME_KWH,
    f.NEG_IE_GAS_ANNUAL_THERMS,
    f.NEG_IE_GAS_LIFETIME_THERMS,
    -- Total Net Site
    f.TOTAL_NET_SITE_ELEC_ANNUAL_KWH,
    f.TOTAL_NET_SITE_ELEC_LIFETIME_KWH,
    f.TOTAL_NET_SITE_GAS_ANNUAL_THERMS,
    f.TOTAL_NET_SITE_GAS_LIFETIME_THERMS,
    -- Net Source Savings
    f.NET_SRC_ELEC_ANNUAL_MMBTU,
    f.NET_SRC_ELEC_LIFETIME_MMBTU,
    f.NET_SRC_GAS_ANNUAL_MMBTU,
    f.NET_SRC_GAS_LIFETIME_MMBTU,
    f.NET_SRC_ANNUAL_MMBTU,
    f.NET_SRC_LIFETIME_MMBTU,
    -- Target Segments Source
    f.SEG_LMI_OBC_ANNUAL_MMBTU,
    f.SEG_LMI_OBC_LIFETIME_MMBTU,
    f.SEG_LMI_ANNUAL_MMBTU,
    f.SEG_LMI_LIFETIME_MMBTU,
    f.SEG_OBC_ANNUAL_MMBTU,
    f.SEG_OBC_LIFETIME_MMBTU,
    f.SEG_SMALL_BIZ_ANNUAL_MMBTU,
    f.SEG_SMALL_BIZ_LIFETIME_MMBTU,
    f.SEG_MULTIFAMILY_ANNUAL_MMBTU,
    f.SEG_MULTIFAMILY_LIFETIME_MMBTU,
    -- Target Segments Site
    f.SEG_RES_LMI_OBC_ELEC_ANNUAL_KWH,
    f.SEG_RES_LMI_OBC_ELEC_LIFETIME_KWH,
    f.SEG_RES_LMI_OBC_GAS_ANNUAL_THERMS,
    f.SEG_RES_LMI_OBC_GAS_LIFETIME_THERMS,
    f.SEG_RES_LMI_ELEC_ANNUAL_KWH,
    f.SEG_RES_LMI_ELEC_LIFETIME_KWH,
    f.SEG_RES_LMI_GAS_ANNUAL_THERMS,
    f.SEG_RES_LMI_GAS_LIFETIME_THERMS,
    f.SEG_OBC_ELEC_ANNUAL_KWH,
    f.SEG_OBC_ELEC_LIFETIME_KWH,
    f.SEG_OBC_GAS_ANNUAL_THERMS,
    f.SEG_OBC_GAS_LIFETIME_THERMS,
    f.SEG_SMALL_BIZ_ELEC_ANNUAL_KWH,
    f.SEG_SMALL_BIZ_ELEC_LIFETIME_KWH,
    f.SEG_SMALL_BIZ_GAS_ANNUAL_THERMS,
    f.SEG_SMALL_BIZ_GAS_LIFETIME_THERMS,
    f.SEG_MULTIFAMILY_ELEC_ANNUAL_KWH,
    f.SEG_MULTIFAMILY_ELEC_LIFETIME_KWH,
    f.SEG_MULTIFAMILY_GAS_ANNUAL_THERMS,
    f.SEG_MULTIFAMILY_GAS_LIFETIME_THERMS,
    -- Metadata
    f.SOURCE_FILE_NAME,
    f.LOAD_TIMESTAMP,
    f.VERSION_NUMBER
FROM      CES_SAVINGS_FACT_FORECAST      f
JOIN      CES_SAVINGS_DIM_VENDOR         v
    ON    f.VENDOR_SUBPROGRAM_SK         = v.VENDOR_SUBPROGRAM_SK
    AND   v.IS_CURRENT                   = 'Y'
JOIN      CES_SAVINGS_DIM_PERIOD         p
    ON    f.PERIOD_SK                    = p.PERIOD_SK
WHERE     f.IS_CURRENT_VERSION           = 'Y';

COMMENT ON TABLE CES_SAVINGS_VW_FORECAST IS
    'V3: Primary Power BI view for forecasts. DATA_DATE from FACT. Pre-joined to DIM_VENDOR and DIM_PERIOD. Current versions only.';


-- ------------------------------------------------------------
-- 7c. VW_ACTUALS_VS_FORECAST
-- Management variance view
-- V3: Join on DATA_DATE from FACT (proper DATE type)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW CES_SAVINGS_VW_ACTUALS_VS_FORECAST AS
SELECT
    a.DATA_DATE,
    a.REPORTING_PERIOD                      AS PERIOD,
    a.PERIOD_YEAR,
    a.FISCAL_YEAR,
    a.FISCAL_QUARTER,
    a.VENDOR_SUBPROGRAM_KEY,
    a.VENDOR_NAME,
    a.SECTOR,
    a.PROGRAM,
    a.SUBPROGRAM,
    a.TRIENNIUM,
    f.CONFIDENCE_LEVEL,
    -- Investment variance
    a.TOTAL_INVEST_COST                     AS ACTUAL_INVEST_COST,
    f.TOTAL_INVEST_COST                     AS FORECAST_INVEST_COST,
    a.TOTAL_INVEST_COST
        - f.TOTAL_INVEST_COST               AS VARIANCE_INVEST_COST,
    -- Key metric variance
    a.NET_SRC_ANNUAL_MMBTU                  AS ACTUAL_NET_SRC_ANNUAL_MMBTU,
    f.NET_SRC_ANNUAL_MMBTU                  AS FORECAST_NET_SRC_ANNUAL_MMBTU,
    a.NET_SRC_ANNUAL_MMBTU
        - f.NET_SRC_ANNUAL_MMBTU            AS VARIANCE_NET_SRC_ANNUAL_MMBTU,
    a.NET_SRC_LIFETIME_MMBTU                AS ACTUAL_NET_SRC_LIFETIME_MMBTU,
    f.NET_SRC_LIFETIME_MMBTU                AS FORECAST_NET_SRC_LIFETIME_MMBTU,
    a.NET_SRC_LIFETIME_MMBTU
        - f.NET_SRC_LIFETIME_MMBTU          AS VARIANCE_NET_SRC_LIFETIME_MMBTU,
    -- Electric variance
    a.GROSS_SITE_ELEC_ANNUAL_KWH            AS ACTUAL_GROSS_ELEC_KWH,
    f.GROSS_SITE_ELEC_ANNUAL_KWH            AS FORECAST_GROSS_ELEC_KWH,
    a.GROSS_SITE_ELEC_ANNUAL_KWH
        - f.GROSS_SITE_ELEC_ANNUAL_KWH      AS VARIANCE_GROSS_ELEC_KWH,
    -- Gas variance
    a.GROSS_SITE_GAS_ANNUAL_THERMS          AS ACTUAL_GROSS_GAS_THERMS,
    f.GROSS_SITE_GAS_ANNUAL_THERMS          AS FORECAST_GROSS_GAS_THERMS,
    a.GROSS_SITE_GAS_ANNUAL_THERMS
        - f.GROSS_SITE_GAS_ANNUAL_THERMS    AS VARIANCE_GROSS_GAS_THERMS,
    -- Participant variance
    a.PARTICIPANTS_TOTAL                    AS ACTUAL_PARTICIPANTS,
    f.PARTICIPANTS_TOTAL                    AS FORECAST_PARTICIPANTS,
    a.PARTICIPANTS_TOTAL
        - f.PARTICIPANTS_TOTAL              AS VARIANCE_PARTICIPANTS
FROM      CES_SAVINGS_VW_ACTUALS            a
LEFT JOIN CES_SAVINGS_VW_FORECAST           f
    ON    a.VENDOR_SUBPROGRAM_KEY           = f.VENDOR_SUBPROGRAM_KEY
    -- V3: Join on proper DATE type for accurate matching
    AND   a.DATA_DATE                       = f.DATA_DATE;

COMMENT ON TABLE CES_SAVINGS_VW_ACTUALS_VS_FORECAST IS
    'V3: Management variance view. Actuals vs forecast with pre-calculated variances. Join on DATA_DATE from FACT. Expand with additional metrics as needed.';


-- ------------------------------------------------------------
-- 7d. VW_PIPELINE_HEALTH
-- Unchanged structurally - recreate for clean state
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW CES_SAVINGS_VW_PIPELINE_HEALTH AS
SELECT
    b.BATCH_ID,
    b.BATCH_START_DT,
    b.BATCH_END_DT,
    b.STATUS                                AS BATCH_STATUS,
    b.TOTAL_FILES_EXPECTED,
    b.FILES_SUCCEEDED,
    b.FILES_FAILED,
    b.FILES_QUARANTINED,
    b.FILES_SKIPPED,
    b.TOTAL_ROWS_LOADED,
    b.TOTAL_ROWS_FAILED,
    b.JENKINS_JOB_NAME,
    b.JENKINS_BUILD_NUMBER,
    ROUND(
        (b.BATCH_END_DT - b.BATCH_START_DT) * 24 * 60, 2
    )                                       AS PROCESSING_MINUTES,
    CASE
        WHEN b.TOTAL_FILES_EXPECTED > 0
        THEN ROUND(
            b.FILES_SUCCEEDED
            / b.TOTAL_FILES_EXPECTED * 100, 1)
        ELSE 0
    END                                     AS SUCCESS_RATE_PCT
FROM      CES_SAVINGS_ETL_BATCH_LOG         b
ORDER BY  b.BATCH_START_DT DESC;

COMMENT ON TABLE CES_SAVINGS_VW_PIPELINE_HEALTH IS
    'Operations monitoring. ETL run history with file counts, processing time and success rate. Use for pipeline health dashboard.';


-- ============================================================
-- STEP 8: VERIFY CHANGES
-- Run after all steps complete
-- ============================================================

-- Confirm DIM_PERIOD columns
SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
FROM   USER_TAB_COLUMNS
WHERE  TABLE_NAME = 'CES_SAVINGS_DIM_PERIOD'
ORDER BY COLUMN_ID;

-- Confirm DIM_VENDOR columns
SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
FROM   USER_TAB_COLUMNS
WHERE  TABLE_NAME = 'CES_SAVINGS_DIM_VENDOR'
ORDER BY COLUMN_ID;

-- Confirm DATA_DATE added to FACT tables
SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE
FROM   USER_TAB_COLUMNS
WHERE  TABLE_NAME IN (
    'CES_SAVINGS_FACT_ACTUALS',
    'CES_SAVINGS_FACT_FORECAST',
    'CES_SAVINGS_FACT_ACTUALS_ARCH',
    'CES_SAVINGS_FACT_FORECAST_ARCH'
)
AND    COLUMN_NAME = 'DATA_DATE'
ORDER BY TABLE_NAME;

-- Confirm views compiled successfully
SELECT VIEW_NAME, STATUS
FROM   USER_VIEWS
WHERE  VIEW_NAME LIKE 'CES_SAVINGS_VW%'
ORDER BY VIEW_NAME;

-- ============================================================
-- END OF ALTER SCRIPTS V3
-- ============================================================
-- Tables modified : 6
-- Columns dropped : 8
-- Columns added   : 5 (VENDOR_NAME + DATA_DATE x4)
-- Indexes added   : 2
-- Views recreated : 4
-- ============================================================
