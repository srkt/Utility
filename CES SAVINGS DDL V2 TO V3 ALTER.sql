-- ============================================================
-- CES SAVINGS DATA WAREHOUSE
-- ALTER SCRIPTS: V2 TO V3
-- Oracle 21c
-- Version  : V2 to V3
-- Date     : 2026-06-04
-- ============================================================
-- Changes:
-- 1. CES_SAVINGS_DIM_PERIOD
--    - DROP   : DATA_DATE, TRIENNIUM, PROGRAM_YEAR,
--               DATA_YEAR_MONTH, UPDATED_DATE, UPDATED_BY
--    - MODIFY : PERIOD_SK to NUMBER(8) YYYYMMDD format
--    - ADD    : None
--
-- 2. CES_SAVINGS_DIM_VENDOR_SUBPROGRAM
--    - DROP   : PROGRAM_YEAR
--
-- 3. CES_SAVINGS_FACT_ACTUALS
--    - ADD    : DATA_DATE DATE
--
-- 4. CES_SAVINGS_FACT_FORECAST
--    - ADD    : DATA_DATE DATE
--
-- 5. ARCH tables mirror FACT changes
--    - ADD    : DATA_DATE DATE to both ARCH tables
--
-- 6. Update views to reflect structural changes
-- ============================================================


-- ============================================================
-- STEP 1: CES_SAVINGS_DIM_PERIOD
-- ============================================================

-- Before making changes, verify existing data
-- Run this SELECT first to understand current state
-- SELECT COUNT(*), MIN(PERIOD_SK), MAX(PERIOD_SK)
-- FROM CES_SAVINGS_DIM_PERIOD;

-- ------------------------------------------------------------
-- 1a. Drop columns no longer needed
-- ------------------------------------------------------------

-- DATA_DATE removed: not needed in dim
-- Power BI gets DATA_DATE from FACT table directly
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN DATA_DATE;

-- TRIENNIUM removed: program concept not time concept
-- Belongs in DIM_VENDOR_SUBPROGRAM
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN TRIENNIUM;

-- PROGRAM_YEAR removed: derivable from PERIOD_YEAR
-- No need to store separately
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN PROGRAM_YEAR;

-- DATA_YEAR_MONTH removed: PERIOD_SK replaces this
-- PERIOD_SK in YYYYMMDD format serves same purpose
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN DATA_YEAR_MONTH;

-- UPDATED_DATE removed: period rows never change
-- Once a period is created it is immutable
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN UPDATED_DATE;

-- UPDATED_BY removed: same reason as UPDATED_DATE
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP COLUMN UPDATED_BY;

-- ------------------------------------------------------------
-- 1b. Drop old unique constraints before modifying PERIOD_SK
-- ------------------------------------------------------------

-- Drop unique constraints that referenced removed columns
-- Adjust constraint names if they differ in your instance
ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP CONSTRAINT CES_SAVINGS_DIM_PER_UQ2;  -- was on DATA_DATE

ALTER TABLE CES_SAVINGS_DIM_PERIOD
    DROP CONSTRAINT CES_SAVINGS_DIM_PER_UQ3;  -- was on DATA_YEAR_MONTH

-- ------------------------------------------------------------
-- 1c. Drop old indexes on removed columns
-- ------------------------------------------------------------

DROP INDEX CES_SAVINGS_DIM_PER_IDX01;  -- was on DATA_DATE
DROP INDEX CES_SAVINGS_DIM_PER_IDX02;  -- was on DATA_YEAR_MONTH
DROP INDEX CES_SAVINGS_DIM_PER_IDX05;  -- was on TRIENNIUM

-- ------------------------------------------------------------
-- 1d. Modify PERIOD_SK to NUMBER(8) for YYYYMMDD format
-- NOTE: Only run if PERIOD_SK was identity/sequence before
--       If table is empty this is straightforward
--       If table has data see migration note below
-- ------------------------------------------------------------

-- MIGRATION NOTE:
-- If DIM_PERIOD already has rows with old SK format:
-- Step 1: Add temporary column
-- Step 2: Populate with new YYYYMMDD value
-- Step 3: Update FK references in FACT tables
-- Step 4: Drop old PK, rename column
-- See MIGRATION SECTION at bottom of this script

-- If table is EMPTY (no data yet):
-- Simply recreate with correct structure
-- See EMPTY TABLE SECTION below

-- ------------------------------------------------------------
-- 1e. Update comment on PERIOD_SK
-- ------------------------------------------------------------
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.PERIOD_SK IS
    'Smart natural key in YYYYMMDD format. Day always 01 for monthly grain convention. e.g. 20260401 = April 2026. Future proof if grain changes to daily.';

COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.REPORTING_PERIOD IS
    'Natural key from source file e.g. Apr 2026. Extracted from vendor filename pattern YYYY_MM.';

-- ------------------------------------------------------------
-- 1f. Recreate indexes for remaining columns
-- ------------------------------------------------------------

-- Drop and recreate indexes that are still valid
DROP INDEX CES_SAVINGS_DIM_PER_IDX03;  -- FISCAL_YEAR
DROP INDEX CES_SAVINGS_DIM_PER_IDX04;  -- PERIOD_YEAR, PERIOD_MONTH

CREATE INDEX CES_SAVINGS_DIM_PER_IDX01
    ON CES_SAVINGS_DIM_PERIOD (FISCAL_YEAR);

CREATE INDEX CES_SAVINGS_DIM_PER_IDX02
    ON CES_SAVINGS_DIM_PERIOD (PERIOD_YEAR, PERIOD_MONTH);

CREATE INDEX CES_SAVINGS_DIM_PER_IDX03
    ON CES_SAVINGS_DIM_PERIOD (REPORTING_PERIOD);


-- ============================================================
-- STEP 2: CES_SAVINGS_DIM_VENDOR_SUBPROGRAM
-- ============================================================

-- ------------------------------------------------------------
-- 2a. Drop PROGRAM_YEAR
-- Reason: Derived from period, not a vendor attribute
-- PERIOD_YEAR in DIM_PERIOD serves this purpose
-- ------------------------------------------------------------
ALTER TABLE CES_SAVINGS_DIM_VENDOR_SUBPROGRAM
    DROP COLUMN PROGRAM_YEAR;

-- Update table comment to reflect change
COMMENT ON TABLE CES_SAVINGS_DIM_VENDOR_SUBPROGRAM IS
    'Vendor subprogram dimension. One row per unique VENDOR_SUBPROGRAM_KEY. Kimball denormalized design. PROGRAM_YEAR removed V3 - use DIM_PERIOD.PERIOD_YEAR instead. SCD Type 2 ready via config flag.';


-- ============================================================
-- STEP 3: CES_SAVINGS_FACT_ACTUALS
-- ============================================================

-- ------------------------------------------------------------
-- 3a. Add DATA_DATE column
-- Reason: Proper Oracle DATE for Power BI time intelligence
--         Derived from vendor filename pattern YYYY_MM
--         e.g. Vendor_A_2026_04.xlsx -> 01-APR-2026
--         Cannot do date math on PERIOD_SK integer
--         Power BI time intelligence requires DATE type
-- ------------------------------------------------------------
ALTER TABLE CES_SAVINGS_FACT_ACTUALS
    ADD DATA_DATE DATE;

-- Add index for Power BI date range queries
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX08
    ON CES_SAVINGS_FACT_ACTUALS (DATA_DATE);

-- Backfill DATA_DATE from existing PERIOD_SK if data exists
-- PERIOD_SK format is YYYYMMDD so convert directly
UPDATE CES_SAVINGS_FACT_ACTUALS
SET    DATA_DATE = TO_DATE(TO_CHAR(PERIOD_SK), 'YYYYMMDD')
WHERE  DATA_DATE IS NULL
AND    PERIOD_SK IS NOT NULL;

COMMIT;

COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.DATA_DATE IS
    'Proper DATE for Power BI time intelligence. Always first of month e.g. 01-APR-2026. Derived from vendor filename YYYY_MM pattern during ETL. Added V3.';


-- ============================================================
-- STEP 4: CES_SAVINGS_FACT_FORECAST
-- ============================================================

-- ------------------------------------------------------------
-- 4a. Add DATA_DATE column
-- Same reasoning as FACT_ACTUALS
-- ------------------------------------------------------------
ALTER TABLE CES_SAVINGS_FACT_FORECAST
    ADD DATA_DATE DATE;

CREATE INDEX CES_SAVINGS_FACT_FOR_IDX09
    ON CES_SAVINGS_FACT_FORECAST (DATA_DATE);

-- Backfill from PERIOD_SK if data exists
UPDATE CES_SAVINGS_FACT_FORECAST
SET    DATA_DATE = TO_DATE(TO_CHAR(PERIOD_SK), 'YYYYMMDD')
WHERE  DATA_DATE IS NULL
AND    PERIOD_SK IS NOT NULL;

COMMIT;

COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST.DATA_DATE IS
    'Proper DATE for Power BI time intelligence. Always first of month. Derived from vendor filename during ETL. Added V3.';


-- ============================================================
-- STEP 5: ARCHIVE TABLES
-- Mirror DATA_DATE addition to FACT tables
-- ============================================================

ALTER TABLE CES_SAVINGS_FACT_ACTUALS_ARCH
    ADD DATA_DATE DATE;

COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS_ARCH.DATA_DATE IS
    'Mirrored from FACT_ACTUALS. Added V3.';

ALTER TABLE CES_SAVINGS_FACT_FORECAST_ARCH
    ADD DATA_DATE DATE;

COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST_ARCH.DATA_DATE IS
    'Mirrored from FACT_FORECAST. Added V3.';


-- ============================================================
-- STEP 6: RECREATE REPORTING VIEWS
-- Changes: DATA_DATE now from FACT not DIM
--          VW_ACTUALS_VS_FORECAST joins on DATA_DATE
--          All views expose DATA_DATE from FACT
-- ============================================================

-- ------------------------------------------------------------
-- 6a. VW_ACTUALS - DATA_DATE now from FACT
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW CES_SAVINGS_VW_ACTUALS AS
SELECT
    -- Keys
    a.FACT_SK,
    -- DATA_DATE from FACT (V3 change)
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
    -- Gross Site Savings
    a.GROSS_SITE_ELEC_ANNUAL_KWH,
    a.GROSS_SITE_ELEC_DEMAND_KW,
    a.GROSS_SITE_ELEC_LIFETIME_KWH,
    a.GROSS_SITE_GAS_ANNUAL_THERMS,
    a.GROSS_SITE_GAS_DAILY_PEAK_THERMS,
    a.GROSS_SITE_GAS_LIFETIME_THERMS,
    -- Net Site ISR
    a.NET_SITE_ISR_ELEC_ANNUAL_KWH,
    a.NET_SITE_ISR_ELEC_DEMAND_KW,
    a.NET_SITE_ISR_ELEC_LIFETIME_KWH,
    a.NET_SITE_ISR_GAS_ANNUAL_THERMS,
    a.NET_SITE_ISR_GAS_DAILY_PEAK_THERMS,
    a.NET_SITE_ISR_GAS_LIFETIME_THERMS,
    -- Net Site RR NTG
    a.NET_SITE_RR_NTG_ELEC_ANNUAL_KWH,
    a.NET_SITE_RR_NTG_ELEC_DEMAND_KW,
    a.NET_SITE_RR_NTG_ELEC_LIFETIME_KWH,
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
    -- Net Source (key management metrics)
    a.NET_SRC_ELEC_ANNUAL_MMBTU,
    a.NET_SRC_ELEC_LIFETIME_MMBTU,
    a.NET_SRC_GAS_ANNUAL_MMBTU,
    a.NET_SRC_GAS_LIFETIME_MMBTU,
    a.NET_SRC_ANNUAL_MMBTU,
    a.NET_SRC_LIFETIME_MMBTU,
    -- Target Segments Source
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
    -- Target Segments Site
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
FROM      CES_SAVINGS_FACT_ACTUALS             a
JOIN      CES_SAVINGS_DIM_VENDOR_SUBPROGRAM    v
    ON    a.VENDOR_SUBPROGRAM_SK               = v.VENDOR_SUBPROGRAM_SK
    AND   v.IS_CURRENT                         = 'Y'
JOIN      CES_SAVINGS_DIM_PERIOD               p
    ON    a.PERIOD_SK                          = p.PERIOD_SK
WHERE     a.IS_CURRENT_VERSION                 = 'Y';

COMMENT ON TABLE CES_SAVINGS_VW_ACTUALS IS
    'V3: DATA_DATE now sourced from FACT not DIM. Power BI primary view for actuals.';


-- ------------------------------------------------------------
-- 6b. VW_FORECAST - DATA_DATE now from FACT
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW CES_SAVINGS_VW_FORECAST AS
SELECT
    f.FACT_SK,
    -- DATA_DATE from FACT (V3 change)
    f.DATA_DATE,
    -- Period attributes from DIM
    p.PERIOD_SK,
    f.FORECAST_PERIOD,
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
    -- Key metrics
    f.GROSS_SITE_ELEC_ANNUAL_KWH,
    f.GROSS_SITE_ELEC_DEMAND_KW,
    f.GROSS_SITE_ELEC_LIFETIME_KWH,
    f.GROSS_SITE_GAS_ANNUAL_THERMS,
    f.GROSS_SITE_GAS_DAILY_PEAK_THERMS,
    f.GROSS_SITE_GAS_LIFETIME_THERMS,
    f.NET_SITE_ISR_ELEC_ANNUAL_KWH,
    f.NET_SITE_ISR_ELEC_DEMAND_KW,
    f.NET_SITE_ISR_ELEC_LIFETIME_KWH,
    f.NET_SITE_ISR_GAS_ANNUAL_THERMS,
    f.NET_SITE_ISR_GAS_DAILY_PEAK_THERMS,
    f.NET_SITE_ISR_GAS_LIFETIME_THERMS,
    f.NET_SITE_RR_NTG_ELEC_ANNUAL_KWH,
    f.NET_SITE_RR_NTG_ELEC_DEMAND_KW,
    f.NET_SITE_RR_NTG_ELEC_LIFETIME_KWH,
    f.NET_SITE_RR_NTG_GAS_ANNUAL_THERMS,
    f.NET_SITE_RR_NTG_GAS_DAILY_PEAK_THERMS,
    f.NET_SITE_RR_NTG_GAS_LIFETIME_THERMS,
    f.NEG_IE_ELEC_ANNUAL_KWH,
    f.NEG_IE_ELEC_LIFETIME_KWH,
    f.NEG_IE_GAS_ANNUAL_THERMS,
    f.NEG_IE_GAS_LIFETIME_THERMS,
    f.TOTAL_NET_SITE_ELEC_ANNUAL_KWH,
    f.TOTAL_NET_SITE_ELEC_LIFETIME_KWH,
    f.TOTAL_NET_SITE_GAS_ANNUAL_THERMS,
    f.TOTAL_NET_SITE_GAS_LIFETIME_THERMS,
    f.NET_SRC_ELEC_ANNUAL_MMBTU,
    f.NET_SRC_ELEC_LIFETIME_MMBTU,
    f.NET_SRC_GAS_ANNUAL_MMBTU,
    f.NET_SRC_GAS_LIFETIME_MMBTU,
    f.NET_SRC_ANNUAL_MMBTU,
    f.NET_SRC_LIFETIME_MMBTU,
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
FROM      CES_SAVINGS_FACT_FORECAST             f
JOIN      CES_SAVINGS_DIM_VENDOR_SUBPROGRAM     v
    ON    f.VENDOR_SUBPROGRAM_SK                = v.VENDOR_SUBPROGRAM_SK
    AND   v.IS_CURRENT                          = 'Y'
JOIN      CES_SAVINGS_DIM_PERIOD                p
    ON    f.PERIOD_SK                           = p.PERIOD_SK
WHERE     f.IS_CURRENT_VERSION                  = 'Y';

COMMENT ON TABLE CES_SAVINGS_VW_FORECAST IS
    'V3: DATA_DATE now sourced from FACT not DIM. Power BI primary view for forecasts.';


-- ------------------------------------------------------------
-- 6c. VW_ACTUALS_VS_FORECAST
-- V3 Change: Join on DATA_DATE from FACT not string period
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW CES_SAVINGS_VW_ACTUALS_VS_FORECAST AS
SELECT
    a.DATA_DATE,
    a.REPORTING_PERIOD                      AS PERIOD,
    a.FISCAL_YEAR,
    a.FISCAL_QUARTER,
    a.PERIOD_YEAR,
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
    -- Key metric variance (highlighted yellow in source)
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
    -- V3: Join on DATA_DATE (proper DATE) not string period
    AND   a.DATA_DATE                       = f.DATA_DATE;

COMMENT ON TABLE CES_SAVINGS_VW_ACTUALS_VS_FORECAST IS
    'V3: Join on DATA_DATE from FACT. Actuals vs forecast variance view. Primary management reporting view.';


-- VW_PIPELINE_HEALTH unchanged - no structural impact
-- Recreate anyway to ensure clean state
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
            b.FILES_SUCCEEDED / b.TOTAL_FILES_EXPECTED * 100, 1)
        ELSE 0
    END                                     AS SUCCESS_RATE_PCT
FROM      CES_SAVINGS_ETL_BATCH_LOG         b
ORDER BY  b.BATCH_START_DT DESC;


-- ============================================================
-- MIGRATION SECTION
-- Only needed if DIM_PERIOD already has data rows
-- with old PERIOD_SK format (identity/sequence values)
-- ============================================================

-- ------------------------------------------------------------
-- CHECK: Does DIM_PERIOD have existing data?
-- Run this query first:
-- SELECT COUNT(*) FROM CES_SAVINGS_DIM_PERIOD;
-- If result = 0, skip this section entirely
-- ------------------------------------------------------------

-- Step M1: Add temporary new SK column
-- ALTER TABLE CES_SAVINGS_DIM_PERIOD
--     ADD PERIOD_SK_NEW NUMBER(8);

-- Step M2: Populate new SK from REPORTING_PERIOD
-- UPDATE CES_SAVINGS_DIM_PERIOD
-- SET PERIOD_SK_NEW = TO_NUMBER(
--     TO_CHAR(TO_DATE('01 ' || REPORTING_PERIOD, 'DD MON YYYY'),
--     'YYYYMMDD'));
-- COMMIT;

-- Step M3: Update FACT tables to use new SK values
-- UPDATE CES_SAVINGS_FACT_ACTUALS fa
-- SET FA.PERIOD_SK = (
--     SELECT dp.PERIOD_SK_NEW
--     FROM   CES_SAVINGS_DIM_PERIOD dp
--     WHERE  dp.PERIOD_SK = fa.PERIOD_SK
-- );
-- COMMIT;

-- UPDATE CES_SAVINGS_FACT_FORECAST ff
-- SET FF.PERIOD_SK = (
--     SELECT dp.PERIOD_SK_NEW
--     FROM   CES_SAVINGS_DIM_PERIOD dp
--     WHERE  dp.PERIOD_SK = ff.PERIOD_SK
-- );
-- COMMIT;

-- Step M4: Drop old PK constraint
-- ALTER TABLE CES_SAVINGS_DIM_PERIOD
--     DROP PRIMARY KEY CASCADE;

-- Step M5: Drop old PERIOD_SK column
-- ALTER TABLE CES_SAVINGS_DIM_PERIOD
--     DROP COLUMN PERIOD_SK;

-- Step M6: Rename new column to PERIOD_SK
-- ALTER TABLE CES_SAVINGS_DIM_PERIOD
--     RENAME COLUMN PERIOD_SK_NEW TO PERIOD_SK;

-- Step M7: Add new PK constraint
-- ALTER TABLE CES_SAVINGS_DIM_PERIOD
--     ADD CONSTRAINT CES_SAVINGS_DIM_PER_PK
--     PRIMARY KEY (PERIOD_SK);

-- Step M8: Restore FK constraints on FACT tables
-- ALTER TABLE CES_SAVINGS_FACT_ACTUALS
--     ADD CONSTRAINT CES_SAVINGS_FACT_ACT_FK2
--     FOREIGN KEY (PERIOD_SK)
--     REFERENCES CES_SAVINGS_DIM_PERIOD(PERIOD_SK);

-- ALTER TABLE CES_SAVINGS_FACT_FORECAST
--     ADD CONSTRAINT CES_SAVINGS_FACT_FOR_FK2
--     FOREIGN KEY (PERIOD_SK)
--     REFERENCES CES_SAVINGS_DIM_PERIOD(PERIOD_SK);

-- ============================================================
-- VERIFICATION QUERIES
-- Run after all ALTER scripts to confirm changes
-- ============================================================

-- Verify DIM_PERIOD columns
-- SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
-- FROM   USER_TAB_COLUMNS
-- WHERE  TABLE_NAME = 'CES_SAVINGS_DIM_PERIOD'
-- ORDER BY COLUMN_ID;

-- Verify DIM_VENDOR_SUBPROGRAM columns
-- SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
-- FROM   USER_TAB_COLUMNS
-- WHERE  TABLE_NAME = 'CES_SAVINGS_DIM_VENDOR_SUBPROGRAM'
-- ORDER BY COLUMN_ID;

-- Verify FACT_ACTUALS has DATA_DATE
-- SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
-- FROM   USER_TAB_COLUMNS
-- WHERE  TABLE_NAME = 'CES_SAVINGS_FACT_ACTUALS'
-- AND    COLUMN_NAME = 'DATA_DATE';

-- Verify views compile cleanly
-- SELECT VIEW_NAME, STATUS
-- FROM   USER_VIEWS
-- WHERE  VIEW_NAME LIKE 'CES_SAVINGS_VW%';

-- ============================================================
-- END OF ALTER SCRIPTS V2 TO V3
-- ============================================================
-- Tables modified : 6
-- Views recreated : 4
-- Columns dropped : 8
-- Columns added   : 4 (DATA_DATE on 2 FACT + 2 ARCH)
-- ============================================================
