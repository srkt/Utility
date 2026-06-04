-- ============================================================
-- CES SAVINGS DATA WAREHOUSE - COMPLETE DDL V2
-- Oracle 21c
-- Version  : 2.0
-- Updated  : 2026-06-04
-- Changes  : 1. Renamed DIM_VENDOR to DIM_VENDOR_SUBPROGRAM
--            2. Added VENDOR_NAME to DIM_VENDOR_SUBPROGRAM
--            3. Removed DIM_CONFIDENCE (stored in FACT with CHECK)
--            4. Added CHECK constraint on CONFIDENCE_LEVEL
--            5. Added DATA_DATE to DIM_PERIOD
--            6. Added DATA_YEAR_MONTH to DIM_PERIOD
--            7. Updated all FK references
--            8. Added unique constraint for monthly cycle
--               duplicate detection on FACT tables
-- ============================================================
-- Table List:
-- STG Layer  : CES_SAVINGS_STG_ACTUALS
--              CES_SAVINGS_STG_FORECAST
-- DIM Layer  : CES_SAVINGS_DIM_VENDOR_SUBPROGRAM
--              CES_SAVINGS_DIM_PERIOD
-- FACT Layer : CES_SAVINGS_FACT_ACTUALS
--              CES_SAVINGS_FACT_FORECAST
-- ARCH Layer : CES_SAVINGS_FACT_ACTUALS_ARCH
--              CES_SAVINGS_FACT_FORECAST_ARCH
-- AUDIT Layer: CES_SAVINGS_ETL_BATCH_LOG
--              CES_SAVINGS_ETL_FILE_LOG
--              CES_SAVINGS_ETL_ROW_LOG
-- Views      : CES_SAVINGS_VW_ACTUALS
--              CES_SAVINGS_VW_FORECAST
--              CES_SAVINGS_VW_ACTUALS_VS_FORECAST
--              CES_SAVINGS_VW_PIPELINE_HEALTH
-- ============================================================


-- ============================================================
-- SECTION 1: STAGING TABLES
-- Purpose : Exact mirror of source Excel files
--           All columns VARCHAR2 - no constraints
--           Truncated and reloaded every ETL run
--           One row per source Excel row
-- ============================================================

-- ------------------------------------------------------------
-- STG: SAVINGS ACTUALS
-- Source  : Sheet3 - Savings Results tab
-- Refresh : Truncate and reload every monthly batch
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_STG_ACTUALS (

    -- Metadata columns
    -- Added by ETL, not from source file
    STG_ID                              NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    SOURCE_FILE_NAME                    VARCHAR2(500),
    BATCH_ID                            NUMBER,
    LOAD_TIMESTAMP                      TIMESTAMP       DEFAULT SYSTIMESTAMP,

    -- Section 1: Identity/Dimension columns
    -- Raw values exactly as vendor submitted
    TRIENNIUM                           VARCHAR2(50),
    SECTOR                              VARCHAR2(200),
    VENDOR_SUBPROGRAM_KEY               VARCHAR2(200),
    PROGRAM                             VARCHAR2(200),
    SUBPROGRAM                          VARCHAR2(200),
    PROGRAM_YEAR                        VARCHAR2(50),
    REPORTING_PERIOD                    VARCHAR2(50),

    -- Section 2: Financials (Costs)
    -- All VARCHAR2 - vendor may submit '1,234.56' or 'N/A'
    INVEST_COST_REBATE                  VARCHAR2(50),
    INVEST_COST_OBR                     VARCHAR2(50),
    INVEST_COST_OTHER                   VARCHAR2(50),
    TOTAL_INVEST_COST                   VARCHAR2(50),

    -- Section 3: Participants
    PARTICIPANTS_TOTAL                  VARCHAR2(50),
    PARTICIPANTS_RES_LMI_OBC            VARCHAR2(50),
    PARTICIPANTS_RES_LMI_ONLY           VARCHAR2(50),
    PARTICIPANTS_OBC_ONLY               VARCHAR2(50),
    PARTICIPANTS_SMALL_BIZ              VARCHAR2(50),

    -- Section 4: Gross Site Savings - Electric
    -- Exclude IEs | TRM Protocol
    GROSS_SITE_ELEC_ANNUAL_KWH          VARCHAR2(50),
    GROSS_SITE_ELEC_DEMAND_KW           VARCHAR2(50),
    GROSS_SITE_ELEC_LIFETIME_KWH        VARCHAR2(50),

    -- Section 5: Gross Site Savings - Natural Gas
    -- Exclude IEs | TRM Protocol
    GROSS_SITE_GAS_ANNUAL_THERMS        VARCHAR2(50),
    GROSS_SITE_GAS_DAILY_PEAK_THERMS    VARCHAR2(50),
    GROSS_SITE_GAS_LIFETIME_THERMS      VARCHAR2(50),

    -- Section 6: Net Realized Site Savings - Electric
    -- Exclude Negative IEs | TRM x ISR
    NET_SITE_ISR_ELEC_ANNUAL_KWH        VARCHAR2(50),
    NET_SITE_ISR_ELEC_DEMAND_KW         VARCHAR2(50),
    NET_SITE_ISR_ELEC_LIFETIME_KWH      VARCHAR2(50),

    -- Section 7: Net Realized Site Savings - Gas
    -- Exclude Negative IEs | TRM x ISR
    NET_SITE_ISR_GAS_ANNUAL_THERMS      VARCHAR2(50),
    NET_SITE_ISR_GAS_DAILY_PEAK_THERMS  VARCHAR2(50),
    NET_SITE_ISR_GAS_LIFETIME_THERMS    VARCHAR2(50),

    -- Section 8: Net Realized Site Savings - Electric
    -- Exclude Negative IEs | TRM x ISR x RR x NTG
    NET_SITE_RR_NTG_ELEC_ANNUAL_KWH     VARCHAR2(50),
    NET_SITE_RR_NTG_ELEC_DEMAND_KW      VARCHAR2(50),
    NET_SITE_RR_NTG_ELEC_LIFETIME_KWH   VARCHAR2(50),

    -- Section 9: Net Realized Site Savings - Gas
    -- Exclude Negative IEs | TRM x ISR x RR x NTG
    NET_SITE_RR_NTG_GAS_ANNUAL_THERMS   VARCHAR2(50),
    NET_SITE_RR_NTG_GAS_DAILY_PEAK_THERMS VARCHAR2(50),
    NET_SITE_RR_NTG_GAS_LIFETIME_THERMS VARCHAR2(50),

    -- Section 10: Negative Interactive Effects (IEs)
    NEG_IE_ELEC_ANNUAL_KWH              VARCHAR2(50),
    NEG_IE_ELEC_LIFETIME_KWH            VARCHAR2(50),
    NEG_IE_GAS_ANNUAL_THERMS            VARCHAR2(50),
    NEG_IE_GAS_LIFETIME_THERMS          VARCHAR2(50),

    -- Section 11: Total Net Realized Site Savings
    -- Include Negative IEs | ISR, RR, NTG Applied
    TOTAL_NET_SITE_ELEC_ANNUAL_KWH      VARCHAR2(50),
    TOTAL_NET_SITE_ELEC_LIFETIME_KWH    VARCHAR2(50),
    TOTAL_NET_SITE_GAS_ANNUAL_THERMS    VARCHAR2(50),
    TOTAL_NET_SITE_GAS_LIFETIME_THERMS  VARCHAR2(50),

    -- Section 12: Net Realized Source Savings
    -- Includes Negative IEs | TRM x ISR x RR x NTG x Source Conversion
    NET_SRC_ELEC_ANNUAL_MMBTU           VARCHAR2(50),
    NET_SRC_ELEC_LIFETIME_MMBTU         VARCHAR2(50),
    NET_SRC_GAS_ANNUAL_MMBTU            VARCHAR2(50),
    NET_SRC_GAS_LIFETIME_MMBTU          VARCHAR2(50),
    NET_SRC_ANNUAL_MMBTU                VARCHAR2(50),
    NET_SRC_LIFETIME_MMBTU              VARCHAR2(50),

    -- Section 13: Target Segments - Net Realized Source Savings (MMBtu)
    SEG_LMI_OBC_ANNUAL_MMBTU            VARCHAR2(50),
    SEG_LMI_OBC_LIFETIME_MMBTU          VARCHAR2(50),
    SEG_LMI_ANNUAL_MMBTU                VARCHAR2(50),
    SEG_LMI_LIFETIME_MMBTU              VARCHAR2(50),
    SEG_OBC_ANNUAL_MMBTU                VARCHAR2(50),
    SEG_OBC_LIFETIME_MMBTU              VARCHAR2(50),
    SEG_SMALL_BIZ_ANNUAL_MMBTU          VARCHAR2(50),
    SEG_SMALL_BIZ_LIFETIME_MMBTU        VARCHAR2(50),
    SEG_MULTIFAMILY_ANNUAL_MMBTU        VARCHAR2(50),
    SEG_MULTIFAMILY_LIFETIME_MMBTU      VARCHAR2(50),

    -- Section 14: Target Segments - Net Realized Site Savings
    -- Exclude Negative IEs | TRM x ISR x RR x NTG
    -- Residential LMI or OBC
    SEG_RES_LMI_OBC_ELEC_ANNUAL_KWH     VARCHAR2(50),
    SEG_RES_LMI_OBC_ELEC_LIFETIME_KWH   VARCHAR2(50),
    SEG_RES_LMI_OBC_GAS_ANNUAL_THERMS   VARCHAR2(50),
    SEG_RES_LMI_OBC_GAS_LIFETIME_THERMS VARCHAR2(50),
    -- Residential LMI Only
    SEG_RES_LMI_ELEC_ANNUAL_KWH         VARCHAR2(50),
    SEG_RES_LMI_ELEC_LIFETIME_KWH       VARCHAR2(50),
    SEG_RES_LMI_GAS_ANNUAL_THERMS       VARCHAR2(50),
    SEG_RES_LMI_GAS_LIFETIME_THERMS     VARCHAR2(50),
    -- OBC Only
    SEG_OBC_ELEC_ANNUAL_KWH             VARCHAR2(50),
    SEG_OBC_ELEC_LIFETIME_KWH           VARCHAR2(50),
    SEG_OBC_GAS_ANNUAL_THERMS           VARCHAR2(50),
    SEG_OBC_GAS_LIFETIME_THERMS         VARCHAR2(50),
    -- Small Business
    SEG_SMALL_BIZ_ELEC_ANNUAL_KWH       VARCHAR2(50),
    SEG_SMALL_BIZ_ELEC_LIFETIME_KWH     VARCHAR2(50),
    SEG_SMALL_BIZ_GAS_ANNUAL_THERMS     VARCHAR2(50),
    SEG_SMALL_BIZ_GAS_LIFETIME_THERMS   VARCHAR2(50),
    -- Multifamily
    SEG_MULTIFAMILY_ELEC_ANNUAL_KWH     VARCHAR2(50),
    SEG_MULTIFAMILY_ELEC_LIFETIME_KWH   VARCHAR2(50),
    SEG_MULTIFAMILY_GAS_ANNUAL_THERMS   VARCHAR2(50),
    SEG_MULTIFAMILY_GAS_LIFETIME_THERMS VARCHAR2(50)
);

COMMENT ON TABLE  CES_SAVINGS_STG_ACTUALS                IS 'Staging table for actual savings. Mirrors source Excel Savings Results tab exactly. All VARCHAR2 - no constraints. Truncated each monthly ETL run.';
COMMENT ON COLUMN CES_SAVINGS_STG_ACTUALS.STG_ID         IS 'Identity PK. Links to ETL_ROW_LOG for row level error tracking.';
COMMENT ON COLUMN CES_SAVINGS_STG_ACTUALS.SOURCE_FILE_NAME IS 'Original vendor filename. Used for audit trail and resubmission detection.';
COMMENT ON COLUMN CES_SAVINGS_STG_ACTUALS.BATCH_ID       IS 'Links to ETL_BATCH_LOG. Used for batch level reprocessing if ETL fails.';


-- ------------------------------------------------------------
-- STG: SAVINGS FORECAST
-- Source  : Sheet4 - Savings Forecast tab
-- Refresh : Truncate and reload every monthly batch
-- Difference from actuals: FORECAST_PERIOD + CONFIDENCE_LEVEL
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_STG_FORECAST (

    -- Metadata columns
    STG_ID                              NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    SOURCE_FILE_NAME                    VARCHAR2(500),
    BATCH_ID                            NUMBER,
    LOAD_TIMESTAMP                      TIMESTAMP       DEFAULT SYSTIMESTAMP,

    -- Section 1: Identity/Dimension columns
    TRIENNIUM                           VARCHAR2(50),
    SECTOR                              VARCHAR2(200),
    VENDOR_SUBPROGRAM_KEY               VARCHAR2(200),
    PROGRAM                             VARCHAR2(200),
    SUBPROGRAM                          VARCHAR2(200),
    PROGRAM_YEAR                        VARCHAR2(50),
    FORECAST_PERIOD                     VARCHAR2(50),   -- Different from actuals REPORTING_PERIOD
    CONFIDENCE_LEVEL                    VARCHAR2(50),   -- Forecast only column

    -- Section 2: Financials (Costs)
    INVEST_COST_REBATE                  VARCHAR2(50),
    INVEST_COST_OBR                     VARCHAR2(50),
    INVEST_COST_OTHER                   VARCHAR2(50),
    TOTAL_INVEST_COST                   VARCHAR2(50),

    -- Section 3: Participants
    PARTICIPANTS_TOTAL                  VARCHAR2(50),
    PARTICIPANTS_RES_LMI_OBC            VARCHAR2(50),
    PARTICIPANTS_RES_LMI_ONLY           VARCHAR2(50),
    PARTICIPANTS_OBC_ONLY               VARCHAR2(50),
    PARTICIPANTS_SMALL_BIZ              VARCHAR2(50),

    -- Section 4: Gross Site Savings - Electric
    GROSS_SITE_ELEC_ANNUAL_KWH          VARCHAR2(50),
    GROSS_SITE_ELEC_DEMAND_KW           VARCHAR2(50),
    GROSS_SITE_ELEC_LIFETIME_KWH        VARCHAR2(50),

    -- Section 5: Gross Site Savings - Natural Gas
    GROSS_SITE_GAS_ANNUAL_THERMS        VARCHAR2(50),
    GROSS_SITE_GAS_DAILY_PEAK_THERMS    VARCHAR2(50),
    GROSS_SITE_GAS_LIFETIME_THERMS      VARCHAR2(50),

    -- Section 6: Net Realized Site Savings - Electric (TRM x ISR)
    NET_SITE_ISR_ELEC_ANNUAL_KWH        VARCHAR2(50),
    NET_SITE_ISR_ELEC_DEMAND_KW         VARCHAR2(50),
    NET_SITE_ISR_ELEC_LIFETIME_KWH      VARCHAR2(50),

    -- Section 7: Net Realized Site Savings - Gas (TRM x ISR)
    NET_SITE_ISR_GAS_ANNUAL_THERMS      VARCHAR2(50),
    NET_SITE_ISR_GAS_DAILY_PEAK_THERMS  VARCHAR2(50),
    NET_SITE_ISR_GAS_LIFETIME_THERMS    VARCHAR2(50),

    -- Section 8: Net Realized Site Savings - Electric (TRM x ISR x RR x NTG)
    NET_SITE_RR_NTG_ELEC_ANNUAL_KWH     VARCHAR2(50),
    NET_SITE_RR_NTG_ELEC_DEMAND_KW      VARCHAR2(50),
    NET_SITE_RR_NTG_ELEC_LIFETIME_KWH   VARCHAR2(50),

    -- Section 9: Net Realized Site Savings - Gas (TRM x ISR x RR x NTG)
    NET_SITE_RR_NTG_GAS_ANNUAL_THERMS   VARCHAR2(50),
    NET_SITE_RR_NTG_GAS_DAILY_PEAK_THERMS VARCHAR2(50),
    NET_SITE_RR_NTG_GAS_LIFETIME_THERMS VARCHAR2(50),

    -- Section 10: Negative Interactive Effects
    NEG_IE_ELEC_ANNUAL_KWH              VARCHAR2(50),
    NEG_IE_ELEC_LIFETIME_KWH            VARCHAR2(50),
    NEG_IE_GAS_ANNUAL_THERMS            VARCHAR2(50),
    NEG_IE_GAS_LIFETIME_THERMS          VARCHAR2(50),

    -- Section 11: Total Net Realized Site Savings
    TOTAL_NET_SITE_ELEC_ANNUAL_KWH      VARCHAR2(50),
    TOTAL_NET_SITE_ELEC_LIFETIME_KWH    VARCHAR2(50),
    TOTAL_NET_SITE_GAS_ANNUAL_THERMS    VARCHAR2(50),
    TOTAL_NET_SITE_GAS_LIFETIME_THERMS  VARCHAR2(50),

    -- Section 12: Net Realized Source Savings
    NET_SRC_ELEC_ANNUAL_MMBTU           VARCHAR2(50),
    NET_SRC_ELEC_LIFETIME_MMBTU         VARCHAR2(50),
    NET_SRC_GAS_ANNUAL_MMBTU            VARCHAR2(50),
    NET_SRC_GAS_LIFETIME_MMBTU          VARCHAR2(50),
    NET_SRC_ANNUAL_MMBTU                VARCHAR2(50),
    NET_SRC_LIFETIME_MMBTU              VARCHAR2(50),

    -- Section 13: Target Segments - Source Savings (MMBtu)
    SEG_LMI_OBC_ANNUAL_MMBTU            VARCHAR2(50),
    SEG_LMI_OBC_LIFETIME_MMBTU          VARCHAR2(50),
    SEG_LMI_ANNUAL_MMBTU                VARCHAR2(50),
    SEG_LMI_LIFETIME_MMBTU              VARCHAR2(50),
    SEG_OBC_ANNUAL_MMBTU                VARCHAR2(50),
    SEG_OBC_LIFETIME_MMBTU              VARCHAR2(50),
    SEG_SMALL_BIZ_ANNUAL_MMBTU          VARCHAR2(50),
    SEG_SMALL_BIZ_LIFETIME_MMBTU        VARCHAR2(50),
    SEG_MULTIFAMILY_ANNUAL_MMBTU        VARCHAR2(50),
    SEG_MULTIFAMILY_LIFETIME_MMBTU      VARCHAR2(50),

    -- Section 14: Target Segments - Site Savings (kWh/Therms)
    -- Residential LMI or OBC
    SEG_RES_LMI_OBC_ELEC_ANNUAL_KWH     VARCHAR2(50),
    SEG_RES_LMI_OBC_ELEC_LIFETIME_KWH   VARCHAR2(50),
    SEG_RES_LMI_OBC_GAS_ANNUAL_THERMS   VARCHAR2(50),
    SEG_RES_LMI_OBC_GAS_LIFETIME_THERMS VARCHAR2(50),
    -- Residential LMI Only
    SEG_RES_LMI_ELEC_ANNUAL_KWH         VARCHAR2(50),
    SEG_RES_LMI_ELEC_LIFETIME_KWH       VARCHAR2(50),
    SEG_RES_LMI_GAS_ANNUAL_THERMS       VARCHAR2(50),
    SEG_RES_LMI_GAS_LIFETIME_THERMS     VARCHAR2(50),
    -- OBC Only
    SEG_OBC_ELEC_ANNUAL_KWH             VARCHAR2(50),
    SEG_OBC_ELEC_LIFETIME_KWH           VARCHAR2(50),
    SEG_OBC_GAS_ANNUAL_THERMS           VARCHAR2(50),
    SEG_OBC_GAS_LIFETIME_THERMS         VARCHAR2(50),
    -- Small Business
    SEG_SMALL_BIZ_ELEC_ANNUAL_KWH       VARCHAR2(50),
    SEG_SMALL_BIZ_ELEC_LIFETIME_KWH     VARCHAR2(50),
    SEG_SMALL_BIZ_GAS_ANNUAL_THERMS     VARCHAR2(50),
    SEG_SMALL_BIZ_GAS_LIFETIME_THERMS   VARCHAR2(50),
    -- Multifamily
    SEG_MULTIFAMILY_ELEC_ANNUAL_KWH     VARCHAR2(50),
    SEG_MULTIFAMILY_ELEC_LIFETIME_KWH   VARCHAR2(50),
    SEG_MULTIFAMILY_GAS_ANNUAL_THERMS   VARCHAR2(50),
    SEG_MULTIFAMILY_GAS_LIFETIME_THERMS VARCHAR2(50)
);

COMMENT ON TABLE  CES_SAVINGS_STG_FORECAST                   IS 'Staging table for forecast savings. Mirrors source Excel Savings Forecast tab. All VARCHAR2. Truncated each monthly ETL run. Adds FORECAST_PERIOD and CONFIDENCE_LEVEL vs actuals.';
COMMENT ON COLUMN CES_SAVINGS_STG_FORECAST.FORECAST_PERIOD   IS 'Period this forecast covers. Different column name from actuals REPORTING_PERIOD intentionally.';
COMMENT ON COLUMN CES_SAVINGS_STG_FORECAST.CONFIDENCE_LEVEL  IS 'Vendor confidence in forecast. Valid values: High, Medium, Low. Validated in FACT table CHECK constraint.';


-- ============================================================
-- SECTION 2: DIMENSION TABLES
-- Purpose : Conformed dimensions shared by both fact tables
--           Kimball dimensional model approach
--           Low cardinality attributes stored directly in dim
--           No separate tables for sector, program, subprogram
--           SCD Type 2 columns included, off by default
--           Activated by ETL config flag scd_enabled
-- ============================================================

-- ------------------------------------------------------------
-- DIM: VENDOR SUBPROGRAM
-- One row per unique VENDOR_SUBPROGRAM_KEY
-- Stores all vendor attributes including low cardinality ones
-- Kimball approach - denormalized by design
-- VENDOR_NAME added for Power BI rollup across subprograms
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_DIM_VENDOR_SUBPROGRAM (

    -- Surrogate Key
    VENDOR_SUBPROGRAM_SK                NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Natural Key
    -- Used for ETL lookup and migration safety backup
    VENDOR_SUBPROGRAM_KEY               VARCHAR2(200)   NOT NULL,

    -- Vendor Company Name
    -- Enables Power BI rollup across all subprograms for one vendor
    -- Example: ICF appears on multiple subprogram rows
    -- Power BI can group/drill: VENDOR_NAME > SUBPROGRAM > VENDOR_SUBPROGRAM_KEY
    VENDOR_NAME                         VARCHAR2(200),

    -- Low Cardinality Attributes
    -- Stored directly here per Kimball dimensional modeling best practice
    -- Avoids unnecessary joins for small lookup tables
    -- Sector: Residential, Commercial, Industrial etc (4-5 values)
    SECTOR                              VARCHAR2(200),
    -- Program: CEF-EE II etc (3-5 values)
    PROGRAM                             VARCHAR2(200),
    -- Subprogram: Res LMI, Res OBC, Small Biz etc (10-15 values)
    SUBPROGRAM                          VARCHAR2(200),
    -- Triennium: 2022-2024, 2024-2026 etc (2-3 values)
    TRIENNIUM                           VARCHAR2(50),
    -- Program Year: 2024, 2025, 2026 etc (3-5 values)
    PROGRAM_YEAR                        VARCHAR2(50),

    -- SCD Type 2 Columns
    -- Controlled by ETL config flag: scd_enabled: true/false
    -- Default behavior (scd_enabled=false):
    --   EFFECTIVE_DATE = load date, EXPIRY_DATE = NULL, IS_CURRENT = Y
    -- When activated (scd_enabled=true):
    --   Full history tracking, old rows expired, new rows inserted
    EFFECTIVE_DATE                      DATE            DEFAULT SYSDATE,
    EXPIRY_DATE                         DATE,
    IS_CURRENT                          VARCHAR2(1)     DEFAULT 'Y',

    -- Audit Columns
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    UPDATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CREATED_BY                          VARCHAR2(100)   DEFAULT USER,
    UPDATED_BY                          VARCHAR2(100)   DEFAULT USER,

    -- Constraints
    CONSTRAINT CES_SAVINGS_DIM_VS_CK1   CHECK (IS_CURRENT IN ('Y','N'))
);

COMMENT ON TABLE  CES_SAVINGS_DIM_VENDOR_SUBPROGRAM                  IS 'Vendor subprogram dimension. One row per unique VENDOR_SUBPROGRAM_KEY. Kimball denormalized design - low cardinality attributes stored directly. SCD Type 2 ready, activated by ETL config flag scd_enabled.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR_SUBPROGRAM.VENDOR_SUBPROGRAM_SK    IS 'Surrogate PK. Identity column. Used for fast FK joins to fact tables.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR_SUBPROGRAM.VENDOR_SUBPROGRAM_KEY   IS 'Natural key from source. Composite key combining vendor and subprogram. Used for ETL lookup and FK rebuild after server migration.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR_SUBPROGRAM.VENDOR_NAME             IS 'Vendor company name. Enables Power BI rollup across all subprograms for one vendor. Derive from VENDOR_SUBPROGRAM_KEY or client provided master list.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR_SUBPROGRAM.SECTOR                  IS 'Low cardinality. Residential, Commercial etc. Stored here per Kimball best practice.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR_SUBPROGRAM.PROGRAM                 IS 'Low cardinality. CEF-EE II etc. Stored here per Kimball best practice.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR_SUBPROGRAM.SUBPROGRAM              IS 'Low cardinality. Res LMI, Small Biz etc. Stored here per Kimball best practice.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR_SUBPROGRAM.TRIENNIUM               IS 'Low cardinality. 2024-2026 etc. Stored here per Kimball best practice.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR_SUBPROGRAM.IS_CURRENT              IS 'Y=Active record. N=Expired record. Always Y when scd_enabled=false.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR_SUBPROGRAM.EFFECTIVE_DATE          IS 'Date record became active. SCD Type 2. Set to load date when scd_enabled=false.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR_SUBPROGRAM.EXPIRY_DATE             IS 'Date record was superseded. NULL=still active. Populated only when scd_enabled=true.';

-- Indexes
CREATE UNIQUE INDEX CES_SAVINGS_DIM_VS_IDX01 ON CES_SAVINGS_DIM_VENDOR_SUBPROGRAM (VENDOR_SUBPROGRAM_KEY, IS_CURRENT);
CREATE INDEX        CES_SAVINGS_DIM_VS_IDX02 ON CES_SAVINGS_DIM_VENDOR_SUBPROGRAM (VENDOR_NAME);
CREATE INDEX        CES_SAVINGS_DIM_VS_IDX03 ON CES_SAVINGS_DIM_VENDOR_SUBPROGRAM (PROGRAM);
CREATE INDEX        CES_SAVINGS_DIM_VS_IDX04 ON CES_SAVINGS_DIM_VENDOR_SUBPROGRAM (SECTOR);
CREATE INDEX        CES_SAVINGS_DIM_VS_IDX05 ON CES_SAVINGS_DIM_VENDOR_SUBPROGRAM (SUBPROGRAM);
CREATE INDEX        CES_SAVINGS_DIM_VS_IDX06 ON CES_SAVINGS_DIM_VENDOR_SUBPROGRAM (TRIENNIUM);


-- ------------------------------------------------------------
-- DIM: PERIOD
-- One row per unique reporting month
-- Maximum 24 rows for 2 years history
-- DATA_DATE enables Power BI time intelligence
-- DATA_YEAR_MONTH enables fast duplicate detection
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_DIM_PERIOD (

    -- Surrogate Key
    PERIOD_SK                           NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Natural Key
    REPORTING_PERIOD                    VARCHAR2(50)    NOT NULL,

    -- DATA_DATE: Proper DATE column for Power BI time intelligence
    -- Always first day of reporting month
    -- Jan 2026 -> 01-JAN-2026
    -- Feb 2026 -> 01-FEB-2026
    -- Enables: year over year, rolling 12 month, fiscal calcs
    DATA_DATE                           DATE            NOT NULL,

    -- DATA_YEAR_MONTH: Fast filtering and duplicate detection
    -- Format: YYYYMM
    -- Jan 2026 -> 202601
    -- Feb 2026 -> 202602
    -- ETL uses: IF vendor + DATA_YEAR_MONTH exists -> resubmission
    DATA_YEAR_MONTH                     NUMBER(6)       NOT NULL,

    -- Period breakdown for Power BI slicers
    PERIOD_YEAR                         NUMBER(4),
    PERIOD_MONTH                        NUMBER(2),
    PERIOD_MONTH_NAME                   VARCHAR2(20),
    PERIOD_QUARTER                      NUMBER(1),

    -- Fiscal attributes for management reporting
    -- Derive based on client fiscal year definition
    FISCAL_YEAR                         VARCHAR2(20),
    FISCAL_QUARTER                      VARCHAR2(10),

    -- Program cycle attributes
    TRIENNIUM                           VARCHAR2(50),
    PROGRAM_YEAR                        NUMBER(4),

    -- Audit Columns
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    UPDATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CREATED_BY                          VARCHAR2(100)   DEFAULT USER,
    UPDATED_BY                          VARCHAR2(100)   DEFAULT USER,

    -- Constraints
    CONSTRAINT CES_SAVINGS_DIM_PER_UQ1  UNIQUE (REPORTING_PERIOD),
    CONSTRAINT CES_SAVINGS_DIM_PER_UQ2  UNIQUE (DATA_DATE),
    CONSTRAINT CES_SAVINGS_DIM_PER_UQ3  UNIQUE (DATA_YEAR_MONTH)
);

COMMENT ON TABLE  CES_SAVINGS_DIM_PERIOD                    IS 'Time dimension. One row per reporting month. Maximum 24 rows for 2 year history. DATA_DATE enables Power BI time intelligence. DATA_YEAR_MONTH enables fast duplicate detection in ETL.';
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.REPORTING_PERIOD   IS 'Natural key. Original period value from source e.g. Jan 2026.';
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.DATA_DATE          IS 'First day of reporting month. Proper DATE for Power BI time intelligence. Derived by ETL from REPORTING_PERIOD.';
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.DATA_YEAR_MONTH    IS 'YYYYMM format e.g. 202601. Used by ETL for fast duplicate and resubmission detection. IF vendor + DATA_YEAR_MONTH already in FACT then resubmission.';
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.FISCAL_YEAR        IS 'Fiscal year e.g. FY2026. Derive based on client fiscal year definition.';
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.FISCAL_QUARTER     IS 'Fiscal quarter e.g. Q1. Derive based on client fiscal year definition.';
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.TRIENNIUM          IS 'Program triennium this period belongs to e.g. 2024-2026.';

-- Indexes
CREATE INDEX CES_SAVINGS_DIM_PER_IDX01 ON CES_SAVINGS_DIM_PERIOD (DATA_DATE);
CREATE INDEX CES_SAVINGS_DIM_PER_IDX02 ON CES_SAVINGS_DIM_PERIOD (DATA_YEAR_MONTH);
CREATE INDEX CES_SAVINGS_DIM_PER_IDX03 ON CES_SAVINGS_DIM_PERIOD (FISCAL_YEAR);
CREATE INDEX CES_SAVINGS_DIM_PER_IDX04 ON CES_SAVINGS_DIM_PERIOD (PERIOD_YEAR, PERIOD_MONTH);
CREATE INDEX CES_SAVINGS_DIM_PER_IDX05 ON CES_SAVINGS_DIM_PERIOD (TRIENNIUM);


-- ============================================================
-- SECTION 3: FACT TABLES
-- Purpose : Core metric storage
--           Typed and validated columns
--           FK to dimension tables
--           Natural keys stored as migration safety backup
--           Wide table format - all 78 columns
--           Monthly cycle: one row per vendor per month
--           Versioning for resubmissions - never delete
-- ============================================================

-- ------------------------------------------------------------
-- FACT: SAVINGS ACTUALS
-- Source    : CES_SAVINGS_STG_ACTUALS after validation
-- Grain     : One row per vendor subprogram per reporting month
-- Retention : 2 years hot, older moved to ARCH table annually
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_FACT_ACTUALS (

    -- Surrogate Key
    FACT_SK                             NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Foreign Keys to Dimensions
    VENDOR_SUBPROGRAM_SK                NUMBER          NOT NULL,
    PERIOD_SK                           NUMBER          NOT NULL,

    -- Natural Keys
    -- Stored alongside SKs as migration safety backup
    -- If server migration corrupts identity SKs:
    -- Rebuild dim lookups using natural keys
    -- Repoint FKs correctly, data never lost
    VENDOR_SUBPROGRAM_KEY               VARCHAR2(200),
    REPORTING_PERIOD                    VARCHAR2(50),

    -- ETL Metadata
    SOURCE_FILE_NAME                    VARCHAR2(500),
    BATCH_ID                            NUMBER,
    LOAD_TIMESTAMP                      TIMESTAMP       DEFAULT SYSTIMESTAMP,

    -- Resubmission Versioning
    -- Monthly rule: one vendor + one month = one current row
    -- Vendor resubmits: old row versioned, new row inserted
    -- Never delete historical versions - audit trail
    VERSION_NUMBER                      NUMBER          DEFAULT 1,
    IS_CURRENT_VERSION                  VARCHAR2(1)     DEFAULT 'Y',

    -- Section 1: Additional Dimension Attributes
    -- Stored in FACT as snapshot at time of load
    -- Protects against SCD Type 1 overwrites changing history
    TRIENNIUM                           VARCHAR2(50),
    SECTOR                              VARCHAR2(200),
    PROGRAM                             VARCHAR2(200),
    SUBPROGRAM                          VARCHAR2(200),
    PROGRAM_YEAR                        VARCHAR2(50),

    -- Section 2: Financials (Costs)
    -- NUMBER(18,2) = dollars and cents precision
    INVEST_COST_REBATE                  NUMBER(18,2),
    INVEST_COST_OBR                     NUMBER(18,2),
    INVEST_COST_OTHER                   NUMBER(18,2),
    TOTAL_INVEST_COST                   NUMBER(18,2),

    -- Section 3: Participants (whole numbers)
    -- NUMBER(10,0) = no decimals, counts only
    -- NULL = vendor did not report
    -- 0    = vendor reported zero participants
    PARTICIPANTS_TOTAL                  NUMBER(10,0),
    PARTICIPANTS_RES_LMI_OBC            NUMBER(10,0),
    PARTICIPANTS_RES_LMI_ONLY           NUMBER(10,0),
    PARTICIPANTS_OBC_ONLY               NUMBER(10,0),
    PARTICIPANTS_SMALL_BIZ              NUMBER(10,0),

    -- Section 4: Gross Site Savings - Electric
    -- Exclude IEs | TRM Protocol
    -- NUMBER(18,4) = 4 decimal precision for energy values
    -- NULL = not reported, 0 = reported as zero (different meanings)
    GROSS_SITE_ELEC_ANNUAL_KWH          NUMBER(18,4),
    GROSS_SITE_ELEC_DEMAND_KW           NUMBER(18,4),
    GROSS_SITE_ELEC_LIFETIME_KWH        NUMBER(18,4),

    -- Section 5: Gross Site Savings - Natural Gas
    -- Exclude IEs | TRM Protocol
    GROSS_SITE_GAS_ANNUAL_THERMS        NUMBER(18,4),
    GROSS_SITE_GAS_DAILY_PEAK_THERMS    NUMBER(18,4),
    GROSS_SITE_GAS_LIFETIME_THERMS      NUMBER(18,4),

    -- Section 6: Net Realized Site Savings - Electric
    -- Exclude Negative IEs | TRM x ISR
    NET_SITE_ISR_ELEC_ANNUAL_KWH        NUMBER(18,4),
    NET_SITE_ISR_ELEC_DEMAND_KW         NUMBER(18,4),
    NET_SITE_ISR_ELEC_LIFETIME_KWH      NUMBER(18,4),

    -- Section 7: Net Realized Site Savings - Gas
    -- Exclude Negative IEs | TRM x ISR
    NET_SITE_ISR_GAS_ANNUAL_THERMS      NUMBER(18,4),
    NET_SITE_ISR_GAS_DAILY_PEAK_THERMS  NUMBER(18,4),
    NET_SITE_ISR_GAS_LIFETIME_THERMS    NUMBER(18,4),

    -- Section 8: Net Realized Site Savings - Electric
    -- Exclude Negative IEs | TRM x ISR x RR x NTG
    NET_SITE_RR_NTG_ELEC_ANNUAL_KWH     NUMBER(18,4),
    NET_SITE_RR_NTG_ELEC_DEMAND_KW      NUMBER(18,4),
    NET_SITE_RR_NTG_ELEC_LIFETIME_KWH   NUMBER(18,4),

    -- Section 9: Net Realized Site Savings - Gas
    -- Exclude Negative IEs | TRM x ISR x RR x NTG
    NET_SITE_RR_NTG_GAS_ANNUAL_THERMS   NUMBER(18,4),
    NET_SITE_RR_NTG_GAS_DAILY_PEAK_THERMS NUMBER(18,4),
    NET_SITE_RR_NTG_GAS_LIFETIME_THERMS NUMBER(18,4),

    -- Section 10: Negative Interactive Effects (IEs)
    NEG_IE_ELEC_ANNUAL_KWH              NUMBER(18,4),
    NEG_IE_ELEC_LIFETIME_KWH            NUMBER(18,4),
    NEG_IE_GAS_ANNUAL_THERMS            NUMBER(18,4),
    NEG_IE_GAS_LIFETIME_THERMS          NUMBER(18,4),

    -- Section 11: Total Net Realized Site Savings
    -- Include Negative IEs | ISR, RR, NTG Applied
    TOTAL_NET_SITE_ELEC_ANNUAL_KWH      NUMBER(18,4),
    TOTAL_NET_SITE_ELEC_LIFETIME_KWH    NUMBER(18,4),
    TOTAL_NET_SITE_GAS_ANNUAL_THERMS    NUMBER(18,4),
    TOTAL_NET_SITE_GAS_LIFETIME_THERMS  NUMBER(18,4),

    -- Section 12: Net Realized Source Savings
    -- Includes Negative IEs | TRM x ISR x RR x NTG x Source Conversion
    -- NET_SRC_ANNUAL_MMBTU and NET_SRC_LIFETIME_MMBTU
    -- highlighted yellow in source = key management metrics
    NET_SRC_ELEC_ANNUAL_MMBTU           NUMBER(18,4),
    NET_SRC_ELEC_LIFETIME_MMBTU         NUMBER(18,4),
    NET_SRC_GAS_ANNUAL_MMBTU            NUMBER(18,4),
    NET_SRC_GAS_LIFETIME_MMBTU          NUMBER(18,4),
    NET_SRC_ANNUAL_MMBTU                NUMBER(18,4),
    NET_SRC_LIFETIME_MMBTU              NUMBER(18,4),

    -- Section 13: Target Segments - Net Realized Source Savings (MMBtu)
    SEG_LMI_OBC_ANNUAL_MMBTU            NUMBER(18,4),
    SEG_LMI_OBC_LIFETIME_MMBTU          NUMBER(18,4),
    SEG_LMI_ANNUAL_MMBTU                NUMBER(18,4),
    SEG_LMI_LIFETIME_MMBTU              NUMBER(18,4),
    SEG_OBC_ANNUAL_MMBTU                NUMBER(18,4),
    SEG_OBC_LIFETIME_MMBTU              NUMBER(18,4),
    SEG_SMALL_BIZ_ANNUAL_MMBTU          NUMBER(18,4),
    SEG_SMALL_BIZ_LIFETIME_MMBTU        NUMBER(18,4),
    SEG_MULTIFAMILY_ANNUAL_MMBTU        NUMBER(18,4),
    SEG_MULTIFAMILY_LIFETIME_MMBTU      NUMBER(18,4),

    -- Section 14: Target Segments - Net Realized Site Savings
    -- Exclude Negative IEs | TRM x ISR x RR x NTG
    -- Residential LMI or OBC
    SEG_RES_LMI_OBC_ELEC_ANNUAL_KWH     NUMBER(18,4),
    SEG_RES_LMI_OBC_ELEC_LIFETIME_KWH   NUMBER(18,4),
    SEG_RES_LMI_OBC_GAS_ANNUAL_THERMS   NUMBER(18,4),
    SEG_RES_LMI_OBC_GAS_LIFETIME_THERMS NUMBER(18,4),
    -- Residential LMI Only
    SEG_RES_LMI_ELEC_ANNUAL_KWH         NUMBER(18,4),
    SEG_RES_LMI_ELEC_LIFETIME_KWH       NUMBER(18,4),
    SEG_RES_LMI_GAS_ANNUAL_THERMS       NUMBER(18,4),
    SEG_RES_LMI_GAS_LIFETIME_THERMS     NUMBER(18,4),
    -- OBC Only
    SEG_OBC_ELEC_ANNUAL_KWH             NUMBER(18,4),
    SEG_OBC_ELEC_LIFETIME_KWH           NUMBER(18,4),
    SEG_OBC_GAS_ANNUAL_THERMS           NUMBER(18,4),
    SEG_OBC_GAS_LIFETIME_THERMS         NUMBER(18,4),
    -- Small Business
    SEG_SMALL_BIZ_ELEC_ANNUAL_KWH       NUMBER(18,4),
    SEG_SMALL_BIZ_ELEC_LIFETIME_KWH     NUMBER(18,4),
    SEG_SMALL_BIZ_GAS_ANNUAL_THERMS     NUMBER(18,4),
    SEG_SMALL_BIZ_GAS_LIFETIME_THERMS   NUMBER(18,4),
    -- Multifamily
    SEG_MULTIFAMILY_ELEC_ANNUAL_KWH     NUMBER(18,4),
    SEG_MULTIFAMILY_ELEC_LIFETIME_KWH   NUMBER(18,4),
    SEG_MULTIFAMILY_GAS_ANNUAL_THERMS   NUMBER(18,4),
    SEG_MULTIFAMILY_GAS_LIFETIME_THERMS NUMBER(18,4),

    -- Audit Columns
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    UPDATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CREATED_BY                          VARCHAR2(100)   DEFAULT USER,
    UPDATED_BY                          VARCHAR2(100)   DEFAULT USER,

    -- Constraints
    CONSTRAINT CES_SAVINGS_FACT_ACT_FK1 FOREIGN KEY (VENDOR_SUBPROGRAM_SK)
        REFERENCES CES_SAVINGS_DIM_VENDOR_SUBPROGRAM(VENDOR_SUBPROGRAM_SK),
    CONSTRAINT CES_SAVINGS_FACT_ACT_FK2 FOREIGN KEY (PERIOD_SK)
        REFERENCES CES_SAVINGS_DIM_PERIOD(PERIOD_SK),
    CONSTRAINT CES_SAVINGS_FACT_ACT_CK1 CHECK (IS_CURRENT_VERSION IN ('Y','N'))
);

COMMENT ON TABLE  CES_SAVINGS_FACT_ACTUALS                       IS 'Fact table for actual savings. Grain: one row per vendor subprogram per reporting month. Wide table 78 metric columns. Versioned for resubmissions. Natural keys stored as migration backup.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.VENDOR_SUBPROGRAM_SK  IS 'FK to DIM_VENDOR_SUBPROGRAM. Renamed from VENDOR_SK V2.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.VERSION_NUMBER        IS 'Resubmission counter. Starts at 1. Increments each time vendor resubmits for same period.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.IS_CURRENT_VERSION    IS 'Y=Latest version. N=Superseded by resubmission. Power BI views filter Y only.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.VENDOR_SUBPROGRAM_KEY IS 'Natural key backup. If identity SKs corrupt during server migration use this to rebuild FK references.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.NET_SRC_ANNUAL_MMBTU  IS 'Key management metric. Highlighted yellow in source file.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.NET_SRC_LIFETIME_MMBTU IS 'Key management metric. Highlighted yellow in source file.';

-- Indexes
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX01 ON CES_SAVINGS_FACT_ACTUALS (VENDOR_SUBPROGRAM_SK);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX02 ON CES_SAVINGS_FACT_ACTUALS (PERIOD_SK);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX03 ON CES_SAVINGS_FACT_ACTUALS (VENDOR_SUBPROGRAM_KEY);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX04 ON CES_SAVINGS_FACT_ACTUALS (REPORTING_PERIOD);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX05 ON CES_SAVINGS_FACT_ACTUALS (IS_CURRENT_VERSION);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX06 ON CES_SAVINGS_FACT_ACTUALS (BATCH_ID);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX07 ON CES_SAVINGS_FACT_ACTUALS (VENDOR_SUBPROGRAM_SK, PERIOD_SK, IS_CURRENT_VERSION);


-- ------------------------------------------------------------
-- FACT: SAVINGS FORECAST
-- Source    : CES_SAVINGS_STG_FORECAST after validation
-- Grain     : One row per vendor subprogram per forecast month
-- Retention : 2 years hot, older moved to ARCH table annually
-- Extra     : FORECAST_PERIOD + CONFIDENCE_LEVEL vs actuals
-- Note      : CONFIDENCE_LEVEL enforced by CHECK not FK
--             Kimball approach - 3 values not worth a dim table
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_FACT_FORECAST (

    -- Surrogate Key
    FACT_SK                             NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Foreign Keys to Dimensions
    VENDOR_SUBPROGRAM_SK                NUMBER          NOT NULL,
    PERIOD_SK                           NUMBER          NOT NULL,

    -- Natural Keys (migration safety backup)
    VENDOR_SUBPROGRAM_KEY               VARCHAR2(200),
    FORECAST_PERIOD                     VARCHAR2(50),

    -- Forecast Specific Columns
    -- CONFIDENCE_LEVEL: controlled values High/Medium/Low
    -- Stored directly per Kimball best practice
    -- 3 values not worth a separate DIM_CONFIDENCE table
    -- Enforced by CHECK constraint instead of FK
    CONFIDENCE_LEVEL                    VARCHAR2(20),

    -- ETL Metadata
    SOURCE_FILE_NAME                    VARCHAR2(500),
    BATCH_ID                            NUMBER,
    LOAD_TIMESTAMP                      TIMESTAMP       DEFAULT SYSTIMESTAMP,

    -- Resubmission Versioning
    VERSION_NUMBER                      NUMBER          DEFAULT 1,
    IS_CURRENT_VERSION                  VARCHAR2(1)     DEFAULT 'Y',

    -- Section 1: Additional Dimension Attributes (snapshot)
    TRIENNIUM                           VARCHAR2(50),
    SECTOR                              VARCHAR2(200),
    PROGRAM                             VARCHAR2(200),
    SUBPROGRAM                          VARCHAR2(200),
    PROGRAM_YEAR                        VARCHAR2(50),

    -- Section 2: Financials (Costs)
    INVEST_COST_REBATE                  NUMBER(18,2),
    INVEST_COST_OBR                     NUMBER(18,2),
    INVEST_COST_OTHER                   NUMBER(18,2),
    TOTAL_INVEST_COST                   NUMBER(18,2),

    -- Section 3: Participants
    PARTICIPANTS_TOTAL                  NUMBER(10,0),
    PARTICIPANTS_RES_LMI_OBC            NUMBER(10,0),
    PARTICIPANTS_RES_LMI_ONLY           NUMBER(10,0),
    PARTICIPANTS_OBC_ONLY               NUMBER(10,0),
    PARTICIPANTS_SMALL_BIZ              NUMBER(10,0),

    -- Section 4: Gross Site Savings - Electric
    GROSS_SITE_ELEC_ANNUAL_KWH          NUMBER(18,4),
    GROSS_SITE_ELEC_DEMAND_KW           NUMBER(18,4),
    GROSS_SITE_ELEC_LIFETIME_KWH        NUMBER(18,4),

    -- Section 5: Gross Site Savings - Natural Gas
    GROSS_SITE_GAS_ANNUAL_THERMS        NUMBER(18,4),
    GROSS_SITE_GAS_DAILY_PEAK_THERMS    NUMBER(18,4),
    GROSS_SITE_GAS_LIFETIME_THERMS      NUMBER(18,4),

    -- Section 6: Net Realized Site Savings - Electric (TRM x ISR)
    NET_SITE_ISR_ELEC_ANNUAL_KWH        NUMBER(18,4),
    NET_SITE_ISR_ELEC_DEMAND_KW         NUMBER(18,4),
    NET_SITE_ISR_ELEC_LIFETIME_KWH      NUMBER(18,4),

    -- Section 7: Net Realized Site Savings - Gas (TRM x ISR)
    NET_SITE_ISR_GAS_ANNUAL_THERMS      NUMBER(18,4),
    NET_SITE_ISR_GAS_DAILY_PEAK_THERMS  NUMBER(18,4),
    NET_SITE_ISR_GAS_LIFETIME_THERMS    NUMBER(18,4),

    -- Section 8: Net Realized Site Savings - Electric (TRM x ISR x RR x NTG)
    NET_SITE_RR_NTG_ELEC_ANNUAL_KWH     NUMBER(18,4),
    NET_SITE_RR_NTG_ELEC_DEMAND_KW      NUMBER(18,4),
    NET_SITE_RR_NTG_ELEC_LIFETIME_KWH   NUMBER(18,4),

    -- Section 9: Net Realized Site Savings - Gas (TRM x ISR x RR x NTG)
    NET_SITE_RR_NTG_GAS_ANNUAL_THERMS   NUMBER(18,4),
    NET_SITE_RR_NTG_GAS_DAILY_PEAK_THERMS NUMBER(18,4),
    NET_SITE_RR_NTG_GAS_LIFETIME_THERMS NUMBER(18,4),

    -- Section 10: Negative Interactive Effects
    NEG_IE_ELEC_ANNUAL_KWH              NUMBER(18,4),
    NEG_IE_ELEC_LIFETIME_KWH            NUMBER(18,4),
    NEG_IE_GAS_ANNUAL_THERMS            NUMBER(18,4),
    NEG_IE_GAS_LIFETIME_THERMS          NUMBER(18,4),

    -- Section 11: Total Net Realized Site Savings
    TOTAL_NET_SITE_ELEC_ANNUAL_KWH      NUMBER(18,4),
    TOTAL_NET_SITE_ELEC_LIFETIME_KWH    NUMBER(18,4),
    TOTAL_NET_SITE_GAS_ANNUAL_THERMS    NUMBER(18,4),
    TOTAL_NET_SITE_GAS_LIFETIME_THERMS  NUMBER(18,4),

    -- Section 12: Net Realized Source Savings
    NET_SRC_ELEC_ANNUAL_MMBTU           NUMBER(18,4),
    NET_SRC_ELEC_LIFETIME_MMBTU         NUMBER(18,4),
    NET_SRC_GAS_ANNUAL_MMBTU            NUMBER(18,4),
    NET_SRC_GAS_LIFETIME_MMBTU          NUMBER(18,4),
    NET_SRC_ANNUAL_MMBTU                NUMBER(18,4),
    NET_SRC_LIFETIME_MMBTU              NUMBER(18,4),

    -- Section 13: Target Segments - Source Savings (MMBtu)
    SEG_LMI_OBC_ANNUAL_MMBTU            NUMBER(18,4),
    SEG_LMI_OBC_LIFETIME_MMBTU          NUMBER(18,4),
    SEG_LMI_ANNUAL_MMBTU                NUMBER(18,4),
    SEG_LMI_LIFETIME_MMBTU              NUMBER(18,4),
    SEG_OBC_ANNUAL_MMBTU                NUMBER(18,4),
    SEG_OBC_LIFETIME_MMBTU              NUMBER(18,4),
    SEG_SMALL_BIZ_ANNUAL_MMBTU          NUMBER(18,4),
    SEG_SMALL_BIZ_LIFETIME_MMBTU        NUMBER(18,4),
    SEG_MULTIFAMILY_ANNUAL_MMBTU        NUMBER(18,4),
    SEG_MULTIFAMILY_LIFETIME_MMBTU      NUMBER(18,4),

    -- Section 14: Target Segments - Site Savings (kWh/Therms)
    -- Residential LMI or OBC
    SEG_RES_LMI_OBC_ELEC_ANNUAL_KWH     NUMBER(18,4),
    SEG_RES_LMI_OBC_ELEC_LIFETIME_KWH   NUMBER(18,4),
    SEG_RES_LMI_OBC_GAS_ANNUAL_THERMS   NUMBER(18,4),
    SEG_RES_LMI_OBC_GAS_LIFETIME_THERMS NUMBER(18,4),
    -- Residential LMI Only
    SEG_RES_LMI_ELEC_ANNUAL_KWH         NUMBER(18,4),
    SEG_RES_LMI_ELEC_LIFETIME_KWH       NUMBER(18,4),
    SEG_RES_LMI_GAS_ANNUAL_THERMS       NUMBER(18,4),
    SEG_RES_LMI_GAS_LIFETIME_THERMS     NUMBER(18,4),
    -- OBC Only
    SEG_OBC_ELEC_ANNUAL_KWH             NUMBER(18,4),
    SEG_OBC_ELEC_LIFETIME_KWH           NUMBER(18,4),
    SEG_OBC_GAS_ANNUAL_THERMS           NUMBER(18,4),
    SEG_OBC_GAS_LIFETIME_THERMS         NUMBER(18,4),
    -- Small Business
    SEG_SMALL_BIZ_ELEC_ANNUAL_KWH       NUMBER(18,4),
    SEG_SMALL_BIZ_ELEC_LIFETIME_KWH     NUMBER(18,4),
    SEG_SMALL_BIZ_GAS_ANNUAL_THERMS     NUMBER(18,4),
    SEG_SMALL_BIZ_GAS_LIFETIME_THERMS   NUMBER(18,4),
    -- Multifamily
    SEG_MULTIFAMILY_ELEC_ANNUAL_KWH     NUMBER(18,4),
    SEG_MULTIFAMILY_ELEC_LIFETIME_KWH   NUMBER(18,4),
    SEG_MULTIFAMILY_GAS_ANNUAL_THERMS   NUMBER(18,4),
    SEG_MULTIFAMILY_GAS_LIFETIME_THERMS NUMBER(18,4),

    -- Audit Columns
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    UPDATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CREATED_BY                          VARCHAR2(100)   DEFAULT USER,
    UPDATED_BY                          VARCHAR2(100)   DEFAULT USER,

    -- Constraints
    CONSTRAINT CES_SAVINGS_FACT_FOR_FK1 FOREIGN KEY (VENDOR_SUBPROGRAM_SK)
        REFERENCES CES_SAVINGS_DIM_VENDOR_SUBPROGRAM(VENDOR_SUBPROGRAM_SK),
    CONSTRAINT CES_SAVINGS_FACT_FOR_FK2 FOREIGN KEY (PERIOD_SK)
        REFERENCES CES_SAVINGS_DIM_PERIOD(PERIOD_SK),
    CONSTRAINT CES_SAVINGS_FACT_FOR_CK1 CHECK (IS_CURRENT_VERSION IN ('Y','N')),
    -- CONFIDENCE_LEVEL enforced here instead of FK to separate dim
    -- Kimball best practice: 3 values not worth a dimension table
    CONSTRAINT CES_SAVINGS_FACT_FOR_CK2 CHECK (CONFIDENCE_LEVEL IN ('High','Medium','Low') OR CONFIDENCE_LEVEL IS NULL)
);

COMMENT ON TABLE  CES_SAVINGS_FACT_FORECAST                          IS 'Fact table for forecast savings through 6/30/2027. Same structure as FACT_ACTUALS plus FORECAST_PERIOD and CONFIDENCE_LEVEL. Grain: one row per vendor per forecast month.';
COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST.CONFIDENCE_LEVEL         IS 'Vendor forecast confidence. Valid values: High, Medium, Low. Enforced by CHECK constraint not FK. 3 values too few to justify separate dim table per Kimball best practice.';
COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST.FORECAST_PERIOD          IS 'Period this forecast covers. Intentionally different name from actuals REPORTING_PERIOD to avoid confusion.';
COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST.VERSION_NUMBER           IS 'Resubmission counter. Starts at 1.';
COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST.IS_CURRENT_VERSION       IS 'Y=Latest version. N=Superseded. Power BI views filter Y only.';

-- Indexes
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX01 ON CES_SAVINGS_FACT_FORECAST (VENDOR_SUBPROGRAM_SK);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX02 ON CES_SAVINGS_FACT_FORECAST (PERIOD_SK);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX03 ON CES_SAVINGS_FACT_FORECAST (VENDOR_SUBPROGRAM_KEY);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX04 ON CES_SAVINGS_FACT_FORECAST (FORECAST_PERIOD);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX05 ON CES_SAVINGS_FACT_FORECAST (IS_CURRENT_VERSION);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX06 ON CES_SAVINGS_FACT_FORECAST (BATCH_ID);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX07 ON CES_SAVINGS_FACT_FORECAST (CONFIDENCE_LEVEL);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX08 ON CES_SAVINGS_FACT_FORECAST (VENDOR_SUBPROGRAM_SK, PERIOD_SK, IS_CURRENT_VERSION);


-- ============================================================
-- SECTION 4: ARCHIVE TABLES
-- Purpose : Cold storage for data older than 2 years
--           Identical structure to FACT tables
--           Additional archive metadata columns
--           Populated by annual archiving process
--           No FK constraints - standalone historical records
-- ============================================================

CREATE TABLE CES_SAVINGS_FACT_ACTUALS_ARCH AS
SELECT
    f.*,
    CAST(NULL AS TIMESTAMP)     AS ARCHIVE_DATE,
    CAST(NULL AS VARCHAR2(200)) AS ARCHIVE_REASON,
    CAST(NULL AS VARCHAR2(100)) AS ARCHIVED_BY
FROM CES_SAVINGS_FACT_ACTUALS f
WHERE 1=0;

COMMENT ON TABLE CES_SAVINGS_FACT_ACTUALS_ARCH IS 'Archive for actual savings older than 2 years. Identical to FACT_ACTUALS structure. No FK constraints - standalone historical snapshot. Populated by annual archiving job.';

-- Archive index for period based queries
CREATE INDEX CES_SAVINGS_ACT_ARCH_IDX01 ON CES_SAVINGS_FACT_ACTUALS_ARCH (REPORTING_PERIOD);
CREATE INDEX CES_SAVINGS_ACT_ARCH_IDX02 ON CES_SAVINGS_FACT_ACTUALS_ARCH (VENDOR_SUBPROGRAM_KEY);
CREATE INDEX CES_SAVINGS_ACT_ARCH_IDX03 ON CES_SAVINGS_FACT_ACTUALS_ARCH (ARCHIVE_DATE);


CREATE TABLE CES_SAVINGS_FACT_FORECAST_ARCH AS
SELECT
    f.*,
    CAST(NULL AS TIMESTAMP)     AS ARCHIVE_DATE,
    CAST(NULL AS VARCHAR2(200)) AS ARCHIVE_REASON,
    CAST(NULL AS VARCHAR2(100)) AS ARCHIVED_BY
FROM CES_SAVINGS_FACT_FORECAST f
WHERE 1=0;

COMMENT ON TABLE CES_SAVINGS_FACT_FORECAST_ARCH IS 'Archive for forecast savings older than 2 years. Identical to FACT_FORECAST structure. No FK constraints - standalone historical snapshot. Populated by annual archiving job.';

CREATE INDEX CES_SAVINGS_FOR_ARCH_IDX01 ON CES_SAVINGS_FACT_FORECAST_ARCH (FORECAST_PERIOD);
CREATE INDEX CES_SAVINGS_FOR_ARCH_IDX02 ON CES_SAVINGS_FACT_FORECAST_ARCH (VENDOR_SUBPROGRAM_KEY);
CREATE INDEX CES_SAVINGS_FOR_ARCH_IDX03 ON CES_SAVINGS_FACT_FORECAST_ARCH (ARCHIVE_DATE);


-- ============================================================
-- SECTION 5: AUDIT/LOGGING TABLES
-- Purpose : Complete ETL traceability
--           Three levels: Batch, File, Row
--           Answers: when, what, how many, why failed
--           Powers pipeline health monitoring dashboard
-- ============================================================

-- ------------------------------------------------------------
-- AUDIT: BATCH LOG
-- Level   : Top level - one record per Jenkins run
-- Answers : Did monthly batch complete? How many files?
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_ETL_BATCH_LOG (
    BATCH_ID                            NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    BATCH_START_DT                      TIMESTAMP,
    BATCH_END_DT                        TIMESTAMP,
    STATUS                              VARCHAR2(20),
    -- File counts pre-aggregated for fast dashboard queries
    TOTAL_FILES_EXPECTED                NUMBER,
    FILES_PROCESSED                     NUMBER          DEFAULT 0,
    FILES_SUCCEEDED                     NUMBER          DEFAULT 0,
    FILES_FAILED                        NUMBER          DEFAULT 0,
    FILES_SKIPPED                       NUMBER          DEFAULT 0,
    FILES_QUARANTINED                   NUMBER          DEFAULT 0,
    -- Row counts
    TOTAL_ROWS_LOADED                   NUMBER          DEFAULT 0,
    TOTAL_ROWS_FAILED                   NUMBER          DEFAULT 0,
    -- Jenkins linkage - connect warehouse log to Jenkins log
    TRIGGERED_BY                        VARCHAR2(200),
    JENKINS_JOB_NAME                    VARCHAR2(200),
    JENKINS_BUILD_NUMBER                VARCHAR2(50),
    ERROR_MESSAGE                       VARCHAR2(4000),
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,

    CONSTRAINT CES_SAVINGS_BATCH_CK1    CHECK (STATUS IN ('RUNNING','SUCCESS','FAILED','PARTIAL'))
);

COMMENT ON TABLE  CES_SAVINGS_ETL_BATCH_LOG                      IS 'Top level ETL audit. One record per Jenkins pipeline run. Pre-aggregated file and row counts for fast dashboard queries.';
COMMENT ON COLUMN CES_SAVINGS_ETL_BATCH_LOG.STATUS               IS 'RUNNING=in progress. SUCCESS=all files loaded. FAILED=pipeline error. PARTIAL=some files failed some succeeded.';
COMMENT ON COLUMN CES_SAVINGS_ETL_BATCH_LOG.FILES_QUARANTINED    IS 'Files moved to quarantine folder due to validation failure.';
COMMENT ON COLUMN CES_SAVINGS_ETL_BATCH_LOG.JENKINS_BUILD_NUMBER IS 'Link to Jenkins build. Use to find full technical logs for failed batches.';

CREATE INDEX CES_SAVINGS_BATCH_IDX01 ON CES_SAVINGS_ETL_BATCH_LOG (STATUS);
CREATE INDEX CES_SAVINGS_BATCH_IDX02 ON CES_SAVINGS_ETL_BATCH_LOG (BATCH_START_DT);


-- ------------------------------------------------------------
-- AUDIT: FILE LOG
-- Level   : Mid level - one record per vendor file
-- Answers : Did specific vendor file load? Resubmission?
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_ETL_FILE_LOG (
    FILE_LOG_ID                         NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    BATCH_ID                            NUMBER          NOT NULL,
    FILE_NAME                           VARCHAR2(500),
    -- ACTUALS or FORECAST - which sheet was processed
    FILE_TYPE                           VARCHAR2(20),
    VENDOR_KEY                          VARCHAR2(200),
    REPORTING_PERIOD                    VARCHAR2(50),
    FILE_RECEIVED_DT                    TIMESTAMP,
    FILE_PROCESSED_DT                   TIMESTAMP,
    STATUS                              VARCHAR2(20),
    -- Row count comparison catches silent data loss
    ROW_COUNT_SOURCE                    NUMBER,
    ROW_COUNT_LOADED                    NUMBER          DEFAULT 0,
    ROW_COUNT_FAILED                    NUMBER          DEFAULT 0,
    -- Resubmission chain tracking
    -- IS_RESUBMISSION=Y + PREV_FILE_LOG_ID creates audit chain
    IS_RESUBMISSION                     VARCHAR2(1)     DEFAULT 'N',
    PREV_FILE_LOG_ID                    NUMBER,
    -- File location tracking
    FILE_PATH_SOURCE                    VARCHAR2(1000),
    FILE_PATH_ARCHIVE                   VARCHAR2(1000),
    FILE_PATH_QUARANTINE                VARCHAR2(1000),
    VALIDATION_ERRORS                   VARCHAR2(4000),
    ERROR_MESSAGE                       VARCHAR2(4000),
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,

    CONSTRAINT CES_SAVINGS_FILE_FK1     FOREIGN KEY (BATCH_ID)
        REFERENCES CES_SAVINGS_ETL_BATCH_LOG(BATCH_ID),
    CONSTRAINT CES_SAVINGS_FILE_FK2     FOREIGN KEY (PREV_FILE_LOG_ID)
        REFERENCES CES_SAVINGS_ETL_FILE_LOG(FILE_LOG_ID),
    CONSTRAINT CES_SAVINGS_FILE_CK1     CHECK (STATUS IN ('PROCESSING','SUCCESS','FAILED','QUARANTINE','SKIPPED','RESUBMISSION')),
    CONSTRAINT CES_SAVINGS_FILE_CK2     CHECK (IS_RESUBMISSION IN ('Y','N')),
    CONSTRAINT CES_SAVINGS_FILE_CK3     CHECK (FILE_TYPE IN ('ACTUALS','FORECAST'))
);

COMMENT ON TABLE  CES_SAVINGS_ETL_FILE_LOG                       IS 'Mid level ETL audit. One record per vendor file. Links batch to row errors. Tracks resubmission chain via PREV_FILE_LOG_ID.';
COMMENT ON COLUMN CES_SAVINGS_ETL_FILE_LOG.FILE_TYPE             IS 'ACTUALS=from Savings Results tab. FORECAST=from Savings Forecast tab.';
COMMENT ON COLUMN CES_SAVINGS_ETL_FILE_LOG.IS_RESUBMISSION       IS 'Y=Vendor resubmitted corrected file. N=Original submission.';
COMMENT ON COLUMN CES_SAVINGS_ETL_FILE_LOG.PREV_FILE_LOG_ID      IS 'Points to original file when IS_RESUBMISSION=Y. Creates resubmission audit chain.';
COMMENT ON COLUMN CES_SAVINGS_ETL_FILE_LOG.ROW_COUNT_SOURCE      IS 'Total rows in source file. Compare to ROW_COUNT_LOADED to detect silent data loss.';

CREATE INDEX CES_SAVINGS_FILE_IDX01 ON CES_SAVINGS_ETL_FILE_LOG (BATCH_ID);
CREATE INDEX CES_SAVINGS_FILE_IDX02 ON CES_SAVINGS_ETL_FILE_LOG (VENDOR_KEY);
CREATE INDEX CES_SAVINGS_FILE_IDX03 ON CES_SAVINGS_ETL_FILE_LOG (STATUS);
CREATE INDEX CES_SAVINGS_FILE_IDX04 ON CES_SAVINGS_ETL_FILE_LOG (IS_RESUBMISSION);
CREATE INDEX CES_SAVINGS_FILE_IDX05 ON CES_SAVINGS_ETL_FILE_LOG (REPORTING_PERIOD);


-- ------------------------------------------------------------
-- AUDIT: ROW LOG
-- Level   : Lowest level - one record per failed row
-- Answers : Which row failed? What value? How to fix?
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_ETL_ROW_LOG (
    ROW_LOG_ID                          NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    FILE_LOG_ID                         NUMBER          NOT NULL,
    BATCH_ID                            NUMBER          NOT NULL,
    -- Row number in source Excel for vendor communication
    ROW_NUMBER                          NUMBER,
    COLUMN_NAME                         VARCHAR2(200),
    -- Exact value vendor sent - evidence for dispute resolution
    SOURCE_VALUE                        VARCHAR2(4000),
    EXPECTED_FORMAT                     VARCHAR2(200),
    ERROR_TYPE                          VARCHAR2(50),
    ERROR_MESSAGE                       VARCHAR2(4000),
    STATUS                              VARCHAR2(20)    DEFAULT 'FAILED',
    -- Resolution tracking builds knowledge base
    RESOLVED_DT                         TIMESTAMP,
    RESOLVED_BY                         VARCHAR2(100),
    RESOLUTION_NOTES                    VARCHAR2(4000),
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,

    CONSTRAINT CES_SAVINGS_ROW_FK1      FOREIGN KEY (FILE_LOG_ID)
        REFERENCES CES_SAVINGS_ETL_FILE_LOG(FILE_LOG_ID),
    CONSTRAINT CES_SAVINGS_ROW_FK2      FOREIGN KEY (BATCH_ID)
        REFERENCES CES_SAVINGS_ETL_BATCH_LOG(BATCH_ID),
    CONSTRAINT CES_SAVINGS_ROW_CK1      CHECK (ERROR_TYPE IN
        ('NULL_VALUE','TYPE_MISMATCH','OUT_OF_RANGE','DUPLICATE','MISSING_COLUMN','OTHER')),
    CONSTRAINT CES_SAVINGS_ROW_CK2      CHECK (STATUS IN ('FAILED','RESOLVED','IGNORED'))
);

COMMENT ON TABLE  CES_SAVINGS_ETL_ROW_LOG                        IS 'Lowest level ETL audit. One record per failed row or cell. Used for vendor communication and debugging. RESOLUTION_NOTES builds knowledge base for recurring errors.';
COMMENT ON COLUMN CES_SAVINGS_ETL_ROW_LOG.ROW_NUMBER             IS 'Row number in source Excel including header rows. Tell vendor exactly which row to fix.';
COMMENT ON COLUMN CES_SAVINGS_ETL_ROW_LOG.SOURCE_VALUE           IS 'Exact value vendor submitted. Evidence for dispute resolution. Cannot be disputed.';
COMMENT ON COLUMN CES_SAVINGS_ETL_ROW_LOG.RESOLUTION_NOTES       IS 'How error was resolved. Builds knowledge base. Same error next month check here first.';

CREATE INDEX CES_SAVINGS_ROW_IDX01 ON CES_SAVINGS_ETL_ROW_LOG (FILE_LOG_ID);
CREATE INDEX CES_SAVINGS_ROW_IDX02 ON CES_SAVINGS_ETL_ROW_LOG (BATCH_ID);
CREATE INDEX CES_SAVINGS_ROW_IDX03 ON CES_SAVINGS_ETL_ROW_LOG (STATUS);
CREATE INDEX CES_SAVINGS_ROW_IDX04 ON CES_SAVINGS_ETL_ROW_LOG (ERROR_TYPE);


-- ============================================================
-- SECTION 6: REPORTING VIEWS
-- Purpose : Power BI connects to these only
--           Never expose raw FACT tables to Power BI
--           Pre-filtered for current versions
--           Pre-joined to dimensions
--           Business friendly column names
--           Expand with calculated columns as needed
-- ============================================================

-- Current Actuals - Primary Power BI source
CREATE OR REPLACE VIEW CES_SAVINGS_VW_ACTUALS AS
SELECT
    -- Keys
    a.FACT_SK,
    -- Period attributes from DIM
    p.DATA_DATE,
    p.DATA_YEAR_MONTH,
    p.REPORTING_PERIOD,
    p.PERIOD_YEAR,
    p.PERIOD_MONTH,
    p.PERIOD_MONTH_NAME,
    p.FISCAL_YEAR,
    p.FISCAL_QUARTER,
    p.TRIENNIUM,
    p.PROGRAM_YEAR,
    -- Vendor attributes from DIM
    v.VENDOR_SUBPROGRAM_KEY,
    v.VENDOR_NAME,
    v.SECTOR,
    v.PROGRAM,
    v.SUBPROGRAM,
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
    -- Net Site Savings ISR
    a.NET_SITE_ISR_ELEC_ANNUAL_KWH,
    a.NET_SITE_ISR_ELEC_DEMAND_KW,
    a.NET_SITE_ISR_ELEC_LIFETIME_KWH,
    a.NET_SITE_ISR_GAS_ANNUAL_THERMS,
    a.NET_SITE_ISR_GAS_LIFETIME_THERMS,
    -- Net Site Savings RR NTG
    a.NET_SITE_RR_NTG_ELEC_ANNUAL_KWH,
    a.NET_SITE_RR_NTG_ELEC_DEMAND_KW,
    a.NET_SITE_RR_NTG_ELEC_LIFETIME_KWH,
    a.NET_SITE_RR_NTG_GAS_ANNUAL_THERMS,
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
FROM      CES_SAVINGS_FACT_ACTUALS          a
JOIN      CES_SAVINGS_DIM_VENDOR_SUBPROGRAM v
    ON    a.VENDOR_SUBPROGRAM_SK            = v.VENDOR_SUBPROGRAM_SK
    AND   v.IS_CURRENT                      = 'Y'
JOIN      CES_SAVINGS_DIM_PERIOD            p
    ON    a.PERIOD_SK                       = p.PERIOD_SK
WHERE     a.IS_CURRENT_VERSION              = 'Y';

COMMENT ON TABLE CES_SAVINGS_VW_ACTUALS IS 'Primary Power BI view for actuals. Pre-joined to all dims. Filtered to current versions and current dim records only. Add calculated columns here as business needs evolve.';


-- Current Forecast - Power BI forecast source
CREATE OR REPLACE VIEW CES_SAVINGS_VW_FORECAST AS
SELECT
    -- Keys
    f.FACT_SK,
    -- Period attributes
    p.DATA_DATE,
    p.DATA_YEAR_MONTH,
    f.FORECAST_PERIOD,
    p.PERIOD_YEAR,
    p.PERIOD_MONTH,
    p.PERIOD_MONTH_NAME,
    p.FISCAL_YEAR,
    p.FISCAL_QUARTER,
    p.TRIENNIUM,
    p.PROGRAM_YEAR,
    -- Vendor attributes
    v.VENDOR_SUBPROGRAM_KEY,
    v.VENDOR_NAME,
    v.SECTOR,
    v.PROGRAM,
    v.SUBPROGRAM,
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
    f.GROSS_SITE_ELEC_LIFETIME_KWH,
    f.GROSS_SITE_GAS_ANNUAL_THERMS,
    f.GROSS_SITE_GAS_LIFETIME_THERMS,
    f.NET_SRC_ANNUAL_MMBTU,
    f.NET_SRC_LIFETIME_MMBTU,
    f.TOTAL_NET_SITE_ELEC_ANNUAL_KWH,
    f.TOTAL_NET_SITE_ELEC_LIFETIME_KWH,
    f.TOTAL_NET_SITE_GAS_ANNUAL_THERMS,
    f.TOTAL_NET_SITE_GAS_LIFETIME_THERMS,
    -- Metadata
    f.SOURCE_FILE_NAME,
    f.LOAD_TIMESTAMP,
    f.VERSION_NUMBER
FROM      CES_SAVINGS_FACT_FORECAST          f
JOIN      CES_SAVINGS_DIM_VENDOR_SUBPROGRAM  v
    ON    f.VENDOR_SUBPROGRAM_SK             = v.VENDOR_SUBPROGRAM_SK
    AND   v.IS_CURRENT                       = 'Y'
JOIN      CES_SAVINGS_DIM_PERIOD             p
    ON    f.PERIOD_SK                        = p.PERIOD_SK
WHERE     f.IS_CURRENT_VERSION               = 'Y';

COMMENT ON TABLE CES_SAVINGS_VW_FORECAST IS 'Primary Power BI view for forecasts. Pre-joined to all dims. Filtered to current versions only. Includes CONFIDENCE_LEVEL for management planning.';


-- Actuals vs Forecast Variance
CREATE OR REPLACE VIEW CES_SAVINGS_VW_ACTUALS_VS_FORECAST AS
SELECT
    a.DATA_DATE,
    a.REPORTING_PERIOD                      AS PERIOD,
    a.FISCAL_YEAR,
    a.FISCAL_QUARTER,
    a.VENDOR_SUBPROGRAM_KEY,
    a.VENDOR_NAME,
    a.SECTOR,
    a.PROGRAM,
    a.SUBPROGRAM,
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
    AND   a.DATA_DATE                       = f.DATA_DATE;

COMMENT ON TABLE CES_SAVINGS_VW_ACTUALS_VS_FORECAST IS 'Variance view. Actuals vs forecast side by side with pre-calculated variances. Primary management reporting view. Expand with additional metric comparisons as needed.';


-- Pipeline Health Monitoring
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
    -- Processing time in minutes
    ROUND(
        (b.BATCH_END_DT - b.BATCH_START_DT) * 24 * 60, 2
    )                                       AS PROCESSING_MINUTES,
    -- Success rate percentage
    CASE
        WHEN b.TOTAL_FILES_EXPECTED > 0
        THEN ROUND(b.FILES_SUCCEEDED / b.TOTAL_FILES_EXPECTED * 100, 1)
        ELSE 0
    END                                     AS SUCCESS_RATE_PCT
FROM      CES_SAVINGS_ETL_BATCH_LOG         b
ORDER BY  b.BATCH_START_DT DESC;

COMMENT ON TABLE CES_SAVINGS_VW_PIPELINE_HEALTH IS 'Operations monitoring view. ETL run history with file counts, processing time and success rate. Use for pipeline health dashboard in Power BI.';


-- ============================================================
-- SECTION 7: ARCHIVING STORED PROCEDURE
-- Purpose : Annual job to move data older than 2 years
--           From FACT to ARCH tables
--           Run via Jenkins annually
-- ============================================================

CREATE OR REPLACE PROCEDURE CES_SAVINGS_ARCHIVE_OLD_DATA AS
    v_cutoff_date DATE := ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -24);
BEGIN
    -- Archive actuals older than 2 years
    INSERT INTO CES_SAVINGS_FACT_ACTUALS_ARCH
    SELECT
        f.*,
        SYSTIMESTAMP,
        'ANNUAL_ARCHIVE',
        USER
    FROM CES_SAVINGS_FACT_ACTUALS f
    JOIN CES_SAVINGS_DIM_PERIOD   p ON f.PERIOD_SK = p.PERIOD_SK
    WHERE p.DATA_DATE < v_cutoff_date;

    DELETE FROM CES_SAVINGS_FACT_ACTUALS f
    WHERE EXISTS (
        SELECT 1
        FROM   CES_SAVINGS_DIM_PERIOD p
        WHERE  p.PERIOD_SK  = f.PERIOD_SK
        AND    p.DATA_DATE  < v_cutoff_date
    );

    -- Archive forecasts older than 2 years
    INSERT INTO CES_SAVINGS_FACT_FORECAST_ARCH
    SELECT
        f.*,
        SYSTIMESTAMP,
        'ANNUAL_ARCHIVE',
        USER
    FROM CES_SAVINGS_FACT_FORECAST f
    JOIN CES_SAVINGS_DIM_PERIOD    p ON f.PERIOD_SK = p.PERIOD_SK
    WHERE p.DATA_DATE < v_cutoff_date;

    DELETE FROM CES_SAVINGS_FACT_FORECAST f
    WHERE EXISTS (
        SELECT 1
        FROM   CES_SAVINGS_DIM_PERIOD p
        WHERE  p.PERIOD_SK  = f.PERIOD_SK
        AND    p.DATA_DATE  < v_cutoff_date
    );

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END CES_SAVINGS_ARCHIVE_OLD_DATA;
/

COMMENT ON PROCEDURE CES_SAVINGS_ARCHIVE_OLD_DATA IS 'Annual archiving job. Moves data older than 2 years from FACT to ARCH tables. Run via Jenkins annually. Rolls back completely on any error.';


-- ============================================================
-- END OF DDL V2
-- ============================================================
-- Summary:
-- STG Tables  : 2
-- DIM Tables  : 2  (renamed and updated from V1)
-- FACT Tables : 2  (updated FK references)
-- ARCH Tables : 2
-- AUDIT Tables: 3
-- Views       : 4
-- Procedures  : 1  (archiving)
-- Total Tables: 11
-- ============================================================
