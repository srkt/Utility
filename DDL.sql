-- ============================================================
-- CES SAVINGS DATA WAREHOUSE - COMPLETE DDL
-- Oracle 21c
-- Generated: 2026-06-02
-- ============================================================
-- Table List:
-- STG Layer  : CES_SAVINGS_STG_ACTUALS, CES_SAVINGS_STG_FORECAST
-- DIM Layer  : CES_SAVINGS_DIM_VENDOR, CES_SAVINGS_DIM_PERIOD
-- FACT Layer : CES_SAVINGS_FACT_ACTUALS, CES_SAVINGS_FACT_FORECAST
-- ARCH Layer : CES_SAVINGS_FACT_ACTUALS_ARCH, CES_SAVINGS_FACT_FORECAST_ARCH
-- AUDIT Layer: CES_SAVINGS_ETL_BATCH_LOG, CES_SAVINGS_ETL_FILE_LOG, CES_SAVINGS_ETL_ROW_LOG
-- ============================================================


-- ============================================================
-- SECTION 1: STAGING TABLES
-- Purpose : Exact mirror of source Excel files
--           All columns VARCHAR2 - no constraints
--           Truncated and reloaded every ETL run
-- ============================================================

-- ------------------------------------------------------------
-- STG: SAVINGS ACTUALS
-- Source: Sheet3 - Savings Results tab
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_STG_ACTUALS (
    -- Metadata
    STG_ID                              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    SOURCE_FILE_NAME                    VARCHAR2(500),
    BATCH_ID                            NUMBER,
    LOAD_TIMESTAMP                      TIMESTAMP DEFAULT SYSTIMESTAMP,

    -- Section 1: Identity/Dimensions
    TRIENNIUM                           VARCHAR2(50),
    SECTOR                              VARCHAR2(200),
    VENDOR_SUBPROGRAM_KEY               VARCHAR2(200),
    PROGRAM                             VARCHAR2(200),
    SUBPROGRAM                          VARCHAR2(200),
    PROGRAM_YEAR                        VARCHAR2(50),
    REPORTING_PERIOD                    VARCHAR2(50),

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

    -- Section 4: Gross Site Savings - Electric (Exclude IEs, TRM Protocol)
    GROSS_SITE_ELEC_ANNUAL_KWH          VARCHAR2(50),
    GROSS_SITE_ELEC_DEMAND_KW           VARCHAR2(50),
    GROSS_SITE_ELEC_LIFETIME_KWH        VARCHAR2(50),

    -- Section 5: Gross Site Savings - Natural Gas (Exclude IEs, TRM Protocol)
    GROSS_SITE_GAS_ANNUAL_THERMS        VARCHAR2(50),
    GROSS_SITE_GAS_DAILY_PEAK_THERMS    VARCHAR2(50),
    GROSS_SITE_GAS_LIFETIME_THERMS      VARCHAR2(50),

    -- Section 6: Net Realized Site Savings - Electric (Exclude Negative IEs, TRM x ISR)
    NET_SITE_ISR_ELEC_ANNUAL_KWH        VARCHAR2(50),
    NET_SITE_ISR_ELEC_DEMAND_KW         VARCHAR2(50),
    NET_SITE_ISR_ELEC_LIFETIME_KWH      VARCHAR2(50),

    -- Section 7: Net Realized Site Savings - Gas (Exclude Negative IEs, TRM x ISR)
    NET_SITE_ISR_GAS_ANNUAL_THERMS      VARCHAR2(50),
    NET_SITE_ISR_GAS_DAILY_PEAK_THERMS  VARCHAR2(50),
    NET_SITE_ISR_GAS_LIFETIME_THERMS    VARCHAR2(50),

    -- Section 8: Net Realized Site Savings - Electric (Exclude Negative IEs, TRM x ISR x RR x NTG)
    NET_SITE_RR_NTG_ELEC_ANNUAL_KWH     VARCHAR2(50),
    NET_SITE_RR_NTG_ELEC_DEMAND_KW      VARCHAR2(50),
    NET_SITE_RR_NTG_ELEC_LIFETIME_KWH   VARCHAR2(50),

    -- Section 9: Net Realized Site Savings - Gas (Exclude Negative IEs, TRM x ISR x RR x NTG)
    NET_SITE_RR_NTG_GAS_ANNUAL_THERMS   VARCHAR2(50),
    NET_SITE_RR_NTG_GAS_DAILY_PEAK_THERMS VARCHAR2(50),
    NET_SITE_RR_NTG_GAS_LIFETIME_THERMS VARCHAR2(50),

    -- Section 10: Negative Interactive Effects (IEs)
    NEG_IE_ELEC_ANNUAL_KWH              VARCHAR2(50),
    NEG_IE_ELEC_LIFETIME_KWH            VARCHAR2(50),
    NEG_IE_GAS_ANNUAL_THERMS            VARCHAR2(50),
    NEG_IE_GAS_LIFETIME_THERMS          VARCHAR2(50),

    -- Section 11: Total Net Realized Site Savings (Include Negative IEs)
    TOTAL_NET_SITE_ELEC_ANNUAL_KWH      VARCHAR2(50),
    TOTAL_NET_SITE_ELEC_LIFETIME_KWH    VARCHAR2(50),
    TOTAL_NET_SITE_GAS_ANNUAL_THERMS    VARCHAR2(50),
    TOTAL_NET_SITE_GAS_LIFETIME_THERMS  VARCHAR2(50),

    -- Section 12: Net Realized Source Savings (Includes Negative IEs, TRM x ISR x RR x NTG x Source Conversion)
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

    -- Section 14: Target Segments - Net Realized Site Savings (Exclude Negative IEs, TRM x ISR x RR x NTG)
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

COMMENT ON TABLE CES_SAVINGS_STG_ACTUALS IS 'Staging table for actual savings data. Mirrors source Excel Savings Results tab. All columns VARCHAR2. Truncated each ETL run.';


-- ------------------------------------------------------------
-- STG: SAVINGS FORECAST
-- Source: Sheet4 - Savings Forecast tab
-- Same structure as actuals + FORECAST_PERIOD + CONFIDENCE_LEVEL
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_STG_FORECAST (
    -- Metadata
    STG_ID                              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    SOURCE_FILE_NAME                    VARCHAR2(500),
    BATCH_ID                            NUMBER,
    LOAD_TIMESTAMP                      TIMESTAMP DEFAULT SYSTIMESTAMP,

    -- Section 1: Identity/Dimensions
    TRIENNIUM                           VARCHAR2(50),
    SECTOR                              VARCHAR2(200),
    VENDOR_SUBPROGRAM_KEY               VARCHAR2(200),
    PROGRAM                             VARCHAR2(200),
    SUBPROGRAM                          VARCHAR2(200),
    PROGRAM_YEAR                        VARCHAR2(50),
    FORECAST_PERIOD                     VARCHAR2(50),   -- Different from actuals
    CONFIDENCE_LEVEL                    VARCHAR2(50),   -- Forecast only

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

COMMENT ON TABLE CES_SAVINGS_STG_FORECAST IS 'Staging table for forecast savings data. Mirrors source Excel Savings Forecast tab. All columns VARCHAR2. Truncated each ETL run.';


-- ============================================================
-- SECTION 2: DIMENSION TABLES
-- Purpose : Conformed dimensions shared by both fact tables
--           SCD Type 2 columns included, controlled by ETL flag
-- ============================================================

-- ------------------------------------------------------------
-- DIM: VENDOR
-- One row per unique Vendor Subprogram Key
-- Autofills Program, Subprogram, Sector, Triennium
-- SCD columns present, activated by ETL config flag
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_DIM_VENDOR (
    -- Surrogate Key
    VENDOR_SK                           NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Natural Key (used as backup during migrations)
    VENDOR_SUBPROGRAM_KEY               VARCHAR2(200)   NOT NULL,

    -- Vendor Attributes
    SECTOR                              VARCHAR2(200),
    PROGRAM                             VARCHAR2(200),
    SUBPROGRAM                          VARCHAR2(200),
    PROGRAM_YEAR                        VARCHAR2(50),
    TRIENNIUM                           VARCHAR2(50),

    -- SCD Type 2 Columns
    -- Controlled by ETL config: scd_enabled: true/false
    -- When scd_enabled=false: EFFECTIVE_DATE=load date, EXPIRY_DATE=NULL, IS_CURRENT=Y
    -- When scd_enabled=true:  Full history tracking enabled
    EFFECTIVE_DATE                      DATE            DEFAULT SYSDATE,
    EXPIRY_DATE                         DATE,
    IS_CURRENT                          VARCHAR2(1)     DEFAULT 'Y',

    -- Audit Columns
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    UPDATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CREATED_BY                          VARCHAR2(100)   DEFAULT USER,
    UPDATED_BY                          VARCHAR2(100)   DEFAULT USER,

    -- Constraints
    CONSTRAINT CES_SAVINGS_DIM_VEN_CK1 CHECK (IS_CURRENT IN ('Y','N'))
);

COMMENT ON TABLE  CES_SAVINGS_DIM_VENDOR                    IS 'Vendor dimension. One row per unique Vendor Subprogram Key. SCD Type 2 ready, controlled by ETL config flag scd_enabled.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR.VENDOR_SK          IS 'Surrogate primary key. Identity column.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR.VENDOR_SUBPROGRAM_KEY IS 'Natural key from source file. Used as backup key during server migrations.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR.IS_CURRENT         IS 'Y=Current active record. N=Expired record (SCD Type 2). Always Y when scd_enabled=false.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR.EFFECTIVE_DATE     IS 'Date this record became active. Used by SCD Type 2.';
COMMENT ON COLUMN CES_SAVINGS_DIM_VENDOR.EXPIRY_DATE        IS 'Date this record was superseded. NULL=still active. Used by SCD Type 2.';

-- Indexes
CREATE UNIQUE INDEX CES_SAVINGS_DIM_VEN_IDX01 ON CES_SAVINGS_DIM_VENDOR (VENDOR_SUBPROGRAM_KEY, IS_CURRENT);
CREATE INDEX        CES_SAVINGS_DIM_VEN_IDX02 ON CES_SAVINGS_DIM_VENDOR (PROGRAM);
CREATE INDEX        CES_SAVINGS_DIM_VEN_IDX03 ON CES_SAVINGS_DIM_VENDOR (SECTOR);


-- ------------------------------------------------------------
-- DIM: PERIOD
-- One row per unique reporting period
-- Enriched with fiscal year, quarter, triennium
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_DIM_PERIOD (
    -- Surrogate Key
    PERIOD_SK                           NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Natural Key
    REPORTING_PERIOD                    VARCHAR2(50)    NOT NULL,

    -- Period Attributes
    PERIOD_YEAR                         NUMBER(4),
    PERIOD_MONTH                        NUMBER(2),
    PERIOD_MONTH_NAME                   VARCHAR2(20),
    FISCAL_YEAR                         VARCHAR2(20),
    FISCAL_QUARTER                      VARCHAR2(10),
    TRIENNIUM                           VARCHAR2(50),
    PROGRAM_YEAR                        NUMBER(4),

    -- Audit Columns
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    UPDATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CREATED_BY                          VARCHAR2(100)   DEFAULT USER,
    UPDATED_BY                          VARCHAR2(100)   DEFAULT USER,

    -- Constraints
    CONSTRAINT CES_SAVINGS_DIM_PER_UQ1 UNIQUE (REPORTING_PERIOD)
);

COMMENT ON TABLE  CES_SAVINGS_DIM_PERIOD                        IS 'Time dimension. One row per unique reporting period. Enriched with fiscal year and quarter for reporting.';
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.PERIOD_SK              IS 'Surrogate primary key. Identity column.';
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.REPORTING_PERIOD       IS 'Natural key. Original period value from source file e.g. Jan 2026.';
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.FISCAL_YEAR            IS 'Fiscal year derived from reporting period e.g. FY2026.';
COMMENT ON COLUMN CES_SAVINGS_DIM_PERIOD.FISCAL_QUARTER         IS 'Fiscal quarter derived from reporting period e.g. Q1.';

-- Indexes
CREATE INDEX CES_SAVINGS_DIM_PER_IDX01 ON CES_SAVINGS_DIM_PERIOD (PERIOD_YEAR, PERIOD_MONTH);
CREATE INDEX CES_SAVINGS_DIM_PER_IDX02 ON CES_SAVINGS_DIM_PERIOD (FISCAL_YEAR);
CREATE INDEX CES_SAVINGS_DIM_PER_IDX03 ON CES_SAVINGS_DIM_PERIOD (TRIENNIUM);


-- ============================================================
-- SECTION 3: FACT TABLES
-- Purpose : Core metric storage
--           Typed columns, FK to dimensions
--           Natural keys stored as backup
--           Wide table format (all 78 columns)
-- ============================================================

-- ------------------------------------------------------------
-- FACT: SAVINGS ACTUALS
-- Source: CES_SAVINGS_STG_ACTUALS after validation
-- One row per vendor + reporting period combination
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_FACT_ACTUALS (
    -- Surrogate Key
    FACT_SK                             NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Foreign Keys to Dimensions
    VENDOR_SK                           NUMBER          NOT NULL,
    PERIOD_SK                           NUMBER          NOT NULL,

    -- Natural Keys (backup for migration safety)
    VENDOR_SUBPROGRAM_KEY               VARCHAR2(200),
    REPORTING_PERIOD                    VARCHAR2(50),

    -- ETL Metadata
    SOURCE_FILE_NAME                    VARCHAR2(500),
    BATCH_ID                            NUMBER,
    VERSION_NUMBER                      NUMBER          DEFAULT 1,
    IS_CURRENT_VERSION                  VARCHAR2(1)     DEFAULT 'Y',
    LOAD_TIMESTAMP                      TIMESTAMP       DEFAULT SYSTIMESTAMP,

    -- Section 1: Additional Dimension Attributes
    -- Stored here to avoid over-normalization
    TRIENNIUM                           VARCHAR2(50),
    SECTOR                              VARCHAR2(200),
    PROGRAM                             VARCHAR2(200),
    SUBPROGRAM                          VARCHAR2(200),
    PROGRAM_YEAR                        VARCHAR2(50),

    -- Section 2: Financials (Costs) NUMBER(18,2) = dollars and cents
    INVEST_COST_REBATE                  NUMBER(18,2),
    INVEST_COST_OBR                     NUMBER(18,2),
    INVEST_COST_OTHER                   NUMBER(18,2),
    TOTAL_INVEST_COST                   NUMBER(18,2),

    -- Section 3: Participants (whole numbers)
    PARTICIPANTS_TOTAL                  NUMBER(10,0),
    PARTICIPANTS_RES_LMI_OBC            NUMBER(10,0),
    PARTICIPANTS_RES_LMI_ONLY           NUMBER(10,0),
    PARTICIPANTS_OBC_ONLY               NUMBER(10,0),
    PARTICIPANTS_SMALL_BIZ              NUMBER(10,0),

    -- Section 4: Gross Site Savings - Electric (Exclude IEs, TRM Protocol)
    GROSS_SITE_ELEC_ANNUAL_KWH          NUMBER(18,4),
    GROSS_SITE_ELEC_DEMAND_KW           NUMBER(18,4),
    GROSS_SITE_ELEC_LIFETIME_KWH        NUMBER(18,4),

    -- Section 5: Gross Site Savings - Natural Gas (Exclude IEs, TRM Protocol)
    GROSS_SITE_GAS_ANNUAL_THERMS        NUMBER(18,4),
    GROSS_SITE_GAS_DAILY_PEAK_THERMS    NUMBER(18,4),
    GROSS_SITE_GAS_LIFETIME_THERMS      NUMBER(18,4),

    -- Section 6: Net Realized Site Savings - Electric (Exclude Negative IEs, TRM x ISR)
    NET_SITE_ISR_ELEC_ANNUAL_KWH        NUMBER(18,4),
    NET_SITE_ISR_ELEC_DEMAND_KW         NUMBER(18,4),
    NET_SITE_ISR_ELEC_LIFETIME_KWH      NUMBER(18,4),

    -- Section 7: Net Realized Site Savings - Gas (Exclude Negative IEs, TRM x ISR)
    NET_SITE_ISR_GAS_ANNUAL_THERMS      NUMBER(18,4),
    NET_SITE_ISR_GAS_DAILY_PEAK_THERMS  NUMBER(18,4),
    NET_SITE_ISR_GAS_LIFETIME_THERMS    NUMBER(18,4),

    -- Section 8: Net Realized Site Savings - Electric (Exclude Negative IEs, TRM x ISR x RR x NTG)
    NET_SITE_RR_NTG_ELEC_ANNUAL_KWH     NUMBER(18,4),
    NET_SITE_RR_NTG_ELEC_DEMAND_KW      NUMBER(18,4),
    NET_SITE_RR_NTG_ELEC_LIFETIME_KWH   NUMBER(18,4),

    -- Section 9: Net Realized Site Savings - Gas (Exclude Negative IEs, TRM x ISR x RR x NTG)
    NET_SITE_RR_NTG_GAS_ANNUAL_THERMS   NUMBER(18,4),
    NET_SITE_RR_NTG_GAS_DAILY_PEAK_THERMS NUMBER(18,4),
    NET_SITE_RR_NTG_GAS_LIFETIME_THERMS NUMBER(18,4),

    -- Section 10: Negative Interactive Effects (IEs)
    NEG_IE_ELEC_ANNUAL_KWH              NUMBER(18,4),
    NEG_IE_ELEC_LIFETIME_KWH            NUMBER(18,4),
    NEG_IE_GAS_ANNUAL_THERMS            NUMBER(18,4),
    NEG_IE_GAS_LIFETIME_THERMS          NUMBER(18,4),

    -- Section 11: Total Net Realized Site Savings (Include Negative IEs)
    TOTAL_NET_SITE_ELEC_ANNUAL_KWH      NUMBER(18,4),
    TOTAL_NET_SITE_ELEC_LIFETIME_KWH    NUMBER(18,4),
    TOTAL_NET_SITE_GAS_ANNUAL_THERMS    NUMBER(18,4),
    TOTAL_NET_SITE_GAS_LIFETIME_THERMS  NUMBER(18,4),

    -- Section 12: Net Realized Source Savings (Includes Negative IEs, TRM x ISR x RR x NTG x Source Conversion)
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

    -- Section 14: Target Segments - Net Realized Site Savings (Exclude Negative IEs, TRM x ISR x RR x NTG)
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
    CONSTRAINT CES_SAVINGS_FACT_ACT_FK1 FOREIGN KEY (VENDOR_SK) REFERENCES CES_SAVINGS_DIM_VENDOR(VENDOR_SK),
    CONSTRAINT CES_SAVINGS_FACT_ACT_FK2 FOREIGN KEY (PERIOD_SK) REFERENCES CES_SAVINGS_DIM_PERIOD(PERIOD_SK),
    CONSTRAINT CES_SAVINGS_FACT_ACT_CK1 CHECK (IS_CURRENT_VERSION IN ('Y','N'))
);

COMMENT ON TABLE  CES_SAVINGS_FACT_ACTUALS                          IS 'Fact table for actual savings data. One row per vendor subprogram and reporting period. Wide table format. Natural keys stored as migration backup.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.VERSION_NUMBER           IS 'Increments on vendor resubmission. Starts at 1.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.IS_CURRENT_VERSION       IS 'Y=Latest submission. N=Superseded by resubmission.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.VENDOR_SUBPROGRAM_KEY    IS 'Natural key backup. Used to repoint FKs after server migration if needed.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.NET_SRC_ANNUAL_MMBTU     IS 'Highlighted yellow in source. Key management metric.';
COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.NET_SRC_LIFETIME_MMBTU   IS 'Highlighted yellow in source. Key management metric.';

-- Indexes
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX01 ON CES_SAVINGS_FACT_ACTUALS (VENDOR_SK);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX02 ON CES_SAVINGS_FACT_ACTUALS (PERIOD_SK);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX03 ON CES_SAVINGS_FACT_ACTUALS (VENDOR_SUBPROGRAM_KEY);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX04 ON CES_SAVINGS_FACT_ACTUALS (REPORTING_PERIOD);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX05 ON CES_SAVINGS_FACT_ACTUALS (IS_CURRENT_VERSION);
CREATE INDEX CES_SAVINGS_FACT_ACT_IDX06 ON CES_SAVINGS_FACT_ACTUALS (BATCH_ID);


-- ------------------------------------------------------------
-- FACT: SAVINGS FORECAST
-- Source: CES_SAVINGS_STG_FORECAST after validation
-- Same structure as actuals + FORECAST_PERIOD + CONFIDENCE_LEVEL
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_FACT_FORECAST (
    -- Surrogate Key
    FACT_SK                             NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Foreign Keys to Dimensions
    VENDOR_SK                           NUMBER          NOT NULL,
    PERIOD_SK                           NUMBER          NOT NULL,

    -- Natural Keys (backup for migration safety)
    VENDOR_SUBPROGRAM_KEY               VARCHAR2(200),
    FORECAST_PERIOD                     VARCHAR2(50),

    -- Forecast Specific Columns
    CONFIDENCE_LEVEL                    VARCHAR2(50),

    -- ETL Metadata
    SOURCE_FILE_NAME                    VARCHAR2(500),
    BATCH_ID                            NUMBER,
    VERSION_NUMBER                      NUMBER          DEFAULT 1,
    IS_CURRENT_VERSION                  VARCHAR2(1)     DEFAULT 'Y',
    LOAD_TIMESTAMP                      TIMESTAMP       DEFAULT SYSTIMESTAMP,

    -- Section 1: Additional Dimension Attributes
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
    CONSTRAINT CES_SAVINGS_FACT_FOR_FK1 FOREIGN KEY (VENDOR_SK) REFERENCES CES_SAVINGS_DIM_VENDOR(VENDOR_SK),
    CONSTRAINT CES_SAVINGS_FACT_FOR_FK2 FOREIGN KEY (PERIOD_SK) REFERENCES CES_SAVINGS_DIM_PERIOD(PERIOD_SK),
    CONSTRAINT CES_SAVINGS_FACT_FOR_CK1 CHECK (IS_CURRENT_VERSION IN ('Y','N'))
);

COMMENT ON TABLE  CES_SAVINGS_FACT_FORECAST                         IS 'Fact table for forecast savings data through 6/30/2027. Same structure as actuals plus FORECAST_PERIOD and CONFIDENCE_LEVEL.';
COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST.CONFIDENCE_LEVEL        IS 'Forecast confidence level as submitted by vendor e.g. High/Medium/Low.';
COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST.VERSION_NUMBER          IS 'Increments on vendor resubmission. Starts at 1.';
COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST.IS_CURRENT_VERSION      IS 'Y=Latest submission. N=Superseded by resubmission.';

-- Indexes
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX01 ON CES_SAVINGS_FACT_FORECAST (VENDOR_SK);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX02 ON CES_SAVINGS_FACT_FORECAST (PERIOD_SK);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX03 ON CES_SAVINGS_FACT_FORECAST (VENDOR_SUBPROGRAM_KEY);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX04 ON CES_SAVINGS_FACT_FORECAST (FORECAST_PERIOD);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX05 ON CES_SAVINGS_FACT_FORECAST (IS_CURRENT_VERSION);
CREATE INDEX CES_SAVINGS_FACT_FOR_IDX06 ON CES_SAVINGS_FACT_FORECAST (BATCH_ID);


-- ============================================================
-- SECTION 4: ARCHIVE TABLES
-- Purpose : Long term storage of data older than 2 years
--           Identical structure to FACT tables
--           Additional archive metadata columns
--           Moved here by annual archiving process
-- ============================================================

CREATE TABLE CES_SAVINGS_FACT_ACTUALS_ARCH AS
SELECT
    f.*,
    CAST(NULL AS TIMESTAMP)  AS ARCHIVE_DATE,
    CAST(NULL AS VARCHAR2(200)) AS ARCHIVE_REASON,
    CAST(NULL AS VARCHAR2(100)) AS ARCHIVED_BY
FROM CES_SAVINGS_FACT_ACTUALS f
WHERE 1=0;

COMMENT ON TABLE CES_SAVINGS_FACT_ACTUALS_ARCH IS 'Archive table for actual savings data older than 2 years. Identical structure to FACT_ACTUALS. Populated by annual archiving process.';

CREATE TABLE CES_SAVINGS_FACT_FORECAST_ARCH AS
SELECT
    f.*,
    CAST(NULL AS TIMESTAMP)  AS ARCHIVE_DATE,
    CAST(NULL AS VARCHAR2(200)) AS ARCHIVE_REASON,
    CAST(NULL AS VARCHAR2(100)) AS ARCHIVED_BY
FROM CES_SAVINGS_FACT_FORECAST f
WHERE 1=0;

COMMENT ON TABLE CES_SAVINGS_FACT_FORECAST_ARCH IS 'Archive table for forecast savings data older than 2 years. Identical structure to FACT_FORECAST. Populated by annual archiving process.';


-- ============================================================
-- SECTION 5: AUDIT/LOGGING TABLES
-- Purpose : Full ETL traceability
--           Track every batch run, file, and row error
-- ============================================================

-- ------------------------------------------------------------
-- AUDIT: BATCH LOG
-- One record per Jenkins pipeline run
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_ETL_BATCH_LOG (
    BATCH_ID                            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    BATCH_START_DT                      TIMESTAMP,
    BATCH_END_DT                        TIMESTAMP,
    STATUS                              VARCHAR2(20),
    TOTAL_FILES_EXPECTED                NUMBER,
    FILES_PROCESSED                     NUMBER          DEFAULT 0,
    FILES_SUCCEEDED                     NUMBER          DEFAULT 0,
    FILES_FAILED                        NUMBER          DEFAULT 0,
    FILES_SKIPPED                       NUMBER          DEFAULT 0,
    FILES_QUARANTINED                   NUMBER          DEFAULT 0,
    TOTAL_ROWS_LOADED                   NUMBER          DEFAULT 0,
    TOTAL_ROWS_FAILED                   NUMBER          DEFAULT 0,
    TRIGGERED_BY                        VARCHAR2(200),
    JENKINS_JOB_NAME                    VARCHAR2(200),
    JENKINS_BUILD_NUMBER                VARCHAR2(50),
    ERROR_MESSAGE                       VARCHAR2(4000),
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,

    CONSTRAINT CES_SAVINGS_BATCH_LOG_CK1 CHECK (STATUS IN ('RUNNING','SUCCESS','FAILED','PARTIAL'))
);

COMMENT ON TABLE  CES_SAVINGS_ETL_BATCH_LOG                         IS 'One record per Jenkins ETL pipeline run. Top level audit trail.';
COMMENT ON COLUMN CES_SAVINGS_ETL_BATCH_LOG.STATUS                  IS 'RUNNING=in progress. SUCCESS=all files loaded. FAILED=pipeline error. PARTIAL=some files failed.';
COMMENT ON COLUMN CES_SAVINGS_ETL_BATCH_LOG.FILES_SKIPPED           IS 'Files skipped due to duplicate detection.';
COMMENT ON COLUMN CES_SAVINGS_ETL_BATCH_LOG.FILES_QUARANTINED       IS 'Files moved to quarantine due to validation failure.';

CREATE INDEX CES_SAVINGS_BATCH_LOG_IDX01 ON CES_SAVINGS_ETL_BATCH_LOG (STATUS);
CREATE INDEX CES_SAVINGS_BATCH_LOG_IDX02 ON CES_SAVINGS_ETL_BATCH_LOG (BATCH_START_DT);


-- ------------------------------------------------------------
-- AUDIT: FILE LOG
-- One record per vendor file processed
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_ETL_FILE_LOG (
    FILE_LOG_ID                         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    BATCH_ID                            NUMBER          NOT NULL,
    FILE_NAME                           VARCHAR2(500),
    FILE_TYPE                           VARCHAR2(20),
    VENDOR_KEY                          VARCHAR2(200),
    REPORTING_PERIOD                    VARCHAR2(50),
    FILE_RECEIVED_DT                    TIMESTAMP,
    FILE_PROCESSED_DT                   TIMESTAMP,
    STATUS                              VARCHAR2(20),
    ROW_COUNT_SOURCE                    NUMBER,
    ROW_COUNT_LOADED                    NUMBER          DEFAULT 0,
    ROW_COUNT_FAILED                    NUMBER          DEFAULT 0,
    IS_RESUBMISSION                     VARCHAR2(1)     DEFAULT 'N',
    PREV_FILE_LOG_ID                    NUMBER,
    FILE_PATH_SOURCE                    VARCHAR2(1000),
    FILE_PATH_ARCHIVE                   VARCHAR2(1000),
    FILE_PATH_QUARANTINE                VARCHAR2(1000),
    VALIDATION_ERRORS                   VARCHAR2(4000),
    ERROR_MESSAGE                       VARCHAR2(4000),
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,

    CONSTRAINT CES_SAVINGS_FILE_LOG_FK1 FOREIGN KEY (BATCH_ID)         REFERENCES CES_SAVINGS_ETL_BATCH_LOG(BATCH_ID),
    CONSTRAINT CES_SAVINGS_FILE_LOG_FK2 FOREIGN KEY (PREV_FILE_LOG_ID) REFERENCES CES_SAVINGS_ETL_FILE_LOG(FILE_LOG_ID),
    CONSTRAINT CES_SAVINGS_FILE_LOG_CK1 CHECK (STATUS       IN ('PROCESSING','SUCCESS','FAILED','QUARANTINE','SKIPPED','RESUBMISSION')),
    CONSTRAINT CES_SAVINGS_FILE_LOG_CK2 CHECK (IS_RESUBMISSION IN ('Y','N')),
    CONSTRAINT CES_SAVINGS_FILE_LOG_CK3 CHECK (FILE_TYPE    IN ('ACTUALS','FORECAST'))
);

COMMENT ON TABLE  CES_SAVINGS_ETL_FILE_LOG                          IS 'One record per vendor file. Mid level audit trail. Links to batch and row logs.';
COMMENT ON COLUMN CES_SAVINGS_ETL_FILE_LOG.IS_RESUBMISSION          IS 'Y=Vendor resubmitted corrected file. N=Original submission.';
COMMENT ON COLUMN CES_SAVINGS_ETL_FILE_LOG.PREV_FILE_LOG_ID         IS 'Points to original file log record when IS_RESUBMISSION=Y.';
COMMENT ON COLUMN CES_SAVINGS_ETL_FILE_LOG.FILE_PATH_QUARANTINE     IS 'Path where file was moved if validation failed.';
COMMENT ON COLUMN CES_SAVINGS_ETL_FILE_LOG.VALIDATION_ERRORS        IS 'Summary of validation errors found during schema check.';

CREATE INDEX CES_SAVINGS_FILE_LOG_IDX01 ON CES_SAVINGS_ETL_FILE_LOG (BATCH_ID);
CREATE INDEX CES_SAVINGS_FILE_LOG_IDX02 ON CES_SAVINGS_ETL_FILE_LOG (VENDOR_KEY);
CREATE INDEX CES_SAVINGS_FILE_LOG_IDX03 ON CES_SAVINGS_ETL_FILE_LOG (STATUS);
CREATE INDEX CES_SAVINGS_FILE_LOG_IDX04 ON CES_SAVINGS_ETL_FILE_LOG (IS_RESUBMISSION);
CREATE INDEX CES_SAVINGS_FILE_LOG_IDX05 ON CES_SAVINGS_ETL_FILE_LOG (REPORTING_PERIOD);


-- ------------------------------------------------------------
-- AUDIT: ROW LOG
-- One record per failed/problematic row
-- ------------------------------------------------------------
CREATE TABLE CES_SAVINGS_ETL_ROW_LOG (
    ROW_LOG_ID                          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    FILE_LOG_ID                         NUMBER          NOT NULL,
    BATCH_ID                            NUMBER          NOT NULL,
    ROW_NUMBER                          NUMBER,
    COLUMN_NAME                         VARCHAR2(200),
    SOURCE_VALUE                        VARCHAR2(4000),
    EXPECTED_FORMAT                     VARCHAR2(200),
    ERROR_TYPE                          VARCHAR2(50),
    ERROR_MESSAGE                       VARCHAR2(4000),
    STATUS                              VARCHAR2(20)    DEFAULT 'FAILED',
    RESOLVED_DT                         TIMESTAMP,
    RESOLVED_BY                         VARCHAR2(100),
    RESOLUTION_NOTES                    VARCHAR2(4000),
    CREATED_DATE                        TIMESTAMP       DEFAULT SYSTIMESTAMP,

    CONSTRAINT CES_SAVINGS_ROW_LOG_FK1  FOREIGN KEY (FILE_LOG_ID) REFERENCES CES_SAVINGS_ETL_FILE_LOG(FILE_LOG_ID),
    CONSTRAINT CES_SAVINGS_ROW_LOG_FK2  FOREIGN KEY (BATCH_ID)    REFERENCES CES_SAVINGS_ETL_BATCH_LOG(BATCH_ID),
    CONSTRAINT CES_SAVINGS_ROW_LOG_CK1  CHECK (ERROR_TYPE IN ('NULL_VALUE','TYPE_MISMATCH','OUT_OF_RANGE','DUPLICATE','MISSING_COLUMN','OTHER')),
    CONSTRAINT CES_SAVINGS_ROW_LOG_CK2  CHECK (STATUS IN ('FAILED','RESOLVED','IGNORED'))
);

COMMENT ON TABLE  CES_SAVINGS_ETL_ROW_LOG                           IS 'One record per failed row or cell. Lowest level audit trail. Used for debugging and vendor communication.';
COMMENT ON COLUMN CES_SAVINGS_ETL_ROW_LOG.ROW_NUMBER                IS 'Row number in source Excel file including header rows.';
COMMENT ON COLUMN CES_SAVINGS_ETL_ROW_LOG.SOURCE_VALUE              IS 'Actual value received from vendor that caused the error.';
COMMENT ON COLUMN CES_SAVINGS_ETL_ROW_LOG.EXPECTED_FORMAT           IS 'What format was expected e.g. NUMBER, DATE, VARCHAR2.';
COMMENT ON COLUMN CES_SAVINGS_ETL_ROW_LOG.ERROR_TYPE                IS 'NULL_VALUE=missing required data. TYPE_MISMATCH=wrong data type. OUT_OF_RANGE=value outside bounds. DUPLICATE=duplicate row. MISSING_COLUMN=column not found.';
COMMENT ON COLUMN CES_SAVINGS_ETL_ROW_LOG.RESOLUTION_NOTES          IS 'Notes on how the error was resolved for future reference.';

CREATE INDEX CES_SAVINGS_ROW_LOG_IDX01 ON CES_SAVINGS_ETL_ROW_LOG (FILE_LOG_ID);
CREATE INDEX CES_SAVINGS_ROW_LOG_IDX02 ON CES_SAVINGS_ETL_ROW_LOG (BATCH_ID);
CREATE INDEX CES_SAVINGS_ROW_LOG_IDX03 ON CES_SAVINGS_ETL_ROW_LOG (STATUS);
CREATE INDEX CES_SAVINGS_ROW_LOG_IDX04 ON CES_SAVINGS_ETL_ROW_LOG (ERROR_TYPE);


-- ============================================================
-- SECTION 6: REPORTING VIEWS
-- Purpose : Power BI reads these only
--           Business friendly names
--           Pre-filtered for current versions only
--           Expand later with calculations
-- ============================================================

-- Current Actuals View (Power BI primary source)
CREATE OR REPLACE VIEW CES_SAVINGS_VW_ACTUALS AS
SELECT
    a.FACT_SK,
    a.REPORTING_PERIOD,
    a.VENDOR_SUBPROGRAM_KEY,
    v.SECTOR,
    v.PROGRAM,
    v.SUBPROGRAM,
    v.TRIENNIUM,
    a.PROGRAM_YEAR,
    p.FISCAL_YEAR,
    p.FISCAL_QUARTER,
    p.PERIOD_YEAR,
    p.PERIOD_MONTH,
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
    -- Key Metrics (add all columns as needed)
    a.GROSS_SITE_ELEC_ANNUAL_KWH,
    a.GROSS_SITE_ELEC_LIFETIME_KWH,
    a.GROSS_SITE_GAS_ANNUAL_THERMS,
    a.GROSS_SITE_GAS_LIFETIME_THERMS,
    a.NET_SRC_ANNUAL_MMBTU,
    a.NET_SRC_LIFETIME_MMBTU,
    -- Metadata
    a.SOURCE_FILE_NAME,
    a.LOAD_TIMESTAMP
FROM CES_SAVINGS_FACT_ACTUALS a
JOIN CES_SAVINGS_DIM_VENDOR v ON a.VENDOR_SK = v.VENDOR_SK AND v.IS_CURRENT = 'Y'
JOIN CES_SAVINGS_DIM_PERIOD p ON a.PERIOD_SK = p.PERIOD_SK
WHERE a.IS_CURRENT_VERSION = 'Y';

COMMENT ON TABLE CES_SAVINGS_VW_ACTUALS IS 'Power BI primary view for actuals. Pre-filtered for current versions and current dimension records. Expand with calculation columns as needed.';


-- Current Forecast View (Power BI)
CREATE OR REPLACE VIEW CES_SAVINGS_VW_FORECAST AS
SELECT
    f.FACT_SK,
    f.FORECAST_PERIOD,
    f.CONFIDENCE_LEVEL,
    f.VENDOR_SUBPROGRAM_KEY,
    v.SECTOR,
    v.PROGRAM,
    v.SUBPROGRAM,
    v.TRIENNIUM,
    f.PROGRAM_YEAR,
    p.FISCAL_YEAR,
    p.FISCAL_QUARTER,
    p.PERIOD_YEAR,
    p.PERIOD_MONTH,
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
    -- Key Metrics
    f.GROSS_SITE_ELEC_ANNUAL_KWH,
    f.GROSS_SITE_ELEC_LIFETIME_KWH,
    f.GROSS_SITE_GAS_ANNUAL_THERMS,
    f.GROSS_SITE_GAS_LIFETIME_THERMS,
    f.NET_SRC_ANNUAL_MMBTU,
    f.NET_SRC_LIFETIME_MMBTU,
    -- Metadata
    f.SOURCE_FILE_NAME,
    f.LOAD_TIMESTAMP
FROM CES_SAVINGS_FACT_FORECAST f
JOIN CES_SAVINGS_DIM_VENDOR v ON f.VENDOR_SK = v.VENDOR_SK AND v.IS_CURRENT = 'Y'
JOIN CES_SAVINGS_DIM_PERIOD p ON f.PERIOD_SK = p.PERIOD_SK
WHERE f.IS_CURRENT_VERSION = 'Y';

COMMENT ON TABLE CES_SAVINGS_VW_FORECAST IS 'Power BI primary view for forecast. Pre-filtered for current versions and current dimension records.';


-- Actuals vs Forecast Comparison View
CREATE OR REPLACE VIEW CES_SAVINGS_VW_ACTUALS_VS_FORECAST AS
SELECT
    a.REPORTING_PERIOD                  AS PERIOD,
    a.VENDOR_SUBPROGRAM_KEY,
    a.TOTAL_INVEST_COST                 AS ACTUAL_INVEST_COST,
    f.TOTAL_INVEST_COST                 AS FORECAST_INVEST_COST,
    a.NET_SRC_ANNUAL_MMBTU              AS ACTUAL_NET_SRC_ANNUAL_MMBTU,
    f.NET_SRC_ANNUAL_MMBTU              AS FORECAST_NET_SRC_ANNUAL_MMBTU,
    a.NET_SRC_ANNUAL_MMBTU
        - f.NET_SRC_ANNUAL_MMBTU        AS VARIANCE_NET_SRC_ANNUAL_MMBTU,
    a.GROSS_SITE_ELEC_ANNUAL_KWH        AS ACTUAL_GROSS_ELEC_KWH,
    f.GROSS_SITE_ELEC_ANNUAL_KWH        AS FORECAST_GROSS_ELEC_KWH,
    a.GROSS_SITE_GAS_ANNUAL_THERMS      AS ACTUAL_GROSS_GAS_THERMS,
    f.GROSS_SITE_GAS_ANNUAL_THERMS      AS FORECAST_GROSS_GAS_THERMS
FROM CES_SAVINGS_VW_ACTUALS a
LEFT JOIN CES_SAVINGS_VW_FORECAST f
    ON  a.VENDOR_SUBPROGRAM_KEY = f.VENDOR_SUBPROGRAM_KEY
    AND a.REPORTING_PERIOD      = f.FORECAST_PERIOD;

COMMENT ON TABLE CES_SAVINGS_VW_ACTUALS_VS_FORECAST IS 'Variance view comparing actuals vs forecast. Expand with additional metric comparisons as needed.';


-- Pipeline Health View (for monitoring dashboard)
CREATE OR REPLACE VIEW CES_SAVINGS_VW_PIPELINE_HEALTH AS
SELECT
    b.BATCH_ID,
    b.BATCH_START_DT,
    b.BATCH_END_DT,
    b.STATUS                            AS BATCH_STATUS,
    b.TOTAL_FILES_EXPECTED,
    b.FILES_SUCCEEDED,
    b.FILES_FAILED,
    b.FILES_QUARANTINED,
    b.TOTAL_ROWS_LOADED,
    b.TOTAL_ROWS_FAILED,
    b.JENKINS_JOB_NAME,
    b.JENKINS_BUILD_NUMBER,
    ROUND(
        (b.BATCH_END_DT - b.BATCH_START_DT) * 24 * 60, 2
    )                                   AS PROCESSING_MINUTES
FROM CES_SAVINGS_ETL_BATCH_LOG b
ORDER BY b.BATCH_START_DT DESC;

COMMENT ON TABLE CES_SAVINGS_VW_PIPELINE_HEALTH IS 'Pipeline monitoring view. Shows ETL run history, file counts, and processing times. Use for operations dashboard.';


-- ============================================================
-- END OF DDL
-- ============================================================
-- Summary:
-- STG Tables  : 2  (CES_SAVINGS_STG_ACTUALS, CES_SAVINGS_STG_FORECAST)
-- DIM Tables  : 2  (CES_SAVINGS_DIM_VENDOR, CES_SAVINGS_DIM_PERIOD)
-- FACT Tables : 2  (CES_SAVINGS_FACT_ACTUALS, CES_SAVINGS_FACT_FORECAST)
-- ARCH Tables : 2  (CES_SAVINGS_FACT_ACTUALS_ARCH, CES_SAVINGS_FACT_FORECAST_ARCH)
-- AUDIT Tables: 3  (CES_SAVINGS_ETL_BATCH_LOG, CES_SAVINGS_ETL_FILE_LOG, CES_SAVINGS_ETL_ROW_LOG)
-- Views       : 4  (VW_ACTUALS, VW_FORECAST, VW_ACTUALS_VS_FORECAST, VW_PIPELINE_HEALTH)
-- Total Tables: 11
-- ============================================================
