-- ============================================================
-- CES SAVINGS V4 - ALTER + CLEAN MAPPING
-- 1. Add PARTICIPANTS_MULTIFAMILY to STG + FACT tables
-- 2. Fix unique constraint on mapping table
-- 3. Truncate and reload mapping with correct indexes
-- Run in order top to bottom
-- ============================================================


-- ============================================================
-- STEP 1: ADD MULTIFAMILY PARTICIPANT COLUMN
-- ============================================================

-- STG Actuals
ALTER TABLE CES_SAVINGS_STG_ACTUALS
    ADD PARTICIPANTS_MULTIFAMILY VARCHAR2(50);

-- STG Forecast
ALTER TABLE CES_SAVINGS_STG_FORECAST
    ADD PARTICIPANTS_MULTIFAMILY VARCHAR2(50);

-- FACT Actuals
ALTER TABLE CES_SAVINGS_FACT_ACTUALS
    ADD PARTICIPANTS_MULTIFAMILY NUMBER(10,0);

-- FACT Forecast
ALTER TABLE CES_SAVINGS_FACT_FORECAST
    ADD PARTICIPANTS_MULTIFAMILY NUMBER(10,0);

-- ARCH tables mirror FACT
ALTER TABLE CES_SAVINGS_FACT_ACTUALS_ARCH
    ADD PARTICIPANTS_MULTIFAMILY NUMBER(10,0);

ALTER TABLE CES_SAVINGS_FACT_FORECAST_ARCH
    ADD PARTICIPANTS_MULTIFAMILY NUMBER(10,0);

COMMENT ON COLUMN CES_SAVINGS_FACT_ACTUALS.PARTICIPANTS_MULTIFAMILY  IS 'Count of multifamily participants. Added V4.';
COMMENT ON COLUMN CES_SAVINGS_FACT_FORECAST.PARTICIPANTS_MULTIFAMILY IS 'Count of multifamily participants. Added V4.';


-- ============================================================
-- STEP 2: FIX MAPPING TABLE CONSTRAINTS
-- ============================================================

-- Drop old unique constraint based on header text
-- This was causing duplicate key errors
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE CES_SAVINGS_COLUMN_MAP DROP CONSTRAINT CES_SAVINGS_COL_MAP_UQ1';
EXCEPTION
    WHEN OTHERS THEN NULL; -- ignore if not exists
END;
/

-- Add correct unique constraint based on sheet + index
-- Index is our primary key now
ALTER TABLE CES_SAVINGS_COLUMN_MAP
    ADD CONSTRAINT CES_SAVINGS_COL_MAP_UQ1
    UNIQUE (SHEET_NAME, EXCEL_COL_POSITION);

-- Add new columns if not already present
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE CES_SAVINGS_COLUMN_MAP ADD FUZZY_THRESHOLD NUMBER(5,2) DEFAULT 75';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE CES_SAVINGS_COLUMN_MAP ADD ROW1_EXPECTED VARCHAR2(500)';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE CES_SAVINGS_COLUMN_MAP ADD ROW2_EXPECTED VARCHAR2(500)';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/


-- ============================================================
-- STEP 3: CLEAN AND RELOAD MAPPING TABLE
-- Truncate first then merge insert
-- ============================================================
TRUNCATE TABLE CES_SAVINGS_COLUMN_MAP;


-- ============================================================
-- MERGE INSERT - SAVINGS RESULTS
-- Indexes are 1-based matching Excel column positions
-- Col A = 1, Col B = 2 etc
-- ============================================================
MERGE INTO CES_SAVINGS_COLUMN_MAP t
USING (
-- DIMENSION COLUMNS
SELECT 'Savings Results' SN, 1  CP, '' R1, '' R2, 'Triennium'             R3, '' E1, '' E2, 75 FT, 'CES_SAVINGS_STG_ACTUALS' ST, 'TRIENNIUM'              SC, 'CES_SAVINGS_FACT_ACTUALS' FT2, 'TRIENNIUM'              FC, 'VARCHAR2'   DT, 'Y' ID, 'N' IR, 'N' IS2, 'Y' IA FROM DUAL UNION ALL
SELECT 'Savings Results', 2,  '', '', 'Sector',               '', '', 75, 'CES_SAVINGS_STG_ACTUALS', 'SECTOR',               'CES_SAVINGS_FACT_ACTUALS', 'SECTOR',               'VARCHAR2',   'Y', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 3,  '', '', 'Vendor Subprogram Key','', '', 75, 'CES_SAVINGS_STG_ACTUALS', 'VENDOR_SUBPROGRAM_KEY','CES_SAVINGS_FACT_ACTUALS', 'VENDOR_SUBPROGRAM_KEY','VARCHAR2',   'Y', 'Y', 'Y', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 4,  '', '', 'Program',              '', '', 75, 'CES_SAVINGS_STG_ACTUALS', 'PROGRAM',              'CES_SAVINGS_FACT_ACTUALS', 'PROGRAM',              'VARCHAR2',   'Y', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 5,  '', '', 'Subprogram',           '', '', 75, 'CES_SAVINGS_STG_ACTUALS', 'SUBPROGRAM',           'CES_SAVINGS_FACT_ACTUALS', 'SUBPROGRAM',           'VARCHAR2',   'Y', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 6,  '', '', 'Program Year',         '', '', 75, 'CES_SAVINGS_STG_ACTUALS', 'PROGRAM_YEAR',         'CES_SAVINGS_FACT_ACTUALS', 'PROGRAM_YEAR',         'VARCHAR2',   'Y', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 7,  '', '', 'Reporting Period',     '', '', 75, 'CES_SAVINGS_STG_ACTUALS', 'REPORTING_PERIOD',     'CES_SAVINGS_FACT_ACTUALS', 'REPORTING_PERIOD',     'VARCHAR2',   'Y', 'Y', 'N', 'Y' FROM DUAL UNION ALL
-- FINANCIALS
SELECT 'Savings Results', 8,  '', 'Financials', '1) Investment - Rebate', '', 'Financials', 75, 'CES_SAVINGS_STG_ACTUALS', 'INVEST_COST_REBATE',  'CES_SAVINGS_FACT_ACTUALS', 'INVEST_COST_REBATE',  'NUMBER_2',   'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 9,  '', 'Financials', '2) Investment - OBR',    '', 'Financials', 75, 'CES_SAVINGS_STG_ACTUALS', 'INVEST_COST_OBR',     'CES_SAVINGS_FACT_ACTUALS', 'INVEST_COST_OBR',     'NUMBER_2',   'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 10, '', 'Financials', '3) Investment - Other',  '', 'Financials', 75, 'CES_SAVINGS_STG_ACTUALS', 'INVEST_COST_OTHER',   'CES_SAVINGS_FACT_ACTUALS', 'INVEST_COST_OTHER',   'NUMBER_2',   'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 11, '', 'Financials', 'Total Investment',        '', 'Financials', 75, 'CES_SAVINGS_STG_ACTUALS', 'TOTAL_INVEST_COST',   'CES_SAVINGS_FACT_ACTUALS', 'TOTAL_INVEST_COST',   'NUMBER_2',   'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- PARTICIPANTS
SELECT 'Savings Results', 12, '', 'Participants', 'Total',                  '', 'Participants', 75, 'CES_SAVINGS_STG_ACTUALS', 'PARTICIPANTS_TOTAL',        'CES_SAVINGS_FACT_ACTUALS', 'PARTICIPANTS_TOTAL',        'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 13, '', 'Participants', 'Res LMI or OBC',         '', 'Participants', 75, 'CES_SAVINGS_STG_ACTUALS', 'PARTICIPANTS_RES_LMI_OBC',  'CES_SAVINGS_FACT_ACTUALS', 'PARTICIPANTS_RES_LMI_OBC',  'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 14, '', 'Participants', 'Res LMI Only',           '', 'Participants', 75, 'CES_SAVINGS_STG_ACTUALS', 'PARTICIPANTS_RES_LMI_ONLY', 'CES_SAVINGS_FACT_ACTUALS', 'PARTICIPANTS_RES_LMI_ONLY', 'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 15, '', 'Participants', 'OBC Only (All Projects)', '', 'Participants', 75, 'CES_SAVINGS_STG_ACTUALS', 'PARTICIPANTS_OBC_ONLY',     'CES_SAVINGS_FACT_ACTUALS', 'PARTICIPANTS_OBC_ONLY',     'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 16, '', 'Participants', 'Small Business',          '', 'Participants', 75, 'CES_SAVINGS_STG_ACTUALS', 'PARTICIPANTS_SMALL_BIZ',    'CES_SAVINGS_FACT_ACTUALS', 'PARTICIPANTS_SMALL_BIZ',    'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 17, '', 'Participants', 'Multi Family',            '', 'Participants', 75, 'CES_SAVINGS_STG_ACTUALS', 'PARTICIPANTS_MULTIFAMILY',  'CES_SAVINGS_FACT_ACTUALS', 'PARTICIPANTS_MULTIFAMILY',  'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- GROSS SITE ELECTRIC
SELECT 'Savings Results', 18, 'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Annual Electric Savings - kWh',                    'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_ACTUALS', 'GROSS_SITE_ELEC_ANNUAL_KWH',       'CES_SAVINGS_FACT_ACTUALS', 'GROSS_SITE_ELEC_ANNUAL_KWH',       'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 19, 'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Annual Electric Demand Savings - kW',              'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_ACTUALS', 'GROSS_SITE_ELEC_DEMAND_KW',        'CES_SAVINGS_FACT_ACTUALS', 'GROSS_SITE_ELEC_DEMAND_KW',        'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 20, 'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Lifetime Electric Savings - kWh',                  'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_ACTUALS', 'GROSS_SITE_ELEC_LIFETIME_KWH',     'CES_SAVINGS_FACT_ACTUALS', 'GROSS_SITE_ELEC_LIFETIME_KWH',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- GROSS SITE GAS
SELECT 'Savings Results', 21, 'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Annual Gas Savings - Therms',                      'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_ACTUALS', 'GROSS_SITE_GAS_ANNUAL_THERMS',     'CES_SAVINGS_FACT_ACTUALS', 'GROSS_SITE_GAS_ANNUAL_THERMS',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 22, 'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Annual Gas Demand Daily Peak Fuel Savings',        'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_ACTUALS', 'GROSS_SITE_GAS_DAILY_PEAK_THERMS', 'CES_SAVINGS_FACT_ACTUALS', 'GROSS_SITE_GAS_DAILY_PEAK_THERMS', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 23, 'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Lifetime Gas Savings - Therms',                    'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_ACTUALS', 'GROSS_SITE_GAS_LIFETIME_THERMS',   'CES_SAVINGS_FACT_ACTUALS', 'GROSS_SITE_GAS_LIFETIME_THERMS',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- NET SITE ISR ELECTRIC
SELECT 'Savings Results', 24, 'Electric',    'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Annual Electric Savings - kWh',        'Electric',    'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SITE_ISR_ELEC_ANNUAL_KWH',       'CES_SAVINGS_FACT_ACTUALS', 'NET_SITE_ISR_ELEC_ANNUAL_KWH',       'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 25, 'Electric',    'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Annual Electric Demand Savings - kW',  'Electric',    'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SITE_ISR_ELEC_DEMAND_KW',        'CES_SAVINGS_FACT_ACTUALS', 'NET_SITE_ISR_ELEC_DEMAND_KW',        'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 26, 'Electric',    'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Lifetime Electric Savings - kWh',      'Electric',    'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SITE_ISR_ELEC_LIFETIME_KWH',     'CES_SAVINGS_FACT_ACTUALS', 'NET_SITE_ISR_ELEC_LIFETIME_KWH',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- NET SITE ISR GAS
SELECT 'Savings Results', 27, 'Natural Gas', 'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Annual Gas Savings - Therms',          'Natural Gas', 'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SITE_ISR_GAS_ANNUAL_THERMS',     'CES_SAVINGS_FACT_ACTUALS', 'NET_SITE_ISR_GAS_ANNUAL_THERMS',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 28, 'Natural Gas', 'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Annual Gas Demand Daily Peak',         'Natural Gas', 'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SITE_ISR_GAS_DAILY_PEAK_THERMS', 'CES_SAVINGS_FACT_ACTUALS', 'NET_SITE_ISR_GAS_DAILY_PEAK_THERMS', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 29, 'Natural Gas', 'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Lifetime Gas Savings - Therms',        'Natural Gas', 'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SITE_ISR_GAS_LIFETIME_THERMS',   'CES_SAVINGS_FACT_ACTUALS', 'NET_SITE_ISR_GAS_LIFETIME_THERMS',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- NEGATIVE IEs
SELECT 'Savings Results', 30, 'Natural Gas', 'Negative Interactive Effects (IEs)', 'Net Realized Site Annual Gas Therms for IEs Only',    'Natural Gas', 'Negative Interactive Effects (IEs)', 75, 'CES_SAVINGS_STG_ACTUALS', 'NEG_IE_GAS_ANNUAL_THERMS',   'CES_SAVINGS_FACT_ACTUALS', 'NEG_IE_GAS_ANNUAL_THERMS',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 31, 'Natural Gas', 'Negative Interactive Effects (IEs)', 'Net Realized Site Lifetime Gas Therms for IEs Only',  'Natural Gas', 'Negative Interactive Effects (IEs)', 75, 'CES_SAVINGS_STG_ACTUALS', 'NEG_IE_GAS_LIFETIME_THERMS', 'CES_SAVINGS_FACT_ACTUALS', 'NEG_IE_GAS_LIFETIME_THERMS', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 32, 'Natural Gas', 'Negative Interactive Effects (IEs)', 'Net Realized Site Annual Gas Therms IEs Applied',     'Natural Gas', 'Negative Interactive Effects (IEs)', 75, 'CES_SAVINGS_STG_ACTUALS', 'NEG_IE_ELEC_ANNUAL_KWH',     'CES_SAVINGS_FACT_ACTUALS', 'NEG_IE_ELEC_ANNUAL_KWH',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 33, 'Natural Gas', 'Negative Interactive Effects (IEs)', 'Net Realized Site Lifetime Gas Therms IEs Applied',   'Natural Gas', 'Negative Interactive Effects (IEs)', 75, 'CES_SAVINGS_STG_ACTUALS', 'NEG_IE_ELEC_LIFETIME_KWH',   'CES_SAVINGS_FACT_ACTUALS', 'NEG_IE_ELEC_LIFETIME_KWH',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- NET SOURCE
SELECT 'Savings Results', 34, 'Electric',          'Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion', 'Net Realized Source Annual Electric Savings - MMBtu',   'Electric',          'Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SRC_ELEC_ANNUAL_MMBTU',   'CES_SAVINGS_FACT_ACTUALS', 'NET_SRC_ELEC_ANNUAL_MMBTU',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 35, 'Electric',          'Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion', 'Net Realized Source Lifetime Electric Savings - MMBtu', 'Electric',          'Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SRC_ELEC_LIFETIME_MMBTU', 'CES_SAVINGS_FACT_ACTUALS', 'NET_SRC_ELEC_LIFETIME_MMBTU', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 36, 'Natural Gas',       'Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion', 'Net Realized Source Annual Gas Savings - MMBtu',        'Natural Gas',       'Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SRC_GAS_ANNUAL_MMBTU',    'CES_SAVINGS_FACT_ACTUALS', 'NET_SRC_GAS_ANNUAL_MMBTU',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 37, 'Natural Gas',       'Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion', 'Net Realized Source Lifetime Gas Savings - MMBtu',      'Natural Gas',       'Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SRC_GAS_LIFETIME_MMBTU',  'CES_SAVINGS_FACT_ACTUALS', 'NET_SRC_GAS_LIFETIME_MMBTU',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 38, 'Elec and Gas Total','Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion', 'Net Realized Source Annual Savings - MMBtu',            'Elec and Gas Total','Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SRC_ANNUAL_MMBTU',        'CES_SAVINGS_FACT_ACTUALS', 'NET_SRC_ANNUAL_MMBTU',        'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 39, 'Elec and Gas Total','Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion', 'Net Realized Source Lifetime Savings - MMBtu',          'Elec and Gas Total','Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'NET_SRC_LIFETIME_MMBTU',      'CES_SAVINGS_FACT_ACTUALS', 'NET_SRC_LIFETIME_MMBTU',      'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- TARGET SEGMENTS SOURCE
SELECT 'Savings Results', 40, 'Residential LMI or OBC',  'Target Segments - Net Realized Source Savings', 'LMI OBC Annual Energy Savings (Net Source MMBtu)',           'Residential LMI or OBC',  'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_LMI_OBC_ANNUAL_MMBTU',     'CES_SAVINGS_FACT_ACTUALS', 'SEG_LMI_OBC_ANNUAL_MMBTU',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 41, 'Residential LMI or OBC',  'Target Segments - Net Realized Source Savings', 'LMI OBC Lifetime Energy Savings (Net Source MMBtu)',         'Residential LMI or OBC',  'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_LMI_OBC_LIFETIME_MMBTU',   'CES_SAVINGS_FACT_ACTUALS', 'SEG_LMI_OBC_LIFETIME_MMBTU',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 42, 'Residential LMI Only',    'Target Segments - Net Realized Source Savings', 'LMI Annual Energy Savings (Net Source MMBtu)',               'Residential LMI Only',    'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_LMI_ANNUAL_MMBTU',         'CES_SAVINGS_FACT_ACTUALS', 'SEG_LMI_ANNUAL_MMBTU',         'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 43, 'Residential LMI Only',    'Target Segments - Net Realized Source Savings', 'LMI Lifetime Energy Savings (Net Source MMBtu)',             'Residential LMI Only',    'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_LMI_LIFETIME_MMBTU',       'CES_SAVINGS_FACT_ACTUALS', 'SEG_LMI_LIFETIME_MMBTU',       'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 44, 'OBC Only (All Projects)', 'Target Segments - Net Realized Source Savings', 'OBC Annual Energy Savings (Net Source MMBtu)',               'OBC Only (All Projects)', 'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_OBC_ANNUAL_MMBTU',         'CES_SAVINGS_FACT_ACTUALS', 'SEG_OBC_ANNUAL_MMBTU',         'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 45, 'OBC Only (All Projects)', 'Target Segments - Net Realized Source Savings', 'OBC Lifetime Energy Savings (Net Source MMBtu)',             'OBC Only (All Projects)', 'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_OBC_LIFETIME_MMBTU',       'CES_SAVINGS_FACT_ACTUALS', 'SEG_OBC_LIFETIME_MMBTU',       'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 46, 'Small Business',          'Target Segments - Net Realized Source Savings', 'Small Business Annual Energy Savings (Net Source MMBtu)',    'Small Business',          'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_SMALL_BIZ_ANNUAL_MMBTU',   'CES_SAVINGS_FACT_ACTUALS', 'SEG_SMALL_BIZ_ANNUAL_MMBTU',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 47, 'Small Business',          'Target Segments - Net Realized Source Savings', 'Small Business Lifetime Energy Savings (Net Source MMBtu)', 'Small Business',          'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_SMALL_BIZ_LIFETIME_MMBTU', 'CES_SAVINGS_FACT_ACTUALS', 'SEG_SMALL_BIZ_LIFETIME_MMBTU', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 48, 'Multifamily',             'Target Segments - Net Realized Source Savings', 'Multifamily Annual Energy Savings (Net Source MMBtu)',       'Multifamily',             'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_MULTIFAMILY_ANNUAL_MMBTU',   'CES_SAVINGS_FACT_ACTUALS', 'SEG_MULTIFAMILY_ANNUAL_MMBTU',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 49, 'Multifamily',             'Target Segments - Net Realized Source Savings', 'Multifamily Lifetime Energy Savings (Net Source MMBtu)',     'Multifamily',             'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_MULTIFAMILY_LIFETIME_MMBTU', 'CES_SAVINGS_FACT_ACTUALS', 'SEG_MULTIFAMILY_LIFETIME_MMBTU', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- TARGET SEGMENTS SITE - RES LMI OBC
SELECT 'Savings Results', 50, 'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Annual Electric Savings - kWh',   'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_RES_LMI_OBC_ELEC_ANNUAL_KWH',    'CES_SAVINGS_FACT_ACTUALS', 'SEG_RES_LMI_OBC_ELEC_ANNUAL_KWH',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 51, 'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Lifetime Electric Savings - kWh', 'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_RES_LMI_OBC_ELEC_LIFETIME_KWH',  'CES_SAVINGS_FACT_ACTUALS', 'SEG_RES_LMI_OBC_ELEC_LIFETIME_KWH',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 52, 'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Annual Gas Savings - Therms',     'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_RES_LMI_OBC_GAS_ANNUAL_THERMS',  'CES_SAVINGS_FACT_ACTUALS', 'SEG_RES_LMI_OBC_GAS_ANNUAL_THERMS',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 53, 'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Lifetime Gas Savings - Therms',   'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_RES_LMI_OBC_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS', 'SEG_RES_LMI_OBC_GAS_LIFETIME_THERMS','NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- TARGET SEGMENTS SITE - RES LMI ONLY
SELECT 'Savings Results', 54, 'Residential LMI Only', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Annual Electric Savings - kWh',   'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_RES_LMI_ELEC_ANNUAL_KWH',    'CES_SAVINGS_FACT_ACTUALS', 'SEG_RES_LMI_ELEC_ANNUAL_KWH',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 55, 'Residential LMI Only', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Lifetime Electric Savings - kWh', 'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_RES_LMI_ELEC_LIFETIME_KWH',  'CES_SAVINGS_FACT_ACTUALS', 'SEG_RES_LMI_ELEC_LIFETIME_KWH',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 56, 'Residential LMI Only', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Annual Gas Savings - Therms',     'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_RES_LMI_GAS_ANNUAL_THERMS',  'CES_SAVINGS_FACT_ACTUALS', 'SEG_RES_LMI_GAS_ANNUAL_THERMS',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 57, 'Residential LMI Only', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Lifetime Gas Savings - Therms',   'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_RES_LMI_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS', 'SEG_RES_LMI_GAS_LIFETIME_THERMS','NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- TARGET SEGMENTS SITE - OBC ONLY
SELECT 'Savings Results', 58, 'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Annual Electric Savings - kWh',   'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_OBC_ELEC_ANNUAL_KWH',    'CES_SAVINGS_FACT_ACTUALS', 'SEG_OBC_ELEC_ANNUAL_KWH',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 59, 'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Lifetime Electric Savings - kWh', 'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_OBC_ELEC_LIFETIME_KWH',  'CES_SAVINGS_FACT_ACTUALS', 'SEG_OBC_ELEC_LIFETIME_KWH',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 60, 'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Annual Gas Savings - Therms',     'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_OBC_GAS_ANNUAL_THERMS',  'CES_SAVINGS_FACT_ACTUALS', 'SEG_OBC_GAS_ANNUAL_THERMS',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 61, 'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Lifetime Gas Savings - Therms',   'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_OBC_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS', 'SEG_OBC_GAS_LIFETIME_THERMS','NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- TARGET SEGMENTS SITE - SMALL BUSINESS
SELECT 'Savings Results', 62, 'Small Business', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Annual Electric Savings - kWh',   'Small Business', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_SMALL_BIZ_ELEC_ANNUAL_KWH',    'CES_SAVINGS_FACT_ACTUALS', 'SEG_SMALL_BIZ_ELEC_ANNUAL_KWH',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 63, 'Small Business', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Lifetime Electric Savings - kWh', 'Small Business', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_SMALL_BIZ_ELEC_LIFETIME_KWH',  'CES_SAVINGS_FACT_ACTUALS', 'SEG_SMALL_BIZ_ELEC_LIFETIME_KWH',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 64, 'Small Business', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Annual Gas Savings - Therms',     'Small Business', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_SMALL_BIZ_GAS_ANNUAL_THERMS',  'CES_SAVINGS_FACT_ACTUALS', 'SEG_SMALL_BIZ_GAS_ANNUAL_THERMS',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 65, 'Small Business', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Lifetime Gas Savings - Therms',   'Small Business', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_SMALL_BIZ_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS', 'SEG_SMALL_BIZ_GAS_LIFETIME_THERMS','NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- TARGET SEGMENTS SITE - MULTIFAMILY
SELECT 'Savings Results', 66, 'Multifamily', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Annual Electric Savings - kWh',   'Multifamily', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_MULTIFAMILY_ELEC_ANNUAL_KWH',    'CES_SAVINGS_FACT_ACTUALS', 'SEG_MULTIFAMILY_ELEC_ANNUAL_KWH',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 67, 'Multifamily', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Lifetime Electric Savings - kWh', 'Multifamily', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_MULTIFAMILY_ELEC_LIFETIME_KWH',  'CES_SAVINGS_FACT_ACTUALS', 'SEG_MULTIFAMILY_ELEC_LIFETIME_KWH',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 68, 'Multifamily', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Annual Gas Savings - Therms',     'Multifamily', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_MULTIFAMILY_GAS_ANNUAL_THERMS',  'CES_SAVINGS_FACT_ACTUALS', 'SEG_MULTIFAMILY_GAS_ANNUAL_THERMS',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Results', 69, 'Multifamily', 'Target Segments - Net Realized Site Savings (Exclude Negative IEs)', 'Net Realized Site Lifetime Gas Savings - Therms',   'Multifamily', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_ACTUALS', 'SEG_MULTIFAMILY_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS', 'SEG_MULTIFAMILY_GAS_LIFETIME_THERMS','NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- ============================================================
-- SAVINGS FORECAST
-- Cols 1-6 same, then Forecast Period + Confidence Level
-- Financials shift to 9-12, Participants 13-19
-- All metrics shift +2 from actuals
-- ============================================================
SELECT 'Savings Forecast', 1,  '', '', 'Triennium',             '', '', 75, 'CES_SAVINGS_STG_FORECAST', 'TRIENNIUM',             'CES_SAVINGS_FACT_FORECAST', 'TRIENNIUM',             'VARCHAR2',   'Y', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 2,  '', '', 'Sector',                '', '', 75, 'CES_SAVINGS_STG_FORECAST', 'SECTOR',                'CES_SAVINGS_FACT_FORECAST', 'SECTOR',                'VARCHAR2',   'Y', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 3,  '', '', 'Vendor Subprogram Key', '', '', 75, 'CES_SAVINGS_STG_FORECAST', 'VENDOR_SUBPROGRAM_KEY', 'CES_SAVINGS_FACT_FORECAST', 'VENDOR_SUBPROGRAM_KEY', 'VARCHAR2',   'Y', 'Y', 'Y', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 4,  '', '', 'Program',               '', '', 75, 'CES_SAVINGS_STG_FORECAST', 'PROGRAM',               'CES_SAVINGS_FACT_FORECAST', 'PROGRAM',               'VARCHAR2',   'Y', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 5,  '', '', 'Subprogram',            '', '', 75, 'CES_SAVINGS_STG_FORECAST', 'SUBPROGRAM',            'CES_SAVINGS_FACT_FORECAST', 'SUBPROGRAM',            'VARCHAR2',   'Y', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 6,  '', '', 'Program Year',          '', '', 75, 'CES_SAVINGS_STG_FORECAST', 'PROGRAM_YEAR',          'CES_SAVINGS_FACT_FORECAST', 'PROGRAM_YEAR',          'VARCHAR2',   'Y', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 7,  '', '', 'Forecast Period',       '', '', 75, 'CES_SAVINGS_STG_FORECAST', 'FORECAST_PERIOD',       'CES_SAVINGS_FACT_FORECAST', 'FORECAST_PERIOD',       'VARCHAR2',   'Y', 'Y', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 8,  '', '', 'Confidence Level',      '', '', 75, 'CES_SAVINGS_STG_FORECAST', 'CONFIDENCE_LEVEL',      'CES_SAVINGS_FACT_FORECAST', 'CONFIDENCE_LEVEL',      'VARCHAR2',   'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 9,  '', 'Financials', '1) Investment - Rebate', '', 'Financials', 75, 'CES_SAVINGS_STG_FORECAST', 'INVEST_COST_REBATE',  'CES_SAVINGS_FACT_FORECAST', 'INVEST_COST_REBATE',  'NUMBER_2',   'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 10, '', 'Financials', '2) Investment - OBR',    '', 'Financials', 75, 'CES_SAVINGS_STG_FORECAST', 'INVEST_COST_OBR',     'CES_SAVINGS_FACT_FORECAST', 'INVEST_COST_OBR',     'NUMBER_2',   'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 11, '', 'Financials', '3) Investment - Other',  '', 'Financials', 75, 'CES_SAVINGS_STG_FORECAST', 'INVEST_COST_OTHER',   'CES_SAVINGS_FACT_FORECAST', 'INVEST_COST_OTHER',   'NUMBER_2',   'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 12, '', 'Financials', 'Total Investment',        '', 'Financials', 75, 'CES_SAVINGS_STG_FORECAST', 'TOTAL_INVEST_COST',   'CES_SAVINGS_FACT_FORECAST', 'TOTAL_INVEST_COST',   'NUMBER_2',   'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 13, '', 'Participants', 'Total',                  '', 'Participants', 75, 'CES_SAVINGS_STG_FORECAST', 'PARTICIPANTS_TOTAL',        'CES_SAVINGS_FACT_FORECAST', 'PARTICIPANTS_TOTAL',        'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 14, '', 'Participants', 'Res LMI or OBC',         '', 'Participants', 75, 'CES_SAVINGS_STG_FORECAST', 'PARTICIPANTS_RES_LMI_OBC',  'CES_SAVINGS_FACT_FORECAST', 'PARTICIPANTS_RES_LMI_OBC',  'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 15, '', 'Participants', 'Res LMI Only',           '', 'Participants', 75, 'CES_SAVINGS_STG_FORECAST', 'PARTICIPANTS_RES_LMI_ONLY', 'CES_SAVINGS_FACT_FORECAST', 'PARTICIPANTS_RES_LMI_ONLY', 'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 16, '', 'Participants', 'OBC Only (All Projects)', '', 'Participants', 75, 'CES_SAVINGS_STG_FORECAST', 'PARTICIPANTS_OBC_ONLY',     'CES_SAVINGS_FACT_FORECAST', 'PARTICIPANTS_OBC_ONLY',     'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 17, '', 'Participants', 'Small Business',          '', 'Participants', 75, 'CES_SAVINGS_STG_FORECAST', 'PARTICIPANTS_SMALL_BIZ',    'CES_SAVINGS_FACT_FORECAST', 'PARTICIPANTS_SMALL_BIZ',    'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 18, '', 'Participants', 'Multi Family',            '', 'Participants', 75, 'CES_SAVINGS_STG_FORECAST', 'PARTICIPANTS_MULTIFAMILY',  'CES_SAVINGS_FACT_FORECAST', 'PARTICIPANTS_MULTIFAMILY',  'NUMBER_INT', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
-- GROSS SITE (shift +2 from actuals: 18->20, 19->21 etc)
SELECT 'Savings Forecast', 20, 'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Annual Electric Savings - kWh',           'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_FORECAST', 'GROSS_SITE_ELEC_ANNUAL_KWH',       'CES_SAVINGS_FACT_FORECAST', 'GROSS_SITE_ELEC_ANNUAL_KWH',       'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 21, 'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Annual Electric Demand Savings - kW',     'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_FORECAST', 'GROSS_SITE_ELEC_DEMAND_KW',        'CES_SAVINGS_FACT_FORECAST', 'GROSS_SITE_ELEC_DEMAND_KW',        'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 22, 'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Lifetime Electric Savings - kWh',         'Electric',    'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_FORECAST', 'GROSS_SITE_ELEC_LIFETIME_KWH',     'CES_SAVINGS_FACT_FORECAST', 'GROSS_SITE_ELEC_LIFETIME_KWH',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 23, 'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Annual Gas Savings - Therms',             'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_FORECAST', 'GROSS_SITE_GAS_ANNUAL_THERMS',     'CES_SAVINGS_FACT_FORECAST', 'GROSS_SITE_GAS_ANNUAL_THERMS',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 24, 'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Annual Gas Demand Daily Peak',            'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_FORECAST', 'GROSS_SITE_GAS_DAILY_PEAK_THERMS', 'CES_SAVINGS_FACT_FORECAST', 'GROSS_SITE_GAS_DAILY_PEAK_THERMS', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 25, 'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 'Gross Site Lifetime Gas Savings - Therms',           'Natural Gas', 'Gross Site Savings (Exclude IEs) TRM Protocol', 75, 'CES_SAVINGS_STG_FORECAST', 'GROSS_SITE_GAS_LIFETIME_THERMS',   'CES_SAVINGS_FACT_FORECAST', 'GROSS_SITE_GAS_LIFETIME_THERMS',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 26, 'Electric',    'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Annual Electric Savings - kWh',        'Electric',    'Net Realized Site Savings TRM x ISR', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SITE_ISR_ELEC_ANNUAL_KWH',       'CES_SAVINGS_FACT_FORECAST', 'NET_SITE_ISR_ELEC_ANNUAL_KWH',       'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 27, 'Electric',    'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Annual Electric Demand Savings - kW',  'Electric',    'Net Realized Site Savings TRM x ISR', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SITE_ISR_ELEC_DEMAND_KW',        'CES_SAVINGS_FACT_FORECAST', 'NET_SITE_ISR_ELEC_DEMAND_KW',        'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 28, 'Electric',    'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Lifetime Electric Savings - kWh',      'Electric',    'Net Realized Site Savings TRM x ISR', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SITE_ISR_ELEC_LIFETIME_KWH',     'CES_SAVINGS_FACT_FORECAST', 'NET_SITE_ISR_ELEC_LIFETIME_KWH',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 29, 'Natural Gas', 'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Annual Gas Savings - Therms',          'Natural Gas', 'Net Realized Site Savings TRM x ISR', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SITE_ISR_GAS_ANNUAL_THERMS',     'CES_SAVINGS_FACT_FORECAST', 'NET_SITE_ISR_GAS_ANNUAL_THERMS',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 30, 'Natural Gas', 'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Annual Gas Demand Daily Peak',         'Natural Gas', 'Net Realized Site Savings TRM x ISR', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SITE_ISR_GAS_DAILY_PEAK_THERMS', 'CES_SAVINGS_FACT_FORECAST', 'NET_SITE_ISR_GAS_DAILY_PEAK_THERMS', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 31, 'Natural Gas', 'Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR', 'Net Realized Site Lifetime Gas Savings - Therms',        'Natural Gas', 'Net Realized Site Savings TRM x ISR', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SITE_ISR_GAS_LIFETIME_THERMS',   'CES_SAVINGS_FACT_FORECAST', 'NET_SITE_ISR_GAS_LIFETIME_THERMS',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 32, 'Natural Gas', 'Negative Interactive Effects (IEs)', 'Net Realized Site Annual Gas Therms for IEs Only',   'Natural Gas', 'Negative Interactive Effects (IEs)', 75, 'CES_SAVINGS_STG_FORECAST', 'NEG_IE_GAS_ANNUAL_THERMS',   'CES_SAVINGS_FACT_FORECAST', 'NEG_IE_GAS_ANNUAL_THERMS',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 33, 'Natural Gas', 'Negative Interactive Effects (IEs)', 'Net Realized Site Lifetime Gas Therms for IEs Only', 'Natural Gas', 'Negative Interactive Effects (IEs)', 75, 'CES_SAVINGS_STG_FORECAST', 'NEG_IE_GAS_LIFETIME_THERMS', 'CES_SAVINGS_FACT_FORECAST', 'NEG_IE_GAS_LIFETIME_THERMS', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 34, 'Natural Gas', 'Negative Interactive Effects (IEs)', 'Net Realized Site Annual Gas Therms IEs Applied',    'Natural Gas', 'Negative Interactive Effects (IEs)', 75, 'CES_SAVINGS_STG_FORECAST', 'NEG_IE_ELEC_ANNUAL_KWH',     'CES_SAVINGS_FACT_FORECAST', 'NEG_IE_ELEC_ANNUAL_KWH',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 35, 'Natural Gas', 'Negative Interactive Effects (IEs)', 'Net Realized Site Lifetime Gas Therms IEs Applied',  'Natural Gas', 'Negative Interactive Effects (IEs)', 75, 'CES_SAVINGS_STG_FORECAST', 'NEG_IE_ELEC_LIFETIME_KWH',   'CES_SAVINGS_FACT_FORECAST', 'NEG_IE_ELEC_LIFETIME_KWH',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 36, 'Electric',          'Net Realized Source Savings', 'Net Realized Source Annual Electric Savings - MMBtu',   'Electric',          'Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SRC_ELEC_ANNUAL_MMBTU',   'CES_SAVINGS_FACT_FORECAST', 'NET_SRC_ELEC_ANNUAL_MMBTU',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 37, 'Electric',          'Net Realized Source Savings', 'Net Realized Source Lifetime Electric Savings - MMBtu', 'Electric',          'Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SRC_ELEC_LIFETIME_MMBTU', 'CES_SAVINGS_FACT_FORECAST', 'NET_SRC_ELEC_LIFETIME_MMBTU', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 38, 'Natural Gas',       'Net Realized Source Savings', 'Net Realized Source Annual Gas Savings - MMBtu',        'Natural Gas',       'Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SRC_GAS_ANNUAL_MMBTU',    'CES_SAVINGS_FACT_FORECAST', 'NET_SRC_GAS_ANNUAL_MMBTU',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 39, 'Natural Gas',       'Net Realized Source Savings', 'Net Realized Source Lifetime Gas Savings - MMBtu',      'Natural Gas',       'Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SRC_GAS_LIFETIME_MMBTU',  'CES_SAVINGS_FACT_FORECAST', 'NET_SRC_GAS_LIFETIME_MMBTU',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 40, 'Elec and Gas Total','Net Realized Source Savings', 'Net Realized Source Annual Savings - MMBtu',            'Elec and Gas Total','Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SRC_ANNUAL_MMBTU',        'CES_SAVINGS_FACT_FORECAST', 'NET_SRC_ANNUAL_MMBTU',        'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 41, 'Elec and Gas Total','Net Realized Source Savings', 'Net Realized Source Lifetime Savings - MMBtu',          'Elec and Gas Total','Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'NET_SRC_LIFETIME_MMBTU',      'CES_SAVINGS_FACT_FORECAST', 'NET_SRC_LIFETIME_MMBTU',      'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 42, 'Residential LMI or OBC',  'Target Segments - Net Realized Source Savings', 'LMI OBC Annual Energy Savings (Net Source MMBtu)',           'Residential LMI or OBC',  'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_LMI_OBC_ANNUAL_MMBTU',     'CES_SAVINGS_FACT_FORECAST', 'SEG_LMI_OBC_ANNUAL_MMBTU',     'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 43, 'Residential LMI or OBC',  'Target Segments - Net Realized Source Savings', 'LMI OBC Lifetime Energy Savings (Net Source MMBtu)',         'Residential LMI or OBC',  'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_LMI_OBC_LIFETIME_MMBTU',   'CES_SAVINGS_FACT_FORECAST', 'SEG_LMI_OBC_LIFETIME_MMBTU',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 44, 'Residential LMI Only',    'Target Segments - Net Realized Source Savings', 'LMI Annual Energy Savings (Net Source MMBtu)',               'Residential LMI Only',    'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_LMI_ANNUAL_MMBTU',         'CES_SAVINGS_FACT_FORECAST', 'SEG_LMI_ANNUAL_MMBTU',         'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 45, 'Residential LMI Only',    'Target Segments - Net Realized Source Savings', 'LMI Lifetime Energy Savings (Net Source MMBtu)',             'Residential LMI Only',    'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_LMI_LIFETIME_MMBTU',       'CES_SAVINGS_FACT_FORECAST', 'SEG_LMI_LIFETIME_MMBTU',       'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 46, 'OBC Only (All Projects)', 'Target Segments - Net Realized Source Savings', 'OBC Annual Energy Savings (Net Source MMBtu)',               'OBC Only (All Projects)', 'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_OBC_ANNUAL_MMBTU',         'CES_SAVINGS_FACT_FORECAST', 'SEG_OBC_ANNUAL_MMBTU',         'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 47, 'OBC Only (All Projects)', 'Target Segments - Net Realized Source Savings', 'OBC Lifetime Energy Savings (Net Source MMBtu)',             'OBC Only (All Projects)', 'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_OBC_LIFETIME_MMBTU',       'CES_SAVINGS_FACT_FORECAST', 'SEG_OBC_LIFETIME_MMBTU',       'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 48, 'Small Business',          'Target Segments - Net Realized Source Savings', 'Small Business Annual Energy Savings (Net Source MMBtu)',    'Small Business',          'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_SMALL_BIZ_ANNUAL_MMBTU',   'CES_SAVINGS_FACT_FORECAST', 'SEG_SMALL_BIZ_ANNUAL_MMBTU',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 49, 'Small Business',          'Target Segments - Net Realized Source Savings', 'Small Business Lifetime Energy Savings (Net Source MMBtu)', 'Small Business',          'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_SMALL_BIZ_LIFETIME_MMBTU', 'CES_SAVINGS_FACT_FORECAST', 'SEG_SMALL_BIZ_LIFETIME_MMBTU', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 50, 'Multifamily',             'Target Segments - Net Realized Source Savings', 'Multifamily Annual Energy Savings (Net Source MMBtu)',       'Multifamily',             'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_MULTIFAMILY_ANNUAL_MMBTU',   'CES_SAVINGS_FACT_FORECAST', 'SEG_MULTIFAMILY_ANNUAL_MMBTU',   'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 51, 'Multifamily',             'Target Segments - Net Realized Source Savings', 'Multifamily Lifetime Energy Savings (Net Source MMBtu)',     'Multifamily',             'Target Segments - Net Realized Source Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_MULTIFAMILY_LIFETIME_MMBTU', 'CES_SAVINGS_FACT_FORECAST', 'SEG_MULTIFAMILY_LIFETIME_MMBTU', 'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 52, 'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Annual Electric Savings - kWh',   'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_RES_LMI_OBC_ELEC_ANNUAL_KWH',    'CES_SAVINGS_FACT_FORECAST', 'SEG_RES_LMI_OBC_ELEC_ANNUAL_KWH',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 53, 'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Lifetime Electric Savings - kWh', 'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_RES_LMI_OBC_ELEC_LIFETIME_KWH',  'CES_SAVINGS_FACT_FORECAST', 'SEG_RES_LMI_OBC_ELEC_LIFETIME_KWH',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 54, 'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Annual Gas Savings - Therms',     'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_RES_LMI_OBC_GAS_ANNUAL_THERMS',  'CES_SAVINGS_FACT_FORECAST', 'SEG_RES_LMI_OBC_GAS_ANNUAL_THERMS',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 55, 'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Lifetime Gas Savings - Therms',   'Residential LMI or OBC', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_RES_LMI_OBC_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_FORECAST', 'SEG_RES_LMI_OBC_GAS_LIFETIME_THERMS','NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 56, 'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Annual Electric Savings - kWh',   'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_RES_LMI_ELEC_ANNUAL_KWH',    'CES_SAVINGS_FACT_FORECAST', 'SEG_RES_LMI_ELEC_ANNUAL_KWH',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 57, 'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Lifetime Electric Savings - kWh', 'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_RES_LMI_ELEC_LIFETIME_KWH',  'CES_SAVINGS_FACT_FORECAST', 'SEG_RES_LMI_ELEC_LIFETIME_KWH',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 58, 'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Annual Gas Savings - Therms',     'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_RES_LMI_GAS_ANNUAL_THERMS',  'CES_SAVINGS_FACT_FORECAST', 'SEG_RES_LMI_GAS_ANNUAL_THERMS',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 59, 'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Lifetime Gas Savings - Therms',   'Residential LMI Only', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_RES_LMI_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_FORECAST', 'SEG_RES_LMI_GAS_LIFETIME_THERMS','NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 60, 'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Annual Electric Savings - kWh',   'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_OBC_ELEC_ANNUAL_KWH',    'CES_SAVINGS_FACT_FORECAST', 'SEG_OBC_ELEC_ANNUAL_KWH',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 61, 'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Lifetime Electric Savings - kWh', 'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_OBC_ELEC_LIFETIME_KWH',  'CES_SAVINGS_FACT_FORECAST', 'SEG_OBC_ELEC_LIFETIME_KWH',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 62, 'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Annual Gas Savings - Therms',     'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_OBC_GAS_ANNUAL_THERMS',  'CES_SAVINGS_FACT_FORECAST', 'SEG_OBC_GAS_ANNUAL_THERMS',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 63, 'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Lifetime Gas Savings - Therms',   'OBC Only (All Projects)', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_OBC_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_FORECAST', 'SEG_OBC_GAS_LIFETIME_THERMS','NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 64, 'Small Business', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Annual Electric Savings - kWh',   'Small Business', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_SMALL_BIZ_ELEC_ANNUAL_KWH',    'CES_SAVINGS_FACT_FORECAST', 'SEG_SMALL_BIZ_ELEC_ANNUAL_KWH',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 65, 'Small Business', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Lifetime Electric Savings - kWh', 'Small Business', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_SMALL_BIZ_ELEC_LIFETIME_KWH',  'CES_SAVINGS_FACT_FORECAST', 'SEG_SMALL_BIZ_ELEC_LIFETIME_KWH',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 66, 'Small Business', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Annual Gas Savings - Therms',     'Small Business', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_SMALL_BIZ_GAS_ANNUAL_THERMS',  'CES_SAVINGS_FACT_FORECAST', 'SEG_SMALL_BIZ_GAS_ANNUAL_THERMS',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 67, 'Small Business', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Lifetime Gas Savings - Therms',   'Small Business', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_SMALL_BIZ_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_FORECAST', 'SEG_SMALL_BIZ_GAS_LIFETIME_THERMS','NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 68, 'Multifamily', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Annual Electric Savings - kWh',   'Multifamily', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_MULTIFAMILY_ELEC_ANNUAL_KWH',    'CES_SAVINGS_FACT_FORECAST', 'SEG_MULTIFAMILY_ELEC_ANNUAL_KWH',    'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 69, 'Multifamily', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Lifetime Electric Savings - kWh', 'Multifamily', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_MULTIFAMILY_ELEC_LIFETIME_KWH',  'CES_SAVINGS_FACT_FORECAST', 'SEG_MULTIFAMILY_ELEC_LIFETIME_KWH',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 70, 'Multifamily', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Annual Gas Savings - Therms',     'Multifamily', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_MULTIFAMILY_GAS_ANNUAL_THERMS',  'CES_SAVINGS_FACT_FORECAST', 'SEG_MULTIFAMILY_GAS_ANNUAL_THERMS',  'NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL UNION ALL
SELECT 'Savings Forecast', 71, 'Multifamily', 'Target Segments - Net Realized Site Savings', 'Net Realized Site Lifetime Gas Savings - Therms',   'Multifamily', 'Target Segments - Net Realized Site Savings', 75, 'CES_SAVINGS_STG_FORECAST', 'SEG_MULTIFAMILY_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_FORECAST', 'SEG_MULTIFAMILY_GAS_LIFETIME_THERMS','NUMBER_4', 'N', 'N', 'N', 'Y' FROM DUAL
) s ON (t.SHEET_NAME = s.SN AND t.EXCEL_COL_POSITION = s.CP)
WHEN MATCHED THEN UPDATE SET
    t.EXCEL_ROW1_HEADER  = s.R1, t.EXCEL_ROW2_HEADER = s.R2,
    t.EXCEL_ROW3_HEADER  = s.R3, t.ROW1_EXPECTED     = s.E1,
    t.ROW2_EXPECTED      = s.E2, t.FUZZY_THRESHOLD    = s.FT,
    t.STG_TABLE_NAME     = s.ST, t.STG_COLUMN_NAME    = s.SC,
    t.FACT_TABLE_NAME    = s.FT2,t.FACT_COLUMN_NAME   = s.FC,
    t.DATA_TYPE          = s.DT, t.IS_DIMENSION       = s.ID,
    t.IS_REQUIRED        = s.IR, t.IS_STOP_COLUMN     = s.IS2,
    t.IS_ACTIVE          = s.IA, t.UPDATED_DATE       = SYSTIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    SHEET_NAME, EXCEL_COL_POSITION,
    EXCEL_ROW1_HEADER, EXCEL_ROW2_HEADER, EXCEL_ROW3_HEADER,
    ROW1_EXPECTED, ROW2_EXPECTED, FUZZY_THRESHOLD,
    STG_TABLE_NAME, STG_COLUMN_NAME, FACT_TABLE_NAME, FACT_COLUMN_NAME,
    DATA_TYPE, IS_DIMENSION, IS_REQUIRED, IS_STOP_COLUMN, IS_ACTIVE
) VALUES (
    s.SN, s.CP, s.R1, s.R2, s.R3, s.E1, s.E2, s.FT,
    s.ST, s.SC, s.FT2, s.FC, s.DT, s.ID, s.IR, s.IS2, s.IA
);

COMMIT;

-- ============================================================
-- VERIFY
-- ============================================================
SELECT SHEET_NAME, COUNT(*) ROWS
FROM   CES_SAVINGS_COLUMN_MAP
GROUP BY SHEET_NAME ORDER BY SHEET_NAME;

SELECT EXCEL_COL_POSITION, STG_COLUMN_NAME
FROM   CES_SAVINGS_COLUMN_MAP
WHERE  SHEET_NAME = 'Savings Results'
ORDER BY EXCEL_COL_POSITION;
-- ============================================================
