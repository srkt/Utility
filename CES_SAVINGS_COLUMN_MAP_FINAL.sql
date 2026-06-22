-- ============================================================
-- CES SAVINGS COLUMN MAP - FINAL VERSION
-- Built from confirmed final column names (post-rename)
-- Index + Fuzzy + Row1/2 confidence scoring strategy
-- Run this AFTER all table renames/drops are complete
-- ============================================================

TRUNCATE TABLE CES_SAVINGS_COLUMN_MAP;

-- Make sure unique constraint is index-based
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE CES_SAVINGS_COLUMN_MAP DROP CONSTRAINT CES_SAVINGS_COL_MAP_UQ1';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/
ALTER TABLE CES_SAVINGS_COLUMN_MAP
    ADD CONSTRAINT CES_SAVINGS_COL_MAP_UQ1
    UNIQUE (SHEET_NAME, EXCEL_COL_POSITION);

-- ============================================================
-- SAVINGS RESULTS (69 cols, indexes 1-69)
-- ============================================================
INSERT INTO CES_SAVINGS_COLUMN_MAP
(SHEET_NAME, EXCEL_COL_POSITION, EXCEL_ROW1_HEADER, EXCEL_ROW2_HEADER, EXCEL_ROW3_HEADER,
 ROW1_EXPECTED, ROW2_EXPECTED, FUZZY_THRESHOLD,
 STG_TABLE_NAME, STG_COLUMN_NAME, FACT_TABLE_NAME, FACT_COLUMN_NAME,
 DATA_TYPE, IS_DIMENSION, IS_REQUIRED, IS_STOP_COLUMN, IS_ACTIVE)
SELECT 'Savings Results',1,'','','Triennium','','',75,'CES_SAVINGS_STG_ACTUALS','TRIENNIUM','CES_SAVINGS_FACT_ACTUALS','TRIENNIUM','VARCHAR2','Y','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',2,'','','Sector','','',75,'CES_SAVINGS_STG_ACTUALS','SECTOR','CES_SAVINGS_FACT_ACTUALS','SECTOR','VARCHAR2','Y','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',3,'','','Vendor Subprogram Key','','',75,'CES_SAVINGS_STG_ACTUALS','VENDOR_SUBPROGRAM_KEY','CES_SAVINGS_FACT_ACTUALS','VENDOR_SUBPROGRAM_KEY','VARCHAR2','Y','Y','Y','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',4,'','','Program','','',75,'CES_SAVINGS_STG_ACTUALS','PROGRAM','CES_SAVINGS_FACT_ACTUALS','PROGRAM','VARCHAR2','Y','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',5,'','','Subprogram','','',75,'CES_SAVINGS_STG_ACTUALS','SUBPROGRAM','CES_SAVINGS_FACT_ACTUALS','SUBPROGRAM','VARCHAR2','Y','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',6,'','','Program Year','','',75,'CES_SAVINGS_STG_ACTUALS','PROGRAM_YEAR','CES_SAVINGS_FACT_ACTUALS','PROGRAM_YEAR','VARCHAR2','Y','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',7,'','','Reporting Period','','',75,'CES_SAVINGS_STG_ACTUALS','REPORTING_PERIOD','CES_SAVINGS_FACT_ACTUALS','REPORTING_PERIOD','VARCHAR2','Y','Y','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',8,'','Financials','1) Investment - Rebate','','Financials',75,'CES_SAVINGS_STG_ACTUALS','INVEST_COST_REBATE','CES_SAVINGS_FACT_ACTUALS','INVEST_COST_REBATE','NUMBER_2','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',9,'','Financials','2) Investment - OBR','','Financials',75,'CES_SAVINGS_STG_ACTUALS','INVEST_COST_OBR','CES_SAVINGS_FACT_ACTUALS','INVEST_COST_OBR','NUMBER_2','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',10,'','Financials','3) Investment - Other','','Financials',75,'CES_SAVINGS_STG_ACTUALS','INVEST_COST_OTHER','CES_SAVINGS_FACT_ACTUALS','INVEST_COST_OTHER','NUMBER_2','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',11,'','Financials','Total Investment','','Financials',75,'CES_SAVINGS_STG_ACTUALS','TOTAL_INVEST_COST','CES_SAVINGS_FACT_ACTUALS','TOTAL_INVEST_COST','NUMBER_2','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',12,'','Participants','Total','','Participants',75,'CES_SAVINGS_STG_ACTUALS','PARTICIPANTS_TOTAL','CES_SAVINGS_FACT_ACTUALS','PARTICIPANTS_TOTAL','NUMBER_INT','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',13,'','Participants','Res LMI or OBC','','Participants',75,'CES_SAVINGS_STG_ACTUALS','PARTICIPANTS_RES_LMI_OBC','CES_SAVINGS_FACT_ACTUALS','PARTICIPANTS_RES_LMI_OBC','NUMBER_INT','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',14,'','Participants','Res LMI Only','','Participants',75,'CES_SAVINGS_STG_ACTUALS','PARTICIPANTS_RES_LMI_ONLY','CES_SAVINGS_FACT_ACTUALS','PARTICIPANTS_RES_LMI_ONLY','NUMBER_INT','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',15,'','Participants','OBC Only (All Projects)','','Participants',75,'CES_SAVINGS_STG_ACTUALS','PARTICIPANTS_OBC_ONLY','CES_SAVINGS_FACT_ACTUALS','PARTICIPANTS_OBC_ONLY','NUMBER_INT','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',16,'','Participants','Small Business','','Participants',75,'CES_SAVINGS_STG_ACTUALS','PARTICIPANTS_SMB','CES_SAVINGS_FACT_ACTUALS','PARTICIPANTS_SMB','NUMBER_INT','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',17,'','Participants','Multi Family','','Participants',75,'CES_SAVINGS_STG_ACTUALS','PARTICIPANTS_MFLY','CES_SAVINGS_FACT_ACTUALS','PARTICIPANTS_MFLY','NUMBER_INT','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',18,'Electric','Gross Site Savings (Exclude IEs) TRM Protocol','Gross Site Annual Electric Savings - kWh','Electric','Gross Site Savings (Exclude IEs) TRM Protocol',75,'CES_SAVINGS_STG_ACTUALS','GROSS_SITE_EXC_IES_ELEC_ANNUAL_KWH','CES_SAVINGS_FACT_ACTUALS','GROSS_SITE_EXC_IES_ELEC_ANNUAL_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',19,'Electric','Gross Site Savings (Exclude IEs) TRM Protocol','Gross Site Annual Electric Demand Savings - kW','Electric','Gross Site Savings (Exclude IEs) TRM Protocol',75,'CES_SAVINGS_STG_ACTUALS','GROSS_SITE_EXC_IES_ELEC_DEMAND_KW','CES_SAVINGS_FACT_ACTUALS','GROSS_SITE_EXC_IES_ELEC_DEMAND_KW','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',20,'Electric','Gross Site Savings (Exclude IEs) TRM Protocol','Gross Site Lifetime Electric Savings - kWh','Electric','Gross Site Savings (Exclude IEs) TRM Protocol',75,'CES_SAVINGS_STG_ACTUALS','GROSS_SITE_EXC_IES_ELEC_LIFETIME_KWH','CES_SAVINGS_FACT_ACTUALS','GROSS_SITE_EXC_IES_ELEC_LIFETIME_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',21,'Natural Gas','Gross Site Savings (Exclude IEs) TRM Protocol','Gross Site Annual Gas Savings - Therms','Natural Gas','Gross Site Savings (Exclude IEs) TRM Protocol',75,'CES_SAVINGS_STG_ACTUALS','GROSS_SITE_EXC_IES_GAS_ANNUAL_THERMS','CES_SAVINGS_FACT_ACTUALS','GROSS_SITE_EXC_IES_GAS_ANNUAL_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',22,'Natural Gas','Gross Site Savings (Exclude IEs) TRM Protocol','Gross Site Annual Gas Demand Daily Peak Fuel Savings','Natural Gas','Gross Site Savings (Exclude IEs) TRM Protocol',75,'CES_SAVINGS_STG_ACTUALS','GROSS_SITE_EXC_IES_GAS_DAILY_PEAK_THERMS','CES_SAVINGS_FACT_ACTUALS','GROSS_SITE_EXC_IES_GAS_DAILY_PEAK_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',23,'Natural Gas','Gross Site Savings (Exclude IEs) TRM Protocol','Gross Site Lifetime Gas Savings - Therms','Natural Gas','Gross Site Savings (Exclude IEs) TRM Protocol',75,'CES_SAVINGS_STG_ACTUALS','GROSS_SITE_EXC_IES_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS','GROSS_SITE_EXC_IES_GAS_LIFETIME_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',24,'Electric','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR','Net Realized Site Annual Electric Savings - kWh','Electric','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SITE_EXC_NIES_ELEC_ANNUAL_KWH','CES_SAVINGS_FACT_ACTUALS','NET_REL_SITE_EXC_NIES_ELEC_ANNUAL_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',25,'Electric','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR','Net Realized Site Annual Electric Demand Savings - kW','Electric','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SITE_EXC_NIES_ELEC_DEMAND_KW','CES_SAVINGS_FACT_ACTUALS','NET_REL_SITE_EXC_NIES_ELEC_DEMAND_KW','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',26,'Electric','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR','Net Realized Site Lifetime Electric Savings - kWh','Electric','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SITE_EXC_NIES_ELEC_LIFETIME_KWH','CES_SAVINGS_FACT_ACTUALS','NET_REL_SITE_EXC_NIES_ELEC_LIFETIME_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',27,'Natural Gas','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR','Net Realized Site Annual Gas Savings - Therms','Natural Gas','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SITE_EXC_NIES_GAS_ANNUAL_THERMS','CES_SAVINGS_FACT_ACTUALS','NET_REL_SITE_EXC_NIES_GAS_ANNUAL_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',28,'Natural Gas','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR','Net Realized Site Annual Gas Demand Daily Peak Fuel Savings','Natural Gas','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SITE_EXC_NIES_GAS_DAILY_PEAK_FUEL_THERMS_PDAY','CES_SAVINGS_FACT_ACTUALS','NET_REL_SITE_EXC_NIES_GAS_DAILY_PEAK_FUEL_THERMS_PDAY','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',29,'Natural Gas','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR','Net Realized Site Lifetime Gas Savings - Therms','Natural Gas','Net Realized Site Savings (Exclude Negative IEs) TRM Protocol x ISR',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SITE_EXC_NIES_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS','NET_REL_SITE_EXC_NIES_GAS_LIFETIME_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',30,'Natural Gas','Total Net Realized Site Savings (Include Negative IEs)','Net Realized Site Annual Gas Therms IEs Applied','Natural Gas','Total Net Realized Site Savings (Include Negative IEs)',75,'CES_SAVINGS_STG_ACTUALS','TOT_NET_REL_SITE_GAS_ANNUAL_IES_APPL_THERMS','CES_SAVINGS_FACT_ACTUALS','TOT_NET_REL_SITE_GAS_ANNUAL_IES_APPL_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',31,'Natural Gas','Total Net Realized Site Savings (Include Negative IEs)','Net Realized Site Lifetime Gas Therms IEs Applied','Natural Gas','Total Net Realized Site Savings (Include Negative IEs)',75,'CES_SAVINGS_STG_ACTUALS','TOT_NET_REL_SITE_GAS_LIFETIME_IES_APPL_THERMS','CES_SAVINGS_FACT_ACTUALS','TOT_NET_REL_SITE_GAS_LIFETIME_IES_APPL_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',32,'Natural Gas','Negative Interactive Effects (IEs)','Net Realized Site Annual Gas Therms for IEs Only','Natural Gas','Negative Interactive Effects (IEs)',75,'CES_SAVINGS_STG_ACTUALS','NEG_IES_REL_SITE_GAS_IES_ONLY_ANNUAL_THERMS','CES_SAVINGS_FACT_ACTUALS','NEG_IES_REL_SITE_GAS_IES_ONLY_ANNUAL_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',33,'Natural Gas','Negative Interactive Effects (IEs)','Net Realized Site Lifetime Gas Therms for IEs Only','Natural Gas','Negative Interactive Effects (IEs)',75,'CES_SAVINGS_STG_ACTUALS','NEG_IES_REL_SITE_GAS_IES_ONLY_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS','NEG_IES_REL_SITE_GAS_IES_ONLY_LIFETIME_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',34,'Electric','Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion','Net Realized Source Annual Electric Savings - MMBtu','Electric','Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SRC_INC_NEGIES_ELEC_ANNUAL_MMBTU','CES_SAVINGS_FACT_ACTUALS','NET_REL_SRC_INC_NEGIES_ELEC_ANNUAL_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',35,'Electric','Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion','Net Realized Source Lifetime Electric Savings - MMBtu','Electric','Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SRC_INC_NEGIES_ELEC_LIFETIME_MMBTU','CES_SAVINGS_FACT_ACTUALS','NET_REL_SRC_INC_NEGIES_ELEC_LIFETIME_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',36,'Natural Gas','Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion','Net Realized Source Annual Gas Savings - MMBtu','Natural Gas','Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SRC_INC_NEGIES_GAS_ANNUAL_MMBTU','CES_SAVINGS_FACT_ACTUALS','NET_REL_SRC_INC_NEGIES_GAS_ANNUAL_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',37,'Natural Gas','Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion','Net Realized Source Lifetime Gas Savings - MMBtu','Natural Gas','Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SRC_INC_NEGIES_GAS_LIFETIME_MMBTU','CES_SAVINGS_FACT_ACTUALS','NET_REL_SRC_INC_NEGIES_GAS_LIFETIME_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',38,'Elec and Gas Total','Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion','Net Realized Source Annual Savings - MMBtu','Elec and Gas Total','Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SRC_INC_NEGIES_ANNUAL_MMBTU','CES_SAVINGS_FACT_ACTUALS','NET_REL_SRC_INC_NEGIES_ANNUAL_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',39,'Elec and Gas Total','Net Realized Source Savings (Includes Negative IEs) TRM x ISR x RR x NTG x Source Conversion','Net Realized Source Lifetime Savings - MMBtu','Elec and Gas Total','Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','NET_REL_SRC_INC_NEGIES_LIFETIME_MMBTU','CES_SAVINGS_FACT_ACTUALS','NET_REL_SRC_INC_NEGIES_LIFETIME_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',40,'Residential LMI or OBC','Target Segments - Net Realized Source Savings','LMI OBC Annual Energy Savings (Net Source MMBtu)','Residential LMI or OBC','Target Segments - Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_INC_NIE_NET_REL_LMI_OBC_ANNUAL_MMBTU','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_INC_NIE_NET_REL_LMI_OBC_ANNUAL_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',41,'Residential LMI or OBC','Target Segments - Net Realized Source Savings','LMI OBC Lifetime Energy Savings (Net Source MMBtu)','Residential LMI or OBC','Target Segments - Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_INC_NIE_NET_REL_LMI_OBC_LIFETIME_MMBTU','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_INC_NIE_NET_REL_LMI_OBC_LIFETIME_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',42,'Residential LMI Only','Target Segments - Net Realized Source Savings','LMI Annual Energy Savings (Net Source MMBtu)','Residential LMI Only','Target Segments - Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_INC_NIE_NET_REL_LMI_ANNUAL_MMBTU','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_INC_NIE_NET_REL_LMI_ANNUAL_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',43,'Residential LMI Only','Target Segments - Net Realized Source Savings','LMI Lifetime Energy Savings (Net Source MMBtu)','Residential LMI Only','Target Segments - Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_INC_NIE_NET_REL_LMI_LIFETIME_MMBTU','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_INC_NIE_NET_REL_LMI_LIFETIME_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',44,'OBC Only (All Projects)','Target Segments - Net Realized Source Savings','OBC Annual Energy Savings (Net Source MMBtu)','OBC Only (All Projects)','Target Segments - Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_INC_NIE_NET_REL_OBC_ANNUAL_MMBTU','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_INC_NIE_NET_REL_OBC_ANNUAL_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',45,'OBC Only (All Projects)','Target Segments - Net Realized Source Savings','OBC Lifetime Energy Savings (Net Source MMBtu)','OBC Only (All Projects)','Target Segments - Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_INC_NIE_NET_REL_OBC_LIFETIME_MMBTU','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_INC_NIE_NET_REL_OBC_LIFETIME_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',46,'Small Business','Target Segments - Net Realized Source Savings','Small Business Annual Energy Savings (Net Source MMBtu)','Small Business','Target Segments - Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_INC_NIE_NET_REL_SMB_ANNUAL_MMBTU','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_INC_NIE_NET_REL_SMB_ANNUAL_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',47,'Small Business','Target Segments - Net Realized Source Savings','Small Business Lifetime Energy Savings (Net Source MMBtu)','Small Business','Target Segments - Net Realized Source Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_INC_NIE_NET_REL_SMB_LIFETIME_MMBTU','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_INC_NIE_NET_REL_SMB_LIFETIME_MMBTU','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',48,'Residential LMI or OBC','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Annual Electric Savings - kWh','Residential LMI or OBC','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_OBC_ELEC_ANNUAL_KWH','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_OBC_ELEC_ANNUAL_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',49,'Residential LMI or OBC','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Lifetime Electric Savings - kWh','Residential LMI or OBC','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_OBC_ELEC_LIFETIME_KWH','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_OBC_ELEC_LIFETIME_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',50,'Residential LMI or OBC','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Annual Gas Savings - Therms','Residential LMI or OBC','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_OBC_GAS_ANNUAL_THERMS','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_OBC_GAS_ANNUAL_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',51,'Residential LMI or OBC','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Lifetime Gas Savings - Therms','Residential LMI or OBC','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_OBC_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_OBC_GAS_LIFETIME_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',52,'Residential LMI Only','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Annual Electric Savings - kWh','Residential LMI Only','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_ELEC_ANNUAL_KWH','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_ELEC_ANNUAL_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',53,'Residential LMI Only','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Lifetime Electric Savings - kWh','Residential LMI Only','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_ELEC_LIFETIME_KWH','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_ELEC_LIFETIME_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',54,'Residential LMI Only','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Annual Gas Savings - Therms','Residential LMI Only','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_GAS_ANNUAL_THERMS','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_GAS_ANNUAL_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',55,'Residential LMI Only','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Lifetime Gas Savings - Therms','Residential LMI Only','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_RES_LMI_GAS_LIFETIME_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',56,'OBC Only (All Projects)','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Annual Electric Savings - kWh','OBC Only (All Projects)','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_OBC_ELEC_ANNUAL_KWH','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_OBC_ELEC_ANNUAL_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',57,'OBC Only (All Projects)','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Lifetime Electric Savings - kWh','OBC Only (All Projects)','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_OBC_ELEC_LIFETIME_KWH','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_OBC_ELEC_LIFETIME_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',58,'OBC Only (All Projects)','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Annual Gas Savings - Therms','OBC Only (All Projects)','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_OBC_GAS_ANNUAL_THERMS','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_OBC_GAS_ANNUAL_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',59,'OBC Only (All Projects)','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Lifetime Gas Savings - Therms','OBC Only (All Projects)','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_OBC_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_OBC_GAS_LIFETIME_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',60,'Small Business','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Annual Electric Savings - kWh','Small Business','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_SMB_ELEC_ANNUAL_KWH','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_SMB_ELEC_ANNUAL_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',61,'Small Business','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Lifetime Electric Savings - kWh','Small Business','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_SMB_ELEC_LIFETIME_KWH','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_SMB_ELEC_LIFETIME_KWH','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',62,'Small Business','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Annual Gas Savings - Therms','Small Business','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_SMB_GAS_ANNUAL_THERMS','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_SMB_GAS_ANNUAL_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL UNION ALL
SELECT 'Savings Results',63,'Small Business','Target Segments - Net Realized Site Savings (Exclude Negative IEs)','Net Realized Site Lifetime Gas Savings - Therms','Small Business','Target Segments - Net Realized Site Savings',75,'CES_SAVINGS_STG_ACTUALS','TGT_SEG_EXC_NIE_SMB_GAS_LIFETIME_THERMS','CES_SAVINGS_FACT_ACTUALS','TGT_SEG_EXC_NIE_SMB_GAS_LIFETIME_THERMS','NUMBER_4','N','N','N','Y' FROM DUAL;

-- NOTE: Multifamily removed from Target Segments Source and
-- Target Segments Site sections per stakeholder decision.
-- Multifamily now exists ONLY as PARTICIPANTS_MFLY (col 17).
-- Total Savings Results columns: 63 (was 69 with Multifamily)

-- NOTE: Multifamily Target Segments Site (TGT_SEG_EXC_NIE_MFLY_*)
-- columns 66-69 were not present in the latest FACT_FORECAST
-- screenshot (table ended at SMB_GAS_LIFETIME_THERMS, row 73,
-- then jumped to system cols). Confirm whether these 4 columns
-- should still exist before adding them back. See note at bottom.

COMMIT;

-- ============================================================
-- SAVINGS FORECAST (71 cols: 69 + Forecast Period + Confidence Level)
-- Same as Results but col 7-8 = Forecast Period + Confidence Level
-- All metric columns shift +1 from col 8 onward (only +1, not +2,
-- since Forecast Period replaces Reporting Period 1-for-1 and
-- Confidence Level is the only true addition)
-- ============================================================
INSERT INTO CES_SAVINGS_COLUMN_MAP
(SHEET_NAME, EXCEL_COL_POSITION, EXCEL_ROW1_HEADER, EXCEL_ROW2_HEADER, EXCEL_ROW3_HEADER,
 ROW1_EXPECTED, ROW2_EXPECTED, FUZZY_THRESHOLD,
 STG_TABLE_NAME, STG_COLUMN_NAME, FACT_TABLE_NAME, FACT_COLUMN_NAME,
 DATA_TYPE, IS_DIMENSION, IS_REQUIRED, IS_STOP_COLUMN, IS_ACTIVE)
SELECT 'Savings Forecast', EXCEL_COL_POSITION + CASE WHEN EXCEL_COL_POSITION >= 8 THEN 1 ELSE 0 END,
       EXCEL_ROW1_HEADER, EXCEL_ROW2_HEADER, EXCEL_ROW3_HEADER,
       ROW1_EXPECTED, ROW2_EXPECTED, FUZZY_THRESHOLD,
       'CES_SAVINGS_STG_FORECAST',
       CASE WHEN STG_COLUMN_NAME = 'REPORTING_PERIOD' THEN 'FORECAST_PERIOD' ELSE STG_COLUMN_NAME END,
       'CES_SAVINGS_FACT_FORECAST',
       CASE WHEN FACT_COLUMN_NAME = 'REPORTING_PERIOD' THEN 'FORECAST_PERIOD' ELSE FACT_COLUMN_NAME END,
       DATA_TYPE, IS_DIMENSION,
       CASE WHEN STG_COLUMN_NAME = 'REPORTING_PERIOD' THEN 'Y' ELSE IS_REQUIRED END,
       IS_STOP_COLUMN, IS_ACTIVE
FROM CES_SAVINGS_COLUMN_MAP
WHERE SHEET_NAME = 'Savings Results';

-- Insert Confidence Level as its own row at position 8
INSERT INTO CES_SAVINGS_COLUMN_MAP
(SHEET_NAME, EXCEL_COL_POSITION, EXCEL_ROW1_HEADER, EXCEL_ROW2_HEADER, EXCEL_ROW3_HEADER,
 ROW1_EXPECTED, ROW2_EXPECTED, FUZZY_THRESHOLD,
 STG_TABLE_NAME, STG_COLUMN_NAME, FACT_TABLE_NAME, FACT_COLUMN_NAME,
 DATA_TYPE, IS_DIMENSION, IS_REQUIRED, IS_STOP_COLUMN, IS_ACTIVE)
VALUES
('Savings Forecast', 8, '', '', 'Confidence Level', '', '', 75,
 'CES_SAVINGS_STG_FORECAST', 'CONFIDENCE_LEVEL',
 'CES_SAVINGS_FACT_FORECAST', 'CONFIDENCE_LEVEL',
 'VARCHAR2', 'Y', 'N', 'N', 'Y');

COMMIT;

-- ============================================================
-- VERIFY
-- ============================================================
SELECT SHEET_NAME, COUNT(*) ROWS
FROM   CES_SAVINGS_COLUMN_MAP
GROUP BY SHEET_NAME ORDER BY SHEET_NAME;

SELECT EXCEL_COL_POSITION, STG_COLUMN_NAME
FROM   CES_SAVINGS_COLUMN_MAP
WHERE  SHEET_NAME = 'Savings Forecast'
ORDER BY EXCEL_COL_POSITION;
