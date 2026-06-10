-- ============================================================
-- MAPPING VERIFICATION
-- Compare:
-- 1. Column mapping table
-- 2. STG table columns
-- 3. FACT table columns
-- Run this in Oracle to find any mismatches
-- ============================================================

-- ============================================================
-- CHECK 1: Columns in mapping but NOT in STG table
-- These will silently fail during load
-- ============================================================
SELECT 
    m.SHEET_NAME,
    m.EXCEL_COL_POSITION,
    m.STG_COLUMN_NAME,
    'MISSING FROM STG TABLE' AS ISSUE
FROM CES_SAVINGS_COLUMN_MAP m
WHERE m.SHEET_NAME = 'Savings Results'
AND   m.IS_ACTIVE  = 'Y'
AND   m.STG_COLUMN_NAME NOT IN (
    SELECT COLUMN_NAME 
    FROM   USER_TAB_COLUMNS 
    WHERE  TABLE_NAME = 'CES_SAVINGS_STG_ACTUALS'
)
UNION ALL
SELECT 
    m.SHEET_NAME,
    m.EXCEL_COL_POSITION,
    m.STG_COLUMN_NAME,
    'MISSING FROM STG TABLE' AS ISSUE
FROM CES_SAVINGS_COLUMN_MAP m
WHERE m.SHEET_NAME = 'Savings Forecast'
AND   m.IS_ACTIVE  = 'Y'
AND   m.STG_COLUMN_NAME NOT IN (
    SELECT COLUMN_NAME 
    FROM   USER_TAB_COLUMNS 
    WHERE  TABLE_NAME = 'CES_SAVINGS_STG_FORECAST'
)
ORDER BY 1, 2;

-- ============================================================
-- CHECK 2: Columns in mapping but NOT in FACT table
-- ============================================================
SELECT 
    m.SHEET_NAME,
    m.EXCEL_COL_POSITION,
    m.FACT_COLUMN_NAME,
    'MISSING FROM FACT TABLE' AS ISSUE
FROM CES_SAVINGS_COLUMN_MAP m
WHERE m.SHEET_NAME  = 'Savings Results'
AND   m.IS_ACTIVE   = 'Y'
AND   m.IS_DIMENSION = 'N'
AND   m.FACT_COLUMN_NAME IS NOT NULL
AND   m.FACT_COLUMN_NAME NOT IN (
    SELECT COLUMN_NAME 
    FROM   USER_TAB_COLUMNS 
    WHERE  TABLE_NAME = 'CES_SAVINGS_FACT_ACTUALS'
)
UNION ALL
SELECT 
    m.SHEET_NAME,
    m.EXCEL_COL_POSITION,
    m.FACT_COLUMN_NAME,
    'MISSING FROM FACT TABLE' AS ISSUE
FROM CES_SAVINGS_COLUMN_MAP m
WHERE m.SHEET_NAME  = 'Savings Forecast'
AND   m.IS_ACTIVE   = 'Y'
AND   m.IS_DIMENSION = 'N'
AND   m.FACT_COLUMN_NAME IS NOT NULL
AND   m.FACT_COLUMN_NAME NOT IN (
    SELECT COLUMN_NAME 
    FROM   USER_TAB_COLUMNS 
    WHERE  TABLE_NAME = 'CES_SAVINGS_FACT_FORECAST'
)
ORDER BY 1, 2;

-- ============================================================
-- CHECK 3: STG columns not in mapping
-- These columns exist in table but nothing loads them
-- ============================================================
SELECT 
    'CES_SAVINGS_STG_ACTUALS'  TABLE_NAME,
    c.COLUMN_NAME,
    'NOT IN MAPPING'           ISSUE
FROM USER_TAB_COLUMNS c
WHERE c.TABLE_NAME  = 'CES_SAVINGS_STG_ACTUALS'
AND   c.COLUMN_NAME NOT IN (
    'STG_ID','SOURCE_FILE_NAME','BATCH_ID','LOAD_TIMESTAMP'
)
AND   c.COLUMN_NAME NOT IN (
    SELECT STG_COLUMN_NAME
    FROM   CES_SAVINGS_COLUMN_MAP
    WHERE  SHEET_NAME = 'Savings Results'
    AND    IS_ACTIVE  = 'Y'
)
UNION ALL
SELECT 
    'CES_SAVINGS_STG_FORECAST' TABLE_NAME,
    c.COLUMN_NAME,
    'NOT IN MAPPING'           ISSUE
FROM USER_TAB_COLUMNS c
WHERE c.TABLE_NAME  = 'CES_SAVINGS_STG_FORECAST'
AND   c.COLUMN_NAME NOT IN (
    'STG_ID','SOURCE_FILE_NAME','BATCH_ID','LOAD_TIMESTAMP'
)
AND   c.COLUMN_NAME NOT IN (
    SELECT STG_COLUMN_NAME
    FROM   CES_SAVINGS_COLUMN_MAP
    WHERE  SHEET_NAME = 'Savings Forecast'
    AND    IS_ACTIVE  = 'Y'
)
ORDER BY 1, 2;

-- ============================================================
-- CHECK 4: FACT columns not in mapping
-- ============================================================
SELECT 
    'CES_SAVINGS_FACT_ACTUALS' TABLE_NAME,
    c.COLUMN_NAME,
    'NOT IN MAPPING'           ISSUE
FROM USER_TAB_COLUMNS c
WHERE c.TABLE_NAME  = 'CES_SAVINGS_FACT_ACTUALS'
AND   c.COLUMN_NAME NOT IN (
    'FACT_SK','VENDOR_SUBPROGRAM_SK','PERIOD_SK',
    'VENDOR_SUBPROGRAM_KEY','REPORTING_PERIOD',
    'SOURCE_FILE_NAME','BATCH_ID','LOAD_TIMESTAMP',
    'VERSION_NUMBER','IS_CURRENT_VERSION',
    'DATA_DATE','TRIENNIUM','SECTOR','PROGRAM',
    'SUBPROGRAM','PROGRAM_YEAR',
    'CREATED_DATE','UPDATED_DATE','CREATED_BY','UPDATED_BY'
)
AND   c.COLUMN_NAME NOT IN (
    SELECT FACT_COLUMN_NAME
    FROM   CES_SAVINGS_COLUMN_MAP
    WHERE  SHEET_NAME = 'Savings Results'
    AND    IS_ACTIVE  = 'Y'
    AND    FACT_COLUMN_NAME IS NOT NULL
)
ORDER BY 2;

-- ============================================================
-- CHECK 5: Full side by side view
-- Mapping vs STG vs FACT for Savings Results
-- ============================================================
SELECT
    m.EXCEL_COL_POSITION,
    m.STG_COLUMN_NAME                                       MAP_STG_COL,
    s.COLUMN_NAME                                           ACTUAL_STG_COL,
    CASE WHEN s.COLUMN_NAME IS NOT NULL
         THEN 'OK' ELSE 'MISSING' END                      STG_STATUS,
    m.FACT_COLUMN_NAME                                      MAP_FACT_COL,
    f.COLUMN_NAME                                           ACTUAL_FACT_COL,
    CASE WHEN f.COLUMN_NAME IS NOT NULL OR m.IS_DIMENSION='Y'
         THEN 'OK' ELSE 'MISSING' END                      FACT_STATUS
FROM CES_SAVINGS_COLUMN_MAP m
LEFT JOIN USER_TAB_COLUMNS s
    ON  s.TABLE_NAME  = 'CES_SAVINGS_STG_ACTUALS'
    AND s.COLUMN_NAME = m.STG_COLUMN_NAME
LEFT JOIN USER_TAB_COLUMNS f
    ON  f.TABLE_NAME  = 'CES_SAVINGS_FACT_ACTUALS'
    AND f.COLUMN_NAME = m.FACT_COLUMN_NAME
WHERE m.SHEET_NAME = 'Savings Results'
AND   m.IS_ACTIVE  = 'Y'
ORDER BY m.EXCEL_COL_POSITION;

-- ============================================================
-- CHECK 6: Summary counts
-- ============================================================
SELECT
    'Mapping rows'              ITEM,
    COUNT(*)                    CNT
FROM CES_SAVINGS_COLUMN_MAP
WHERE SHEET_NAME = 'Savings Results'
AND   IS_ACTIVE  = 'Y'
UNION ALL
SELECT 'STG columns', COUNT(*)
FROM USER_TAB_COLUMNS
WHERE TABLE_NAME = 'CES_SAVINGS_STG_ACTUALS'
AND   COLUMN_NAME NOT IN ('STG_ID','SOURCE_FILE_NAME','BATCH_ID','LOAD_TIMESTAMP')
UNION ALL
SELECT 'FACT columns', COUNT(*)
FROM USER_TAB_COLUMNS
WHERE TABLE_NAME = 'CES_SAVINGS_FACT_ACTUALS'
AND   COLUMN_NAME NOT IN (
    'FACT_SK','VENDOR_SUBPROGRAM_SK','PERIOD_SK',
    'SOURCE_FILE_NAME','BATCH_ID','LOAD_TIMESTAMP',
    'VERSION_NUMBER','IS_CURRENT_VERSION','DATA_DATE',
    'CREATED_DATE','UPDATED_DATE','CREATED_BY','UPDATED_BY'
);
