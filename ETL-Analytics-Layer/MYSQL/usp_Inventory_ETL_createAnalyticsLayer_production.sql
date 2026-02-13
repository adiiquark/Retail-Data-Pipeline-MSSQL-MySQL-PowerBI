

-- 1. SETUP AND INITIAL PROFILING

-- Create and use the database
CREATE DATABASE IF NOT EXISTS prod_inv;
USE prod_inv;

-- Count number of records in products_table
SELECT COUNT(*) AS Total_records FROM products_table;
--  100 records

-- Count number of records in inventory_production_with_issues
SELECT COUNT(*) AS Total_Records FROM inventory_production_with_issues;
--  51000 records

-- View the data 
SELECT * FROM products_table LIMIT 10;
SELECT * FROM inventory_production_with_issues LIMIT 10;

-- Check information schema of the tables
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'products_table';
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'inventory_production_with_issues';


-- 2. DATA QUALITY CHECKS

-- Check for null values, missing values and zeroes in products table
SELECT *
FROM products_table
WHERE
    ProductID IS NULL OR ProductID = 0
    OR ProductName IS NULL OR TRIM(ProductName) = ''
    OR Category IS NULL OR TRIM(Category) = ''
    OR Supplier IS NULL OR TRIM(Supplier) = ''
    OR CostPrice IS NULL OR CostPrice = 0
    OR UnitPrice IS NULL OR UnitPrice = 0
    OR WarehouseLocation IS NULL OR TRIM(WarehouseLocation) = '';

-- Checking the data Quality in the inventory table
SELECT
    SUM(CASE WHEN SnapshotInvID IS NULL OR SnapshotInvID = 0 THEN 1 ELSE 0 END) AS Bad_SnapshotInvID,
    SUM(CASE WHEN ProductID IS NULL OR ProductID = 0 THEN 1 ELSE 0 END) AS Bad_ProductID,
    SUM(CASE WHEN ProductName IS NULL OR TRIM(ProductName) = '' THEN 1 ELSE 0 END) AS Bad_ProductName,
    SUM(CASE WHEN WarehouseLocation IS NULL OR TRIM(WarehouseLocation) = '' THEN 1 ELSE 0 END) AS Bad_WarehouseLocation,
    SUM(CASE WHEN unitsAvailable IS NULL THEN 1 ELSE 0 END) AS Bad_Availability,
    SUM(CASE WHEN unitsDemanded IS NULL THEN 1 ELSE 0 END) AS Bad_Demand,
    SUM(CASE WHEN SnapshotDate IS NULL THEN 1 ELSE 0 END) AS Bad_SnapshotDate
FROM inventory_production_with_issues;
-- ~2501 bad_Availability and ~2536 Bad_Demand


-- 3. NULL IMPUTATION
-- =================================================================================

-- Imputing nulls with avg values as per the respective columns
UPDATE inventory_production_with_issues i
JOIN (
    SELECT
        ProductID,
        AVG(unitsDemanded) AS avg_demand,
        AVG(unitsAvailable) AS avg_availability
    FROM inventory_production_with_issues
    WHERE unitsDemanded IS NOT NULL AND unitsAvailable IS NOT NULL
    GROUP BY ProductID
) sub ON i.ProductID = sub.ProductID
SET
    i.unitsDemanded = IF(i.unitsDemanded IS NULL, sub.avg_demand, i.unitsDemanded),
    i.unitsAvailable = IF(i.unitsAvailable IS NULL, sub.avg_availability, i.unitsAvailable);

-- Correction validation
SELECT
    COUNT(*) AS NullCount,
    'unitsDemanded' AS ColumnName
FROM inventory_production_with_issues
WHERE unitsDemanded IS NULL
UNION ALL
SELECT
    COUNT(*),
    'unitsAvailable'
FROM inventory_production_with_issues
WHERE unitsAvailable IS NULL
UNION ALL
SELECT
    COUNT(*),
    'SnapshotDate'
FROM inventory_production_with_issues
WHERE SnapshotDate IS NULL;


-- 4. DUPLICATE HANDLING

-- 4.1. DUPLICATE DETECTION
WITH NumberedRecords AS (
    SELECT
        SnapshotInvID,
        ProductID,
        ProductName,
        WarehouseLocation,
        unitsAvailable,
        unitsDemanded,
        SnapshotDate,
        ROW_NUMBER() OVER(PARTITION BY ProductID, WarehouseLocation, SnapshotDate ORDER BY SnapshotInvID) AS rn
    FROM
        inventory_production_with_issues
)
SELECT
    SnapshotInvID,
    ProductID,
    WarehouseLocation,
    SnapshotDate,
    'This is a duplicate row' AS Status
FROM
    NumberedRecords
WHERE
    rn > 1;
-- 1000 duplicates found


-- 4.2. DEDUPLICATION
CREATE TABLE inventory_production_with_issues_deduplicated AS
WITH NumberedRecords AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY ProductID, WarehouseLocation, SnapshotDate ORDER BY SnapshotInvID) AS rn
    FROM
        inventory_production_with_issues
)
SELECT
    SnapshotInvID,
    ProductID,
    ProductName,
    WarehouseLocation,
    unitsAvailable,
    unitsDemanded,
    SnapshotDate
FROM
    NumberedRecords
WHERE
    rn = 1;

SELECT 'Deduplication complete. Clean table created.' AS 'Status';


-- 4.3. DEDUPLICATION VALIDATION
SELECT
    ProductID,
    WarehouseLocation,
    SnapshotDate,
    COUNT(*) AS DuplicateCount
FROM
    inventory_production_with_issues_deduplicated
GROUP BY
    ProductID,
    WarehouseLocation,
    SnapshotDate
HAVING
    COUNT(*) > 1;
-- no duplicates found


-- 5. OUTLIER HANDLING

-- 5.1. Profile the data to determine a data-driven capping threshold
SET @DemandCap = (SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY unitsDemanded) FROM inventory_production_with_issues_deduplicated);
SET @AvailabilityCap = (SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY unitsAvailable) FROM inventory_production_with_issues_deduplicated);

SELECT CONCAT('Capping Demand at: ', @DemandCap) AS 'Status';
SELECT CONCAT('Capping Availability at: ', @AvailabilityCap) AS 'Status';


-- 5.2. Apply the capping to BOTH columns
UPDATE inventory_production_with_issues_deduplicated
SET unitsDemanded = @DemandCap
WHERE unitsDemanded > @DemandCap;

UPDATE inventory_production_with_issues_deduplicated
SET unitsAvailable = @AvailabilityCap
WHERE unitsAvailable > @AvailabilityCap;


-- 5.3. CORRECT VALIDATION - Re-profile the data to confirm the capping worked
SELECT
    'unitsDemanded After Capping' AS Metric,
    MIN(unitsDemanded) AS MinValue,
    MAX(unitsDemanded) AS MaxValue,
    AVG(unitsDemanded) AS AvgValue
FROM
    inventory_production_with_issues_deduplicated
UNION ALL
SELECT
    'unitsAvailable After Capping' AS Metric,
    MIN(unitsAvailable) AS MinValue,
    MAX(unitsAvailable) AS MaxValue,
    AVG(unitsAvailable) AS AvgValue
FROM
    inventory_production_with_issues_deduplicated;


-- 6. BUSINESS LOGIC ENFORCEMENT

-- 6.1. Pre-cleaning validation
SELECT '--- Pre-Cleaning Business Logic Validation ---' AS 'Status';
SELECT
    'Availability > Demand' AS IssueType,
    COUNT(*) AS IssueCount
FROM inventory_production_with_issues_deduplicated
WHERE unitsAvailable > unitsDemanded
UNION ALL
SELECT
    'Negative Availability' AS IssueType,
    COUNT(*) AS IssueCount
FROM inventory_production_with_issues_deduplicated
WHERE unitsAvailable < 0;

-- 6.2. Create a new clean table, applying the business rules during the SELECT.
CREATE TABLE production_inv_Clean AS
SELECT
    SnapshotInvID,
    ProductID,
    ProductName,
    WarehouseLocation,
    SnapshotDate,
    unitsDemanded,
    CASE
        WHEN unitsAvailable < 0 THEN 0
        WHEN unitsAvailable > unitsDemanded THEN unitsDemanded
        ELSE unitsAvailable
    END AS unitsAvailable
FROM
    inventory_production_with_issues_deduplicated;

SELECT 'Business logic applied. New table production_inv_Clean created.' AS 'Status';


-- 6.3. FINAL VALIDATION
SELECT '--- Post-Cleaning Business Logic Validation ---' AS 'Status';
SELECT
    'Availability > Demand' AS IssueType,
    COUNT(*) AS IssueCount
FROM production_inv_Clean
WHERE unitsAvailable > unitsDemanded
UNION ALL
SELECT
    'Negative Availability' AS IssueType,
    COUNT(*) AS IssueCount
FROM production_inv_Clean
WHERE unitsAvailable < 0;

SELECT 'Business Logic Step Complete.' AS 'Status';


-- 7. BUILD FACT AND DIMENSION TABLES (STAR SCHEMA)

-- 7.1. Create the DimProduct table
CREATE TABLE DimProduct (
    ProductKey INT AUTO_INCREMENT PRIMARY KEY,
    ProductID INT,
    ProductName VARCHAR(255),
    Category VARCHAR(100),
    Supplier VARCHAR(255),
    UnitPrice DECIMAL(10, 2)
);

INSERT INTO DimProduct (ProductID, ProductName, Category, Supplier, UnitPrice)
SELECT DISTINCT ProductID, ProductName, Category, Supplier, UnitPrice FROM Products_table;


-- 7.2. Create the DimWarehouse table
CREATE TABLE DimWarehouse (
    WarehouseKey INT AUTO_INCREMENT PRIMARY KEY,
    WarehouseLocation VARCHAR(100)
);

INSERT INTO DimWarehouse (WarehouseLocation)
SELECT DISTINCT WarehouseLocation FROM production_inv_Clean;


-- 7.3. Create the DimDate table using a Recursive CTE
CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
    FullDate DATE,
    Year INT,
    Month INT,
    DayOfMonth INT,
    MonthName VARCHAR(20),
    DayOfWeekName VARCHAR(20)
);

INSERT INTO DimDate (DateKey, FullDate, Year, Month, DayOfMonth, MonthName, DayOfWeekName)
WITH RECURSIVE DateCTE AS (
    SELECT CAST('2022-01-01' AS DATE) AS FullDate
    UNION ALL
    SELECT DATE_ADD(FullDate, INTERVAL 1 DAY)
    FROM DateCTE
    WHERE FullDate < '2024-12-31'
)
SELECT
    DATE_FORMAT(FullDate, '%Y%m%d') AS DateKey,
    FullDate,
    YEAR(FullDate) AS Year,
    MONTH(FullDate) AS Month,
    DAYOFMONTH(FullDate) AS DayOfMonth,
    MONTHNAME(FullDate) AS MonthName,
    DAYNAME(FullDate) AS DayOfWeekName
FROM DateCTE;


-- 7.4. Create the central FactInventory table
CREATE TABLE FactInventory (
    DateKey INT,
    ProductKey INT,
    WarehouseKey INT,
    unitsDemanded DECIMAL(10, 2),
    unitsAvailable DECIMAL(10, 2),
    PRIMARY KEY (DateKey, ProductKey, WarehouseKey) -- Good practice to define the business key
);

INSERT INTO FactInventory (DateKey, ProductKey, WarehouseKey, unitsDemanded, unitsAvailable)
SELECT
    d.DateKey,
    p.ProductKey,
    w.WarehouseKey,
    c.unitsDemanded,
    c.unitsAvailable
FROM
    production_inv_Clean c
INNER JOIN DimDate d ON c.SnapshotDate = d.FullDate
INNER JOIN DimProduct p ON c.ProductID = p.ProductID
INNER JOIN DimWarehouse w ON c.WarehouseLocation = w.WarehouseLocation;

SELECT 'Star Schema (FactInventory, DimProduct, DimWarehouse, DimDate) created successfully.' AS 'Status';