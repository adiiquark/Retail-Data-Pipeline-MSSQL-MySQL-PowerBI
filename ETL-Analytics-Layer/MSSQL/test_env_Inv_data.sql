

create database test_inv
use test_inv

-- count number of records in dbo.products_table
select count(*) as Total_records from [dbo].[products_table];
--100 records

-- count number of records in [dbo].[inventory_test_with_issues]
select count(*) as Total_Records from [dbo].[inventory_test_with_issues];
--5100 records


-- view the data
select * from [dbo].[products_table];

select * from [dbo].[inventory_test_with_issues];

-- check information schema of the tables
select * from INFORMATION_SCHEMA.columns where TABLE_NAME = 'products_table';

select * from INFORMATION_SCHEMA.columns where TABLE_NAME = 'inventory_test_with_issues';


---check for null values, missing values and zeroes in products table
select *
from [dbo].[products_table]
where 
    ProductID IS NULL OR ProductID = 0 --tinyint
 OR ProductName IS NULL OR LTRIM(RTRIM(ProductName)) = '' --nvarchar 50
 OR Category IS NULL OR LTRIM(RTRIM(Category)) = '' 
 OR Supplier IS NULL OR LTRIM(RTRIM(Supplier)) = ''
 OR CostPrice IS NULL OR CostPrice=0
 OR UnitPrice IS NULL OR UnitPrice = 0 --tinyint
 OR WarehouseLocation IS NULL or LTRIM(RTRIM(WarehouseLocation))='';

--Checking the data Quality in the data and performing relavant operations
 --check for null values, missing values and zeroes in [inventory_test_with_issues]

select 
    SUM(case when SnapshotInvID IS NULL OR SnapshotInvID = 0 THEN 1 ELSE 0 END) as Bad_SnapshotInvID,
    SUM(case when ProductID IS NULL OR ProductID = 0 THEN 1 ELSE 0 END) as Bad_ProductID,
    SUM(case when ProductName IS NULL OR LTRIM(RTRIM(ProductName)) = '' THEN 1 ELSE 0 END) as Bad_ProductName,
    SUM(case when WarehouseLocation IS NULL OR LTRIM(RTRIM(WarehouseLocation)) = '' THEN 1 ELSE 0 END) as Bad_WarehouseLocation,
    SUM(case when unitsAvailable IS NULL  THEN 1 ELSE 0 END) as Bad_Availability,
    SUM(case when unitsDemanded IS NULL THEN 1 ELSE 0 END) as Bad_Demand,
    SUM(case when SnapshotDate IS NULL THEN 1 ELSE 0 END) as Bad_SnapshotDate
from [dbo].[inventory_test_with_issues];

-- 264 bad_Availability and 269 Bad_Demand


-- imputing nulls with avg values as per the respective columns
-- Replace NULL Demand/Availability with avg values per ProductID
update i
set unitsDemanded = COALESCE(unitsDemanded, sub.avg_demand),
    unitsAvailable = COALESCE(unitsAvailable, sub.avg_availability)
from [dbo].[inventory_test_with_issues] i
join (
    select ProductID,
           avg(unitsDemanded) as avg_demand,
           avg(unitsAvailable) as avg_availability
    from [dbo].[inventory_test_with_issues]
   where unitsDemanded IS NOT NULL and unitsAvailable IS NOT NULL
    group by ProductID
) sub
on i.ProductID = sub.ProductID;


-- correction validation
select 
    count(*) as NullCount, 
    'unitsDemanded' as ColumnName
from [dbo].[inventory_test_with_issues]
where unitsDemanded IS NULL
union ALL
select 
    count(*), 
    'unitsAvailable'
from [dbo].[inventory_test_with_issues]
where unitsAvailable IS NULL
union all
select 
    count(*),
    'SnapshotDate'
from [dbo].[inventory_test_with_issues]
where SnapshotDate IS NULL;


--- DUPLICATES HANDLING

-- 1. DUPLICATES DETECTION

with NumberedRecords as (
    select
        SnapshotInvID,
        ProductID,
        ProductName,
        WarehouseLocation,
        unitsAvailable,
        unitsDemanded,
        SnapshotDate,
        -- Partition by the business key and order by the ID to number them
        ROW_NUMBER() OVER(PARTITION BY ProductID, WarehouseLocation, SnapshotDate ORDER BY SnapshotInvID) as rn
    from
        [dbo].[inventory_test_with_issues] 
)
-- Select only the rows that are duplicates (rn > 1)
select 
    SnapshotInvID,
    ProductID,
    WarehouseLocation,
    SnapshotDate,
    'This is a duplicate row' as Status
from 
    NumberedRecords
where 
    rn > 1;

--- 100 duplicates found



-- 2. DEDUPLICATION

-- Use a CTE to number the rows within each duplicate group
with NumberedRecords as (
    select
        *,
        -- Partition by the business key. Rows with the same key get numbers 1, 2, 3...
        ROW_NUMBER() OVER(PARTITION BY ProductID, WarehouseLocation, SnapshotDate ORDER BY SnapshotInvID) as rn
    from
        [dbo].[inventory_test_with_issues]
)
-- Select only the first row (rn = 1) from each group into a new table
select 
    SnapshotInvID,
    ProductID,
    ProductName,
    WarehouseLocation,
    unitsAvailable,
    unitsDemanded,
    SnapshotDate
into 
    [dbo].[inventory_test_with_issues_deduplicated] -- This will be our new, clean table
from 
    NumberedRecords
where 
    rn = 1; -- Keep only the first occurrence of each duplicate set

-- Print a message to confirm completion
print 'Deduplication complete. Clean data created in dbo.FactInventory_NoDuplicates.';



-- 3. DEDUPLICATION VALIDATION
SELECT 
    ProductID, 
    WarehouseLocation, 
    SnapshotDate,
    COUNT(*) AS DuplicateCount
FROM 
    [dbo].[inventory_test_with_issues_deduplicated]
GROUP BY 
    ProductID, 
    WarehouseLocation, 
    SnapshotDate
HAVING 
    COUNT(*) > 1;
--- no duplicates found in the deduplicated data



--- OUTLIER HANDLING

-- 1. Profile the data to determine a data-driven capping threshold
-- Using the 99th percentile as the threshold
select 
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY unitsDemanded) OVER() as DemandCap,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY unitsAvailable) OVER() as AvailabilityCap
into #Thresholds -- Store the calculated caps in a temp table
from dbo.inventory_test_with_issues_deduplicated;

-- Get the values from the temp table
DECLARE @DemandCap DECIMAL(10, 2);
DECLARE @AvailabilityCap DECIMAL(10, 2);
SELECT @DemandCap = DemandCap, @AvailabilityCap = AvailabilityCap FROM #Thresholds;

print 'Capping Demand at: ' + CAST(@DemandCap AS VARCHAR(20));
print 'Capping Availability at: ' + CAST(@AvailabilityCap AS VARCHAR(20));


-- 2. Apply the capping to BOTH columns
update dbo.inventory_test_with_issues_deduplicated
set 
    unitsDemanded = @DemandCap
where 
    unitsDemanded > @DemandCap;

update dbo.inventory_test_with_issues_deduplicated
set 
    unitsAvailable = @AvailabilityCap
where 
    unitsAvailable > @AvailabilityCap;


-- 3. CORRECT VALIDATION - Re-profile the data to confirm the capping worked
select 
    'unitsDemanded After Capping' as Metric,
    MIN(unitsDemanded) as MinValue,
    MAX(unitsDemanded) as MaxValue,  -- This should now equal @DemandCap
    AVG(unitsDemanded) as AvgValue
from 
    dbo.inventory_test_with_issues_deduplicated
UNION ALL
select 
    'unitsAvailable After Capping' as Metric,
    MIN(unitsAvailable) as MinValue,
    MAX(unitsAvailable) as MaxValue,  -- This should now equal @AvailabilityCap
    AVG(unitsAvailable) as AvgValue
from 
    dbo.inventory_test_with_issues_deduplicated;

-- Clean up the temp table
drop table #Thresholds;


--- CHECKING BUSINESS LOGIC

-- STEP 1: Enforce Business Logic on the Deduplicated Data

-- We start with the table that has duplicates removed: [dbo].[FactInventory_NoDuplicates]
-- The goal is to create a new table, [dbo].[FactInventory_Clean], where all business rules are satisfied.

-- First, let's validate the state of the data BEFORE we fix it.
-- This helps us quantify the issues and proves our cleaning process has value.
print '--- Pre-Cleaning Business Logic Validation ---';
select 
    'Availability > Demand' as IssueType,
    COUNT(*) as IssueCount
from dbo.inventory_test_with_issues_deduplicated
WHERE unitsAvailable > unitsDemanded

UNION ALL

select 
    'Negative Availability' as IssueType,
    COUNT(*) as IssueCount
from dbo.inventory_test_with_issues_deduplicated
where unitsAvailable < 0;

-- 1.2. Now, create the new clean table, applying the business rules during the SELECT.
-- Rule 1: Availability cannot be negative. If it is, set it to 0.
-- Rule 2: Availability cannot be greater than Demand. If it is, cap it at the Demand level.
SELECT 
    SnapshotInvID,
    ProductID,
    ProductName,
    WarehouseLocation,
    SnapshotDate,
    unitsDemanded,
    -- Apply business logic to the 'unitsAvailable' column
    CASE 
        WHEN unitsAvailable < 0 THEN 0 -- Rule 1: Fix negative values
        WHEN unitsAvailable > unitsDemanded THEN unitsDemanded -- Rule 2: Cap at demand
        ELSE unitsAvailable -- Otherwise, keep the original value
    END AS unitsAvailable
INTO 
    [dbo].[test_inv_Clean]
FROM 
    [dbo].[inventory_test_with_issues_deduplicated];

PRINT 'Business logic applied. New table [dbo].[test_inv_Clean] created.';


-- FINAL VALIDATION: Prove that the new table is clean and has no business logic violations.
-- This query should return (0, 0) if our logic was applied correctly.
PRINT '--- Post-Cleaning Business Logic Validation ---';
SELECT 
    'Availability > Demand' AS IssueType,
    COUNT(*) AS IssueCount
FROM [dbo].[test_inv_Clean]
WHERE unitsAvailable > unitsDemanded

UNION ALL

SELECT 
    'Negative Availability' AS IssueType,
    COUNT(*) AS IssueCount
FROM [dbo].[test_inv_Clean]
WHERE unitsAvailable < 0;

PRINT 'Business Logic Step Complete.';



--- Build Fact and dimension table for star schema dashboarding

-- 1. Create the DimProduct table
Drop table if exists DimProduct;
GO
select 
    IDENTITY(INT, 1, 1) AS ProductKey,
    ProductID,
    ProductName,
    Category,
    Supplier,
    UnitPrice,
    CostPrice 

into 
    DimProduct
from 
    (SELECT DISTINCT ProductID, ProductName, Category, Supplier, UnitPrice, CostPrice from [dbo].[Products_table]) as UniqueProducts;

-- 2. Create the DimWarehouse table
select 
    IDENTITY(INT, 1, 1) as WarehouseKey,
    WarehouseLocation
into 
    DimWarehouse
from 
    (SELECT DISTINCT WarehouseLocation from [dbo].[test_inv_Clean]) as UniqueWarehouses;

-- 3. Create the DimDate table (a standard utility table)
-- Use a Tally Table to generate dates without recursion.

WITH Tally AS (
    SELECT TOP 1100 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS N
    FROM sys.objects a CROSS JOIN sys.objects b -- Cross join to get enough rows
)
-- Now, use the Tally table to generate the date series
SELECT 
    CAST(FORMAT(DATEADD(day, N, '2022-01-01'), 'yyyyMMdd') AS INT) AS DateKey,
    DATEADD(day, N, '2022-01-01') AS FullDate,
    YEAR(DATEADD(day, N, '2022-01-01')) AS Year,
    MONTH(DATEADD(day, N, '2022-01-01')) AS Month,
    DAY(DATEADD(day, N, '2022-01-01')) AS DayOfMonth,
    DATENAME(month, DATEADD(day, N, '2022-01-01')) AS MonthName,
    DATENAME(weekday, DATEADD(day, N, '2022-01-01')) AS DayOfWeekName
INTO 
    DimDate
FROM 
    Tally
WHERE 
    DATEADD(day, N, '2022-01-01') <= '2024-12-31';

-- 4. Create the central FactInventory table by joining the NOW-CLEANED data to our new dimensions
select 
    d.DateKey,
    p.ProductKey,
    w.WarehouseKey,
    c.unitsDemanded,
    c.unitsAvailable -- This value is now already cleaned!
into 
    FactInventory
from 
    [dbo].[test_inv_Clean] c -- Using our cleaned source table
INNER JOIN 
    DimDate d ON c.SnapshotDate = d.FullDate
INNER JOIN 
    DimProduct p ON c.ProductID = p.ProductID
INNER JOIN 
    DimWarehouse w ON c.WarehouseLocation = w.WarehouseLocation;

print 'Star Schema (FactInventory, DimProduct, DimWarehouse, DimDate) created successfully from the cleaned source data.';