
create database [Test Inventory]

use [Test Inventory]



GO


-- =============================================
-- SCHEMA PREPARATION
-- =============================================

-- Rename column if needed
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.inventory_test_with_issues') AND name = 'InvID')
BEGIN
    EXEC sp_rename 'inventory_test_with_issues.InvID', 'OrderID', 'COLUMN';
END
GO

-- Create analytics schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'analytics')
BEGIN
    EXEC('CREATE SCHEMA analytics');
END
GO

-- =============================================
-- DATA QUALITY ASSESSMENT
-- =============================================

-- Check for data quality issues in products table
SELECT 
    SUM(CASE WHEN ProductID IS NULL OR ProductID = 0 THEN 1 ELSE 0 END) AS Bad_ProductID,
    SUM(CASE WHEN ProductName IS NULL OR LTRIM(RTRIM(ProductName)) = '' THEN 1 ELSE 0 END) AS Bad_ProductName,
    SUM(CASE WHEN Category IS NULL OR LTRIM(RTRIM(Category)) = '' THEN 1 ELSE 0 END) AS Bad_Category,
    SUM(CASE WHEN Supplier IS NULL OR LTRIM(RTRIM(Supplier)) = '' THEN 1 ELSE 0 END) AS Bad_Supplier,
    SUM(CASE WHEN CostPrice IS NULL OR CostPrice = 0 THEN 1 ELSE 0 END) AS Bad_CostPrice,
    SUM(CASE WHEN UnitPrice IS NULL OR UnitPrice = 0 THEN 1 ELSE 0 END) AS Bad_UnitPrice,
    SUM(CASE WHEN WarehouseLocation IS NULL OR LTRIM(RTRIM(WarehouseLocation)) = '' THEN 1 ELSE 0 END) AS Bad_WarehouseLocation
FROM dbo.products_table;

-- Check for data quality issues in inventory table
SELECT 
    SUM(CASE WHEN OrderID IS NULL OR OrderID = 0 THEN 1 ELSE 0 END) AS Bad_OrderID,
    SUM(CASE WHEN ProductID IS NULL OR ProductID = 0 THEN 1 ELSE 0 END) AS Bad_ProductID,
    SUM(CASE WHEN ProductName IS NULL OR LTRIM(RTRIM(ProductName)) = '' THEN 1 ELSE 0 END) AS Bad_ProductName,
    SUM(CASE WHEN WarehouseLocation IS NULL OR LTRIM(RTRIM(WarehouseLocation)) = '' THEN 1 ELSE 0 END) AS Bad_WarehouseLocation,
    SUM(CASE WHEN Availability IS NULL THEN 1 ELSE 0 END) AS Bad_Availability,
    SUM(CASE WHEN Demand IS NULL THEN 1 ELSE 0 END) AS Bad_Demand,
    SUM(CASE WHEN OrderDate IS NULL THEN 1 ELSE 0 END) AS Bad_OrderDate
FROM dbo.inventory_test_with_issues;
GO

-- =============================================
-- HIERARCHICAL NULL IMPUTATION
-- =============================================

-- Calculate product-specific averages
WITH ProductAvgs AS (
    SELECT 
        ProductID,
        AVG(CAST(Demand AS FLOAT)) AS avg_demand,
        AVG(CAST(Availability AS FLOAT)) AS avg_availability
    FROM dbo.inventory_test_with_issues
    WHERE Demand IS NOT NULL OR Availability IS NOT NULL
    GROUP BY ProductID
),
-- Calculate global averages as fallback
GlobalAvgs AS (
    SELECT 
        AVG(CAST(Demand AS FLOAT)) AS global_avg_demand,
        AVG(CAST(Availability AS FLOAT)) AS global_avg_availability
    FROM dbo.inventory_test_with_issues
    WHERE Demand IS NOT NULL OR Availability IS NOT NULL
)
-- Apply hierarchical imputation
UPDATE i
SET 
    Demand = COALESCE(
        i.Demand, 
        p.avg_demand, 
        g.global_avg_demand,
        0  -- Final fallback
    ),
    Availability = COALESCE(
        i.Availability, 
        p.avg_availability, 
        g.global_avg_availability,
        0  -- Final fallback
    )
FROM dbo.inventory_test_with_issues i
LEFT JOIN ProductAvgs p ON i.ProductID = p.ProductID
CROSS JOIN GlobalAvgs g;
GO

-- =============================================
-- DEDUPLICATION
-- =============================================

-- Remove duplicates while prioritizing complete records
WITH RankedData AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY OrderID
            ORDER BY 
                CASE WHEN Availability IS NOT NULL THEN 0 ELSE 1 END,
                CASE WHEN Demand IS NOT NULL THEN 0 ELSE 1 END,
                OrderDate DESC  -- Keep most recent record
        ) AS rn
    FROM dbo.inventory_test_with_issues
)
DELETE FROM RankedData WHERE rn > 1;
GO

-- =============================================
-- STATISTICAL OUTLIER HANDLING
-- =============================================

-- Calculate IQR-based bounds for outlier detection
WITH Stats AS (
    SELECT DISTINCT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Demand) OVER() AS Q1_Demand,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Demand) OVER() AS Q3_Demand,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Availability) OVER() AS Q1_Avail,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Availability) OVER() AS Q3_Avail
    FROM dbo.inventory_test_with_issues
),
Bounds AS (
    SELECT 
        Q1_Demand,
        Q3_Demand,
        Q3_Demand + 1.5 * (Q3_Demand - Q1_Demand) AS Upper_Demand,
        Q1_Avail,
        Q3_Avail,
        Q3_Avail + 1.5 * (Q3_Avail - Q1_Avail) AS Upper_Avail
    FROM Stats
)
-- Apply capping
UPDATE i
SET 
    Demand = CASE 
        WHEN i.Demand > b.Upper_Demand THEN b.Upper_Demand
        ELSE i.Demand
    END,
    Availability = CASE 
        WHEN i.Availability > b.Upper_Avail THEN b.Upper_Avail
        ELSE i.Availability
    END
FROM dbo.inventory_test_with_issues i
CROSS JOIN Bounds b;
GO

-- =============================================
-- BUSINESS LOGIC VALIDATION
-- =============================================

-- Ensure Availability doesn't exceed Demand
UPDATE dbo.inventory_test_with_issues
SET Availability = Demand
WHERE Availability > Demand;

-- Remove records with invalid dates
DELETE FROM dbo.inventory_test_with_issues
WHERE OrderDate < '2022-01-01' OR OrderDate > GETDATE();

-- Validate referential integrity
SELECT i.ProductID
FROM dbo.inventory_test_with_issues i
LEFT JOIN dbo.products_table p ON i.ProductID = p.ProductID
WHERE p.ProductID IS NULL;
GO

-- =============================================
-- ANALYTICS LAYER CREATION
-- =============================================

-- Create product dimension table with corrected stock status logic
IF OBJECT_ID('analytics.DimProduct', 'U') IS NOT NULL
    DROP TABLE analytics.DimProduct;
GO

-- Recreate with corrected logic
SELECT 
    p.ProductID,
    p.ProductName,
    p.Category,
    p.Supplier,
    p.CostPrice,
    p.UnitPrice,
    COUNT(i.OrderID) AS TotalOrders,
    SUM(i.Demand) AS TotalDemand,
    SUM(i.Availability) AS TotalAvailability,
    AVG(i.Demand) AS AvgDemand,
    AVG(i.Availability) AS AvgAvailability,
    SUM(i.Demand * p.UnitPrice) AS PotentialRevenue,
    SUM(i.Availability * p.CostPrice) AS InventoryValue,
    -- Calculate availability ratio
    SUM(i.Availability) * 100.0 / NULLIF(SUM(i.Demand), 1) AS AvailabilityRatio,
    -- Create HasStock using a more explicit approach
    CASE 
        WHEN SUM(i.Demand) = 0 THEN 1  -- No demand, consider as having stock
        WHEN SUM(i.Availability) = 0 THEN 0  -- No availability, out of stock
        WHEN (SUM(i.Availability) * 100.0 / SUM(i.Demand)) > 30 THEN 1  -- More than 30% ratio
        ELSE 0  -- 30% or less ratio
    END AS HasStock,
    -- Understocked flag
    CASE 
        WHEN SUM(i.Demand) = 0 THEN 0  -- No demand, not understocked
        WHEN (SUM(i.Availability) * 100.0 / SUM(i.Demand)) < 30 THEN 1  -- Less than 30% ratio
        ELSE 0  -- 30% or more ratio
    END AS Understocked,
    MIN(i.OrderDate) AS FirstOrderDate,
    MAX(i.OrderDate) AS LastOrderDate
INTO analytics.DimProduct
FROM dbo.products_table p
LEFT JOIN dbo.inventory_test_with_issues i 
    ON p.ProductID = i.ProductID
GROUP BY 
    p.ProductID, p.ProductName, p.Category, 
    p.Supplier, p.CostPrice, p.UnitPrice;
GO
------------------------------------------------------------------------
-- Check HasStock distribution
SELECT 
    HasStock,
    COUNT(*) AS ProductCount,
    MIN(AvailabilityRatio) AS MinRatio,
    MAX(AvailabilityRatio) AS MaxRatio,
    AVG(AvailabilityRatio) AS AvgRatio
FROM analytics.DimProduct
GROUP BY HasStock;

-- Check the distribution by AvailabilityRatio
SELECT 
    CASE 
        WHEN AvailabilityRatio = 0 THEN 'Out of Stock'
        WHEN AvailabilityRatio < 10 THEN 'Critical'
        WHEN AvailabilityRatio < 30 THEN 'Low'
        WHEN AvailabilityRatio < 70 THEN 'Adequate'
        ELSE 'Overstocked'
    END AS StockCategory,
    COUNT(*) AS ProductCount,
    MIN(AvailabilityRatio) AS MinRatio,
    MAX(AvailabilityRatio) AS MaxRatio,
    AVG(AvailabilityRatio) AS AvgRatio
FROM analytics.DimProduct
GROUP BY 
    CASE 
        WHEN AvailabilityRatio = 0 THEN 'Out of Stock'
        WHEN AvailabilityRatio < 10 THEN 'Critical'
        WHEN AvailabilityRatio < 30 THEN 'Low'
        WHEN AvailabilityRatio < 70 THEN 'Adequate'
        ELSE 'Overstocked'
    END;

-- Show sample data with HasStock
SELECT TOP 10 
    ProductID,
    ProductName,
    TotalAvailability,
    TotalDemand,
    AvailabilityRatio,
    HasStock,
    Understocked
FROM analytics.DimProduct
ORDER BY AvailabilityRatio;
------------------------------------------------------------------------




-- Create time-based aggregation table
IF OBJECT_ID('analytics.DailyInventory', 'U') IS NOT NULL
    DROP TABLE analytics.DailyInventory;
GO

SELECT 
    CAST(OrderDate AS DATE) AS InventoryDate,
    ProductID,
    SUM(Demand) AS DailyDemand,
    SUM(Availability) AS DailyAvailability,
    AVG(Demand) AS AvgDailyDemand,
    AVG(Availability) AS AvgDailyAvailability,
    COUNT(OrderID) AS DailyOrderCount
INTO analytics.DailyInventory
FROM dbo.inventory_test_with_issues
GROUP BY CAST(OrderDate AS DATE), ProductID;
GO

-- Create warehouse-level aggregation table with ProductID for relationships
IF OBJECT_ID('analytics.WarehouseMetrics', 'U') IS NOT NULL
    DROP TABLE analytics.WarehouseMetrics;
GO

SELECT 
    WarehouseLocation,
    ProductID,
    SUM(Demand) AS TotalDemand,
    SUM(Availability) AS TotalAvailability,
    AVG(Demand) AS AvgDemandPerProduct,
    AVG(Availability) AS AvgAvailabilityPerProduct,
    COUNT(OrderID) AS TotalOrders,
    MIN(OrderDate) AS EarliestOrder,
    MAX(OrderDate) AS LatestOrder
INTO analytics.WarehouseMetrics
FROM dbo.inventory_test_with_issues
GROUP BY WarehouseLocation, ProductID;
GO

-- =============================================
-- FINAL VALIDATION
-- =============================================

-- Check for remaining nulls in key columns
SELECT 
    COUNT(*) AS NullCount, 
    'Demand' AS ColumnName
FROM dbo.inventory_test_with_issues
WHERE Demand IS NULL
UNION ALL
SELECT 
    COUNT(*), 
    'Availability'
FROM dbo.inventory_test_with_issues
WHERE Availability IS NULL
UNION ALL
SELECT 
    COUNT(*),
    'OrderDate'
FROM dbo.inventory_test_with_issues
WHERE OrderDate IS NULL;

-- Verify no duplicates remain
SELECT OrderID, COUNT(*) AS cnt
FROM dbo.inventory_test_with_issues
GROUP BY OrderID
HAVING COUNT(*) > 1;

-- Verify business logic compliance
SELECT COUNT(*) AS Violations
FROM dbo.inventory_test_with_issues
WHERE Availability < 0
   OR Availability > Demand
   OR OrderDate < '2022-01-01'
   OR OrderDate > GETDATE();

-- Verify referential integrity
SELECT COUNT(*) AS OrphanedRecords
FROM dbo.inventory_test_with_issues i
LEFT JOIN dbo.products_table p ON i.ProductID = p.ProductID
WHERE p.ProductID IS NULL;

-- Check stock status distribution
SELECT 
    CASE 
        WHEN AvailabilityRatio = 0 THEN 'Out of Stock'
        WHEN AvailabilityRatio < 10 THEN 'Critical'
        WHEN AvailabilityRatio < 30 THEN 'Low'
        WHEN AvailabilityRatio < 70 THEN 'Adequate'
        ELSE 'Overstocked'
    END AS StockStatus,
    COUNT(*) AS ProductCount,
    AVG(PotentialRevenue) AS AvgPotentialRevenue
FROM analytics.DimProduct
GROUP BY 
    CASE 
        WHEN AvailabilityRatio = 0 THEN 'Out of Stock'
        WHEN AvailabilityRatio < 10 THEN 'Critical'
        WHEN AvailabilityRatio < 30 THEN 'Low'
        WHEN AvailabilityRatio < 70 THEN 'Adequate'
        ELSE 'Overstocked'
    END;
GO

-- =============================================
-- PERFORMANCE OPTIMIZATION
-- =============================================

-- Create indexes for analytics tables
CREATE INDEX IX_DimProduct_ProductID ON analytics.DimProduct(ProductID);
CREATE INDEX IX_DimProduct_Category ON analytics.DimProduct(Category);
CREATE INDEX IX_DimProduct_HasStock ON analytics.DimProduct(HasStock);
CREATE INDEX IX_DimProduct_AvailabilityRatio ON analytics.DimProduct(AvailabilityRatio);
CREATE INDEX IX_DailyInventory_DateProduct ON analytics.DailyInventory(InventoryDate, ProductID);
CREATE INDEX IX_WarehouseMetrics_LocationProduct ON analytics.WarehouseMetrics(WarehouseLocation, ProductID);
CREATE INDEX IX_WarehouseMetrics_Location ON analytics.WarehouseMetrics(WarehouseLocation);
GO

-- =============================================
-- DOCUMENTATION
-- =============================================

-- Add table descriptions
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Product dimension with pre-aggregated inventory metrics and stock status indicators',
    @level0type = N'Schema', @level0name = 'analytics',
    @level1type = N'Table',  @level1name = 'DimProduct';
GO

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Daily inventory metrics by product for time series analysis',
    @level0type = N'Schema', @level0name = 'analytics',
    @level1type = N'Table',  @level1name = 'DailyInventory';
GO

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Warehouse-level inventory metrics with product granularity for location-based analysis',
    @level0type = N'Schema', @level0name = 'analytics',
    @level1type = N'Table',  @level1name = 'WarehouseMetrics';
GO

-- =============================================
-- SCRIPT COMPLETE
-- =============================================
PRINT 'Data cleaning and analytics layer creation completed successfully.';
PRINT 'Ready for Power BI connection to analytics schema tables.';
PRINT 'Stock status calculation now uses availability ratio for more accurate classification.';
GO
