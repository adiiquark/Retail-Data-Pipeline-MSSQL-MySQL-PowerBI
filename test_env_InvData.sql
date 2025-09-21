
use InventoryDB

-- count number of records in dbo.products_table
select count(*) as Total_records from [dbo].[products_table];
--100 records


-- count number of records in [dbo].[inventory_test_with_issues]
select count(*) as Total_Records from [dbo].[inventory_test_with_issues];
--5100 records


-- view the data
select * from [dbo].[products_table];

select * from [dbo].[inventory_test_with_issues];

--rename InvID to OrderID
exec sp_rename 'inventory_test_with_issues.InvID', 'OrderID', 'COLUMN';

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
    SUM(case when OrderID IS NULL OR OrderID = 0 THEN 1 ELSE 0 END) as Bad_OrderID,
    SUM(case when ProductID IS NULL OR ProductID = 0 THEN 1 ELSE 0 END) as Bad_ProductID,
    SUM(case when ProductName IS NULL OR LTRIM(RTRIM(ProductName)) = '' THEN 1 ELSE 0 END) as Bad_ProductName,
    SUM(case when WarehouseLocation IS NULL OR LTRIM(RTRIM(WarehouseLocation)) = '' THEN 1 ELSE 0 END) as Bad_WarehouseLocation,
    SUM(case when Availability IS NULL  THEN 1 ELSE 0 END) as Bad_Availability,
    SUM(case when Demand IS NULL THEN 1 ELSE 0 END) as Bad_Demand,
    SUM(case when OrderDate IS NULL THEN 1 ELSE 0 END) as Bad_OrderDate
from [dbo].[inventory_test_with_issues];

-- 249 bad availability, 256bad demand

-- imputing nulls with avg values as per the respective columns
-- Replace NULL Demand/Availability with avg values per ProductID
update i
set Demand = COALESCE(Demand, sub.avg_demand),
    Availability = COALESCE(Availability, sub.avg_availability)
from [dbo].[inventory_test_with_issues] i
join (
    select ProductID,
           avg(Demand) as avg_demand,
           avg(Availability) as avg_availability
    from [dbo].[inventory_test_with_issues]
   where Demand IS NOT NULL and Availability IS NOT NULL
    group by ProductID
) sub
on i.ProductID = sub.ProductID;


-- correction validation
select 
    count(*) as NullCount, 
    'Demand' as ColumnName
from [dbo].[inventory_test_with_issues]
where Demand IS NULL
union ALL
select 
    count(*), 
    'Availability'
from [dbo].[inventory_test_with_issues]
where Availability IS NULL
union all
select 
    count(*),
    'OrderDate'
from [dbo].[inventory_test_with_issues]
where OrderDate IS NULL;







-- Find duplicate OrderIDs
select OrderID, count(*) AS cnt
from [dbo].[inventory_test_with_issues]
group by OrderID
having count(*) > 1;

-- deduplication
with ranked as (
    select *,
           ROW_NUMBER() OVER (
               partition by OrderID
               order by(select NULL) -- arbitrary, just need a row to keep
           ) as rn
    from [dbo].[inventory_test_with_issues]
)
delete from ranked
where rn > 1;



--deduplicating validation

-- Check if there are any rows with same OrderID (should be unique)
select OrderID, count(*) as cnt
from inventory_test_with_issues
group by OrderID
having count(*) > 1;

--Post deduplication count number of records in [dbo].[inventory_test_with_issues]
select count(*) as Total_Records from [dbo].[inventory_test_with_issues];





---OUtlier detection
select 
    min(Demand) as MinDemand, 
    max(Demand) as MaxDemand, 
    avg(Demand) as AvgDemand
from [dbo].[inventory_test_with_issues];

select
    min(Availability) as MinAvail, 
    max(Availability) as MaxAvail, 
    avg(Availability) as AvgAvail
from [dbo].[inventory_test_with_issues];

-- cap the outliers
-- Example rule: if Demand > 5000, cap at 5000
update inventory_test_with_issues
set Demand = 5000
where Demand > 5000;

-- outlier capping validation

select ProductID, WarehouseLocation, OrderDate, count(*) as cnt
from inventory_test_with_issues
group by ProductID, WarehouseLocation, OrderDate
having count(*) > 1;




-- If Availability > Demand, reset to Demand (can't have more stock than demand)
update inventory_test_with_issues
set Availability = Demand
where Availability > Demand;

-- Referential Integrity ( whether ProductID exists in Products)

select i.ProductID
from [dbo].[inventory_test_with_issues] i
left join [dbo].[products_table] p ON i.ProductID = p.ProductID
where p.ProductID IS NULL;



---business logic checks
Select *
from [dbo].[inventory_test_with_issues]
where Availability < 0
   OR Availability > Demand
   OR OrderDate < '2022-01-01'
   OR OrderDate > GETDATE();

-- Drop rows with out-of-range dates
Delete from dbo.inventory_test_with_issues
where OrderDate < '2022-01-01' OR OrderDate > GETDATE();
--cap availability to demand
update [dbo].[inventory_test_with_issues]
set Availability = Demand
where Availability > Demand;
--validating availability and demand
Select Availability,Demand
from [dbo].[inventory_test_with_issues]
where Availability < 0
   OR Availability > Demand

-- validating removal of out-of-range dates
select OrderDate from dbo.inventory_test_with_issues
where OrderDate < '2022-01-01' OR OrderDate >GETDATE();



--Creation of a new table with left Join on the two tables using Product_ID columns
select * into ProductOrderAvailability
from
(select 
Inv.OrderID,
Inv.ProductID,
Inv.ProductName,
pdts.category,
Inv.Availability,
Inv.Demand,
Inv.WarehouseLocation,
pdts.Supplier,
pdts.costPrice,
pdts.unitPrice
from 
[dbo].[inventory_test_with_issues] as Inv 
left join
[dbo].[products_table] as pdts 
on
Inv.ProductID=pdts.ProductID) x


-- 1. Check row count matches inventory (since left join was used)
select count(*) FROM [dbo].[inventory_test_with_issues];
select count(*) FROM ProductOrderAvailability;

-- 2. Check for missing product matches
select *
from ProductOrderAvailability
where Category IS NULL OR Supplier IS NULL;