# Retail-Data-Pipeline-MSSQL-MySQL-PowerBI
## Project Overview
Developed a comprehensive data pipeline demonstrating the complete workflow from synthetic data generation to business intelligence reporting. This project showcases best practices in data quality management, ETL processes, and data visualization for inventory management systems.

## Technical Workflow
1. Synthetic Data Generation
Created product information tables using Python's Faker and random libraries
Generated 100 unique products with attributes including:
ProductID
ProductName
Category
Supplier
CostPrice
UnitPrice
WarehouseLocation

## Inventory Dataset Creation
Developed separate inventory datasets for:
Test environment: 50,000 records
Production environment: 5,000 records
Intentionally injected data quality issues to simulate real-world scenarios:
2% duplicate records
2% outlier values
5% null values across various fields
Exported datasets to CSV format for downstream processing

## Data Cleaning & Validation
Utilized SQL Server Management Studio (SSMS) to implement comprehensive data cleaning:
Data profiling and quality assessment
Duplicate identification and removal
Outlier detection and treatment
Null value imputation strategies
Data validation against business rules
Documented all cleaning procedures for reproducibility

## Business Intelligence & Reporting
Designed and developed interactive Power BI dashboards using cleaned test data
Created visualizations highlighting key inventory metrics and insights
Implemented the same SQL cleaning procedures on production data
Updated Power BI reports to leverage production data for business insights
Demonstrated the transition from test to production environment while maintaining report integrity

## Technical Stack
Data Generation: Python (Faker, pandas, numpy, random libraries)
Data Processing: SQL Server Management Studio (SSMS)
Data Visualization: Power BI
Data Storage: CSV, SQL Server

## Key Achievements
Demonstrated end-to-end data pipeline development
Implemented robust data quality management processes
Showcased the transition from test to production environments
Delivered actionable business insights through interactive dashboards
Established reproducible data cleaning and validation procedures
