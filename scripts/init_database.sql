/*
================================
Create Database & Schema
================================

Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exists
  additionally the script setup three schemas within database: 'bronze','silver','gold'.

WARNING:
  Run this query with caution it will remove any database with name DataWarehouse make sure to save your data if there is some */

USE Master;
GO


IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

  
--Create Database

CREATE DATABASE DataWarehouse  
GO
USE DataWarehouse;
GO

--Create Schema

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
