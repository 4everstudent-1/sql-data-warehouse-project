/*
==============================
Create Database and SChemas
==============================

DESCRIPTION
  This script initializes a SQL Server data warehouse environment.
  It drops and recreates the 'DataWarehouse' database and sets up the
  required schemas (bronze, silver, and gold) to support a layered
  data architecture for raw, cleaned, and curated data.

⚠️ WARNING ⚠️
  This script will DROP and RECREATE the DataWarehouse database.
  ALL existing data will be permanently lost.
  Do NOT run this in a production environment.
*/

USE master;
GO

-- Drop the database if it already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the DataWarehouse database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the new database
USE DataWarehouse;
GO

-- Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
