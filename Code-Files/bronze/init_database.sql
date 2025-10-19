/*
=============================================================
Create Database and Schemas
=============================================================
*/
USE master;
GO

-- Drop and recreate the 'HospitalitySectorData' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'HospitalitySectorData')
BEGIN
    ALTER DATABASE HospitalitySectorData SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE HospitalitySectorData;
END;
GO

-- Create the 'HospitalitySectorData' database
CREATE DATABASE HospitalitySectorData;
GO

USE HospitalitySectorData;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO