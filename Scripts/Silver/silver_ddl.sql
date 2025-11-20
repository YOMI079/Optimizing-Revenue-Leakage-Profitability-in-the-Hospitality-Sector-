/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
    It defines the structure for the cleaned hospitality data.
===============================================================================
*/

USE HospitalitySectorData;
GO

-- Create Schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver')
END
GO

-------------------------------------------------------------------
-- 1. silver.dim_date
-------------------------------------------------------------------
IF OBJECT_ID('silver.dim_date', 'U') IS NOT NULL
    DROP TABLE silver.dim_date;
GO

CREATE TABLE silver.dim_date (
    date_id         DATE,
    mmm_yy          NVARCHAR(50),  -- Renamed from 'mmm yy' to remove space
    week_no         NVARCHAR(50),  -- Renamed from 'week no' to remove space
    day_type        NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-------------------------------------------------------------------
-- 2. silver.dim_hotels
-------------------------------------------------------------------
IF OBJECT_ID('silver.dim_hotels', 'U') IS NOT NULL
    DROP TABLE silver.dim_hotels;
GO

CREATE TABLE silver.dim_hotels (
    property_id     INT,
    property_name   NVARCHAR(255),
    category        NVARCHAR(50),
    city            NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-------------------------------------------------------------------
-- 3. silver.dim_rooms
-------------------------------------------------------------------
IF OBJECT_ID('silver.dim_rooms', 'U') IS NOT NULL
    DROP TABLE silver.dim_rooms;
GO

CREATE TABLE silver.dim_rooms (
    room_id         NVARCHAR(50),
    room_class      NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-------------------------------------------------------------------
-- 4. silver.fact_aggregated_bookings
-------------------------------------------------------------------
IF OBJECT_ID('silver.fact_aggregated_bookings', 'U') IS NOT NULL
    DROP TABLE silver.fact_aggregated_bookings;
GO

CREATE TABLE silver.fact_aggregated_bookings (
    property_id         INT,
    check_in_date       DATE,
    room_category       NVARCHAR(50),
    successful_bookings INT,
    capacity            INT,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-------------------------------------------------------------------
-- 5. silver.fact_bookings
-------------------------------------------------------------------
IF OBJECT_ID('silver.fact_bookings', 'U') IS NOT NULL
    DROP TABLE silver.fact_bookings;
GO

CREATE TABLE silver.fact_bookings (
    booking_id          NVARCHAR(50),
    property_id         INT,
    booking_date        DATE,
    check_in_date       DATE,
    checkout_date       DATE,
    no_guests           INT,
    room_category       NVARCHAR(50),
    booking_platform    NVARCHAR(50),
    ratings_given       DECIMAL(3,1),
    booking_status      NVARCHAR(50),
    revenue_generated   INT,
    revenue_realized    INT,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO