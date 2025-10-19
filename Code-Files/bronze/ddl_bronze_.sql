/*
===============================================================================
DDL Script: Create Bronze Tables for Hospitality Data
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
    
    Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

USE HospitalitySectorData; -- Assumes your database is named this
GO

-------------------------------------------------------------------------------
-- Table: bronze.bronze_dim_date
-- Source: dim_date.csv
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.bronze_dim_date', 'U') IS NOT NULL
    DROP TABLE bronze.bronze_dim_date;
GO

CREATE TABLE bronze.bronze_dim_date (
    [date]      DATE,
    [mmm yy]    NVARCHAR(10),
    [week no]   NVARCHAR(10),
    [day_type]  NVARCHAR(10)
);
GO

-------------------------------------------------------------------------------
-- Table: bronze.bronze_dim_hotels
-- Source: dim_hotels.csv
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.bronze_dim_hotels', 'U') IS NOT NULL
    DROP TABLE bronze.bronze_dim_hotels;
GO

CREATE TABLE bronze.bronze_dim_hotels (
    [property_id]    INT,
    [property_name]  NVARCHAR(100),
    [category]       NVARCHAR(20),
    [city]           NVARCHAR(50)
);
GO

-------------------------------------------------------------------------------
-- Table: bronze.bronze_dim_rooms
-- Source: dim_rooms.csv
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.bronze_dim_rooms', 'U') IS NOT NULL
    DROP TABLE bronze.bronze_dim_rooms;
GO

CREATE TABLE bronze.bronze_dim_rooms (
    [room_id]     NVARCHAR(10),
    [room_class]  NVARCHAR(20)
);
GO

-------------------------------------------------------------------------------
-- Table: bronze.bronze_fact_aggregated_bookings
-- Source: fact_aggregated_bookings.csv
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.bronze_fact_aggregated_bookings', 'U') IS NOT NULL
    DROP TABLE bronze.bronze_fact_aggregated_bookings;
GO

CREATE TABLE bronze.bronze_fact_aggregated_bookings (
    [property_id]           INT,
    [check_in_date]         DATE,
    [room_category]         NVARCHAR(10),
    [successful_bookings]   INT,
    [capacity]              INT
);
GO

-------------------------------------------------------------------------------
-- Table: bronze.bronze_fact_bookings
-- Source: fact_bookings.csv
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.bronze_fact_bookings', 'U') IS NOT NULL
    DROP TABLE bronze.bronze_fact_bookings;
GO

CREATE TABLE bronze.bronze_fact_bookings (
    [booking_id]          NVARCHAR(50),
    [property_id]         INT,
    [booking_date]        DATE,
    [check_in_date]       DATE,
    [checkout_date]       DATE,
    [no_guests]           INT,
    [room_category]       NVARCHAR(10),
    [booking_platform]    NVARCHAR(50),
    [ratings_given]       DECIMAL(3, 1),
    [booking_status]      NVARCHAR(20),
    [revenue_generated]   INT,
    [revenue_realized]    INT
);
GO