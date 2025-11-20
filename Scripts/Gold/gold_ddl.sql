/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema).

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

USE HospitalitySectorData;
GO

-- Create Schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold')
END
GO

-- =============================================================================
-- Create Dimension: gold.dim_date
-- =============================================================================
IF OBJECT_ID('gold.dim_date', 'V') IS NOT NULL
    DROP VIEW gold.dim_date;
GO

CREATE VIEW gold.dim_date AS
SELECT
    date_id,
    mmm_yy      AS month_year,
    week_no     AS week_number,
    day_type
FROM silver.dim_date;
GO

-- =============================================================================
-- Create Dimension: gold.dim_hotels
-- =============================================================================
IF OBJECT_ID('gold.dim_hotels', 'V') IS NOT NULL
    DROP VIEW gold.dim_hotels;
GO

CREATE VIEW gold.dim_hotels AS
SELECT
    property_id,
    property_name,
    category,
    city
FROM silver.dim_hotels;
GO

-- =============================================================================
-- Create Dimension: gold.dim_rooms
-- =============================================================================
IF OBJECT_ID('gold.dim_rooms', 'V') IS NOT NULL
    DROP VIEW gold.dim_rooms;
GO

CREATE VIEW gold.dim_rooms AS
SELECT
    room_id,
    room_class
FROM silver.dim_rooms;
GO

-- =============================================================================
-- Create Fact Table: gold.fact_bookings
-- =============================================================================
IF OBJECT_ID('gold.fact_bookings', 'V') IS NOT NULL
    DROP VIEW gold.fact_bookings;
GO

CREATE VIEW gold.fact_bookings AS
SELECT
    booking_id,
    property_id,
    booking_date,
    check_in_date,
    checkout_date,
    no_guests,
    room_category       AS room_id, -- Renamed to match dim_rooms.room_id
    booking_platform,
    ratings_given,
    booking_status,
    revenue_generated,
    revenue_realized
FROM silver.fact_bookings;
GO

-- =============================================================================
-- Create Fact Table: gold.fact_daily_summary
-- =============================================================================
IF OBJECT_ID('gold.fact_daily_summary', 'V') IS NOT NULL
    DROP VIEW gold.fact_daily_summary;
GO

CREATE VIEW gold.fact_daily_summary AS
SELECT
    property_id,
    check_in_date,
    room_category       AS room_id, -- Renamed to match dim_rooms.room_id
    successful_bookings,
    capacity
FROM silver.fact_aggregated_bookings;
GO