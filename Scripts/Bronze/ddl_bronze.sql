USE HospitalitySectorData;
GO

-------------------------------------------------------------------
-- 1. dim_date
-------------------------------------------------------------------
IF OBJECT_ID('bronze.dim_date', 'U') IS NOT NULL DROP TABLE bronze.dim_date;
CREATE TABLE bronze.dim_date (
    [date] DATE,
    [mmm yy] NVARCHAR(50),   -- Note the space
    [week no] NVARCHAR(50),  -- Note the space
    [day_type] NVARCHAR(50)
);
GO

-------------------------------------------------------------------
-- 2. dim_hotels
-------------------------------------------------------------------
IF OBJECT_ID('bronze.dim_hotels', 'U') IS NOT NULL DROP TABLE bronze.dim_hotels;
CREATE TABLE bronze.dim_hotels (
    [property_id] INT,
    [property_name] NVARCHAR(255),
    [category] NVARCHAR(50),
    [city] NVARCHAR(50)
);
GO

-------------------------------------------------------------------
-- 3. dim_rooms
-------------------------------------------------------------------
IF OBJECT_ID('bronze.dim_rooms', 'U') IS NOT NULL DROP TABLE bronze.dim_rooms;
CREATE TABLE bronze.dim_rooms (
    [room_id] NVARCHAR(50),
    [room_class] NVARCHAR(50)
);
GO

-------------------------------------------------------------------
-- 4. fact_aggregated_bookings
-------------------------------------------------------------------
IF OBJECT_ID('bronze.fact_aggregated_bookings', 'U') IS NOT NULL DROP TABLE bronze.fact_aggregated_bookings;
CREATE TABLE bronze.fact_aggregated_bookings (
    [property_id] INT,
    [check_in_date] DATE,
    [room_category] NVARCHAR(50),
    [successful_bookings] INT,
    [capacity] INT
);
GO

-------------------------------------------------------------------
-- 5. fact_bookings
-------------------------------------------------------------------
IF OBJECT_ID('bronze.fact_bookings', 'U') IS NOT NULL DROP TABLE bronze.fact_bookings;
CREATE TABLE bronze.fact_bookings (
    [booking_id] NVARCHAR(50),
    [property_id] INT,
    [booking_date] DATE,
    [check_in_date] DATE,
    [checkout_date] DATE,
    [no_guests] INT,
    [room_category] NVARCHAR(50),
    [booking_platform] NVARCHAR(50),
    [ratings_given] DECIMAL(3,1),
    [booking_status] NVARCHAR(50),
    [revenue_generated] INT,
    [revenue_realized] INT
);
GO