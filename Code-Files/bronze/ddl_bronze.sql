/*
Bronze table for: dim_date.csv
- All columns are NVARCHAR(MAX) to accept raw text.
- No keys or constraints.
*/
IF OBJECT_ID('bronze.bronze_dim_date', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze_dim_date (
    [date] NVARCHAR(MAX) NULL,
    [mmm] NVARCHAR(MAX) NULL,
    [yy] NVARCHAR(MAX) NULL,
    [week no] NVARCHAR(MAX) NULL,
    [day_type] NVARCHAR(MAX) NULL,
    [ingestion_timestamp] DATETIME2 DEFAULT GETDATE(),
    [source_file] NVARCHAR(255) NULL
);

/*
Bronze table for: dim_properties.csv
*/
IF OBJECT_ID('bronze.bronze_dim_properties', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO
CREATE TABLE bronze_dim_properties (
    [property_id] NVARCHAR(MAX) NULL,
    [property_name] NVARCHAR(MAX) NULL,
    [category] NVARCHAR(MAX) NULL,
    [city] NVARCHAR(MAX) NULL,
    [ingestion_timestamp] DATETIME2 DEFAULT GETDATE(),
    [source_file] NVARCHAR(255) NULL
);

/*
Bronze table for: dim_rooms.csv
*/
IF OBJECT_ID('bronze.bronze_dim_rooms', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO
CREATE TABLE bronze_dim_rooms (
    [room_id] NVARCHAR(MAX) NULL,
    [room_class] NVARCHAR(MAX) NULL,
    [ingestion_timestamp] DATETIME2 DEFAULT GETDATE(),
    [source_file] NVARCHAR(255) NULL
);

/*
Bronze table for: fact_aggregated_bookings.csv
*/
IF OBJECT_ID('bronze.bronze_fact_aggregated_bookings', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO
CREATE TABLE bronze_fact_aggregated_bookings (
    [property_id] NVARCHAR(MAX) NULL,
    [check_in_date] NVARCHAR(MAX) NULL,
    [room_category] NVARCHAR(MAX) NULL,
    [successful_bookings] NVARCHAR(MAX) NULL,
    [capacity] NVARCHAR(MAX) NULL,
    [ingestion_timestamp] DATETIME2 DEFAULT GETDATE(),
    [source_file] NVARCHAR(255) NULL
);

/*
Bronze table for: fact_bookings.csv
*/
IF OBJECT_ID('bronze.bronze_fact_bookings', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO
CREATE TABLE bronze_fact_bookings (
    [booking_id] NVARCHAR(MAX) NULL,
    [property_id] NVARCHAR(MAX) NULL,
    [booking_date] NVARCHAR(MAX) NULL,
    [check_in_date] NVARCHAR(MAX) NULL,
    [checkout_date] NVARCHAR(MAX) NULL,
    [no_guests] NVARCHAR(MAX) NULL,
    [room_category] NVARCHAR(MAX) NULL,
    [booking_platform] NVARCHAR(MAX) NULL,
    [ratings_given] NVARCHAR(MAX) NULL,
    [booking_status] NVARCHAR(MAX) NULL,
    [revenue_generated] NVARCHAR(MAX) NULL,
    [revenue_realized] NVARCHAR(MAX) NULL,
    [ingestion_timestamp] DATETIME2 DEFAULT GETDATE(),
    [source_file] NVARCHAR(255) NULL
);