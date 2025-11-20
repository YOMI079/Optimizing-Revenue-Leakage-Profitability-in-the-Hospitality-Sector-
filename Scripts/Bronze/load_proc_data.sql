/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Staging -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data from CSV files into the final typed
    bronze tables. It uses temporary staging tables (all NVARCHAR) to 
    land the raw data first, then cleans and converts it before inserting
    into the final destination tables.
===============================================================================
*/
USE HospitalitySectorData;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer (Staging & Converting)';
        PRINT '================================================';

        -----------------------------------------------------------------
        PRINT 'Table: bronze.dim_date';
        PRINT '------------------------------------------------';
        SET @start_time = GETDATE();

        -- Clean up temp table if exists
        IF OBJECT_ID('tempdb..#stage_dim_date') IS NOT NULL DROP TABLE #stage_dim_date;

        -- 1. Create staging table
        CREATE TABLE #stage_dim_date (
            [date] NVARCHAR(50),
            [mmm yy] NVARCHAR(50),
            [week no] NVARCHAR(50),
            [day_type] NVARCHAR(50)
        );

        -- 2. Bulk Insert
        PRINT '>> Inserting raw data into staging...';
        BULK INSERT #stage_dim_date
        FROM 'C:\Hospitality\Database\dim_date.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        -- 3. Transform and Insert
        PRINT '>> Truncating and inserting into final table...';
        TRUNCATE TABLE bronze.dim_date;

        INSERT INTO bronze.dim_date ([date], [mmm yy], [week no], [day_type])
        SELECT
            TRY_CONVERT(DATE, [date], 106), -- Format: '01-May-22'
            [mmm yy],
            [week no],
            [day_type]
        FROM #stage_dim_date;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -----------------------------------------------------------------
        PRINT 'Table: bronze.dim_hotels';
        PRINT '------------------------------------------------';
        SET @start_time = GETDATE();

        IF OBJECT_ID('tempdb..#stage_dim_hotels') IS NOT NULL DROP TABLE #stage_dim_hotels;

        CREATE TABLE #stage_dim_hotels (
            [property_id] NVARCHAR(50),
            [property_name] NVARCHAR(255),
            [category] NVARCHAR(50),
            [city] NVARCHAR(50)
        );

        PRINT '>> Inserting raw data into staging...';
        BULK INSERT #stage_dim_hotels
        FROM 'C:\Hospitality\Database\dim_hotels.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT '>> Truncating and inserting into final table...';
        TRUNCATE TABLE bronze.dim_hotels;

        INSERT INTO bronze.dim_hotels ([property_id], [property_name], [category], [city])
        SELECT
            TRY_CONVERT(INT, [property_id]),
            [property_name],
            [category],
            [city]
        FROM #stage_dim_hotels;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -----------------------------------------------------------------
        PRINT 'Table: bronze.dim_rooms';
        PRINT '------------------------------------------------';
        SET @start_time = GETDATE();

        IF OBJECT_ID('tempdb..#stage_dim_rooms') IS NOT NULL DROP TABLE #stage_dim_rooms;

        CREATE TABLE #stage_dim_rooms (
            [room_id] NVARCHAR(50),
            [room_class] NVARCHAR(50)
        );

        PRINT '>> Inserting raw data into staging...';
        BULK INSERT #stage_dim_rooms
        FROM 'C:\Hospitality\Database\dim_rooms.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT '>> Truncating and inserting into final table...';
        TRUNCATE TABLE bronze.dim_rooms;

        INSERT INTO bronze.dim_rooms ([room_id], [room_class])
        SELECT
            [room_id],
            [room_class]
        FROM #stage_dim_rooms;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -----------------------------------------------------------------
        PRINT 'Table: bronze.fact_aggregated_bookings';
        PRINT '------------------------------------------------';
        SET @start_time = GETDATE(); 

        IF OBJECT_ID('tempdb..#stage_agg_bookings') IS NOT NULL DROP TABLE #stage_agg_bookings;

        CREATE TABLE #stage_agg_bookings (
            [property_id] NVARCHAR(50),
            [check_in_date] NVARCHAR(50),
            [room_category] NVARCHAR(50),
            [successful_bookings] NVARCHAR(50),
            [capacity] NVARCHAR(50)
        );

        PRINT '>> Inserting raw data into staging...';
        BULK INSERT #stage_agg_bookings
        FROM 'C:\Hospitality\Database\fact_aggregated_bookings.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        
        PRINT '>> Truncating and inserting into final table...';
        TRUNCATE TABLE bronze.fact_aggregated_bookings;

        INSERT INTO bronze.fact_aggregated_bookings (
            [property_id], [check_in_date], [room_category], [successful_bookings], [capacity]
        )
        SELECT
            TRY_CONVERT(INT, [property_id]),
            TRY_CONVERT(DATE, [check_in_date], 106), -- Format: '01-May-22'
            [room_category],
            TRY_CONVERT(INT, [successful_bookings]),
            TRY_CONVERT(INT, [capacity])
        FROM #stage_agg_bookings;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -----------------------------------------------------------------
        PRINT 'Table: bronze.fact_bookings';
        PRINT '------------------------------------------------';
        SET @start_time = GETDATE();

        IF OBJECT_ID('tempdb..#stage_fact_bookings') IS NOT NULL DROP TABLE #stage_fact_bookings;

        CREATE TABLE #stage_fact_bookings (
            [booking_id] NVARCHAR(50),
            [property_id] NVARCHAR(50),
            [booking_date] NVARCHAR(50),
            [check_in_date] NVARCHAR(50),
            [checkout_date] NVARCHAR(50),
            [no_guests] NVARCHAR(50),
            [room_category] NVARCHAR(50),
            [booking_platform] NVARCHAR(50),
            [ratings_given] NVARCHAR(50),
            [booking_status] NVARCHAR(50),
            [revenue_generated] NVARCHAR(50),
            [revenue_realized] NVARCHAR(50)
        );
        
        PRINT '>> Inserting raw data into staging...';
        BULK INSERT #stage_fact_bookings
        FROM 'C:\Hospitality\Database\fact_bookings.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        
        PRINT '>> Truncating and inserting into final table...';
        TRUNCATE TABLE bronze.fact_bookings;

        INSERT INTO bronze.fact_bookings (
            [booking_id], [property_id], [booking_date], [check_in_date], [checkout_date], 
            [no_guests], [room_category], [booking_platform], [ratings_given], 
            [booking_status], [revenue_generated], [revenue_realized]
        )
        SELECT
            [booking_id],
            TRY_CONVERT(INT, [property_id]),
            TRY_CONVERT(DATE, [booking_date]), -- Format: YYYY-MM-DD (Auto-detected)
            TRY_CONVERT(DATE, [check_in_date]),
            TRY_CONVERT(DATE, [checkout_date]),
            TRY_CONVERT(INT, [no_guests]),
            [room_category],
            [booking_platform],
            TRY_CONVERT(DECIMAL(3, 1), NULLIF([ratings_given], '')), -- Handle empty strings as NULL
            [booking_status],
            TRY_CONVERT(INT, [revenue_generated]),
            TRY_CONVERT(INT, [revenue_realized])
        FROM #stage_fact_bookings;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        
        -----------------------------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Bronze Layer Load Successful.';
        PRINT 'Total Batch Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT '!!! ERROR OCCURRED LOADING BRONZE LAYER !!!';
        PRINT '------------------------------------------------';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Line:    ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '================================================';
        
        -- Clean up temp tables on failure
        IF OBJECT_ID('tempdb..#stage_dim_date') IS NOT NULL DROP TABLE #stage_dim_date;
        IF OBJECT_ID('tempdb..#stage_dim_hotels') IS NOT NULL DROP TABLE #stage_dim_hotels;
        IF OBJECT_ID('tempdb..#stage_dim_rooms') IS NOT NULL DROP TABLE #stage_dim_rooms;
        IF OBJECT_ID('tempdb..#stage_agg_bookings') IS NOT NULL DROP TABLE #stage_agg_bookings;
        IF OBJECT_ID('tempdb..#stage_fact_bookings') IS NOT NULL DROP TABLE #stage_fact_bookings;
    END CATCH
END
GO