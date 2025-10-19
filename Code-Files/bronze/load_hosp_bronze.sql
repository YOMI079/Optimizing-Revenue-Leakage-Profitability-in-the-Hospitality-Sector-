/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Staging -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data from CSV files into the final typed
    bronze tables. It uses temporary staging tables (all NVARCHAR) to 
    land the raw data first, then cleans and converts it before inserting
    into the final destination tables.

    This handles date conversion (e.g., '01-May-22') and NULL values.
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
        PRINT 'Table: bronze.bronze_dim_date';
        PRINT '------------------------------------------------';
        SET @start_time = GETDATE();

        -- 1. Create a temp table for raw data
        CREATE TABLE #stage_dim_date (
            [date] NVARCHAR(MAX) NULL,
            [mmm yy] NVARCHAR(MAX) NULL,
            [week no] NVARCHAR(MAX) NULL,
            [day_type] NVARCHAR(MAX) NULL
        );

        -- 2. BULK INSERT into the temp table
        PRINT '>> Inserting raw data into staging...';
        BULK INSERT #stage_dim_date
        FROM 'C:\Users\ADMIN\Desktop\Compition\Optimizing Revenue Leakage & Profitability in the  Hospitality Sector\dim_date.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- 3. Truncate final table and insert with conversion
        PRINT '>> Truncating final table...';
        TRUNCATE TABLE bronze.bronze_dim_date;
        
        PRINT '>> Converting and inserting into final table...';
        INSERT INTO bronze.bronze_dim_date (
            [date],
            [mmm yy],
            [week no],
            [day_type]
        )
        SELECT
            TRY_CONVERT(DATE, [date], 106), -- Style 106 handles 'dd Mon yy'
            [mmm yy],
            [week no],
            [day_type]
        FROM #stage_dim_date;

        DROP TABLE #stage_dim_date; -- Clean up
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -----------------------------------------------------------------
        PRINT 'Table: bronze.bronze_dim_hotels';
        PRINT '------------------------------------------------';
        SET @start_time = GETDATE();
        
        -- NOTE: Your script loads into 'bronze_dim_properties', but your
        -- DDL created 'bronze_dim_hotels'. I will load into 'bronze_dim_hotels'.
        
        PRINT '>> Truncating Table...';
        TRUNCATE TABLE bronze.bronze_dim_hotels;

        PRINT '>> Inserting Data... (Direct load, no conversion needed)';
        BULK INSERT bronze.bronze_dim_hotels
        FROM 'C:\Users\ADMIN\Desktop\Compition\Optimizing Revenue Leakage & Profitability in the  Hospitality Sector\dim_hotels.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        
        -----------------------------------------------------------------
        PRINT 'Table: bronze.bronze_dim_rooms';
        PRINT '------------------------------------------------';
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table...';
        TRUNCATE TABLE bronze.bronze_dim_rooms;
        
        PRINT '>> Inserting Data... (Direct load, no conversion needed)';
        BULK INSERT bronze.bronze_dim_rooms
        FROM 'C:\Users\ADMIN\Desktop\Compition\Optimizing Revenue Leakage & Profitability in the  Hospitality Sector\dim_rooms.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        
        -----------------------------------------------------------------
        PRINT 'Table: bronze.bronze_fact_aggregated_bookings';
        PRINT '------------------------------------------------';
        SET @start_time = GETDATE(); 

        -- 1. Create a temp table for raw data
        CREATE TABLE #stage_agg_bookings (
            [property_id] NVARCHAR(MAX) NULL,
            [check_in_date] NVARCHAR(MAX) NULL,
            [room_category] NVARCHAR(MAX) NULL,
            [successful_bookings] NVARCHAR(MAX) NULL,
            [capacity] NVARCHAR(MAX) NULL
        );

        -- 2. BULK INSERT into the temp table
        PRINT '>> Inserting raw data into staging...';
        BULK INSERT #stage_agg_bookings
        FROM 'C:\Users\ADMIN\Desktop\Compition\Optimizing Revenue Leakage & Profitability in the  Hospitality Sector\fact_aggregated_bookings.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        
        -- 3. Truncate final table and insert with conversion
        PRINT '>> Truncating final table...';
        TRUNCATE TABLE bronze.bronze_fact_aggregated_bookings;
        
        PRINT '>> Converting and inserting into final table...';
        INSERT INTO bronze.bronze_fact_aggregated_bookings (
            [property_id],
            [check_in_date],
            [room_category],
            [successful_bookings],
            [capacity]
        )
        SELECT
            TRY_CONVERT(INT, [property_id]),
            TRY_CONVERT(DATE, [check_in_date], 106), -- Style 106 handles 'dd Mon yy'
            [room_category],
            TRY_CONVERT(INT, [successful_bookings]),
            TRY_CONVERT(INT, [capacity])
        FROM #stage_agg_bookings;

        DROP TABLE #stage_agg_bookings; -- Clean up
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -----------------------------------------------------------------
        PRINT 'Table: bronze.bronze_fact_bookings';
        PRINT '------------------------------------------------';
        SET @start_time = GETDATE();

        -- 1. Create a temp table for raw data
        CREATE TABLE #stage_fact_bookings (
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
            [revenue_realized] NVARCHAR(MAX) NULL
        );
        
        -- 2. BULK INSERT into the temp table
        PRINT '>> Inserting raw data into staging...';
        BULK INSERT #stage_fact_bookings
        FROM 'C:\Users\ADMIN\Desktop\Compition\Optimizing Revenue Leakage & Profitability in the  Hospitality Sector\fact_bookings.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK,
            KEEPNULLS -- This is vital for the ratings column
        );
        
        -- 3. Truncate final table and insert with conversion
        PRINT '>> Truncating final table...';
        TRUNCATE TABLE bronze.bronze_fact_bookings;
        
        PRINT '>> Converting and inserting into final table...';
        INSERT INTO bronze.bronze_fact_bookings (
            [booking_id],
            [property_id],
            [booking_date],
            [check_in_date],
            [checkout_date],
            [no_guests],
            [room_category],
            [booking_platform],
            [ratings_given],
            [booking_status],
            [revenue_generated],
            [revenue_realized]
        )
        SELECT
            [booking_id],
            TRY_CONVERT(INT, [property_id]),
            TRY_CONVERT(DATE, [booking_date]), -- 'YYYY-MM-DD' converts automatically
            TRY_CONVERT(DATE, [check_in_date]),
            TRY_CONVERT(DATE, [checkout_date]),
            TRY_CONVERT(INT, [no_guests]),
            [room_category],
            [booking_platform],
            TRY_CONVERT(DECIMAL(3, 1), [ratings_given]), -- Handles NULLs
            [booking_status],
            TRY_CONVERT(INT, [revenue_generated]),
            TRY_CONVERT(INT, [revenue_realized])
        FROM #stage_fact_bookings;

        DROP TABLE #stage_fact_bookings; -- Clean up
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        
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
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:   ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Error Line:    ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '================================================';
        
        -- If an error happens, drop any temp tables that might exist
        IF OBJECT_ID('tempdb..#stage_dim_date') IS NOT NULL DROP TABLE #stage_dim_date;
        IF OBJECT_ID('tempdb..#stage_agg_bookings') IS NOT NULL DROP TABLE #stage_agg_bookings;
        IF OBJECT_ID('tempdb..#stage_fact_bookings') IS NOT NULL DROP TABLE #stage_fact_bookings;
        
    END CATCH
END
GO