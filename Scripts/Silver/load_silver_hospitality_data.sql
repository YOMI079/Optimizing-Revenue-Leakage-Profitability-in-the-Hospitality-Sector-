/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
    Actions Performed:
        - Truncates Silver tables.
        - Inserts transformed and cleansed data from Bronze into Silver tables.
        - Fixes typos (e.g., 'weekeday' -> 'Weekday').
        
Parameters: None.
Usage Example: EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading Dimension Tables';
        PRINT '------------------------------------------------';

        -- 1. Loading silver.dim_date
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.dim_date';
        TRUNCATE TABLE silver.dim_date;
        
        PRINT '>> Inserting Data Into: silver.dim_date';
        INSERT INTO silver.dim_date (
            date_id,
            mmm_yy,
            week_no,
            day_type
        )
        SELECT
            [date] AS date_id,
            TRIM([mmm yy]) AS mmm_yy,
            TRIM([week no]) AS week_no,
            -- Fix typo 'weekeday' to 'Weekday' and standardise casing
            CASE 
                WHEN UPPER(TRIM([day_type])) = 'WEEKEDAY' THEN 'Weekday'
                WHEN UPPER(TRIM([day_type])) = 'WEEKEND' THEN 'Weekend'
                ELSE TRIM([day_type])
            END AS day_type
        FROM bronze.dim_date;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- 2. Loading silver.dim_hotels
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.dim_hotels';
        TRUNCATE TABLE silver.dim_hotels;
        
        PRINT '>> Inserting Data Into: silver.dim_hotels';
        INSERT INTO silver.dim_hotels (
            property_id,
            property_name,
            category,
            city
        )
        SELECT
            property_id,
            TRIM(property_name) AS property_name,
            TRIM(category) AS category,
            TRIM(city) AS city
        FROM bronze.dim_hotels;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- 3. Loading silver.dim_rooms
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.dim_rooms';
        TRUNCATE TABLE silver.dim_rooms;
        
        PRINT '>> Inserting Data Into: silver.dim_rooms';
        INSERT INTO silver.dim_rooms (
            room_id,
            room_class
        )
        SELECT
            TRIM(room_id) AS room_id,
            TRIM(room_class) AS room_class
        FROM bronze.dim_rooms;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
        PRINT 'Loading Fact Tables';
        PRINT '------------------------------------------------';

        -- 4. Loading silver.fact_aggregated_bookings
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.fact_aggregated_bookings';
        TRUNCATE TABLE silver.fact_aggregated_bookings;
        
        PRINT '>> Inserting Data Into: silver.fact_aggregated_bookings';
        INSERT INTO silver.fact_aggregated_bookings (
            property_id,
            check_in_date,
            room_category,
            successful_bookings,
            capacity
        )
        SELECT
            property_id,
            check_in_date,
            TRIM(room_category) AS room_category,
            successful_bookings,
            capacity
        FROM bronze.fact_aggregated_bookings;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- 5. Loading silver.fact_bookings
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.fact_bookings';
        TRUNCATE TABLE silver.fact_bookings;
        
        PRINT '>> Inserting Data Into: silver.fact_bookings';
        INSERT INTO silver.fact_bookings (
            booking_id,
            property_id,
            booking_date,
            check_in_date,
            checkout_date,
            no_guests,
            room_category,
            booking_platform,
            ratings_given,
            booking_status,
            revenue_generated,
            revenue_realized
        )
        SELECT
            TRIM(booking_id) AS booking_id,
            property_id,
            booking_date,
            check_in_date,
            checkout_date,
            no_guests,
            TRIM(room_category) AS room_category,
            TRIM(booking_platform) AS booking_platform,
            ratings_given,
            TRIM(booking_status) AS booking_status,
            revenue_generated,
            revenue_realized
        FROM bronze.fact_bookings;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Loading Silver Layer Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='
        
    END TRY
    BEGIN CATCH
        PRINT '=========================================='
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER'
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:   ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================='
    END CATCH
END
GO