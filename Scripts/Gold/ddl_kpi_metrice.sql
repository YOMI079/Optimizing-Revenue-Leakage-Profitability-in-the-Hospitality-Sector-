USE HospitalitySectorData;
GO

-- 1. Check the Daily Summary (Aggregated Data)
-- This shows how full the hotels are (bookings vs capacity)
SELECT TOP 10 * FROM gold.fact_daily_summary;

-- 2. Check the Detailed Bookings
USE HospitalitySectorData;
GO

IF OBJECT_ID('gold.kpi_metrics', 'V') IS NOT NULL
    DROP VIEW gold.kpi_metrics;
GO

CREATE VIEW gold.kpi_metrics AS
SELECT
    ds.property_id,
    ds.check_in_date,
    ds.room_id,
    
    -- Base Metrics
    ds.capacity                                           AS total_capacity,
    ds.successful_bookings                                AS total_successful_bookings,
    ISNULL(bk.total_revenue, 0)                           AS total_revenue,
    ISNULL(bk.checked_out_count, 0)                       AS checked_out_bookings,
    ISNULL(bk.cancelled_count, 0)                         AS cancelled_bookings,
    
    -- KPI 1: Occupancy Percentage (Successful Bookings / Total Capacity)
    CASE 
        WHEN ds.capacity > 0 
        THEN (CAST(ds.successful_bookings AS DECIMAL(10,2)) / ds.capacity) * 100 
        ELSE 0 
    END AS occupancy_pct,

    -- KPI 2: ADR (Average Daily Rate = Revenue / Rooms Sold)
    CASE 
        WHEN ds.successful_bookings > 0 
        THEN (CAST(bk.total_revenue AS DECIMAL(10,2)) / ds.successful_bookings) 
        ELSE 0 
    END AS adr,

    -- KPI 3: RevPAR (Revenue Per Available Room = Revenue / Total Capacity)
    CASE 
        WHEN ds.capacity > 0 
        THEN (CAST(bk.total_revenue AS DECIMAL(10,2)) / ds.capacity) 
        ELSE 0 
    END AS revpar,

    -- KPI 4: Realisation Percentage (Checked Out / (Checked Out + Cancelled + No Show))
    -- Note: We assume 'successful_bookings' in the summary includes Checked Out and others, 
    -- but calculating purely from the bookings status gives a precise realisation rate.
    CASE 
        WHEN (ISNULL(bk.checked_out_count, 0) + ISNULL(bk.cancelled_count, 0) + ISNULL(bk.no_show_count, 0)) > 0
        THEN (CAST(bk.checked_out_count AS DECIMAL(10,2)) / 
             (bk.checked_out_count + bk.cancelled_count + bk.no_show_count)) * 100
        ELSE 0 
    END AS realisation_pct

FROM gold.fact_daily_summary ds
-- Join with aggregated transaction data to get Revenue and Status Counts
LEFT JOIN (
    SELECT 
        property_id,
        check_in_date,
        room_id,
        SUM(revenue_realized) AS total_revenue,
        SUM(CASE WHEN booking_status = 'Checked Out' THEN 1 ELSE 0 END) AS checked_out_count,
        SUM(CASE WHEN booking_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
        SUM(CASE WHEN booking_status = 'No Show' THEN 1 ELSE 0 END) AS no_show_count
    FROM gold.fact_bookings
    GROUP BY property_id, check_in_date, room_id
) bk
ON ds.property_id = bk.property_id 
   AND ds.check_in_date = bk.check_in_date 
   AND ds.room_id = bk.room_id;
GO