-- SmartLogix Transport Optimization - Databricks SQL Commands (FREE EDITION)
-- Execute these commands in Databricks SQL Editor

-- =====================================================
-- FREE EDITION SETUP NOTES
-- =====================================================
-- 1. Free edition has limited compute hours per month
-- 2. File size limit: ~2GB per file
-- 3. No Unity Catalog (uses default catalog)
-- 4. Limited storage space
-- 5. Auto-stop warehouse after 10 minutes to save credits

-- =====================================================
-- STEP 1: CREATE DATABASE AND SCHEMA (FREE EDITION)
-- =====================================================

-- Create database (free edition uses default catalog)
CREATE DATABASE IF NOT EXISTS smartlogix;

-- Use the database
USE smartlogix;

-- Create schema for tables
CREATE SCHEMA IF NOT EXISTS transport_data;

-- Use the schema
USE SCHEMA smartlogix.transport_data;

-- =====================================================
-- STEP 2: CREATE TABLES (FREE EDITION OPTIMIZED)
-- =====================================================

-- 1. Delhivery Data Table (may be split into multiple files)
CREATE TABLE IF NOT EXISTS smartlogix.transport_data.delhivery_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    data_type STRING,
    trip_creation_time TIMESTAMP,
    route_schedule_uuid STRING,
    route_type STRING,
    trip_uuid STRING,
    source_center STRING,
    source_name STRING,
    destination_center STRING,
    destination_name STRING,
    od_start_time TIMESTAMP,
    od_end_time TIMESTAMP,
    start_scan_to_end_scan DOUBLE,
    is_cutoff BOOLEAN,
    cutoff_factor INT,
    cutoff_timestamp TIMESTAMP,
    actual_distance_to_destination DOUBLE,
    actual_time DOUBLE,
    osrm_time DOUBLE,
    osrm_distance DOUBLE,
    factor DOUBLE,
    segment_actual_time DOUBLE,
    segment_osrm_time DOUBLE,
    segment_osrm_distance DOUBLE,
    segment_factor DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- 2. Distance Data Table
CREATE TABLE IF NOT EXISTS smartlogix.transport_data.distance_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    source STRING,
    destination STRING,
    distance_meters INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- 3. Order Data Table
CREATE TABLE IF NOT EXISTS smartlogix.transport_data.order_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    order_id STRING,
    material_id STRING,
    item_id STRING,
    source STRING,
    destination STRING,
    available_time TIMESTAMP,
    deadline TIMESTAMP,
    danger_type STRING,
    area DOUBLE,
    weight DOUBLE,
    order_size STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- 4. Supply Chain Data Table
CREATE TABLE IF NOT EXISTS smartlogix.transport_data.supply_chain_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    product_type STRING,
    sku STRING,
    price DOUBLE,
    availability INT,
    number_of_products_sold INT,
    revenue_generated DOUBLE,
    customer_demographics STRING,
    stock_levels INT,
    lead_times INT,
    order_quantities INT,
    shipping_times INT,
    shipping_carriers STRING,
    shipping_costs DOUBLE,
    supplier_name STRING,
    location STRING,
    lead_time INT,
    production_volumes INT,
    manufacturing_lead_time INT,
    manufacturing_costs DOUBLE,
    inspection_results STRING,
    defect_rates DOUBLE,
    transportation_modes STRING,
    routes STRING,
    costs DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- 5. Weather Data Table (sample data)
CREATE TABLE IF NOT EXISTS smartlogix.transport_data.weather_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    location STRING,
    date DATE,
    temperature DOUBLE,
    humidity DOUBLE,
    wind_speed DOUBLE,
    precipitation DOUBLE,
    weather_condition STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- 6. Inventory Data Table (sample data)
CREATE TABLE IF NOT EXISTS smartlogix.transport_data.inventory_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    product_id STRING,
    product_name STRING,
    category STRING,
    current_stock INT,
    min_stock_level INT,
    max_stock_level INT,
    unit_cost DOUBLE,
    supplier STRING,
    location STRING,
    last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- =====================================================
-- STEP 3: LOAD DATA (FREE EDITION - Manual Upload)
-- =====================================================

-- IMPORTANT: Upload files to DBFS first using Databricks UI
-- Path: /FileStore/shared_uploads/smartlogix/processed/

-- Load Distance data (usually small file)
COPY INTO smartlogix.transport_data.distance_data
FROM '/FileStore/shared_uploads/smartlogix/processed/distance_cleaned.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load Order data (small files)
COPY INTO smartlogix.transport_data.order_data
FROM '/FileStore/shared_uploads/smartlogix/processed/order_small_cleaned.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load large order data (if exists)
COPY INTO smartlogix.transport_data.order_data
FROM '/FileStore/shared_uploads/smartlogix/processed/order_large_cleaned.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load Supply Chain data
COPY INTO smartlogix.transport_data.supply_chain_data
FROM '/FileStore/shared_uploads/smartlogix/processed/supply_chain_cleaned.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load Weather data (sample)
COPY INTO smartlogix.transport_data.weather_data
FROM '/FileStore/shared_uploads/smartlogix/processed/weather_cleaned.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load Inventory data (sample)
COPY INTO smartlogix.transport_data.inventory_data
FROM '/FileStore/shared_uploads/smartlogix/processed/inventory_cleaned.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load Delhivery data (may be split into multiple parts)
-- If the data cleaner created multiple parts, load each one:

-- Part 1 (if exists)
COPY INTO smartlogix.transport_data.delhivery_data
FROM '/FileStore/shared_uploads/smartlogix/processed/delhivery_cleaned_part_1.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Part 2 (if exists)
COPY INTO smartlogix.transport_data.delhivery_data
FROM '/FileStore/shared_uploads/smartlogix/processed/delhivery_cleaned_part_2.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Part 3 (if exists)
COPY INTO smartlogix.transport_data.delhivery_data
FROM '/FileStore/shared_uploads/smartlogix/processed/delhivery_cleaned_part_3.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Continue for additional parts as needed...

-- =====================================================
-- STEP 4: CREATE ANALYTICS VIEWS (FREE EDITION)
-- =====================================================

-- Transport Summary View
CREATE OR REPLACE VIEW smartlogix.transport_data.transport_summary AS
SELECT 
    source_center,
    destination_center,
    COUNT(*) as trip_count,
    AVG(actual_time) as avg_actual_time,
    AVG(osrm_time) as avg_osrm_time,
    AVG(factor) as avg_factor,
    AVG(actual_distance_to_destination) as avg_distance
FROM smartlogix.transport_data.delhivery_data 
GROUP BY source_center, destination_center;

-- Order Summary View
CREATE OR REPLACE VIEW smartlogix.transport_data.order_summary AS
SELECT 
    source,
    destination,
    order_size,
    COUNT(*) as order_count,
    SUM(weight) as total_weight,
    AVG(EXTRACT(EPOCH FROM (deadline - available_time))/3600) as avg_hours_to_deadline
FROM smartlogix.transport_data.order_data 
GROUP BY source, destination, order_size;

-- Dashboard Analytics View (simplified for free edition)
CREATE OR REPLACE VIEW smartlogix.transport_data.dashboard_analytics AS
SELECT 
    'Total Transport Records' as metric,
    (SELECT COUNT(*) FROM smartlogix.transport_data.delhivery_data) as value
UNION ALL
SELECT 
    'Total Orders',
    (SELECT COUNT(*) FROM smartlogix.transport_data.order_data)
UNION ALL
SELECT 
    'Total Routes',
    (SELECT COUNT(DISTINCT CONCAT(source_center, '->', destination_center)) 
     FROM smartlogix.transport_data.delhivery_data)
UNION ALL
SELECT 
    'Average Efficiency',
    (SELECT ROUND(AVG(factor), 2) 
     FROM smartlogix.transport_data.delhivery_data 
     WHERE factor IS NOT NULL);

-- =====================================================
-- STEP 5: VERIFY DATA LOADING (FREE EDITION)
-- =====================================================

-- Check record counts
SELECT 'delhivery_data' as table_name, COUNT(*) as record_count 
FROM smartlogix.transport_data.delhivery_data
UNION ALL
SELECT 'distance_data', COUNT(*) FROM smartlogix.transport_data.distance_data
UNION ALL
SELECT 'order_data', COUNT(*) FROM smartlogix.transport_data.order_data
UNION ALL
SELECT 'supply_chain_data', COUNT(*) FROM smartlogix.transport_data.supply_chain_data
UNION ALL
SELECT 'weather_data', COUNT(*) FROM smartlogix.transport_data.weather_data
UNION ALL
SELECT 'inventory_data', COUNT(*) FROM smartlogix.transport_data.inventory_data;

-- Check data quality for Delhivery data (sample)
SELECT 
    COUNT(*) as total_records,
    COUNT(trip_uuid) as non_null_trip_uuid,
    ROUND(COUNT(trip_uuid) * 100.0 / COUNT(*), 2) as quality_score
FROM smartlogix.transport_data.delhivery_data
LIMIT 1000; -- Limit for free edition

-- Sample data from transport summary
SELECT * FROM smartlogix.transport_data.transport_summary LIMIT 10;

-- Sample data from order summary
SELECT * FROM smartlogix.transport_data.order_summary LIMIT 10;

-- Dashboard analytics
SELECT * FROM smartlogix.transport_data.dashboard_analytics;

-- =====================================================
-- FREE EDITION OPTIMIZED QUERIES
-- =====================================================

-- Top 10 most frequent routes (limited for free edition)
SELECT 
    source_center,
    destination_center,
    trip_count,
    avg_actual_time,
    avg_factor
FROM smartlogix.transport_data.transport_summary
ORDER BY trip_count DESC
LIMIT 10;

-- Orders by size and destination (sample)
SELECT 
    destination,
    order_size,
    COUNT(*) as order_count,
    SUM(weight) as total_weight
FROM smartlogix.transport_data.order_data
GROUP BY destination, order_size
ORDER BY total_weight DESC
LIMIT 20;

-- Route efficiency analysis (top 20)
SELECT 
    source_center,
    destination_center,
    avg_factor,
    CASE 
        WHEN avg_factor < 1.2 THEN 'Excellent'
        WHEN avg_factor < 1.5 THEN 'Good'
        WHEN avg_factor < 2.0 THEN 'Average'
        ELSE 'Poor'
    END as efficiency_rating
FROM smartlogix.transport_data.transport_summary
WHERE avg_factor IS NOT NULL
ORDER BY avg_factor ASC
LIMIT 20;

-- =====================================================
-- FREE EDITION TIPS
-- =====================================================

-- 1. Always use LIMIT in queries to save compute credits
-- 2. Stop the SQL warehouse when not in use
-- 3. Use sample data for testing and development
-- 4. Consider upgrading to paid tier for production use
-- 5. Monitor your compute usage in the Databricks UI

-- Example: Sample query with limit
SELECT * FROM smartlogix.transport_data.delhivery_data 
WHERE source_center = 'IND388121AAA' 
LIMIT 100;

-- Example: Aggregation with limit
SELECT 
    source_center,
    COUNT(*) as trip_count,
    AVG(actual_time) as avg_time
FROM smartlogix.transport_data.delhivery_data
GROUP BY source_center
ORDER BY trip_count DESC
LIMIT 10;

