-- Database initialization script
-- This script runs after schema.sql to set up initial data and configurations

-- Create database if it doesn't exist (this will be handled by Docker)
-- CREATE DATABASE IF NOT EXISTS smartlogix_transport;

-- Set timezone
SET timezone = 'UTC';

-- Create a user for the application (if needed)
-- CREATE USER smartlogix_app WITH PASSWORD 'app_password';
-- GRANT ALL PRIVILEGES ON DATABASE smartlogix_transport TO smartlogix_app;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO smartlogix_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO smartlogix_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO smartlogix_user;

-- Create a function to clean and validate data
CREATE OR REPLACE FUNCTION clean_numeric_value(input_text TEXT) 
RETURNS DECIMAL AS $$
BEGIN
    -- Remove any non-numeric characters except decimal point and minus sign
    input_text := REGEXP_REPLACE(input_text, '[^0-9.-]', '', 'g');
    
    -- Handle empty or invalid strings
    IF input_text = '' OR input_text IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Convert to decimal
    RETURN input_text::DECIMAL;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create a function to validate timestamps
CREATE OR REPLACE FUNCTION validate_timestamp(input_text TEXT) 
RETURNS TIMESTAMP AS $$
BEGIN
    IF input_text = '' OR input_text IS NULL OR input_text = 'nan' THEN
        RETURN NULL;
    END IF;
    
    RETURN input_text::TIMESTAMP;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get data quality metrics
CREATE OR REPLACE FUNCTION get_data_quality_metrics() 
RETURNS TABLE (
    table_name TEXT,
    total_records BIGINT,
    null_records BIGINT,
    quality_score DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'delhivery_data'::TEXT,
        COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE trip_uuid IS NULL)::BIGINT,
        ROUND((COUNT(*) FILTER (WHERE trip_uuid IS NOT NULL)::DECIMAL / COUNT(*)) * 100, 2)
    FROM delhivery_data
    UNION ALL
    SELECT 
        'distance_data'::TEXT,
        COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE source IS NULL OR destination IS NULL)::BIGINT,
        ROUND((COUNT(*) FILTER (WHERE source IS NOT NULL AND destination IS NOT NULL)::DECIMAL / COUNT(*)) * 100, 2)
    FROM distance_data
    UNION ALL
    SELECT 
        'order_data'::TEXT,
        COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE order_id IS NULL)::BIGINT,
        ROUND((COUNT(*) FILTER (WHERE order_id IS NOT NULL)::DECIMAL / COUNT(*)) * 100, 2)
    FROM order_data
    UNION ALL
    SELECT 
        'supply_chain_data'::TEXT,
        COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE sku IS NULL)::BIGINT,
        ROUND((COUNT(*) FILTER (WHERE sku IS NOT NULL)::DECIMAL / COUNT(*)) * 100, 2)
    FROM supply_chain_data;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get transport analytics
CREATE OR REPLACE FUNCTION get_transport_analytics() 
RETURNS TABLE (
    route VARCHAR,
    total_trips BIGINT,
    avg_actual_time DECIMAL,
    avg_osrm_time DECIMAL,
    efficiency_ratio DECIMAL,
    total_distance DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        CONCAT(d.source_center, ' -> ', d.destination_center) as route,
        COUNT(*) as total_trips,
        ROUND(AVG(d.actual_time), 2) as avg_actual_time,
        ROUND(AVG(d.osrm_time), 2) as avg_osrm_time,
        ROUND(AVG(d.factor), 2) as efficiency_ratio,
        ROUND(AVG(d.actual_distance_to_destination), 2) as total_distance
    FROM delhivery_data d
    GROUP BY d.source_center, d.destination_center
    ORDER BY total_trips DESC;
END;
$$ LANGUAGE plpgsql;

-- Insert some sample data for testing (optional)
-- This can be removed if you want to start with empty tables
INSERT INTO weather_data (location, date, temperature, humidity, wind_speed, precipitation, weather_condition) 
VALUES 
    ('Mumbai', CURRENT_DATE, 28.5, 75.0, 12.3, 0.0, 'Clear'),
    ('Delhi', CURRENT_DATE, 32.1, 65.0, 8.7, 0.0, 'Sunny'),
    ('Bangalore', CURRENT_DATE, 26.8, 80.0, 15.2, 2.5, 'Cloudy')
ON CONFLICT DO NOTHING;

-- Create a view for dashboard analytics
CREATE OR REPLACE VIEW dashboard_analytics AS
SELECT 
    'Total Records' as metric,
    (SELECT COUNT(*) FROM delhivery_data) as value
UNION ALL
SELECT 
    'Total Orders',
    (SELECT COUNT(*) FROM order_data)
UNION ALL
SELECT 
    'Total Routes',
    (SELECT COUNT(DISTINCT CONCAT(source_center, '->', destination_center)) FROM delhivery_data)
UNION ALL
SELECT 
    'Average Efficiency',
    (SELECT ROUND(AVG(factor), 2) FROM delhivery_data WHERE factor IS NOT NULL);

-- Create a materialized view for performance optimization
CREATE MATERIALIZED VIEW IF NOT EXISTS route_performance AS
SELECT 
    source_center,
    destination_center,
    COUNT(*) as trip_count,
    AVG(actual_time) as avg_actual_time,
    AVG(osrm_time) as avg_osrm_time,
    AVG(factor) as avg_efficiency,
    AVG(actual_distance_to_destination) as avg_distance,
    MIN(actual_time) as min_time,
    MAX(actual_time) as max_time,
    STDDEV(actual_time) as time_stddev
FROM delhivery_data
WHERE actual_time IS NOT NULL
GROUP BY source_center, destination_center;

-- Create index on materialized view
CREATE INDEX IF NOT EXISTS idx_route_performance_source_dest ON route_performance(source_center, destination_center);

-- Create a function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW route_performance;
END;
$$ LANGUAGE plpgsql;

-- Set up logging for data quality monitoring
CREATE TABLE IF NOT EXISTS data_quality_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    check_type VARCHAR(100),
    check_result TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a function to log data quality issues
CREATE OR REPLACE FUNCTION log_data_quality_issue(
    p_table_name VARCHAR,
    p_check_type VARCHAR,
    p_result TEXT
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO data_quality_log (table_name, check_type, check_result)
    VALUES (p_table_name, p_check_type, p_result);
END;
$$ LANGUAGE plpgsql;
