-- SmartLogix Transport Optimization Database Schema
-- PostgreSQL Database Schema for Transport Optimization System

-- Create database (run this separately)
-- CREATE DATABASE smartlogix_transport;

-- Connect to the database
-- \c smartlogix_transport;

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create tables for different data sources

-- 1. Delhivery Data Table
CREATE TABLE IF NOT EXISTS delhivery_data (
    id SERIAL PRIMARY KEY,
    data_type VARCHAR(50),
    trip_creation_time TIMESTAMP,
    route_schedule_uuid VARCHAR(255),
    route_type VARCHAR(100),
    trip_uuid VARCHAR(255),
    source_center VARCHAR(100),
    source_name VARCHAR(255),
    destination_center VARCHAR(100),
    destination_name VARCHAR(255),
    od_start_time TIMESTAMP,
    od_end_time TIMESTAMP,
    start_scan_to_end_scan DECIMAL(10,2),
    is_cutoff BOOLEAN,
    cutoff_factor INTEGER,
    cutoff_timestamp TIMESTAMP,
    actual_distance_to_destination DECIMAL(15,6),
    actual_time DECIMAL(10,2),
    osrm_time DECIMAL(10,2),
    osrm_distance DECIMAL(15,6),
    factor DECIMAL(10,6),
    segment_actual_time DECIMAL(10,2),
    segment_osrm_time DECIMAL(10,2),
    segment_osrm_distance DECIMAL(15,6),
    segment_factor DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Distance Data Table
CREATE TABLE IF NOT EXISTS distance_data (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100),
    destination VARCHAR(100),
    distance_meters INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Order Data Table (for both small and large orders)
CREATE TABLE IF NOT EXISTS order_data (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(100),
    material_id VARCHAR(100),
    item_id VARCHAR(255),
    source VARCHAR(100),
    destination VARCHAR(100),
    available_time TIMESTAMP,
    deadline TIMESTAMP,
    danger_type VARCHAR(50),
    area DECIMAL(15,2),
    weight DECIMAL(15,2),
    order_size VARCHAR(20) DEFAULT 'small', -- 'small' or 'large'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Supply Chain Data Table
CREATE TABLE IF NOT EXISTS supply_chain_data (
    id SERIAL PRIMARY KEY,
    product_type VARCHAR(100),
    sku VARCHAR(100),
    price DECIMAL(15,6),
    availability INTEGER,
    number_of_products_sold INTEGER,
    revenue_generated DECIMAL(15,6),
    customer_demographics VARCHAR(50),
    stock_levels INTEGER,
    lead_times INTEGER,
    order_quantities INTEGER,
    shipping_times INTEGER,
    shipping_carriers VARCHAR(100),
    shipping_costs DECIMAL(15,6),
    supplier_name VARCHAR(100),
    location VARCHAR(100),
    lead_time INTEGER,
    production_volumes INTEGER,
    manufacturing_lead_time INTEGER,
    manufacturing_costs DECIMAL(15,6),
    inspection_results VARCHAR(50),
    defect_rates DECIMAL(10,6),
    transportation_modes VARCHAR(50),
    routes VARCHAR(100),
    costs DECIMAL(15,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Weather Data Table (for future weather integration)
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    location VARCHAR(100),
    date DATE,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    wind_speed DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    weather_condition VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Inventory Data Table (for Excel files)
CREATE TABLE IF NOT EXISTS inventory_data (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    category VARCHAR(100),
    current_stock INTEGER,
    min_stock_level INTEGER,
    max_stock_level INTEGER,
    unit_cost DECIMAL(10,2),
    supplier VARCHAR(100),
    location VARCHAR(100),
    last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_delhivery_trip_uuid ON delhivery_data(trip_uuid);
CREATE INDEX IF NOT EXISTS idx_delhivery_source_dest ON delhivery_data(source_center, destination_center);
CREATE INDEX IF NOT EXISTS idx_delhivery_trip_creation ON delhivery_data(trip_creation_time);

CREATE INDEX IF NOT EXISTS idx_distance_source_dest ON distance_data(source, destination);

CREATE INDEX IF NOT EXISTS idx_order_id ON order_data(order_id);
CREATE INDEX IF NOT EXISTS idx_order_source_dest ON order_data(source, destination);
CREATE INDEX IF NOT EXISTS idx_order_deadline ON order_data(deadline);

CREATE INDEX IF NOT EXISTS idx_supply_chain_sku ON supply_chain_data(sku);
CREATE INDEX IF NOT EXISTS idx_supply_chain_location ON supply_chain_data(location);
CREATE INDEX IF NOT EXISTS idx_supply_chain_supplier ON supply_chain_data(supplier_name);

CREATE INDEX IF NOT EXISTS idx_weather_location_date ON weather_data(location, date);

CREATE INDEX IF NOT EXISTS idx_inventory_product_id ON inventory_data(product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_location ON inventory_data(location);

-- Create views for common queries
CREATE OR REPLACE VIEW transport_summary AS
SELECT 
    source_center,
    destination_center,
    COUNT(*) as trip_count,
    AVG(actual_time) as avg_actual_time,
    AVG(osrm_time) as avg_osrm_time,
    AVG(factor) as avg_factor,
    AVG(actual_distance_to_destination) as avg_distance
FROM delhivery_data 
GROUP BY source_center, destination_center;

CREATE OR REPLACE VIEW order_summary AS
SELECT 
    source,
    destination,
    COUNT(*) as order_count,
    SUM(weight) as total_weight,
    AVG(EXTRACT(EPOCH FROM (deadline - available_time))/3600) as avg_hours_to_deadline
FROM order_data 
GROUP BY source, destination;

-- Create function to calculate transport efficiency
CREATE OR REPLACE FUNCTION calculate_transport_efficiency(
    p_source VARCHAR,
    p_destination VARCHAR
) RETURNS DECIMAL AS $$
DECLARE
    efficiency DECIMAL;
BEGIN
    SELECT AVG(factor) INTO efficiency
    FROM delhivery_data 
    WHERE source_center = p_source 
    AND destination_center = p_destination;
    
    RETURN COALESCE(efficiency, 0);
END;
$$ LANGUAGE plpgsql;

-- Create function to get route distance
CREATE OR REPLACE FUNCTION get_route_distance(
    p_source VARCHAR,
    p_destination VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    distance INTEGER;
BEGIN
    SELECT distance_meters INTO distance
    FROM distance_data 
    WHERE source = p_source 
    AND destination = p_destination;
    
    RETURN COALESCE(distance, 0);
END;
$$ LANGUAGE plpgsql;

