-- ============================================================
-- SQL QUERIES FOR ROUTE OPTIMIZER
-- Copy and run these in your database, then export to CSV
-- ============================================================

-- ============================================================
-- 1. ORDERS DATA (Required for route optimization)
-- Export as: optimizer_orders.csv
-- ============================================================

SELECT 
    order_id,
    source,
    destination,
    available_time,
    deadline,
    weight,
    order_size,
    material_id
FROM transport_data.order_data
ORDER BY available_time;

-- ============================================================
-- 2. DISTANCE DATA (Required for distance matrix)
-- Export as: optimizer_distances.csv
-- ============================================================

SELECT 
    source,
    destination,
    distance_meters
FROM transport_data.distance_data
ORDER BY source, destination;

-- ============================================================
-- OPTIONAL: If you want to limit for testing
-- ============================================================
-- Add LIMIT 1000 to the orders query above if the file is too large

