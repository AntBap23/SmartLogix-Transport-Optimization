WITH orders AS (
    SELECT * 
    FROM transport_data.order_data
),
delivery AS (
    SELECT *
    FROM transport_data.delhivery_data
),
distance AS (
    SELECT * 
    FROM transport_data.distance_data
),
inventory AS (
    SELECT * 
    FROM transport_data.inventory_data
),
supply AS (
    SELECT *
    FROM transport_data.supply_chain_data
),
weather AS (
    SELECT *
    FROM transport_data.weather_data
)

SELECT
    /* -------------------------------------------------
       Single clean merged ORDER ID (not ambiguous)
    -------------------------------------------------- */
    COALESCE(d.order_id, o.order_id) AS order_id,

    /* -------------------------------------------------
       DELIVERY (delhivery_data) — all columns EXCEPT order_id
    -------------------------------------------------- */
    d.id AS delivery_id,
    d.data_type,
    d.trip_creation_time,
    d.route_schedule_uuid,
    d.route_type,
    d.trip_uuid,
    d.source_center,
    d.source_name,
    d.destination_center,
    d.destination_name,
    d.actual_distance_to_destination,
    d.actual_time,
    d.osrm_time,
    d.osrm_distance,
    d.factor,
    d.segment_actual_time,
    d.segment_osrm_time,
    d.segment_osrm_distance,
    d.segment_factor,
    d.created_at AS delivery_created_at,

    /* -------------------------------------------------
       ORDER DATA — all columns EXCEPT order_id
    -------------------------------------------------- */
    o.id AS order_row_id,
    o.material_id,
    o.item_id,
    o.source AS order_source,
    o.destination AS order_destination,
    o.available_time,
    o.deadline,
    o.danger_type,
    o.area,
    o.weight,
    o.order_size,
    o.created_at AS order_created_at,

    /* -------------------------------------------------
       EXTRA JOINED COLUMNS
    -------------------------------------------------- */
    dt.distance_meters AS expected_distance,
    i.current_stock,
    s.price AS supply_chain_price,
    w.weather_condition

FROM delivery d
LEFT JOIN orders o
    ON o.order_id = d.order_id
LEFT JOIN distance dt
    ON d.source_center      = dt.source
   AND d.destination_center = dt.destination
LEFT JOIN inventory i
    ON o.material_id = i.product_id
   AND o.source      = i.location
LEFT JOIN supply s
    ON COALESCE(i.product_id, o.material_id) = s.sku
LEFT JOIN weather w
    ON (DATE(d.trip_creation_time) = w.date AND d.source_center = w.location)
    OR (DATE(o.available_time)     = w.date AND o.source         = w.location)

WHERE d.order_id IS NOT NULL

ORDER BY COALESCE(d.order_id, o.order_id);

