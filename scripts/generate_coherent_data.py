#!/usr/bin/env python3
"""
Generate Coherent Connected Data for Last Mile Delivery Optimization

This script generates synthetic data where all tables connect:
- All locations match across tables
- All product IDs match across tables
- No NULL columns
- All data is coherent and ready for analysis
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Output directory
OUTPUT_DIR = Path("data/coherent")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# MASTER DATA - Core entities that connect everything
# ============================================================================

# Locations (warehouses, distribution centers, delivery zones)
LOCATIONS = [
    "Mumbai_Warehouse", "Delhi_DC", "Bangalore_Hub", "Chennai_Distribution",
    "Kolkata_Center", "Hyderabad_WH", "Pune_Facility", "Ahmedabad_DC",
    "Jaipur_Hub", "Lucknow_Center", "Kanpur_WH", "Nagpur_Distribution",
    "Indore_Facility", "Bhopal_DC", "Surat_Hub", "Vadodara_Center",
    "Patna_WH", "Ranchi_Distribution", "Chandigarh_Facility", "Amritsar_DC"
]

# Products (consistent IDs across all tables)
PRODUCTS = [
    {"product_id": "PROD001", "name": "Electronics_Phone", "category": "Electronics", "weight_kg": 0.5},
    {"product_id": "PROD002", "name": "Electronics_Laptop", "category": "Electronics", "weight_kg": 2.5},
    {"product_id": "PROD003", "name": "Electronics_Tablet", "category": "Electronics", "weight_kg": 0.8},
    {"product_id": "PROD004", "name": "Clothing_Shirt", "category": "Apparel", "weight_kg": 0.3},
    {"product_id": "PROD005", "name": "Clothing_Pants", "category": "Apparel", "weight_kg": 0.4},
    {"product_id": "PROD006", "name": "Clothing_Shoes", "category": "Apparel", "weight_kg": 0.6},
    {"product_id": "PROD007", "name": "Food_Groceries", "category": "Food", "weight_kg": 5.0},
    {"product_id": "PROD008", "name": "Food_Beverages", "category": "Food", "weight_kg": 2.0},
    {"product_id": "PROD009", "name": "Home_Furniture", "category": "Home", "weight_kg": 15.0},
    {"product_id": "PROD010", "name": "Home_Appliances", "category": "Home", "weight_kg": 8.0},
    {"product_id": "PROD011", "name": "Books_Novel", "category": "Books", "weight_kg": 0.5},
    {"product_id": "PROD012", "name": "Books_Textbook", "category": "Books", "weight_kg": 1.2},
    {"product_id": "PROD013", "name": "Sports_Equipment", "category": "Sports", "weight_kg": 3.0},
    {"product_id": "PROD014", "name": "Toys_Games", "category": "Toys", "weight_kg": 1.0},
    {"product_id": "PROD015", "name": "Health_Supplements", "category": "Health", "weight_kg": 0.3}
]

# Suppliers
SUPPLIERS = ["Supplier_A", "Supplier_B", "Supplier_C", "Supplier_D", "Supplier_E"]

# ============================================================================
# 1. DISTANCE DATA - Distance matrix for all location pairs
# ============================================================================

def generate_distance_data():
    """Generate distance matrix between all locations."""
    logger.info("Generating distance_data...")
    
    distances = []
    base_distances = {}  # Cache for consistency
    
    for source in LOCATIONS:
        for destination in LOCATIONS:
            if source == destination:
                distance = 0
            else:
                # Use consistent distance calculation
                key = tuple(sorted([source, destination]))
                if key not in base_distances:
                    # Generate realistic distances (50-1500 km)
                    base_distances[key] = random.randint(50, 1500)
                distance = base_distances[key]
            
            distances.append({
                "source": source,
                "destination": destination,
                "distance_meters": distance * 1000  # Convert km to meters
            })
    
    df = pd.DataFrame(distances)
    output_path = OUTPUT_DIR / "distance_cleaned.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"✅ Generated {len(df)} distance records -> {output_path}")
    return df

# ============================================================================
# 2. INVENTORY DATA - Products at locations
# ============================================================================

def generate_inventory_data():
    """Generate inventory data with products at locations."""
    logger.info("Generating inventory_data...")
    
    inventory = []
    
    for location in LOCATIONS:
        # Each location has 5-10 products
        location_products = random.sample(PRODUCTS, random.randint(5, 10))
        
        for product in location_products:
            current_stock = random.randint(100, 5000)
            min_stock = int(current_stock * 0.2)
            max_stock = int(current_stock * 1.5)
            unit_cost = round(random.uniform(100, 5000), 2)
            supplier = random.choice(SUPPLIERS)
            
            inventory.append({
                "product_id": product["product_id"],
                "product_name": product["name"],
                "category": product["category"],
                "current_stock": current_stock,
                "min_stock_level": min_stock,
                "max_stock_level": max_stock,
                "unit_cost": unit_cost,
                "supplier": supplier,
                "location": location,
                "last_updated": (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S")
            })
    
    df = pd.DataFrame(inventory)
    output_path = OUTPUT_DIR / "inventory_cleaned.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"✅ Generated {len(df)} inventory records -> {output_path}")
    return df

# ============================================================================
# 3. ORDER DATA - Orders with matching locations and products
# ============================================================================

def generate_order_data(num_orders=30000):
    """Generate order data with matching locations and products."""
    logger.info(f"Generating order_data ({num_orders} orders)...")
    
    orders = []
    
    for i in range(num_orders):
        # Random source and destination (different locations)
        source = random.choice(LOCATIONS)
        destination = random.choice([loc for loc in LOCATIONS if loc != source])
        
        # Random product from our product list
        product = random.choice(PRODUCTS)
        
        # Order timing
        available_time = datetime.now() - timedelta(days=random.randint(0, 60))
        deadline = available_time + timedelta(hours=random.randint(24, 168))  # 1-7 days
        
        # Order details
        order_id = f"ORD{str(i+1).zfill(6)}"
        material_id = product["product_id"]
        item_id = f"ITEM{str(i+1).zfill(6)}"
        weight = round(product["weight_kg"] * random.uniform(0.5, 3.0), 2)  # Multiple units
        area = round(weight * random.uniform(0.1, 0.5), 2)  # m²
        order_size = random.choice(["small", "medium", "large"])
        danger_type = random.choice(["Normal", "Fragile", "Hazardous", "Perishable"])
        
        orders.append({
            "order_id": order_id,
            "material_id": material_id,
            "item_id": item_id,
            "source": source,
            "destination": destination,
            "available_time": available_time.strftime("%Y-%m-%d %H:%M:%S"),
            "deadline": deadline.strftime("%Y-%m-%d %H:%M:%S"),
            "danger_type": danger_type,
            "area": area,
            "weight": weight,
            "order_size": order_size
        })
    
    df = pd.DataFrame(orders)
    output_path = OUTPUT_DIR / "orders_cleaned.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"✅ Generated {len(df)} order records -> {output_path}")
    return df

# ============================================================================
# 4. DELHIVERY DATA - Delivery routes with proper order relationships
# ============================================================================

def generate_delhivery_data(num_trips=800000, orders_df=None):
    """
    Generate delivery trip data with proper order relationships.
    
    Following SQL database principles:
    - order_id: Links trips to orders (NULL for non-order trips like transfers)
    - Supports one-to-many: One order can have multiple trips (partial deliveries)
    - Supports many-to-one: One trip can carry multiple orders (consolidated)
    - For simplicity, each trip row links to ONE order_id (for consolidated trips,
      create multiple trip rows with same trip_uuid but different order_id)
    """
    logger.info(f"Generating delhivery_data ({num_trips} trips)...")
    
    trips = []
    
    # Build order lookup by route for efficient matching
    order_lookup = {}
    if orders_df is not None and len(orders_df) > 0:
        for _, order in orders_df.iterrows():
            route_key = (order['source'], order['destination'])
            if route_key not in order_lookup:
                order_lookup[route_key] = []
            order_lookup[route_key].append({
                'order_id': order['order_id'],
                'available_time': datetime.strptime(order['available_time'], "%Y-%m-%d %H:%M:%S"),
                'deadline': datetime.strptime(order['deadline'], "%Y-%m-%d %H:%M:%S")
            })
    
    # Generate route pairs for non-order trips (transfers, empty trips)
    route_pairs = []
    for _ in range(min(1000, num_trips // 100)):  # 1000 unique routes
        source = random.choice(LOCATIONS)
        destination = random.choice([loc for loc in LOCATIONS if loc != source])
        route_pairs.append((source, destination))
    
    trip_counter = 0
    
    # First, generate trips linked to orders (70% of trips)
    num_order_trips = int(num_trips * 0.7)
    if orders_df is not None and len(orders_df) > 0:
        order_list = orders_df.to_dict('records')
        
        for i in range(num_order_trips):
            # Pick a random order
            order = random.choice(order_list)
            source = order['source']
            destination = order['destination']
            
            # Some orders get multiple trips (partial deliveries) - 20% chance
            is_partial_delivery = random.random() < 0.2 and i < num_order_trips - 1
            
            # Trip timing - should be after order available_time
            order_available = datetime.strptime(order['available_time'], "%Y-%m-%d %H:%M:%S")
            trip_creation = order_available + timedelta(hours=random.randint(0, 24))
            od_start = trip_creation + timedelta(minutes=random.randint(30, 180))
            od_end = od_start + timedelta(hours=random.randint(1, 8))
            
            # Get distance for this route
            if source == destination:
                distance = 0
            else:
                # Use consistent distance calculation
                distance_km = random.randint(50, 1500)
                distance = distance_km * 1000  # meters
            
            # Calculate times based on distance
            avg_speed_kmh = random.uniform(35, 65)
            expected_time_hours = distance / 1000 / avg_speed_kmh
            actual_time = expected_time_hours * random.uniform(0.8, 1.3)
            osrm_time = expected_time_hours
            
            factor = actual_time / osrm_time if osrm_time > 0 else 1.0
            
            # Segment data
            segment_actual_time = actual_time * random.uniform(0.3, 0.7)
            segment_osrm_time = osrm_time * random.uniform(0.3, 0.7)
            segment_osrm_distance = distance * random.uniform(0.3, 0.7)
            segment_factor = segment_actual_time / segment_osrm_time if segment_osrm_time > 0 else 1.0
            
            # Determine if this is a delivery, pickup, or transfer
            data_type = "Delivery" if random.random() > 0.3 else "Pickup"
            
            trips.append({
                "order_id": order['order_id'],  # Link to order
                "data_type": data_type,
                "trip_creation_time": trip_creation.strftime("%Y-%m-%d %H:%M:%S"),
                "route_schedule_uuid": f"ROUTE{str(trip_counter+1).zfill(8)}",
                "route_type": random.choice(["Standard", "Express", "Economy"]),
                "trip_uuid": f"TRIP{str(trip_counter+1).zfill(8)}",
                "source_center": source,
                "source_name": source.replace("_", " "),
                "destination_center": destination,
                "destination_name": destination.replace("_", " "),
                "od_start_time": od_start.strftime("%Y-%m-%d %H:%M:%S"),
                "od_end_time": od_end.strftime("%Y-%m-%d %H:%M:%S"),
                "start_scan_to_end_scan": (od_end - od_start).total_seconds() / 3600,
                "is_cutoff": random.choice([True, False]),
                "cutoff_factor": random.randint(0, 100),
                "cutoff_timestamp": (od_start - timedelta(hours=random.randint(1, 12))).strftime("%Y-%m-%d %H:%M:%S"),
                "actual_distance_to_destination": round(distance, 2),
                "actual_time": round(actual_time, 2),
                "osrm_time": round(osrm_time, 2),
                "osrm_distance": round(distance, 2),
                "factor": round(factor, 3),
                "segment_actual_time": round(segment_actual_time, 2),
                "segment_osrm_time": round(segment_osrm_time, 2),
                "segment_osrm_distance": round(segment_osrm_distance, 2),
                "segment_factor": round(segment_factor, 3)
            })
            trip_counter += 1
            
            # For partial deliveries, create a second trip for the same order
            if is_partial_delivery:
                trip_creation2 = od_end + timedelta(hours=random.randint(1, 24))
                od_start2 = trip_creation2 + timedelta(minutes=random.randint(30, 180))
                od_end2 = od_start2 + timedelta(hours=random.randint(1, 8))
                
                trips.append({
                    "order_id": order['order_id'],  # Same order, different trip
                    "data_type": data_type,
                    "trip_creation_time": trip_creation2.strftime("%Y-%m-%d %H:%M:%S"),
                    "route_schedule_uuid": f"ROUTE{str(trip_counter+1).zfill(8)}",
                    "route_type": random.choice(["Standard", "Express", "Economy"]),
                    "trip_uuid": f"TRIP{str(trip_counter+1).zfill(8)}",
                    "source_center": source,
                    "source_name": source.replace("_", " "),
                    "destination_center": destination,
                    "destination_name": destination.replace("_", " "),
                    "od_start_time": od_start2.strftime("%Y-%m-%d %H:%M:%S"),
                    "od_end_time": od_end2.strftime("%Y-%m-%d %H:%M:%S"),
                    "start_scan_to_end_scan": (od_end2 - od_start2).total_seconds() / 3600,
                    "is_cutoff": random.choice([True, False]),
                    "cutoff_factor": random.randint(0, 100),
                    "cutoff_timestamp": (od_start2 - timedelta(hours=random.randint(1, 12))).strftime("%Y-%m-%d %H:%M:%S"),
                    "actual_distance_to_destination": round(distance, 2),
                    "actual_time": round(actual_time * random.uniform(0.9, 1.1), 2),
                    "osrm_time": round(osrm_time, 2),
                    "osrm_distance": round(distance, 2),
                    "factor": round(factor * random.uniform(0.9, 1.1), 3),
                    "segment_actual_time": round(segment_actual_time * random.uniform(0.9, 1.1), 2),
                    "segment_osrm_time": round(segment_osrm_time, 2),
                    "segment_osrm_distance": round(segment_osrm_distance, 2),
                    "segment_factor": round(segment_factor * random.uniform(0.9, 1.1), 3)
                })
                trip_counter += 1
                i += 1  # Skip one iteration since we added an extra trip
    
    # Generate remaining trips without orders (transfers, empty trips, repositioning)
    num_non_order_trips = num_trips - trip_counter
    for i in range(num_non_order_trips):
        source, destination = random.choice(route_pairs)
        
        # Trip timing
        trip_creation = datetime.now() - timedelta(days=random.randint(0, 90))
        od_start = trip_creation + timedelta(minutes=random.randint(30, 180))
        od_end = od_start + timedelta(hours=random.randint(1, 8))
        
        # Get distance
        if source == destination:
            distance = 0
        else:
            distance_km = random.randint(50, 1500)
            distance = distance_km * 1000
        
        # Calculate times
        avg_speed_kmh = random.uniform(35, 65)
        expected_time_hours = distance / 1000 / avg_speed_kmh
        actual_time = expected_time_hours * random.uniform(0.8, 1.3)
        osrm_time = expected_time_hours
        factor = actual_time / osrm_time if osrm_time > 0 else 1.0
        
        # Segment data
        segment_actual_time = actual_time * random.uniform(0.3, 0.7)
        segment_osrm_time = osrm_time * random.uniform(0.3, 0.7)
        segment_osrm_distance = distance * random.uniform(0.3, 0.7)
        segment_factor = segment_actual_time / segment_osrm_time if segment_osrm_time > 0 else 1.0
        
        trips.append({
            "order_id": None,  # No order - transfer or empty trip
            "data_type": "Transfer",  # Transfers don't have orders
            "trip_creation_time": trip_creation.strftime("%Y-%m-%d %H:%M:%S"),
            "route_schedule_uuid": f"ROUTE{str(trip_counter+1).zfill(8)}",
            "route_type": random.choice(["Standard", "Express", "Economy"]),
            "trip_uuid": f"TRIP{str(trip_counter+1).zfill(8)}",
            "source_center": source,
            "source_name": source.replace("_", " "),
            "destination_center": destination,
            "destination_name": destination.replace("_", " "),
            "od_start_time": od_start.strftime("%Y-%m-%d %H:%M:%S"),
            "od_end_time": od_end.strftime("%Y-%m-%d %H:%M:%S"),
            "start_scan_to_end_scan": (od_end - od_start).total_seconds() / 3600,
            "is_cutoff": random.choice([True, False]),
            "cutoff_factor": random.randint(0, 100),
            "cutoff_timestamp": (od_start - timedelta(hours=random.randint(1, 12))).strftime("%Y-%m-%d %H:%M:%S"),
            "actual_distance_to_destination": round(distance, 2),
            "actual_time": round(actual_time, 2),
            "osrm_time": round(osrm_time, 2),
            "osrm_distance": round(distance, 2),
            "factor": round(factor, 3),
            "segment_actual_time": round(segment_actual_time, 2),
            "segment_osrm_time": round(segment_osrm_time, 2),
            "segment_osrm_distance": round(segment_osrm_distance, 2),
            "segment_factor": round(segment_factor, 3)
        })
        trip_counter += 1
    
    df = pd.DataFrame(trips)
    output_path = OUTPUT_DIR / "delhivery_cleaned.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"✅ Generated {len(df)} delivery trip records -> {output_path}")
    logger.info(f"   - Trips with orders: {df['order_id'].notna().sum()}")
    logger.info(f"   - Trips without orders: {df['order_id'].isna().sum()}")
    return df

# ============================================================================
# 5. WEATHER DATA - Weather conditions by location and date
# ============================================================================

def generate_weather_data(num_days=90):
    """Generate weather data for all locations matching the date range of orders/deliveries."""
    logger.info(f"Generating weather_data ({num_days} days)...")
    
    weather_conditions = ["Clear", "Cloudy", "Rain", "Snow", "Fog", "Thunderstorm"]
    weather = []
    
    # Generate weather for each location for the past num_days days
    start_date = datetime.now() - timedelta(days=num_days)
    
    for location in LOCATIONS:
        for day_offset in range(num_days):
            date = start_date + timedelta(days=day_offset)
            
            # Generate daily weather (can add hourly if needed)
            condition = random.choice(weather_conditions)
            
            # Temperature varies by condition and season
            if condition in ["Clear", "Cloudy"]:
                base_temp = random.uniform(15, 35)  # Moderate temperatures
            elif condition == "Rain":
                base_temp = random.uniform(10, 25)  # Cooler with rain
            elif condition == "Snow":
                base_temp = random.uniform(-5, 10)  # Cold with snow
            elif condition == "Fog":
                base_temp = random.uniform(5, 20)  # Cool with fog
            else:  # Thunderstorm
                base_temp = random.uniform(20, 30)  # Warm with storms
            
            temperature = round(base_temp, 1)
            precipitation = round(random.uniform(0, 10) if condition in ["Rain", "Thunderstorm", "Snow"] else random.uniform(0, 1), 1)
            wind_speed = round(random.uniform(0, 25), 1)
            
            weather.append({
                "date": date.strftime("%Y-%m-%d"),
                "location": location,
                "temperature": temperature,
                "precipitation": precipitation,
                "wind_speed": wind_speed,
                "weather_condition": condition
            })
    
    df = pd.DataFrame(weather)
    output_path = OUTPUT_DIR / "weather_cleaned.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"✅ Generated {len(df)} weather records -> {output_path}")
    return df

# ============================================================================
# 6. SUPPLY CHAIN DATA - Products with matching SKUs
# ============================================================================

def generate_supply_chain_data():
    """Generate supply chain data with matching product SKUs."""
    logger.info("Generating supply_chain_data...")
    
    supply_chain = []
    
    for product in PRODUCTS:
        for supplier in SUPPLIERS:
            # Not all suppliers have all products
            if random.random() > 0.3:  # 70% chance supplier has this product
                location = random.choice(LOCATIONS)
                
                supply_chain.append({
                    "product_type": product["category"],
                    "sku": product["product_id"],
                    "price": round(random.uniform(500, 5000), 2),
                    "availability": random.randint(100, 1000),
                    "number_of_products_sold": random.randint(50, 500),
                    "revenue_generated": round(random.uniform(50000, 500000), 2),
                    "customer_demographics": random.choice(["Urban", "Rural", "Mixed"]),
                    "stock_levels": random.randint(200, 2000),
                    "lead_times": random.randint(1, 14),  # days
                    "order_quantities": random.randint(100, 1000),
                    "shipping_times": random.randint(1, 7),  # days
                    "shipping_carriers": random.choice(["FedEx", "DHL", "UPS", "Local"]),
                    "shipping_costs": round(random.uniform(50, 500), 2),
                    "supplier_name": supplier,
                    "location": location,
                    "lead_time": random.randint(1, 14),
                    "production_volumes": random.randint(1000, 10000),
                    "manufacturing_lead_time": random.randint(7, 30),
                    "manufacturing_costs": round(random.uniform(100, 1000), 2),
                    "inspection_results": random.choice(["Pass", "Pass", "Pass", "Minor Issues"]),
                    "defect_rates": round(random.uniform(0.1, 2.0), 2),
                    "transportation_modes": random.choice(["Truck", "Van", "Bike", "Truck"]),
                    "routes": f"{location}_to_{random.choice([loc for loc in LOCATIONS if loc != location])}",
                    "costs": round(random.uniform(1000, 10000), 2)
                })
    
    df = pd.DataFrame(supply_chain)
    output_path = OUTPUT_DIR / "supply_chain_cleaned.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"✅ Generated {len(df)} supply chain records -> {output_path}")
    return df

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Generate all coherent connected data."""
    logger.info("=" * 60)
    logger.info("Generating Coherent Connected Data for Last Mile Delivery")
    logger.info("=" * 60)
    
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    # Generate all data (order matters: orders must be generated before delivery trips)
    distance_df = generate_distance_data()
    inventory_df = generate_inventory_data()
    order_df = generate_order_data(num_orders=30000)
    # Pass orders to delivery generation so trips can be linked to orders
    delhivery_df = generate_delhivery_data(num_trips=800000, orders_df=order_df)
    weather_df = generate_weather_data(num_days=90)
    supply_chain_df = generate_supply_chain_data()
    
    # Verify connections
    logger.info("\n" + "=" * 60)
    logger.info("Verifying Data Connections")
    logger.info("=" * 60)
    
    # Check location matches
    delhivery_locations = set(delhivery_df['source_center'].unique()) | set(delhivery_df['destination_center'].unique())
    order_locations = set(order_df['source'].unique()) | set(order_df['destination'].unique())
    distance_locations = set(distance_df['source'].unique()) | set(distance_df['destination'].unique())
    inventory_locations = set(inventory_df['location'].unique())
    
    logger.info(f"✅ delhivery_data locations: {len(delhivery_locations)}")
    logger.info(f"✅ order_data locations: {len(order_locations)}")
    logger.info(f"✅ distance_data locations: {len(distance_locations)}")
    logger.info(f"✅ inventory_data locations: {len(inventory_locations)}")
    
    # Check if all locations match
    all_locations_match = (
        delhivery_locations == order_locations == distance_locations == set(LOCATIONS)
    )
    logger.info(f"✅ All location sets match: {all_locations_match}")
    
    # Check product matches
    inventory_products = set(inventory_df['product_id'].unique())
    order_products = set(order_df['material_id'].unique())
    supply_chain_products = set(supply_chain_df['sku'].unique())
    
    logger.info(f"✅ inventory_data products: {len(inventory_products)}")
    logger.info(f"✅ order_data products: {len(order_products)}")
    logger.info(f"✅ supply_chain_data products: {len(supply_chain_products)}")
    
    all_products_match = (
        inventory_products == order_products == supply_chain_products == set([p['product_id'] for p in PRODUCTS])
    )
    logger.info(f"✅ All product sets match: {all_products_match}")
    
    # Check for NULLs
    logger.info("\nChecking for NULL values...")
    for name, df in [("distance", distance_df), ("inventory", inventory_df), 
                     ("order", order_df), ("delhivery", delhivery_df), 
                     ("weather", weather_df), ("supply_chain", supply_chain_df)]:
        null_counts = df.isnull().sum()
        total_nulls = null_counts.sum()
        if total_nulls > 0:
            logger.warning(f"⚠️  {name}_data has {total_nulls} NULL values:")
            for col, count in null_counts[null_counts > 0].items():
                logger.warning(f"   - {col}: {count}")
        else:
            logger.info(f"✅ {name}_data: No NULL values")
    
    logger.info("\n" + "=" * 60)
    logger.info("Data Generation Complete!")
    logger.info(f"All files saved to: {OUTPUT_DIR}")
    logger.info("=" * 60)
    
    # Summary
    logger.info("\nData Summary:")
    logger.info(f"  distance_data: {len(distance_df):,} records")
    logger.info(f"  inventory_data: {len(inventory_df):,} records")
    logger.info(f"  order_data: {len(order_df):,} records")
    logger.info(f"  delhivery_data: {len(delhivery_df):,} records")
    logger.info(f"  weather_data: {len(weather_df):,} records")
    logger.info(f"  supply_chain_data: {len(supply_chain_df):,} records")
    logger.info(f"\n  Total: {sum([len(distance_df), len(inventory_df), len(order_df), len(delhivery_df), len(weather_df), len(supply_chain_df)]):,} records")

if __name__ == "__main__":
    main()

