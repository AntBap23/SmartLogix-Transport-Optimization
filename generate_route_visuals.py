#!/usr/bin/env python3
"""
Standalone Route Optimizer - No imports from app.optimizer
Reimplements the greedy algorithm directly to avoid OR-Tools segfaults
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any
import json

def load_orders_from_csv(limit=20):
    """Load orders from CSV file."""
    csv_path = Path("data/optimizer/optimizer_orders.csv")
    
    if not csv_path.exists():
        print(f"❌ Orders file not found: {csv_path}")
        return None
    
    print(f"📦 Loading orders from {csv_path}...")
    df = pd.read_csv(csv_path, low_memory=False)
    
    # Convert available_time to datetime if needed
    if 'available_time' in df.columns:
        df['available_time'] = pd.to_datetime(df['available_time'])
    
    # Get latest date
    if len(df) > 0:
        latest_date = df['available_time'].dt.date.max()
        df = df[df['available_time'].dt.date == latest_date]
        print(f"   ✅ Using latest date: {latest_date}")
    
    # Limit orders
    if limit and len(df) > limit:
        df = df.head(limit)
        print(f"   ✅ Limited to {limit} orders")
    
    print(f"   ✅ Loaded {len(df)} orders")
    return df

def load_distances_from_csv():
    """Load distance matrix from CSV file."""
    csv_path = Path("data/optimizer/optimizer_distances.csv")
    
    if not csv_path.exists():
        print(f"❌ Distances file not found: {csv_path}")
        return None
    
    print(f"📏 Loading distances from {csv_path}...")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"   ✅ Loaded {len(df)} distance records")
    return df

def build_distance_matrix(locations: List[str], distance_df: pd.DataFrame) -> np.ndarray:
    """Build distance matrix from distance dataframe."""
    location_to_index = {loc: idx for idx, loc in enumerate(locations)}
    n = len(locations)
    distance_matrix = np.zeros((n, n), dtype=int)
    
    # Fill matrix from distance dataframe
    for _, row in distance_df.iterrows():
        source = row['source']
        dest = row['destination']
        distance = row['distance_meters']
        
        if source in location_to_index and dest in location_to_index:
            i = location_to_index[source]
            j = location_to_index[dest]
            distance_matrix[i][j] = int(distance)
    
    print(f"   ✅ Built {n}x{n} distance matrix")
    return distance_matrix

def greedy_route_optimizer(orders_df: pd.DataFrame, distance_df: pd.DataFrame, 
                          num_vehicles: int = 2, vehicle_capacity: float = 1000.0):
    """Greedy nearest-neighbor route optimization."""
    print("🔄 Running greedy route optimization...")
    
    # Get unique locations
    all_locations = list(set(orders_df['source'].tolist() + orders_df['destination'].tolist()))
    depot_location = orders_df['source'].iloc[0] if len(orders_df) > 0 else all_locations[0]
    
    print(f"   📍 Found {len(all_locations)} unique locations")
    print(f"   🏠 Depot: {depot_location}")
    
    # Build distance matrix
    distance_matrix = build_distance_matrix(all_locations, distance_df)
    location_to_index = {loc: idx for idx, loc in enumerate(all_locations)}
    depot_idx = location_to_index[depot_location]
    
    # Group orders by destination
    orders_by_location = {}
    for _, order in orders_df.iterrows():
        dest = order['destination']
        if dest not in orders_by_location:
            orders_by_location[dest] = []
        orders_by_location[dest].append({
            'order_id': order['order_id'],
            'weight': order.get('weight', 0),
            'destination': dest
        })
    
    # Greedy route assignment
    routes = []
    unassigned_locations = [loc for loc in all_locations if loc != depot_location and loc in orders_by_location]
    
    for vehicle_id in range(num_vehicles):
        if not unassigned_locations:
            break
        
        route = [depot_location]
        current_location = depot_location
        current_weight = 0.0
        route_orders = []
        total_distance = 0
        
        while unassigned_locations:
            current_idx = location_to_index[current_location]
            nearest = None
            nearest_dist = float('inf')
            
            for loc in unassigned_locations:
                loc_idx = location_to_index[loc]
                dist = distance_matrix[current_idx][loc_idx]
                
                # Check capacity
                loc_weight = sum([o['weight'] for o in orders_by_location[loc]])
                if current_weight + loc_weight > vehicle_capacity:
                    continue
                
                if dist < nearest_dist and dist > 0:
                    nearest = loc
                    nearest_dist = dist
            
            if nearest is None:
                break
            
            # Add to route
            route.append(nearest)
            route_orders.extend(orders_by_location[nearest])
            current_weight += sum([o['weight'] for o in orders_by_location[nearest]])
            total_distance += nearest_dist
            current_location = nearest
            unassigned_locations.remove(nearest)
        
        # Return to depot
        if len(route) > 1:
            route.append(depot_location)
            last_idx = location_to_index[current_location]
            total_distance += distance_matrix[last_idx][depot_idx]
            
            routes.append({
                'vehicle_id': vehicle_id,
                'route': route,
                'orders': route_orders,
                'total_distance_meters': total_distance,
                'total_weight_kg': current_weight,
                'num_stops': len(route) - 2
            })
    
    total_distance = sum([r['total_distance_meters'] for r in routes])
    
    return {
        'routes': routes,
        'total_distance_meters': total_distance,
        'total_vehicles_used': len(routes),
        'optimization_status': 'FEASIBLE',
        'locations': all_locations,
        'distance_matrix': distance_matrix.tolist()
    }

def create_visualizations(results: Dict[str, Any]):
    """Create visualizations from results."""
    print()
    print("=" * 60)
    print("🗺️ Generating Visualizations")
    print("=" * 60)
    print()
    
    try:
        from app.visualize_routes import RouteVisualizer
        visualizer = RouteVisualizer()
        
        # Create map
        print("📍 Creating interactive map...")
        try:
            map_file = visualizer.create_map(results, "route_optimization_map.html")
            if map_file and Path(map_file).exists():
                print(f"   ✅ Map saved: {map_file}")
                print(f"   💡 Open in browser: open {map_file}")
            else:
                print("   ⚠️ Map file not created")
        except Exception as e:
            print(f"   ⚠️ Map error: {e}")
        
        # Create chart
        print()
        print("📊 Creating chart...")
        try:
            chart = visualizer.create_route_summary_chart(results)
            if chart:
                chart_file = "route_optimization_chart.html"
                chart.write_html(chart_file)
                print(f"   ✅ Chart saved: {chart_file}")
                print(f"   💡 Open in browser: open {chart_file}")
            else:
                print("   ⚠️ Chart not created")
        except Exception as e:
            print(f"   ⚠️ Chart error: {e}")
            
    except ImportError as e:
        print(f"⚠️ Could not import visualizer: {e}")
        print("   Install required packages: pip install folium plotly")
    except Exception as e:
        print(f"⚠️ Visualization error: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function."""
    print("=" * 60)
    print("🚚 SmartLogix Route Optimizer - Standalone Version")
    print("=" * 60)
    print()
    
    # Load data
    orders_df = load_orders_from_csv(limit=20)
    if orders_df is None or orders_df.empty:
        print("❌ No orders loaded. Exiting.")
        return
    
    distance_df = load_distances_from_csv()
    if distance_df is None or distance_df.empty:
        print("❌ No distances loaded. Exiting.")
        return
    
    # Run optimization
    print()
    results = greedy_route_optimizer(orders_df, distance_df, num_vehicles=2, vehicle_capacity=1000.0)
    
    # Display results
    print()
    print("=" * 60)
    print("📈 Optimization Results")
    print("=" * 60)
    print()
    print(f"Status: {results.get('optimization_status', 'UNKNOWN')}")
    print(f"Total Distance: {results.get('total_distance_meters', 0) / 1000:.2f} km")
    print(f"Vehicles Used: {results.get('total_vehicles_used', 0)}")
    print(f"Number of Routes: {len(results.get('routes', []))}")
    print()
    
    routes = results.get('routes', [])
    if routes:
        print("🚚 Route Details:")
        for i, route in enumerate(routes, 1):
            print(f"\nRoute {i} (Vehicle {route.get('vehicle_id', i)}):")
            print(f"  - Stops: {route.get('num_stops', 0)}")
            print(f"  - Distance: {route.get('total_distance_meters', 0) / 1000:.2f} km")
            print(f"  - Weight: {route.get('total_weight_kg', 0):.2f} kg")
            print(f"  - Orders: {len(route.get('orders', []))}")
            route_path = " → ".join(route.get('route', [])[:6])
            if len(route.get('route', [])) > 6:
                route_path += " → ..."
            print(f"  - Path: {route_path}")
    
    # Create visualizations
    create_visualizations(results)
    
    print()
    print("=" * 60)
    print("✅ Complete!")
    print("=" * 60)
    print()
    print("📸 Next Steps:")
    print("   1. Open route_optimization_map.html in your browser")
    print("   2. Open route_optimization_chart.html in your browser")
    print("   3. Take screenshots of both visualizations")
    print("   4. Save screenshots to: app/dashboard/route_visuals/")
    print("   5. Refresh the Streamlit dashboard to see them")
    print()

if __name__ == "__main__":
    main()

