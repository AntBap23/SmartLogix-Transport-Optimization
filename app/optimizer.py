"""
Route Optimizer for Vehicle Routing Problem (VRP) solving.

This module optimizes delivery routes using:
- Greedy nearest-neighbor algorithm (primary, fast and reliable)
- Optional: Google OR-Tools for advanced VRP solving (if available)
- PostgreSQL distance data for distance matrix
- Optional: Google Maps API or OSRM for real-time routing
"""

import os
import logging
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

# OR-Tools imports
try:
    from ortools.constraint_solver import routing_enums_pb2
    from ortools.constraint_solver import pywrapcp
    ORTOOLS_AVAILABLE = True
except ImportError:
    ORTOOLS_AVAILABLE = False
    logging.warning("OR-Tools not available. Install with: pip install ortools")

# PostgreSQL imports
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logging.warning("psycopg2 not available. Install with: pip install psycopg2-binary")

# Optional routing API imports
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

from postgresql.postgresql_config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RouteOptimizer:
    """Route optimizer using greedy algorithm with optional OR-Tools fallback."""
    
    def __init__(self, use_real_time_routing: bool = False, routing_api: str = "osrm", use_ortools: bool = False):
        """
        Initialize route optimizer.
        
        Args:
            use_real_time_routing: If True, use Google Maps or OSRM for real-time distances
            routing_api: "google" or "osrm" (only used if use_real_time_routing=True)
            use_ortools: If True, attempt to use OR-Tools (may fail if not available/compatible)
        """
        self.use_real_time_routing = use_real_time_routing
        self.routing_api = routing_api
        self.use_ortools = use_ortools
        self.connection = None
        self.google_maps_api_key = os.getenv('GOOGLE_MAPS_API_KEY', '')
        self._distance_cache = {}  # Cache for distance matrices
        
        if use_ortools and not ORTOOLS_AVAILABLE:
            logger.warning("OR-Tools requested but not available. Using greedy algorithm instead.")
            self.use_ortools = False
    
    def connect_to_database(self) -> bool:
        """Connect to PostgreSQL database."""
        if not PSYCOPG2_AVAILABLE:
            logger.error("psycopg2 not available")
            return False
        
        try:
            conn_params = config.get_connection_params()
            self.connection = psycopg2.connect(**conn_params)
            logger.info("Connected to PostgreSQL successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect_from_database(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def load_orders(self, date_filter: Optional[datetime] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Load orders from CSV file or database.
        
        Args:
            date_filter: Filter orders by available_time date (default: latest available)
            limit: Maximum number of orders to load
            
        Returns:
            DataFrame with orders
        """
        # Try CSV file first (no database needed)
        csv_path = Path(__file__).parent.parent.parent / "data" / "optimizer" / "optimizer_orders.csv"
        
        if csv_path.exists():
            logger.info(f"Loading orders from CSV: {csv_path}")
            try:
                df = pd.read_csv(csv_path, low_memory=False)
                
                # Convert available_time to datetime if it's a string
                if 'available_time' in df.columns:
                    df['available_time'] = pd.to_datetime(df['available_time'])
                
                # Filter by date if specified
                if date_filter is not None:
                    if isinstance(date_filter, datetime):
                        date_filter = date_filter.date()
                    df = df[pd.to_datetime(df['available_time']).dt.date == date_filter]
                
                # If no date filter or no results, use latest date
                if date_filter is None or len(df) == 0:
                    if len(df) == 0:
                        # Reload full dataset
                        df = pd.read_csv(csv_path, low_memory=False)
                        if 'available_time' in df.columns:
                            df['available_time'] = pd.to_datetime(df['available_time'])
                    
                    # Get latest date
                    latest_date = pd.to_datetime(df['available_time']).dt.date.max()
                    logger.info(f"Using latest available date: {latest_date}")
                    df = df[pd.to_datetime(df['available_time']).dt.date == latest_date]
                
                # Apply limit
                if limit:
                    df = df.head(limit)
                
                # Sort by available_time
                df = df.sort_values('available_time')
                
                logger.info(f"Loaded {len(df)} orders from CSV")
                return df
            except Exception as e:
                logger.warning(f"Error loading from CSV: {e}, trying database...")
        
        # Fallback to database
        if not self.connection:
            if not self.connect_to_database():
                raise ConnectionError("Cannot connect to database and CSV file not available")
        
        if date_filter is None:
            date_filter = datetime.now().date()
        
        query = f"""
            SELECT 
                order_id,
                source,
                destination,
                available_time,
                deadline,
                weight,
                order_size,
                material_id
            FROM {config.get_table_name('order_data')}
            WHERE DATE(available_time) = %s
            ORDER BY available_time
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        df = pd.read_sql_query(query, self.connection, params=(date_filter,))
        
        if len(df) == 0:
            latest_date_query = f"""
                SELECT DATE(available_time) as latest_date
                FROM {config.get_table_name('order_data')}
                ORDER BY available_time DESC
                LIMIT 1
            """
            latest_date_df = pd.read_sql_query(latest_date_query, self.connection)
            if len(latest_date_df) > 0:
                latest_date = latest_date_df.iloc[0]['latest_date']
                logger.info(f"Using latest available date: {latest_date}")
                query = f"""
                    SELECT 
                        order_id,
                        source,
                        destination,
                        available_time,
                        deadline,
                        weight,
                        order_size,
                        material_id
                    FROM {config.get_table_name('order_data')}
                    WHERE DATE(available_time) = %s
                    ORDER BY available_time
                """
                if limit:
                    query += f" LIMIT {limit}"
                df = pd.read_sql_query(query, self.connection, params=(latest_date,))
        
        logger.info(f"Loaded {len(df)} orders from database")
        return df
    
    def __init__(self, use_real_time_routing: bool = False, routing_api: str = "osrm"):
        """
        Initialize route optimizer.
        
        Args:
            use_real_time_routing: If True, use Google Maps or OSRM for real-time distances
            routing_api: "google" or "osrm" (only used if use_real_time_routing=True)
        """
        self.use_real_time_routing = use_real_time_routing
        self.routing_api = routing_api
        self.connection = None
        self.google_maps_api_key = os.getenv('GOOGLE_MAPS_API_KEY', '')
        self._distance_cache = {}  # Cache for distance matrices
        
        if not ORTOOLS_AVAILABLE:
            raise ImportError("OR-Tools is required. Install with: pip install ortools")
    
    def load_distance_matrix(self, locations: List[str]) -> np.ndarray:
        """
        Build distance matrix from database or routing API.
        Uses caching for performance.
        
        Args:
            locations: List of location names
            
        Returns:
            Distance matrix (numpy array) in meters
        """
        # Create cache key
        cache_key = tuple(sorted(locations))
        if cache_key in self._distance_cache:
            logger.debug("Using cached distance matrix")
            return self._distance_cache[cache_key]
        
        # Build matrix
        if self.use_real_time_routing:
            matrix = self._get_real_time_distance_matrix(locations)
        else:
            matrix = self._get_database_distance_matrix(locations)
        
        # Cache it
        self._distance_cache[cache_key] = matrix
        return matrix
    
    def _get_database_distance_matrix(self, locations: List[str]) -> np.ndarray:
        """Get distance matrix from CSV file or PostgreSQL database."""
        # Try CSV file first (no database needed)
        csv_path = Path(__file__).parent.parent.parent / "data" / "optimizer" / "optimizer_distances.csv"
        
        if csv_path.exists():
            logger.info(f"Loading distances from CSV: {csv_path}")
            try:
                distance_df = pd.read_csv(csv_path, low_memory=False)
                
                # Create location lookup
                location_to_index = {loc: idx for idx, loc in enumerate(locations)}
                n = len(locations)
                distance_matrix = np.zeros((n, n), dtype=int)
                
                # Filter to only locations we need
                location_set = set(locations)
                filtered_df = distance_df[
                    distance_df['source'].isin(location_set) & 
                    distance_df['destination'].isin(location_set)
                ]
                
                # Build matrix
                for _, row in filtered_df.iterrows():
                    source = row['source']
                    dest = row['destination']
                    distance = row['distance_meters']
                    
                    if source in location_to_index and dest in location_to_index:
                        i = location_to_index[source]
                        j = location_to_index[dest]
                        distance_matrix[i][j] = int(distance)
                
                logger.info(f"Built distance matrix from CSV: {n}x{n}")
                return distance_matrix
            except Exception as e:
                logger.warning(f"Error loading distances from CSV: {e}, trying database...")
        
        # Fallback to database
        if not self.connection:
            if not self.connect_to_database():
                raise ConnectionError("Cannot connect to database and CSV file not available")
        
        # Create location lookup
        location_to_index = {loc: idx for idx, loc in enumerate(locations)}
        n = len(locations)
        distance_matrix = np.zeros((n, n), dtype=int)
        
        # Query all relevant distances
        placeholders = ','.join(['%s'] * len(locations))
        query = f"""
            SELECT source, destination, distance_meters
            FROM {config.get_table_name('distance_data')}
            WHERE source IN ({placeholders}) AND destination IN ({placeholders})
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query, locations * 2)
            for row in cursor.fetchall():
                source, dest, distance = row
                if source in location_to_index and dest in location_to_index:
                    i = location_to_index[source]
                    j = location_to_index[dest]
                    distance_matrix[i][j] = int(distance)
        
        logger.info(f"Built distance matrix from database: {n}x{n}")
        return distance_matrix
    
    def _get_real_time_distance_matrix(self, locations: List[str]) -> np.ndarray:
        """
        Get real-time distance matrix from Google Maps or OSRM.
        
        Args:
            locations: List of location names
            
        Returns:
            Distance matrix in meters
        """
        n = len(locations)
        distance_matrix = np.zeros((n, n), dtype=int)
        
        if self.routing_api == "google":
            return self._get_google_maps_matrix(locations)
        elif self.routing_api == "osrm":
            return self._get_osrm_matrix(locations)
        else:
            logger.warning(f"Unknown routing API: {self.routing_api}, falling back to database")
            return self._get_database_distance_matrix(locations)
    
    def _get_google_maps_matrix(self, locations: List[str]) -> np.ndarray:
        """Get distance matrix from Google Maps Distance Matrix API."""
        if not self.google_maps_api_key:
            logger.warning("Google Maps API key not set, falling back to database")
            return self._get_database_distance_matrix(locations)
        
        # Note: This is a simplified version. In production, you'd need:
        # 1. Convert location names to lat/lng (geocoding)
        # 2. Use Google Distance Matrix API
        # 3. Handle API rate limits
        
        logger.warning("Google Maps integration not fully implemented, using database")
        return self._get_database_distance_matrix(locations)
    
    def _get_osrm_matrix(self, locations: List[str]) -> np.ndarray:
        """Get distance matrix from OSRM (Open Source Routing Machine)."""
        if not REQUESTS_AVAILABLE:
            logger.warning("requests not available, falling back to database")
            return self._get_database_distance_matrix(locations)
        
        # Note: OSRM requires coordinates. This is a placeholder.
        # In production, you'd need location coordinates and OSRM server URL
        
        logger.warning("OSRM integration not fully implemented, using database")
        return self._get_database_distance_matrix(locations)
    
    def optimize_routes(
        self,
        orders_df: pd.DataFrame,
        num_vehicles: int = 3,
        depot_location: Optional[str] = None,
        vehicle_capacity: float = 1000.0,  # kg
        max_route_duration: int = 480,  # minutes (8 hours)
        max_orders: int = 50,  # Limit orders for speed
        max_locations: int = 30  # Limit unique locations for speed
    ) -> Dict[str, Any]:
        """
        Optimize delivery routes using greedy nearest-neighbor algorithm.
        Optionally uses OR-Tools if available and requested.
        
        Args:
            orders_df: DataFrame with orders to optimize
            num_vehicles: Number of vehicles available
            depot_location: Starting/ending location (default: first order source)
            vehicle_capacity: Maximum weight capacity per vehicle (kg)
            max_route_duration: Maximum route duration in minutes (for OR-Tools)
            max_orders: Limit orders for performance
            max_locations: Limit unique locations for performance
            
        Returns:
            Dictionary with optimized routes and metrics
        """
        if orders_df.empty:
            return {"error": "No orders to optimize"}
        
        # Limit orders for performance
        if len(orders_df) > max_orders:
            logger.info(f"Limiting orders from {len(orders_df)} to {max_orders} for performance")
            orders_df = orders_df.head(max_orders).copy()
        
        # Get unique locations
        all_locations = list(set(orders_df['source'].tolist() + orders_df['destination'].tolist()))
        
        # Limit locations for performance
        if len(all_locations) > max_locations:
            logger.info(f"Limiting locations from {len(all_locations)} to {max_locations} for performance")
            location_counts = {}
            for loc in orders_df['source'].tolist() + orders_df['destination'].tolist():
                location_counts[loc] = location_counts.get(loc, 0) + 1
            top_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)[:max_locations]
            all_locations = [loc for loc, _ in top_locations]
            orders_df = orders_df[
                orders_df['source'].isin(all_locations) & 
                orders_df['destination'].isin(all_locations)
            ].copy()
            if orders_df.empty:
                return {"error": "No orders after location filtering"}
        
        if depot_location is None:
            depot_location = orders_df['source'].iloc[0]
        
        if depot_location not in all_locations:
            all_locations.insert(0, depot_location)
        else:
            all_locations.remove(depot_location)
            all_locations.insert(0, depot_location)
        
        # Build distance matrix
        distance_matrix = self.load_distance_matrix(all_locations)
        
        # Validate distance matrix
        if np.any(np.isnan(distance_matrix)) or np.any(np.isinf(distance_matrix)):
            logger.error("Distance matrix contains NaN or Inf values")
            return {"error": "Invalid distance matrix"}
        
        if np.any(distance_matrix < 0):
            logger.warning("Distance matrix contains negative values, setting to 0")
            distance_matrix = np.maximum(distance_matrix, 0)
        
        # Try OR-Tools if requested, otherwise use greedy algorithm
        if self.use_ortools and ORTOOLS_AVAILABLE:
            try:
                logger.info("Attempting OR-Tools optimization...")
                return self._optimize_with_ortools(
                    orders_df, all_locations, distance_matrix, depot_location,
                    num_vehicles, vehicle_capacity, max_route_duration
                )
            except Exception as e:
                logger.warning(f"OR-Tools optimization failed: {e}. Falling back to greedy algorithm.")
        
        # Use greedy nearest-neighbor algorithm (primary method)
        return self._optimize_with_greedy(
            orders_df, all_locations, distance_matrix, depot_location,
            num_vehicles, vehicle_capacity
        )
    
    def _optimize_with_greedy(
        self,
        orders_df: pd.DataFrame,
        all_locations: List[str],
        distance_matrix: np.ndarray,
        depot_location: str,
        num_vehicles: int,
        vehicle_capacity: float
    ) -> Dict[str, Any]:
        """Optimize routes using greedy nearest-neighbor algorithm."""
        logger.info("Using greedy nearest-neighbor algorithm for route optimization")
        
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
                'weight': order['weight'],
                'deadline': order['deadline'],
                'available_time': order['available_time']
            })
        
        # Greedy route assignment
        routes = []
        unassigned_locations = [loc for loc in all_locations[1:] if loc in orders_by_location]
        
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
    
    def _optimize_with_ortools(
        self,
        orders_df: pd.DataFrame,
        all_locations: List[str],
        distance_matrix: np.ndarray,
        depot_location: str,
        num_vehicles: int,
        vehicle_capacity: float,
        max_route_duration: int
    ) -> Dict[str, Any]:
        """Optimize routes using OR-Tools (if available)."""
        # Create time matrix
        time_matrix = (distance_matrix / 1000 / 50 * 60).astype(int)
        time_matrix = np.maximum(time_matrix, 1)
        time_matrix = np.minimum(time_matrix, max_route_duration)
        
        # Create demands and order mapping
        location_to_index = {loc: idx for idx, loc in enumerate(all_locations)}
        demands = [0] * len(all_locations)
        order_mapping = {}
        
        for _, order in orders_df.iterrows():
            dest_idx = location_to_index.get(order['destination'])
            if dest_idx is not None:
                demands[dest_idx] += order['weight']
                if dest_idx not in order_mapping:
                    order_mapping[dest_idx] = []
                order_mapping[dest_idx].append({
                    'order_id': order['order_id'],
                    'weight': order['weight'],
                    'deadline': order['deadline'],
                    'available_time': order['available_time']
                })
        
        # Create time windows
        time_windows = []
        for loc in all_locations:
            if loc == depot_location:
                time_windows.append((0, max_route_duration))
            else:
                loc_orders = [o for o_idx, o_list in order_mapping.items() 
                             for o in o_list if all_locations[o_idx] == loc]
                if loc_orders:
                    earliest = min([pd.to_datetime(o['available_time']) for o in loc_orders])
                    latest = max([pd.to_datetime(o['deadline']) for o in loc_orders])
                    start_min = (earliest.hour * 60 + earliest.minute)
                    end_min = (latest.hour * 60 + latest.minute)
                    time_windows.append((start_min, end_min))
                else:
                    time_windows.append((0, max_route_duration))
        
        # Solve with OR-Tools
        solution_data = self._solve_vrp(
            distance_matrix=distance_matrix,
            time_matrix=time_matrix,
            demands=demands,
            time_windows=time_windows,
            num_vehicles=num_vehicles,
            depot=0,
            vehicle_capacity=vehicle_capacity,
            max_route_duration=max_route_duration
        )
        
        # Format solution
        routes = []
        for vehicle_id, route_indices in enumerate(solution_data['routes']):
            route_locations = [all_locations[idx] for idx in route_indices]
            route_orders = []
            total_weight = 0
            total_distance = 0
            
            for i, loc_idx in enumerate(route_indices):
                if loc_idx in order_mapping:
                    route_orders.extend(order_mapping[loc_idx])
                    total_weight += sum([o['weight'] for o in order_mapping[loc_idx]])
                
                if i > 0:
                    prev_idx = route_indices[i-1]
                    total_distance += distance_matrix[prev_idx][loc_idx]
            
            routes.append({
                'vehicle_id': vehicle_id,
                'route': route_locations,
                'orders': route_orders,
                'total_distance_meters': total_distance,
                'total_weight_kg': total_weight,
                'num_stops': len(route_indices) - 1
            })
        
        return {
            'routes': routes,
            'total_distance_meters': solution_data['total_distance'],
            'total_vehicles_used': solution_data['vehicles_used'],
            'optimization_status': solution_data['status'],
            'locations': all_locations,
            'distance_matrix': distance_matrix.tolist()
        }
    
    def _solve_vrp(
        self,
        distance_matrix: np.ndarray,
        time_matrix: np.ndarray,
        demands: List[int],
        time_windows: List[Tuple[int, int]],
        num_vehicles: int,
        depot: int,
        vehicle_capacity: float,
        max_route_duration: int
    ) -> Dict[str, Any]:
        """Solve Vehicle Routing Problem using OR-Tools."""
        
        try:
            num_locations = len(distance_matrix)
            logger.info(f"Solving VRP: {num_locations} locations, {num_vehicles} vehicles")
            
            # Validate inputs
            if num_locations < 2:
                logger.error("Need at least 2 locations")
                return {'routes': [], 'total_distance': 0, 'vehicles_used': 0, 'status': 'INVALID_INPUT'}
            
            if num_vehicles < 1:
                logger.error("Need at least 1 vehicle")
                return {'routes': [], 'total_distance': 0, 'vehicles_used': 0, 'status': 'INVALID_INPUT'}
            
            # Create routing index manager
            logger.debug("Creating RoutingIndexManager...")
            manager = pywrapcp.RoutingIndexManager(num_locations, num_vehicles, depot)
            
            # Create routing model
            logger.debug("Creating RoutingModel...")
            routing = pywrapcp.RoutingModel(manager)
            
            # Distance callback
            def distance_callback(from_index, to_index):
                from_node = manager.IndexToNode(from_index)
                to_node = manager.IndexToNode(to_index)
                return int(distance_matrix[from_node][to_node])
            
            transit_callback_index = routing.RegisterTransitCallback(distance_callback)
            routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
            
            # Add capacity constraint
            def demand_callback(from_index):
                from_node = manager.IndexToNode(from_index)
                return int(demands[from_node] * 100)  # Convert to integer (kg * 100)
            
            demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
            routing.AddDimensionWithVehicleCapacity(
                demand_callback_index,
                0,  # slack
                [int(vehicle_capacity * 100)] * num_vehicles,  # vehicle capacities
                True,  # start cumul to zero
                'Capacity'
            )
            
            # Skip time windows for speed (comment out to enable)
            # Time windows make the problem much slower
            logger.debug("Skipping time windows for speed...")
            # Uncomment below to enable time windows (slower):
            """
            try:
                def time_callback(from_index, to_index):
                    from_node = manager.IndexToNode(from_index)
                    to_node = manager.IndexToNode(to_index)
                    return int(time_matrix[from_node][to_node])
                
                time_callback_index = routing.RegisterTransitCallback(time_callback)
                routing.AddDimension(
                    time_callback_index,
                    max_route_duration,
                    max_route_duration,
                    False,
                    'Time'
                )
                
                time_dimension = routing.GetDimensionOrDie('Time')
                for location_idx, (start, end) in enumerate(time_windows):
                    if 0 <= start <= end:
                        try:
                            index = manager.NodeToIndex(location_idx)
                            time_dimension.CumulVar(index).SetRange(start, end)
                        except:
                            pass
            except Exception as e:
                logger.warning(f"Time dimension failed: {e}")
            """
            
            # Set search parameters for speed
            logger.debug("Setting search parameters...")
            search_parameters = pywrapcp.DefaultRoutingSearchParameters()
            # Use fastest strategy
            search_parameters.first_solution_strategy = (
                routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
            )
            # Disable local search for speed (or use minimal)
            search_parameters.local_search_metaheuristic = (
                routing_enums_pb2.LocalSearchMetaheuristic.AUTOMATIC
            )
            # Very short timeout for speed
            search_parameters.time_limit.FromSeconds(5)
            # Limit search
            search_parameters.solution_limit = 1  # Stop after first solution
            
            # Solve
            logger.info("Solving VRP (this may take a moment)...")
            solution = routing.SolveWithParameters(search_parameters)
            
            if not solution:
                return {
                    'routes': [],
                    'total_distance': 0,
                    'vehicles_used': 0,
                    'status': 'NO_SOLUTION_FOUND'
                }
            
            # Extract routes
            routes = []
            total_distance = 0
            vehicles_used = 0
            
            for vehicle_id in range(num_vehicles):
                index = routing.Start(vehicle_id)
                route = []
                route_distance = 0
                
                while not routing.IsEnd(index):
                    node_index = manager.IndexToNode(index)
                    route.append(node_index)
                    previous_index = index
                    index = solution.Value(routing.NextVar(index))
                    route_distance += routing.GetArcCostForVehicle(previous_index, index, vehicle_id)
                
                node_index = manager.IndexToNode(index)
                route.append(node_index)  # Add depot at end
                
                if len(route) > 2:  # More than just depot -> depot
                    routes.append(route)
                    total_distance += route_distance
                    vehicles_used += 1
            
            return {
                'routes': routes,
                'total_distance': total_distance,
                'vehicles_used': vehicles_used,
                'status': 'OPTIMAL' if solution else 'FEASIBLE'
            }
            
        except Exception as e:
            logger.error(f"Error in _solve_vrp: {e}")
            import traceback
            traceback.print_exc()
            return {
                'routes': [],
                'total_distance': 0,
                'vehicles_used': 0,
                'status': 'ERROR'
            }
    
    def save_results_to_database(self, results: Dict[str, Any], optimization_id: Optional[str] = None):
        """Save optimization results to database."""
        if not self.connection:
            if not self.connect_to_database():
                logger.warning("Cannot save to database: not connected")
                return
        
        if optimization_id is None:
            optimization_id = f"OPT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create optimization_results table if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {config.get_table_name('optimization_results')} (
                id BIGSERIAL PRIMARY KEY,
                optimization_id VARCHAR(255),
                vehicle_id INTEGER,
                route_order INTEGER,
                location VARCHAR(255),
                order_id VARCHAR(255),
                distance_meters INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)
            
            # Insert route data
            for route in results['routes']:
                for route_order, location in enumerate(route['route']):
                    for order in route.get('orders', []):
                        insert_query = f"""
                            INSERT INTO {config.get_table_name('optimization_results')}
                            (optimization_id, vehicle_id, route_order, location, order_id, distance_meters)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        cursor.execute(insert_query, (
                            optimization_id,
                            route['vehicle_id'],
                            route_order,
                            location,
                            order['order_id'],
                            route['total_distance_meters']
                        ))
            
            self.connection.commit()
        
        logger.info(f"Saved optimization results to database: {optimization_id}")


def optimize_delivery_routes(
    date_filter: Optional[datetime] = None,
    num_vehicles: int = 3,
    limit: Optional[int] = 30,  # Reduced default for speed
    use_real_time_routing: bool = False,
    max_orders: int = 30,  # Limit for optimization
    max_locations: int = 20  # Limit locations
) -> Dict[str, Any]:
    """
    Convenience function to optimize delivery routes.
    
    Args:
        date_filter: Date to filter orders (default: today)
        num_vehicles: Number of vehicles
        limit: Maximum orders to optimize
        use_real_time_routing: Use real-time routing APIs
        
    Returns:
        Optimization results dictionary
    """
    optimizer = RouteOptimizer(
        use_real_time_routing=use_real_time_routing,
        use_ortools=use_ortools
    )
    
    try:
        # Load orders
        orders_df = optimizer.load_orders(date_filter=date_filter, limit=limit)
        
        if orders_df.empty:
            return {"error": "No orders found for the specified date"}
        
        # Optimize routes with limits for speed
        results = optimizer.optimize_routes(
            orders_df, 
            num_vehicles=num_vehicles,
            max_orders=max_orders,
            max_locations=max_locations
        )
        
        # Save to database
        optimizer.save_results_to_database(results)
        
        return results
    
    finally:
        optimizer.disconnect_from_database()


if __name__ == "__main__":
    # Example usage
    results = optimize_delivery_routes(limit=20, num_vehicles=2)
    print(f"Optimization Status: {results.get('optimization_status')}")
    print(f"Total Distance: {results.get('total_distance_meters')} meters")
    print(f"Vehicles Used: {results.get('total_vehicles_used')}")
    
    for route in results.get('routes', []):
        print(f"\nVehicle {route['vehicle_id']}:")
        print(f"  Route: {' -> '.join(route['route'])}")
        print(f"  Distance: {route['total_distance_meters']}m")
        print(f"  Orders: {len(route['orders'])}")

