"""
Visualization tools for route optimization results.

Supports:
- Interactive maps with Folium
- Plotly charts for route metrics
- JSON export for external tools
- Streamlit dashboard integration
"""

import json
from typing import Dict, List, Any, Optional
import logging

try:
    import folium
    from folium import plugins
    FOLIUM_AVAILABLE = True
except ImportError:
    FOLIUM_AVAILABLE = False
    logging.warning("Folium not available. Install with: pip install folium")

try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    logging.warning("Plotly not available. Install with: pip install plotly")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RouteVisualizer:
    """Visualize optimized routes using maps and charts."""
    
    def __init__(self, location_coordinates: Optional[Dict[str, tuple]] = None):
        """
        Initialize visualizer.
        
        Args:
            location_coordinates: Dict mapping location names to (lat, lng) tuples
                                 If None, will use placeholder coordinates
        """
        self.location_coordinates = location_coordinates or {}
        self._generate_placeholder_coordinates()
    
    def _generate_placeholder_coordinates(self):
        """Generate placeholder coordinates for locations if not provided."""
        # Indian cities approximate coordinates (since your data uses Indian locations)
        indian_cities = {
            "Mumbai_Warehouse": (19.0760, 72.8777),
            "Delhi_DC": (28.6139, 77.2090),
            "Bangalore_Hub": (12.9716, 77.5946),
            "Chennai_Distribution": (13.0827, 80.2707),
            "Kolkata_Center": (22.5726, 88.3639),
            "Hyderabad_WH": (17.3850, 78.4867),
            "Pune_Facility": (18.5204, 73.8567),
            "Ahmedabad_DC": (23.0225, 72.5714),
            "Jaipur_Hub": (26.9124, 75.7873),
            "Lucknow_Center": (26.8467, 80.9462),
            "Kanpur_WH": (26.4499, 80.3319),
            "Nagpur_Distribution": (21.1458, 79.0882),
            "Indore_Facility": (22.7196, 75.8577),
            "Bhopal_DC": (23.2599, 77.4126),
            "Surat_Hub": (21.1702, 72.8311),
            "Vadodara_Center": (22.3072, 73.1812),
            "Patna_WH": (25.5941, 85.1376),
            "Ranchi_Distribution": (23.3441, 85.3096),
            "Chandigarh_Facility": (30.7333, 76.7794),
            "Amritsar_DC": (31.6340, 74.8723)
        }
        
        # Add any missing locations with placeholder coordinates
        for loc in self.location_coordinates:
            if loc not in indian_cities:
                # Generate placeholder based on location name hash
                import hashlib
                hash_val = int(hashlib.md5(loc.encode()).hexdigest()[:8], 16)
                lat = 20.0 + (hash_val % 1000) / 1000 * 10  # 20-30 N
                lng = 70.0 + (hash_val % 1000) / 1000 * 20  # 70-90 E
                indian_cities[loc] = (lat, lng)
        
        self.location_coordinates.update(indian_cities)
    
    def create_map(self, optimization_results: Dict[str, Any], output_file: str = "optimized_routes.html") -> Optional[str]:
        """
        Create interactive Folium map with optimized routes.
        
        Args:
            optimization_results: Results from RouteOptimizer
            output_file: Output HTML file path
            
        Returns:
            Path to saved HTML file
        """
        if not FOLIUM_AVAILABLE:
            logger.error("Folium not available. Cannot create map.")
            return None
        
        # Get center of all locations
        routes = optimization_results.get('routes', [])
        if not routes:
            logger.warning("No routes to visualize")
            return None
        
        # Calculate map center
        all_locations = set()
        for route in routes:
            all_locations.update(route['route'])
        
        if all_locations:
            center_loc = list(all_locations)[0]
            center_coords = self.location_coordinates.get(center_loc, (20.0, 77.0))
        else:
            center_coords = (20.0, 77.0)  # Center of India
        
        # Create map
        m = folium.Map(location=center_coords, zoom_start=6)
        
        # Color palette for vehicles
        colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 
                 'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue',
                 'darkpurple', 'white', 'pink', 'lightblue', 'lightgreen',
                 'gray', 'black', 'lightgray']
        
        # Add routes
        for route in routes:
            vehicle_id = route['vehicle_id']
            route_locations = route['route']
            color = colors[vehicle_id % len(colors)]
            
            # Get coordinates for route
            route_coords = []
            for loc in route_locations:
                coords = self.location_coordinates.get(loc)
                if coords:
                    route_coords.append(coords)
            
            if len(route_coords) > 1:
                # Draw route line
                folium.PolyLine(
                    route_coords,
                    color=color,
                    weight=4,
                    opacity=0.7,
                    popup=f"Vehicle {vehicle_id}: {len(route_locations)} stops, {route['total_distance_meters']/1000:.1f}km"
                ).add_to(m)
                
                # Add markers for each stop
                for i, (loc, coord) in enumerate(zip(route_locations, route_coords)):
                    if i == 0:
                        # Depot/start
                        icon = folium.Icon(color='green', icon='home', prefix='fa')
                        popup_text = f"<b>Depot</b><br>{loc}<br>Vehicle {vehicle_id}"
                    elif i == len(route_locations) - 1:
                        # End
                        icon = folium.Icon(color='red', icon='flag', prefix='fa')
                        popup_text = f"<b>End</b><br>{loc}<br>Vehicle {vehicle_id}"
                    else:
                        # Stop
                        icon = folium.Icon(color=color, icon='truck', prefix='fa')
                        orders_at_stop = [o for o in route.get('orders', []) if loc in str(o)]
                        popup_text = f"<b>Stop {i}</b><br>{loc}<br>Orders: {len(orders_at_stop)}"
                    
                    folium.Marker(
                        coord,
                        popup=popup_text,
                        icon=icon,
                        tooltip=f"{loc} (Vehicle {vehicle_id})"
                    ).add_to(m)
        
        # Add legend
        legend_html = """
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: auto; 
                    background-color: white; z-index:9999; font-size:14px;
                    border:2px solid grey; padding: 10px">
        <h4>Route Legend</h4>
        <p><i class="fa fa-home" style="color:green"></i> Depot/Start</p>
        <p><i class="fa fa-truck" style="color:blue"></i> Delivery Stop</p>
        <p><i class="fa fa-flag" style="color:red"></i> Route End</p>
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Save map
        m.save(output_file)
        logger.info(f"Map saved to {output_file}")
        return output_file
    
    def create_route_summary_chart(self, optimization_results: Dict[str, Any]) -> Optional[go.Figure]:
        """
        Create Plotly chart showing route summary metrics.
        
        Args:
            optimization_results: Results from RouteOptimizer
            
        Returns:
            Plotly figure
        """
        if not PLOTLY_AVAILABLE:
            logger.error("Plotly not available. Cannot create chart.")
            return None
        
        routes = optimization_results.get('routes', [])
        if not routes:
            return None
        
        # Prepare data
        vehicle_ids = [r['vehicle_id'] for r in routes]
        distances = [r['total_distance_meters'] / 1000 for r in routes]  # Convert to km
        num_stops = [r['num_stops'] for r in routes]
        weights = [r['total_weight_kg'] for r in routes]
        
        # Create subplots
        from plotly.subplots import make_subplots
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Route Distance (km)', 'Number of Stops', 
                          'Total Weight (kg)', 'Distance vs Stops'),
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "bar"}, {"type": "scatter"}]]
        )
        
        # Distance bar chart
        fig.add_trace(
            go.Bar(x=vehicle_ids, y=distances, name='Distance (km)'),
            row=1, col=1
        )
        
        # Stops bar chart
        fig.add_trace(
            go.Bar(x=vehicle_ids, y=num_stops, name='Stops'),
            row=1, col=2
        )
        
        # Weight bar chart
        fig.add_trace(
            go.Bar(x=vehicle_ids, y=weights, name='Weight (kg)'),
            row=2, col=1
        )
        
        # Distance vs Stops scatter
        fig.add_trace(
            go.Scatter(x=num_stops, y=distances, mode='markers+text',
                      text=vehicle_ids, textposition='top center',
                      name='Distance vs Stops'),
            row=2, col=2
        )
        
        fig.update_layout(
            height=800,
            title_text="Route Optimization Summary",
            showlegend=False
        )
        
        return fig
    
    def export_to_json(self, optimization_results: Dict[str, Any], output_file: str = "optimized_routes.json") -> str:
        """
        Export optimization results to JSON format.
        
        Args:
            optimization_results: Results from RouteOptimizer
            output_file: Output JSON file path
            
        Returns:
            Path to saved JSON file
        """
        # Convert numpy arrays to lists for JSON serialization
        export_data = {
            'optimization_status': optimization_results.get('optimization_status'),
            'total_distance_meters': optimization_results.get('total_distance_meters'),
            'total_vehicles_used': optimization_results.get('total_vehicles_used'),
            'routes': []
        }
        
        for route in optimization_results.get('routes', []):
            export_route = {
                'vehicle_id': route['vehicle_id'],
                'route': route['route'],
                'total_distance_meters': route['total_distance_meters'],
                'total_weight_kg': route['total_weight_kg'],
                'num_stops': route['num_stops'],
                'orders': route.get('orders', [])
            }
            # Convert datetime objects to strings
            for order in export_route['orders']:
                if 'deadline' in order:
                    order['deadline'] = str(order['deadline'])
                if 'available_time' in order:
                    order['available_time'] = str(order['available_time'])
            
            export_data['routes'].append(export_route)
        
        with open(output_file, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        logger.info(f"Results exported to {output_file}")
        return output_file
    
    def create_streamlit_dashboard(self, optimization_results: Dict[str, Any]):
        """
        Create Streamlit dashboard components.
        
        Args:
            optimization_results: Results from RouteOptimizer
            
        Returns:
            Dictionary of Streamlit components
        """
        try:
            import streamlit as st
        except ImportError:
            logger.error("Streamlit not available")
            return None
        
        components = {}
        
        # Summary metrics
        st.metric("Total Distance", f"{optimization_results.get('total_distance_meters', 0) / 1000:.1f} km")
        st.metric("Vehicles Used", optimization_results.get('total_vehicles_used', 0))
        st.metric("Total Routes", len(optimization_results.get('routes', [])))
        
        # Route table
        routes_data = []
        for route in optimization_results.get('routes', []):
            routes_data.append({
                'Vehicle': route['vehicle_id'],
                'Stops': route['num_stops'],
                'Distance (km)': f"{route['total_distance_meters'] / 1000:.1f}",
                'Weight (kg)': f"{route['total_weight_kg']:.1f}",
                'Orders': len(route.get('orders', []))
            })
        
        if routes_data:
            import pandas as pd
            st.dataframe(pd.DataFrame(routes_data))
        
        # Map visualization
        map_file = self.create_map(optimization_results, "temp_route_map.html")
        if map_file:
            with open(map_file, 'r') as f:
                map_html = f.read()
            st.components.v1.html(map_html, height=600)
        
        # Charts
        if PLOTLY_AVAILABLE:
            fig = self.create_route_summary_chart(optimization_results)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        
        return components


def visualize_optimization_results(
    optimization_results: Dict[str, Any],
    output_dir: str = "output",
    create_map: bool = True,
    create_charts: bool = True,
    export_json: bool = True
) -> Dict[str, str]:
    """
    Convenience function to create all visualizations.
    
    Args:
        optimization_results: Results from RouteOptimizer
        output_dir: Directory to save output files
        create_map: Whether to create interactive map
        create_charts: Whether to create charts
        export_json: Whether to export JSON
        
    Returns:
        Dictionary of output file paths
    """
    import os
    from pathlib import Path
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    visualizer = RouteVisualizer()
    outputs = {}
    
    if create_map:
        map_file = visualizer.create_map(
            optimization_results,
            str(output_path / "optimized_routes.html")
        )
        if map_file:
            outputs['map'] = map_file
    
    if create_charts and PLOTLY_AVAILABLE:
        fig = visualizer.create_route_summary_chart(optimization_results)
        if fig:
            chart_file = output_path / "route_summary.html"
            fig.write_html(str(chart_file))
            outputs['chart'] = str(chart_file)
    
    if export_json:
        json_file = visualizer.export_to_json(
            optimization_results,
            str(output_path / "optimized_routes.json")
        )
        outputs['json'] = json_file
    
    return outputs


if __name__ == "__main__":
    # Example usage
    from optimizer import optimize_delivery_routes
    
    # Optimize routes
    results = optimize_delivery_routes(limit=20, num_vehicles=2)
    
    # Visualize
    outputs = visualize_optimization_results(results)
    print(f"Visualizations created:")
    for key, path in outputs.items():
        print(f"  {key}: {path}")

