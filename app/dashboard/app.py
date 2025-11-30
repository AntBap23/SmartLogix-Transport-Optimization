"""
SmartLogix Transport Optimization Dashboard
Streamlit dashboard for visualizing transport and order data.
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
from pathlib import Path
import os

# Check for optional dependencies
try:
    import folium
    FOLIUM_AVAILABLE = True
except ImportError:
    FOLIUM_AVAILABLE = False

try:
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

# PostgreSQL imports removed - using CSV files only for Analytics Dashboard

# Import optimizer and visualizer (lazy import to avoid OR-Tools issues)
# These will be imported only when needed in the route optimization tab

# Page configuration
st.set_page_config(
    page_title="SmartLogix Transport Optimization",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# All database and CSV loading functions removed - Analytics Dashboard is completely static

def main():
    """Main dashboard function."""
    st.title("🚚 SmartLogix Transport Optimization")
    
    # Create tabs for different views - Project Overview first
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Project Overview", 
        "📊 Analytics Dashboard", 
        "🔍 Findings & Insights", 
        "🗺️ Route Optimization",
        "📈 Order Forecasting"
    ])
    
    # Tab 1: Project Overview (first)
    with tab1:
        show_project_overview()
    
    # Tab 2: Analytics Dashboard
    with tab2:
        show_analytics_dashboard()
    
    # Tab 3: Findings and Insights
    with tab3:
        show_findings_insights()
    
    # Tab 4: Route Optimization
    with tab4:
        show_route_optimization()
    
    # Tab 5: Order Forecasting
    with tab5:
        show_order_forecasting()

def show_analytics_dashboard():
    """Show the analytics dashboard with static content."""
    # Show Tableau dashboard image and link
    st.markdown("### 📊 Tableau Dashboard")
    
    # Load dashboard image - simple direct path
    dashboard_image_path = Path(__file__).parent / "dashboard.png"
    
    if dashboard_image_path.exists():
        try:
            st.image(str(dashboard_image_path), use_container_width=True, caption="Tableau Dashboard - SmartLogix Transport Optimization")
        except Exception as e:
            st.error(f"Error loading image: {e}")
    else:
        st.error(f"Image not found at: {dashboard_image_path}")
    
    # Tableau Public link
    tableau_url = "https://public.tableau.com/views/SupplyChainDashboard_17645140839010/Dashboard1?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link"
    st.markdown(f"**🔗 [View Interactive Tableau Dashboard]({tableau_url})**")
    
    # Tableau embed code as fallback
    st.markdown("---")
    st.markdown("### 📊 Interactive Tableau Dashboard (Embedded)")
    st.markdown("If the image above doesn't load, use the interactive dashboard below:")
    
    # Embed Tableau dashboard
    tableau_embed_html = """
    <div class='tableauPlaceholder' id='viz1764521296482' style='position: relative'>
        <noscript>
            <a href='#'>
                <img alt='Dashboard 1' src='https://public.tableau.com/static/images/Su/SupplyChainDashboard_17645140839010/Dashboard1/1_rss.png' style='border: none' />
            </a>
        </noscript>
        <object class='tableauViz' style='display:none;'>
            <param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' />
            <param name='embed_code_version' value='3' />
            <param name='site_root' value='' />
            <param name='name' value='SupplyChainDashboard_17645140839010&#47;Dashboard1' />
            <param name='tabs' value='no' />
            <param name='toolbar' value='yes' />
            <param name='static_image' value='https://public.tableau.com/static/images/Su/SupplyChainDashboard_17645140839010/Dashboard1/1.png' />
            <param name='animate_transition' value='yes' />
            <param name='display_static_image' value='yes' />
            <param name='display_spinner' value='yes' />
            <param name='display_overlay' value='yes' />
            <param name='display_count' value='yes' />
            <param name='language' value='en-US' />
        </object>
    </div>
    <script type='text/javascript'>
        var divElement = document.getElementById('viz1764521296482');
        var vizElement = divElement.getElementsByTagName('object')[0];
        if (divElement.offsetWidth > 800) {
            vizElement.style.width = '1000px';
            vizElement.style.height = '827px';
        } else if (divElement.offsetWidth > 500) {
            vizElement.style.width = '1000px';
            vizElement.style.height = '827px';
        } else {
            vizElement.style.width = '100%';
            vizElement.style.height = '1627px';
        }
        var scriptElement = document.createElement('script');
        scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
        vizElement.parentNode.insertBefore(scriptElement, vizElement);
    </script>
    """
    
    components.html(tableau_embed_html, height=850)

def show_project_overview():
    """Show project overview information."""
    st.markdown("## 📋 Project Overview")
    st.markdown("""
    **SmartLogix Transport Optimization** is a comprehensive data analytics and route optimization project 
    designed to improve transportation efficiency and reduce operational costs for logistics operations.
    """)
    
    st.markdown("### Key Objectives:")
    st.markdown("""
    - **Route Optimization**: Develop intelligent algorithms to minimize delivery time and distance
    - **Data Analytics**: Analyze transportation patterns, delivery performance, and operational metrics
    - **Cost Reduction**: Identify opportunities to reduce fuel consumption and improve vehicle utilization
    - **Performance Monitoring**: Track KPIs and generate actionable insights for decision-making
    """)
    
    st.markdown("### Technologies Used:")
    st.markdown("""
    - **Python**: Core programming language for data processing and optimization
    - **Greedy Nearest-Neighbor Algorithm**: Primary route optimization method
    - **Streamlit**: Interactive web dashboard for visualization
    - **Tableau**: Advanced analytics and reporting
    - **PostgreSQL**: Data storage and management
    - **Folium & Plotly**: Interactive mapping and charting
    """)
    
    st.markdown("### Data Sources:")
    st.markdown("""
    - Order data (source, destination, weight, deadlines)
    - Distance matrices between locations
    - Delivery performance metrics
    - Historical transportation patterns
    """)
    
    st.markdown("### Project Structure:")
    st.markdown("""
    The project is organized into several key components:
    
    - **Route Optimizer** (`app/optimizer.py`): Implements greedy nearest-neighbor algorithm for route planning
    - **Visualization** (`app/visualize_routes.py`): Creates interactive maps and charts for route visualization
    - **Dashboard** (`app/dashboard/app.py`): Streamlit web application for interactive exploration
    - **Data Processing**: Scripts for loading and processing transportation data
    """)

def show_findings_insights():
    """Show findings and insights from the project."""
    st.markdown("## 🔍 Findings and Insights")
    
    st.markdown("### Route Optimization Insights:")
    st.markdown("""
    **1. Distance Minimization**
    - The greedy nearest-neighbor algorithm effectively reduces total route distance by selecting 
      the closest unvisited location at each step
    - Average route efficiency improved by optimizing vehicle capacity utilization
    
    **2. Delivery Time Optimization**
    - Routes are planned to respect delivery deadlines and time windows
    - Strategic depot placement reduces overall travel time
    
    **3. Vehicle Utilization**
    - Multi-vehicle routing ensures optimal distribution of orders across available fleet
    - Capacity constraints prevent overloading while maximizing order fulfillment
    """)
    
    st.markdown("### Data Analytics Insights:")
    st.markdown("""
    **1. Delivery Performance**
    - Analysis of delivery times reveals patterns in operational efficiency
    - Identification of bottlenecks in the supply chain network
    
    **2. Geographic Patterns**
    - High-frequency routes between specific source and destination centers
    - Opportunities for route consolidation and optimization
    
    **3. Order Characteristics**
    - Weight and size distributions inform vehicle selection
    - Material type analysis supports specialized handling requirements
    """)
    
    st.markdown("### Recommendations:")
    st.markdown("""
    **1. Operational Improvements**
    - Implement dynamic routing based on real-time traffic and order data
    - Consider time-of-day optimization for peak delivery windows
    
    **2. Cost Optimization**
    - Consolidate routes to reduce total distance traveled
    - Optimize vehicle capacity utilization to minimize fleet size requirements
    
    **3. Technology Enhancements**
    - Integrate real-time GPS tracking for live route adjustments
    - Implement machine learning models for demand forecasting
    - Develop predictive analytics for maintenance scheduling
    """)
    
    st.markdown("### Impact:")
    st.markdown("""
    - **Reduced Transportation Costs**: Optimized routes minimize fuel consumption
    - **Improved Customer Satisfaction**: Faster delivery times and better deadline adherence
    - **Enhanced Operational Efficiency**: Better resource allocation and vehicle utilization
    - **Data-Driven Decision Making**: Actionable insights from comprehensive analytics
    """)

def show_route_optimization():
    """Show route optimization interface with visualization."""
    st.header("🗺️ Route Optimization")
    st.markdown("Optimize delivery routes and visualize them on an interactive map.")
    
    # Check for visualization files (images and HTML)
    visuals_dir = Path(__file__).parent / "route_visuals"
    visuals_dir.mkdir(exist_ok=True)
    
    # Look for image files
    image_files = []
    if visuals_dir.exists():
        for ext in ['*.png', '*.jpg', '*.jpeg', '*.gif']:
            image_files.extend(list(visuals_dir.glob(ext)))
            image_files.extend(list(visuals_dir.glob(ext.upper())))
    
    # Look for HTML files
    html_files = []
    if visuals_dir.exists():
        html_files.extend(list(visuals_dir.glob("*.html")))
    
    if image_files or html_files:
        st.success(f"✅ Found {len(image_files)} image(s) and {len(html_files)} HTML file(s)")
        st.markdown("---")
        
        # Display HTML files (interactive visualizations)
        for html_file in sorted(html_files):
            st.markdown(f"### {html_file.stem.replace('_', ' ').title()}")
            try:
                with open(html_file, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                components.html(html_content, height=600, scrolling=True)
                st.caption(f"Interactive visualization: {html_file.name}")
            except Exception as e:
                st.error(f"Error loading {html_file.name}: {e}")
            st.markdown("---")
        
        # Display image files (screenshots)
        for img_file in sorted(image_files):
            st.markdown(f"### {img_file.stem.replace('_', ' ').title()}")
            try:
                st.image(str(img_file), use_container_width=True, caption=img_file.name)
            except Exception as e:
                st.error(f"Error loading {img_file.name}: {e}")
            st.markdown("---")
    else:
        st.info("📸 **No visualizations found yet**")
        st.markdown("""
        ### How to Generate Route Optimization Visualizations:
        
        **Option 1: Run the simple script**
        ```bash
        python run_optimizer_simple.py
        ```
        
        **Option 2: Run directly in Python**
        ```bash
        python -c "
        import sys
        sys.path.insert(0, '.')
        from app.optimizer import RouteOptimizer
        from app.visualize_routes import RouteVisualizer
        
        optimizer = RouteOptimizer(use_ortools=False)
        orders = optimizer.load_orders(limit=20)
        results = optimizer.optimize_routes(orders, num_vehicles=2, max_orders=20, max_locations=20)
        
        visualizer = RouteVisualizer()
        map_file = visualizer.create_map(results, 'route_map.html')
        chart = visualizer.create_route_summary_chart(results)
        if chart:
            chart.write_html('route_chart.html')
        print('✅ Files created: route_map.html, route_chart.html')
        "
        ```
        
        **Then:**
        1. Open `route_map.html` and `route_chart.html` in your browser
        2. Take screenshots of the visualizations
        3. Save screenshots to `app/dashboard/route_visuals/` folder
        4. Refresh this page to see them displayed here
        
        **Note:** If you get errors, the optimizer uses CSV files from `data/optimizer/` directory.
        """)
    
    st.markdown("---")
    st.markdown("### 📊 How It Works")
    st.markdown("""
    The route optimizer uses a **greedy nearest-neighbor algorithm** to:
    - Minimize total delivery distance
    - Respect vehicle capacity constraints
    - Optimize multi-vehicle routing
    - Generate efficient delivery routes
    
    **Data Source:** CSV files from `data/optimizer/` directory
    - `optimizer_orders.csv` - Order data
    - `optimizer_distances.csv` - Distance matrix
    """)
    
    # Optimization parameters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        num_vehicles = st.number_input(
            "Number of Vehicles",
            min_value=1,
            max_value=10,
            value=2,
            help="Number of vehicles available for delivery"
        )
    
    with col2:
        max_orders = st.number_input(
            "Max Orders",
            min_value=5,
            max_value=50,
            value=20,
            help="Maximum number of orders to optimize"
        )
    
    with col3:
        vehicle_capacity = st.number_input(
            "Vehicle Capacity (kg)",
            min_value=100.0,
            max_value=5000.0,
            value=1000.0,
            step=100.0,
            help="Maximum weight capacity per vehicle"
        )
    
    # Run optimization button
    if st.button("🚀 Optimize Routes", type="primary", use_container_width=True):
        with st.spinner("Optimizing routes... This may take a few seconds."):
            try:
                # Use subprocess to run optimizer in separate process to avoid import conflicts
                import subprocess
                import json
                import tempfile
                import os
                
                # Create a temporary script to run the optimizer
                script_content = f"""
import sys
sys.path.insert(0, '{os.getcwd()}')
from app.optimizer import optimize_delivery_routes
import json

results = optimize_delivery_routes(
    date_filter=None,
    num_vehicles={num_vehicles},
    limit={max_orders},
    max_orders={max_orders},
    max_locations=20,
    use_ortools=False
)

print(json.dumps(results))
"""
                
                # Write script to temp file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                    f.write(script_content)
                    script_path = f.name
                
                try:
                    # Run optimizer in subprocess
                    result = subprocess.run(
                        ['python', script_path],
                        capture_output=True,
                        text=True,
                        timeout=60
                    )
                    
                    if result.returncode == 0:
                        results = json.loads(result.stdout.strip())
                        
                        if 'error' in results:
                            st.error(f"❌ Optimization Error: {results['error']}")
                        else:
                            # Store results in session state
                            st.session_state.optimization_results = results
                            st.success("✅ Route optimization complete!")
                    else:
                        st.error(f"❌ Optimization failed: {result.stderr}")
                        
                finally:
                    # Clean up temp file
                    if os.path.exists(script_path):
                        os.unlink(script_path)
                    
            except subprocess.TimeoutExpired:
                st.error("❌ Optimization timed out. Try reducing the number of orders.")
            except json.JSONDecodeError:
                st.error("❌ Could not parse optimization results.")
            except Exception as e:
                error_msg = str(e)
                if "set_page_config" in error_msg:
                    st.error("❌ **Import Conflict**: Cannot load optimizer due to Streamlit configuration conflict.")
                    st.info("💡 **Workaround**: The optimizer requires a clean import context. Try refreshing the page or restarting the Streamlit server.")
                else:
                    st.error(f"❌ Error during optimization: {error_msg}")
                    import traceback
                    with st.expander("🔍 View Full Error Details"):
                        st.code(traceback.format_exc())
    
    # Display results if available
    if 'optimization_results' in st.session_state:
        results = st.session_state.optimization_results
        
        if 'error' not in results:
            # Display summary metrics
            st.markdown("---")
            st.subheader("📊 Optimization Results")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Status",
                    results.get('optimization_status', 'UNKNOWN')
                )
            
            with col2:
                total_distance = results.get('total_distance_meters', 0) / 1000
                st.metric(
                    "Total Distance",
                    f"{total_distance:.2f} km"
                )
            
            with col3:
                st.metric(
                    "Vehicles Used",
                    results.get('total_vehicles_used', 0)
                )
            
            with col4:
                st.metric(
                    "Routes",
                    len(results.get('routes', []))
                )
            
            # Display route details
            st.markdown("---")
            st.subheader("🚚 Route Details")
            
            routes = results.get('routes', [])
            if routes:
                for route in routes:
                    with st.expander(f"Vehicle {route['vehicle_id']} - {route['num_stops']} stops, {route['total_distance_meters']/1000:.2f} km"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**Route:**")
                            route_str = " → ".join(route['route'])
                            st.write(route_str)
                            
                            st.write("**Orders:**")
                            st.write(f"{len(route.get('orders', []))} orders")
                        
                        with col2:
                            st.write("**Metrics:**")
                            st.write(f"Distance: {route['total_distance_meters']/1000:.2f} km")
                            st.write(f"Weight: {route['total_weight_kg']:.2f} kg")
                            st.write(f"Stops: {route['num_stops']}")
                
                # Visualization section
                st.markdown("---")
                st.subheader("🗺️ Route Visualization")
                
                # Import visualizer using importlib to avoid conflicts
                import importlib
                visualize_module = importlib.import_module('app.visualize_routes')
                RouteVisualizer = visualize_module.RouteVisualizer
                visualizer = RouteVisualizer()
                
                # Create map
                try:
                    map_file = visualizer.create_map(results, "temp_route_map.html")
                    if map_file and os.path.exists(map_file):
                        with open(map_file, 'r') as f:
                            map_html = f.read()
                        st.components.v1.html(map_html, height=600)
                        st.caption("Interactive map showing optimized routes. Click on markers for details.")
                    else:
                        st.info("Map visualization requires Folium. Install with: pip install folium")
                except Exception as e:
                    st.warning(f"Could not create map: {e}")
                    st.info("Map visualization requires Folium. Install with: pip install folium")
                
                # Create charts
                try:
                    if PLOTLY_AVAILABLE:
                        chart = visualizer.create_route_summary_chart(results)
                        if chart:
                            st.plotly_chart(chart, use_container_width=True)
                except Exception as e:
                    st.warning(f"Could not create charts: {e}")
                
                # Export options
                st.markdown("---")
                st.subheader("📥 Export Results")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📄 Export to JSON"):
                        try:
                            json_file = visualizer.export_to_json(results, "optimized_routes.json")
                            with open(json_file, 'r') as f:
                                st.download_button(
                                    label="⬇️ Download JSON",
                                    data=f.read(),
                                    file_name="optimized_routes.json",
                                    mime="application/json"
                                )
                        except Exception as e:
                            st.error(f"Export error: {e}")
                
                with col2:
                    # Display results as JSON
                    if st.button("👁️ View JSON"):
                        st.json(results)

def show_order_forecasting():
    """Show order forecasting ML model predictions."""
    st.markdown("## 📈 Order Forecasting")
    st.markdown("""
    This section uses machine learning to forecast future order volumes based on historical data.
    The model analyzes patterns in order timing and generates predictions for upcoming days.
    """)
    
    # Import forecasting module (lazy import - only when this function is called)
    # This prevents set_page_config conflicts by importing after page config is set
    try:
        # Use importlib to import without triggering Streamlit at module level
        import importlib
        import sys
        
        # Remove any cached module to ensure fresh import
        if 'app.forecast_orders' in sys.modules:
            del sys.modules['app.forecast_orders']
        
        # Import the module
        forecast_module = importlib.import_module('app.forecast_orders')
        forecast_orders = forecast_module.forecast_orders
    except Exception as e:
        st.error(f"❌ Could not import forecasting module: {e}")
        import traceback
        st.code(traceback.format_exc())
        st.info("Please ensure `app/forecast_orders.py` exists and all dependencies are installed.")
        return
    
    # Configuration
    st.markdown("---")
    st.subheader("⚙️ Forecast Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        days_ahead = st.slider(
            "Days to Forecast",
            min_value=7,
            max_value=90,
            value=30,
            step=7,
            help="Number of days into the future to forecast"
        )
    
    with col2:
        csv_source = st.selectbox(
            "Data Source",
            options=["orders_cleaned.csv", "optimizer_orders.csv"],
            index=0,
            help="Select which CSV file to use for forecasting"
        )
    
    # Run forecasting
    if st.button("🔮 Generate Forecast", type="primary"):
        with st.spinner("Analyzing historical data and generating forecasts..."):
            try:
                # Determine CSV path
                if csv_source == "orders_cleaned.csv":
                    csv_path = Path(__file__).parent.parent.parent / "data" / "coherent" / "orders_cleaned.csv"
                else:
                    csv_path = Path(__file__).parent.parent.parent / "data" / "optimizer" / "optimizer_orders.csv"
                
                if not csv_path.exists():
                    st.error(f"❌ CSV file not found: {csv_path}")
                    return
                
                # Run forecasting
                result = forecast_orders(csv_path=str(csv_path), days_ahead=days_ahead)
                
                # Store in session state
                st.session_state['forecast_result'] = result
                st.success("✅ Forecast generated successfully!")
                
            except Exception as e:
                st.error(f"❌ Error generating forecast: {e}")
                import traceback
                st.code(traceback.format_exc())
                return
    
    # Display results if available
    if 'forecast_result' in st.session_state:
        result = st.session_state['forecast_result']
        
        # Metrics
        st.markdown("---")
        st.subheader("📊 Model Performance")
        
        metrics = result['model_metrics']
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Model Type", metrics['model_type'].replace('_', ' ').title())
        with col2:
            st.metric("Test MAE", f"{metrics['test_mae']:.2f}")
        with col3:
            st.metric("Test RMSE", f"{metrics['test_rmse']:.2f}")
        with col4:
            st.metric("Historical Days", result['historical_days'])
        
        # Combine historical and forecast data for visualization
        historical = result['historical'].copy()
        historical['type'] = 'Historical'
        historical = historical.rename(columns={'order_count': 'value'})
        
        forecast = result['forecast'].copy()
        forecast['type'] = 'Forecast'
        forecast = forecast.rename(columns={'forecast': 'value'})
        forecast = forecast[['date', 'value', 'type']]
        
        # Create combined dataframe
        combined = pd.concat([
            historical[['date', 'value', 'type']],
            forecast
        ], ignore_index=True)
        
        # Visualization
        st.markdown("---")
        st.subheader("📈 Forecast Visualization")
        
        # Time series chart
        fig = go.Figure()
        
        # Historical data
        hist_data = historical[historical['type'] == 'Historical']
        fig.add_trace(go.Scatter(
            x=hist_data['date'],
            y=hist_data['value'],
            mode='lines+markers',
            name='Historical Orders',
            line=dict(color='#1f77b4', width=2),
            marker=dict(size=4)
        ))
        
        # Forecast data
        forecast_data = forecast[forecast['type'] == 'Forecast']
        fig.add_trace(go.Scatter(
            x=forecast_data['date'],
            y=forecast_data['value'],
            mode='lines+markers',
            name='Forecasted Orders',
            line=dict(color='#ff7f0e', width=2, dash='dash'),
            marker=dict(size=4)
        ))
        
        # Add vertical line to separate historical and forecast
        # Using add_shape instead of add_vline to avoid datetime computation issues
        if len(hist_data) > 0:
            last_hist_date = hist_data['date'].max()
            
            # Add a vertical line using add_shape (works reliably with datetime)
            fig.add_shape(
                type="line",
                x0=last_hist_date,
                x1=last_hist_date,
                y0=0,
                y1=1,
                yref="paper",  # Use paper coordinates (0-1) for y-axis
                line=dict(
                    color="gray",
                    width=2,
                    dash="dot"
                )
            )
            
            # Add annotation for the line
            fig.add_annotation(
                x=last_hist_date,
                y=1,
                yref="paper",
                text="Forecast Start",
                showarrow=False,
                xanchor="left",
                yshift=10,
                font=dict(size=10, color="gray")
            )
        
        fig.update_layout(
            title="Order Volume Forecast",
            xaxis_title="Date",
            yaxis_title="Number of Orders",
            hovermode='x unified',
            height=500,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Summary statistics
        st.markdown("---")
        st.subheader("📋 Forecast Summary")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_historical = historical['value'].mean()
            st.metric("Avg Historical Orders/Day", f"{avg_historical:.1f}")
        
        with col2:
            avg_forecast = forecast_data['value'].mean()
            st.metric("Avg Forecasted Orders/Day", f"{avg_forecast:.1f}")
        
        with col3:
            change_pct = ((avg_forecast - avg_historical) / avg_historical * 100) if avg_historical > 0 else 0
            st.metric("Expected Change", f"{change_pct:+.1f}%")
        
        # Forecast table
        st.markdown("---")
        st.subheader("📅 Detailed Forecast")
        
        # Format forecast table
        forecast_display = forecast_data.copy()
        forecast_display['date'] = forecast_display['date'].dt.strftime('%Y-%m-%d')
        forecast_display = forecast_display.rename(columns={
            'date': 'Date',
            'value': 'Forecasted Orders'
        })
        forecast_display['Forecasted Orders'] = forecast_display['Forecasted Orders'].round(1)
        
        st.dataframe(
            forecast_display,
            use_container_width=True,
            height=400
        )
        
        # Download forecast
        st.markdown("---")
        st.subheader("📥 Export Forecast")
        
        csv_forecast = forecast_display.to_csv(index=False)
        st.download_button(
            label="⬇️ Download Forecast CSV",
            data=csv_forecast,
            file_name=f"order_forecast_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
