# 🚚 SmartLogix Transport Optimization

SmartLogix is an intelligent routing and logistics optimization platform built with **Python**, featuring a **greedy nearest-neighbor algorithm** for efficient route planning and **Streamlit** for interactive visualization. The platform optimizes delivery routes with vehicle capacity constraints and distance minimization, integrated with **Tableau** for advanced analytics.

---

## 🧠 Project Overview

- **Goal**: Optimize last-mile delivery routes and minimize transportation costs
- **Optimization Engine**: Greedy nearest-neighbor algorithm with vehicle capacity constraints (fast, reliable, production-ready)
- **Interface**: Streamlit dashboard with interactive visualizations and Tableau integration
- **Data Source**: CSV files for portability and easy deployment
- **Visualization**: Interactive maps (Folium), charts (Plotly), and embedded Tableau dashboards

---

## 📁 Repository Structure

```
SmartLogix-Transport-Optimization/
│
├── app/                    # Core application modules
│   ├── dashboard/         # Streamlit dashboard
│   │   ├── app.py         # Main dashboard application
│   │   ├── dashboard.png  # Tableau dashboard screenshot
│   │   └── route_visuals/  # Route optimization visualizations
│   ├── optimizer.py       # Route optimization engine
│   └── visualize_routes.py # Visualization tools
│
├── data/                   # Data files
│   ├── coherent/          # Processed data files
│   ├── external/          # External data sources
│   └── optimizer/         # Optimizer-specific data (CSV)
│       ├── optimizer_orders.csv
│       └── optimizer_distances.csv
│
├── postgresql/            # PostgreSQL configuration (optional)
├── scripts/               # Utility scripts
│   ├── generate_coherent_data.py
│   └── verify_data_connections.py
│
├── generate_route_visuals.py  # Standalone route visualization generator
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

---

## ⚙️ Core Technologies

| Component          | Stack/Tools                                      |
|--------------------|--------------------------------------------------|
| Optimization       | Python, Greedy Nearest-Neighbor Algorithm        |
| Route Planning     | Custom distance matrix, vehicle capacity constraints |
| Dashboard          | Streamlit for interactive web interface          |
| Visualization      | Folium for maps, Plotly for charts              |
| Analytics          | Tableau for advanced analytics and reporting     |
| Data Processing    | pandas, numpy for data manipulation             |
| Data Storage       | CSV files (PostgreSQL optional)                  |

---

## 🚀 Deployment

The app can be deployed to **Streamlit Cloud** (free) or other platforms. Some data files are too large for GitHub (>100MB limit), but the app works with the included smaller files.

**Quick Deploy to Streamlit Cloud:**
1. Push your code to GitHub (large files are excluded via `.gitignore`)
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub repo
4. Set main file: `app/dashboard/app.py`
5. Deploy!

For detailed deployment options (including handling large files), see [DEPLOYMENT.md](DEPLOYMENT.md).

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/AntBap23/SmartLogix-Transport-Optimization.git
cd SmartLogix-Transport-Optimization
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Prepare Data

The optimizer uses CSV files from `data/optimizer/`:
- `optimizer_orders.csv` - Order data (order_id, source, destination, available_time, deadline, weight, order_size, material_id)
- `optimizer_distances.csv` - Distance matrix (source, destination, distance_meters)

These files should already be in the repository. If not, export them from your database using the SQL queries in `OPTIMIZER_SQL_QUERIES.sql`.

---

## ▶️ How to Use

### Launch Dashboard

```bash
streamlit run app/dashboard/app.py
```

The dashboard will open at `http://localhost:8501` with four tabs:

1. **📋 Project Overview** - Project description, objectives, and technologies
2. **📊 Analytics Dashboard** - Tableau dashboard integration with embedded interactive visualizations
3. **🔍 Findings & Insights** - Analysis insights, recommendations, and impact
4. **🗺️ Route Optimization** - Route optimization visualizations (maps and charts)

### Generate Route Visualizations

To generate new route optimization visualizations:

```bash
python generate_route_visuals.py
```

This will:
- Load orders and distances from CSV files
- Run the greedy route optimization algorithm
- Generate interactive HTML visualizations:
  - `route_optimization_map.html` - Interactive map
  - `route_optimization_chart.html` - Route statistics charts
- Save files to `app/dashboard/route_visuals/` for display in the dashboard

### Run Route Optimization Programmatically

```python
from app.optimizer import optimize_delivery_routes

# Optimize routes
results = optimize_delivery_routes(
    num_vehicles=2,
    limit=20,
    max_orders=20,
    max_locations=20,
    use_ortools=False  # Use greedy algorithm
)

print(f'Status: {results.get("optimization_status")}')
print(f'Total Distance: {results.get("total_distance_meters", 0) / 1000:.2f} km')
print(f'Vehicles Used: {results.get("total_vehicles_used", 0)}')
```

---

## 📊 Dashboard Features

### Analytics Dashboard Tab
- **Tableau Integration**: Embedded interactive Tableau Public dashboard
- **Static Screenshot**: Tableau dashboard image for quick reference
- **Direct Link**: Access to full Tableau dashboard on Tableau Public

### Route Optimization Tab
- **Interactive Maps**: Folium-based route visualizations
- **Route Charts**: Plotly charts showing route statistics
- **Route Details**: Detailed breakdown of each vehicle route
- **Export Options**: JSON export for further analysis

### Project Overview Tab
- Complete project documentation
- Technology stack overview
- Objectives and goals

### Findings & Insights Tab
- Route optimization insights
- Data analytics findings
- Recommendations for improvement
- Impact analysis

---

## 🔧 Configuration

### Environment Variables (Optional)

If using PostgreSQL, create a `.env` file:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_SCHEMA=transport_data
```

**Note**: The dashboard works with CSV files and doesn't require a database connection.

---

## 📦 Dependencies

Key dependencies (see `requirements.txt` for full list):
- `streamlit` - Web dashboard framework
- `pandas` - Data manipulation
- `numpy` - Numerical operations
- `folium` - Interactive maps
- `plotly` - Interactive charts
- `psycopg2-binary` - PostgreSQL connector (optional)

---

## 🎯 Key Features

- ✅ **Greedy Route Optimization** - Fast, reliable nearest-neighbor algorithm
- ✅ **Interactive Dashboard** - Streamlit-based web interface
- ✅ **Tableau Integration** - Embedded Tableau Public dashboards
- ✅ **CSV-Based** - No database required for core functionality
- ✅ **Route Visualization** - Interactive maps and charts
- ✅ **Multi-Vehicle Routing** - Optimize routes for multiple vehicles
- ✅ **Capacity Constraints** - Respect vehicle weight limits
- ✅ **Distance Minimization** - Optimize total travel distance

---

## 📈 Project Status

- ✅ Route optimization algorithm implemented
- ✅ Streamlit dashboard functional
- ✅ Tableau dashboard integrated
- ✅ CSV data loading working
- ✅ Route visualizations generated
- ⚠️ OR-Tools integration (optional, may have compatibility issues)

---

## 🤝 Contributing

This is a portfolio project. For questions or suggestions, please open an issue.

---

## 📄 License

See `LICENSE` file for details.

---

## 🔗 Links

- **Tableau Dashboard**: [View on Tableau Public](https://public.tableau.com/views/SupplyChainDashboard_17645140839010/Dashboard1?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)
- **GitHub Repository**: [SmartLogix-Transport-Optimization](https://github.com/AntBap23/SmartLogix-Transport-Optimization)

---

## 👤 Author

**Anthony Bapst**
- GitHub: [@AntBap23](https://github.com/AntBap23)

---

*Built with ❤️ for efficient logistics and route optimization*
