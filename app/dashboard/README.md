# SmartLogix Transport Optimization Dashboard

Streamlit dashboard for visualizing transport and order data from PostgreSQL.

## Quick Start

### 1. Make sure PostgreSQL is running and data is loaded

```bash
# Check if PostgreSQL is running
pg_isready

# Load data if needed
python postgresql/postgresql_sql_loader.py
```

### 2. Set up environment variables (optional)

Create a `.env` file in the project root if you need custom database settings:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=smartlogix_transport
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_SCHEMA=transport_data
```

### 3. Run the dashboard

```bash
# From project root
streamlit run app/dashboard/app.py

# Or use the run script
./app/dashboard/run_dashboard.sh
```

The dashboard will open in your browser at `http://localhost:8501`

## Features

- **KPIs**: Number of Orders, Deliveries, Average Delivery Time, Average Weight
- **Filters**: Route Type, Order Size, Danger Type, Product
- **Visualizations**:
  - Top 5 Destination Centers (bar chart)
  - Top 5 Products (bar chart)
  - Source Center Trends (line charts over time)

## Troubleshooting

### Database Connection Error

- Make sure PostgreSQL is running: `pg_isready`
- Check your `.env` file or environment variables
- Verify database and schema exist
- Check that data has been loaded into the tables

### No Data Showing

- Ensure data is loaded: `python postgresql/postgresql_sql_loader.py`
- Check table row counts in PostgreSQL
- Verify schema name matches your configuration

