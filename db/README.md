# SmartLogix Transport Optimization Data Management

This directory contains the data loading and management scripts for the SmartLogix Transport Optimization project, optimized for Databricks.

## Quick Start

### 1. Databricks Setup
```bash
# Load data into Databricks tables
python db/load_data.py
```

### 2. Local Development (Alternative)
```bash
# Install dependencies
pip install -r requirements.txt

# Load data (will create parquet files)
python db/load_data.py
```

## Data Structure

### Databricks Tables
- **delhivery_data**: Transport trip data with timing and efficiency metrics
- **distance_data**: Distance matrix between cities/locations
- **order_data**: Order information with source, destination, and deadlines
- **supply_chain_data**: Supply chain metrics including costs, suppliers, and performance
- **weather_data**: Weather conditions for route optimization
- **inventory_data**: Inventory levels and product information

### Data Formats
- **Delta Tables**: For Databricks optimized storage and ACID transactions
- **Parquet Files**: For efficient columnar storage and analytics
- **CSV Files**: Original data format in `data/raw/` directory

### Spark SQL Views
- **transport_summary**: Aggregated transport metrics by route
- **order_summary**: Order statistics by source-destination pairs
- **dashboard_analytics**: Key performance indicators for dashboard

## Databricks Connection

### Workspace Setup
- **Platform**: Databricks Workspace
- **Cluster**: Python 3.9+ with Spark 3.4+
- **Storage**: DBFS (Databricks File System)
- **Tables**: Managed Delta tables in default catalog

## Data Loading

### Supported File Formats
- CSV files (delhivery.csv, distance.csv, order_*.csv, supply_chain_data.csv)
- Excel files (.xlsx) for inventory data

### Loading Data
```bash
# Load all data
python db/load_data.py

# Clear all tables (use with caution)
python db/load_data.py --clear
```

## Database Management

### Start/Stop Database
```bash
# Start database
docker-compose up -d

# Stop database
docker-compose down

# View logs
docker-compose logs postgres
```

### Backup Database
```bash
# Create backup
docker exec smartlogix_postgres pg_dump -U smartlogix_user smartlogix_transport > backup.sql

# Restore backup
docker exec -i smartlogix_postgres psql -U smartlogix_user smartlogix_transport < backup.sql
```

## Data Quality Monitoring

The database includes built-in data quality monitoring:

```sql
-- Get data quality metrics
SELECT * FROM get_data_quality_metrics();

-- Get transport analytics
SELECT * FROM get_transport_analytics();

-- Get dashboard analytics
SELECT * FROM dashboard_analytics;
```

## Troubleshooting

### Common Issues

1. **Port 5432 already in use**
   ```bash
   # Change port in docker-compose.yml
   ports:
     - "5433:5432"  # Use port 5433 instead
   ```

2. **Permission denied errors**
   ```bash
   # Make scripts executable
   chmod +x db/*.py
   ```

3. **Data loading fails**
   ```bash
   # Check file paths and permissions
   ls -la data/raw/
   ```

4. **Database connection issues**
   ```bash
   # Check if container is running
   docker-compose ps
   
   # Check container logs
   docker-compose logs postgres
   ```

### Reset Database
```bash
# Stop and remove containers
docker-compose down -v

# Remove volumes (this will delete all data)
docker volume rm smartlogix-transport-optimization-1_postgres_data

# Start fresh
docker-compose up -d
```

## Development

### Adding New Tables
1. Update `db/schema.sql` with new table definition
2. Add data loading logic to `db/load_data.py`
3. Update `db/init.sql` with any new functions or views
4. Restart database to apply changes

### Performance Optimization
- Indexes are automatically created for common query patterns
- Materialized views are available for complex analytics
- Use `REFRESH MATERIALIZED VIEW route_performance` to update cached data

## Security Notes

- Default credentials are for development only
- Change passwords in production
- Use environment variables for sensitive data
- Enable SSL for production deployments

