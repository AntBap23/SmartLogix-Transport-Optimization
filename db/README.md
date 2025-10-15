# SmartLogix Transport Optimization Database

This directory contains the database setup and management scripts for the SmartLogix Transport Optimization project.

## Quick Start

### 1. Setup Database
```bash
# Run the automated setup script
python db/setup_database.py
```

### 2. Manual Setup (Alternative)
```bash
# Start PostgreSQL with Docker Compose
docker-compose up -d

# Load data
python db/load_data.py
```

## Database Structure

### Tables
- **delhivery_data**: Transport trip data with timing and efficiency metrics
- **distance_data**: Distance matrix between cities/locations
- **order_data**: Order information with source, destination, and deadlines
- **supply_chain_data**: Supply chain metrics including costs, suppliers, and performance
- **weather_data**: Weather conditions for route optimization
- **inventory_data**: Inventory levels and product information

### Views
- **transport_summary**: Aggregated transport metrics by route
- **order_summary**: Order statistics by source-destination pairs
- **dashboard_analytics**: Key performance indicators for dashboard

### Functions
- **calculate_transport_efficiency()**: Calculate efficiency for specific routes
- **get_route_distance()**: Get distance between two locations
- **get_data_quality_metrics()**: Generate data quality reports
- **get_transport_analytics()**: Get comprehensive transport analytics

## Database Connection

### Connection Details
- **Host**: localhost
- **Port**: 5432
- **Database**: smartlogix_transport
- **Username**: smartlogix_user
- **Password**: smartlogix_password

### PgAdmin Access
- **URL**: http://localhost:8080
- **Email**: admin@smartlogix.com
- **Password**: admin123

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

