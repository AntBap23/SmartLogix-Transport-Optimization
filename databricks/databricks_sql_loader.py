#!/usr/bin/env python3
"""
Databricks SQL Data Loader for SmartLogix Transport Optimization
This script loads cleaned CSV data into Databricks SQL tables.
"""

import os
import sys
import pandas as pd
import logging
from typing import Dict, Any, Optional
import warnings

# Databricks imports
try:
    from databricks import sql
    from databricks.sql import connect
    DATABRICKS_SQL_AVAILABLE = True
except ImportError:
    DATABRICKS_SQL_AVAILABLE = False
    print("Databricks SQL connector not available. Install with: pip install databricks-sql-connector")

# Suppress warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabricksSQLLoader:
    """Databricks SQL data loader."""
    
    def __init__(self, 
                 server_hostname: str,
                 http_path: str,
                 access_token: str,
                 catalog: str = "smartlogix",
                 schema: str = "transport_data"):
        """Initialize Databricks SQL loader."""
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.catalog = catalog
        self.schema = schema
        self.connection = None
        
    def connect(self) -> bool:
        """Establish connection to Databricks SQL."""
        if not DATABRICKS_SQL_AVAILABLE:
            logger.error("Databricks SQL connector not available")
            return False
            
        try:
            self.connection = connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token
            )
            logger.info("Connected to Databricks SQL successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Databricks SQL: {e}")
            return False
    
    def disconnect(self):
        """Close connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Databricks SQL")
    
    def execute_sql(self, sql_query: str) -> bool:
        """Execute SQL query."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_query)
                logger.info("SQL query executed successfully")
                return True
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {e}")
            logger.error(f"Query: {sql_query}")
            return False
    
    def create_database_and_schema(self) -> bool:
        """Create database and schema."""
        logger.info("Creating database and schema...")
        
        queries = [
            f"CREATE CATALOG IF NOT EXISTS {self.catalog}",
            f"USE CATALOG {self.catalog}",
            f"CREATE SCHEMA IF NOT EXISTS {self.schema}",
            f"USE SCHEMA {self.catalog}.{self.schema}"
        ]
        
        for query in queries:
            if not self.execute_sql(query):
                return False
        
        logger.info("Database and schema created successfully")
        return True
    
    def create_tables(self) -> bool:
        """Create tables in Databricks SQL."""
        logger.info("Creating tables...")
        
        # Table creation queries
        table_queries = {
            'delhivery_data': """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.delhivery_data (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    data_type STRING,
                    trip_creation_time TIMESTAMP,
                    route_schedule_uuid STRING,
                    route_type STRING,
                    trip_uuid STRING,
                    source_center STRING,
                    source_name STRING,
                    destination_center STRING,
                    destination_name STRING,
                    od_start_time TIMESTAMP,
                    od_end_time TIMESTAMP,
                    start_scan_to_end_scan DOUBLE,
                    is_cutoff BOOLEAN,
                    cutoff_factor INT,
                    cutoff_timestamp TIMESTAMP,
                    actual_distance_to_destination DOUBLE,
                    actual_time DOUBLE,
                    osrm_time DOUBLE,
                    osrm_distance DOUBLE,
                    factor DOUBLE,
                    segment_actual_time DOUBLE,
                    segment_osrm_time DOUBLE,
                    segment_osrm_distance DOUBLE,
                    segment_factor DOUBLE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                ) USING DELTA
            """,
            
            'distance_data': """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.distance_data (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    source STRING,
                    destination STRING,
                    distance_meters INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                ) USING DELTA
            """,
            
            'order_data': """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.order_data (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    order_id STRING,
                    material_id STRING,
                    item_id STRING,
                    source STRING,
                    destination STRING,
                    available_time TIMESTAMP,
                    deadline TIMESTAMP,
                    danger_type STRING,
                    area DOUBLE,
                    weight DOUBLE,
                    order_size STRING,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                ) USING DELTA
            """,
            
            'supply_chain_data': """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.supply_chain_data (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    product_type STRING,
                    sku STRING,
                    price DOUBLE,
                    availability INT,
                    number_of_products_sold INT,
                    revenue_generated DOUBLE,
                    customer_demographics STRING,
                    stock_levels INT,
                    lead_times INT,
                    order_quantities INT,
                    shipping_times INT,
                    shipping_carriers STRING,
                    shipping_costs DOUBLE,
                    supplier_name STRING,
                    location STRING,
                    lead_time INT,
                    production_volumes INT,
                    manufacturing_lead_time INT,
                    manufacturing_costs DOUBLE,
                    inspection_results STRING,
                    defect_rates DOUBLE,
                    transportation_modes STRING,
                    routes STRING,
                    costs DOUBLE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                ) USING DELTA
            """,
            
            'weather_data': """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.weather_data (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    location STRING,
                    date DATE,
                    temperature DOUBLE,
                    humidity DOUBLE,
                    wind_speed DOUBLE,
                    precipitation DOUBLE,
                    weather_condition STRING,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                ) USING DELTA
            """,
            
            'inventory_data': """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.inventory_data (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    product_id STRING,
                    product_name STRING,
                    category STRING,
                    current_stock INT,
                    min_stock_level INT,
                    max_stock_level INT,
                    unit_cost DOUBLE,
                    supplier STRING,
                    location STRING,
                    last_updated TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                ) USING DELTA
            """
        }
        
        for table_name, query in table_queries.items():
            formatted_query = query.format(catalog=self.catalog, schema=self.schema)
            if not self.execute_sql(formatted_query):
                logger.error(f"Failed to create table: {table_name}")
                return False
            logger.info(f"Created table: {table_name}")
        
        logger.info("All tables created successfully")
        return True
    
    def load_data_from_csv(self, table_name: str, csv_file_path: str) -> bool:
        """Load data from CSV file using COPY INTO."""
        logger.info(f"Loading data into {table_name} from {csv_file_path}")
        
        # Upload file to DBFS first (this would be done manually in Databricks UI)
        # For now, we'll use a placeholder path
        dbfs_path = f"/FileStore/shared_uploads/smartlogix/processed/{os.path.basename(csv_file_path)}"
        
        copy_query = f"""
            COPY INTO {self.catalog}.{self.schema}.{table_name}
            FROM '{dbfs_path}'
            FILEFORMAT = CSV
            FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
            COPY_OPTIONS ('mergeSchema' = 'true')
        """
        
        return self.execute_sql(copy_query)
    
    def create_views(self) -> bool:
        """Create analytics views."""
        logger.info("Creating analytics views...")
        
        views = {
            'transport_summary': f"""
                CREATE OR REPLACE VIEW {self.catalog}.{self.schema}.transport_summary AS
                SELECT 
                    source_center,
                    destination_center,
                    COUNT(*) as trip_count,
                    AVG(actual_time) as avg_actual_time,
                    AVG(osrm_time) as avg_osrm_time,
                    AVG(factor) as avg_factor,
                    AVG(actual_distance_to_destination) as avg_distance
                FROM {self.catalog}.{self.schema}.delhivery_data 
                GROUP BY source_center, destination_center
            """,
            
            'order_summary': f"""
                CREATE OR REPLACE VIEW {self.catalog}.{self.schema}.order_summary AS
                SELECT 
                    source,
                    destination,
                    order_size,
                    COUNT(*) as order_count,
                    SUM(weight) as total_weight,
                    AVG(EXTRACT(EPOCH FROM (deadline - available_time))/3600) as avg_hours_to_deadline
                FROM {self.catalog}.{self.schema}.order_data 
                GROUP BY source, destination, order_size
            """,
            
            'dashboard_analytics': f"""
                CREATE OR REPLACE VIEW {self.catalog}.{self.schema}.dashboard_analytics AS
                SELECT 
                    'Total Transport Records' as metric,
                    (SELECT COUNT(*) FROM {self.catalog}.{self.schema}.delhivery_data) as value
                UNION ALL
                SELECT 
                    'Total Orders',
                    (SELECT COUNT(*) FROM {self.catalog}.{self.schema}.order_data)
                UNION ALL
                SELECT 
                    'Total Routes',
                    (SELECT COUNT(DISTINCT CONCAT(source_center, '->', destination_center)) 
                     FROM {self.catalog}.{self.schema}.delhivery_data)
                UNION ALL
                SELECT 
                    'Average Efficiency',
                    (SELECT ROUND(AVG(factor), 2) 
                     FROM {self.catalog}.{self.schema}.delhivery_data 
                     WHERE factor IS NOT NULL)
            """
        }
        
        for view_name, query in views.items():
            if not self.execute_sql(query):
                logger.error(f"Failed to create view: {view_name}")
                return False
            logger.info(f"Created view: {view_name}")
        
        logger.info("All views created successfully")
        return True
    
    def verify_data_loading(self) -> Dict[str, Any]:
        """Verify data was loaded correctly."""
        logger.info("Verifying data loading...")
        
        verification_queries = {
            'delhivery_data': f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.delhivery_data",
            'distance_data': f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.distance_data",
            'order_data': f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.order_data",
            'supply_chain_data': f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.supply_chain_data",
            'weather_data': f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.weather_data",
            'inventory_data': f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.inventory_data"
        }
        
        results = {}
        
        for table_name, query in verification_queries.items():
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchone()
                    count = result[0] if result else 0
                    results[table_name] = count
                    logger.info(f"{table_name}: {count:,} records")
            except Exception as e:
                logger.error(f"Failed to verify {table_name}: {e}")
                results[table_name] = -1
        
        return results

def main():
    """Main function to load data into Databricks SQL."""
    logger.info("Databricks SQL Data Loading Started")
    logger.info("=" * 50)
    
    # Configuration - Update these with your actual values
    config = {
        'server_hostname': os.getenv('DATABRICKS_SERVER_HOSTNAME', 'your-workspace.cloud.databricks.com'),
        'http_path': os.getenv('DATABRICKS_HTTP_PATH', '/sql/1.0/warehouses/your-warehouse-id'),
        'access_token': os.getenv('DATABRICKS_ACCESS_TOKEN', 'your-access-token'),
        'catalog': 'smartlogix',
        'schema': 'transport_data'
    }
    
    # Initialize loader
    loader = DatabricksSQLLoader(**config)
    
    if not loader.connect():
        logger.error("Failed to connect to Databricks SQL")
        return False
    
    try:
        # Step 1: Create database and schema
        if not loader.create_database_and_schema():
            logger.error("Failed to create database and schema")
            return False
        
        # Step 2: Create tables
        if not loader.create_tables():
            logger.error("Failed to create tables")
            return False
        
        # Step 3: Load data (Note: Files must be uploaded to DBFS first)
        processed_data_dir = "data/processed"
        data_files = {
            'delhivery_data': 'delhivery_cleaned.csv',
            'distance_data': 'distance_cleaned.csv',
            'order_data': 'orders_cleaned.csv',
            'supply_chain_data': 'supply_chain_cleaned.csv',
            'weather_data': 'weather_cleaned.csv',
            'inventory_data': 'inventory_cleaned.csv'
        }
        
        for table_name, filename in data_files.items():
            file_path = os.path.join(processed_data_dir, filename)
            if os.path.exists(file_path):
                if loader.load_data_from_csv(table_name, file_path):
                    logger.info(f"Successfully loaded data into {table_name}")
                else:
                    logger.error(f"Failed to load data into {table_name}")
            else:
                logger.warning(f"File not found: {file_path}")
        
        # Step 4: Create views
        if not loader.create_views():
            logger.error("Failed to create views")
            return False
        
        # Step 5: Verify data loading
        verification_results = loader.verify_data_loading()
        
        # Print summary
        print("\n" + "=" * 50)
        print("DATA LOADING SUMMARY")
        print("=" * 50)
        total_records = 0
        for table_name, count in verification_results.items():
            if count >= 0:
                print(f"✅ {table_name}: {count:,} records")
                total_records += count
            else:
                print(f"❌ {table_name}: Failed to verify")
        
        print(f"\nTotal records loaded: {total_records:,}")
        print(f"Database: {config['catalog']}.{config['schema']}")
        
        logger.info("Data loading completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        return False
        
    finally:
        loader.disconnect()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
