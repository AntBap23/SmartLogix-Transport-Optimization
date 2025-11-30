#!/usr/bin/env python3
"""
PostgreSQL Data Loader for SmartLogix Transport Optimization
This script loads cleaned CSV data into PostgreSQL tables.
"""

import os
import sys
import pandas as pd
import logging
from typing import Dict, Any, Optional
import warnings
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not available, will use system environment variables only
    pass

# PostgreSQL imports
try:
    import psycopg2
    from psycopg2.extras import execute_values
    from psycopg2.pool import SimpleConnectionPool
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    print("psycopg2 not available. Install with: pip install psycopg2-binary")

# Suppress warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PostgreSQLDataLoader:
    """PostgreSQL data loader."""
    
    def __init__(self, 
                 host: str,
                 port: int,
                 database: str,
                 user: str,
                 password: str,
                 schema: str = "transport_data"):
        """Initialize PostgreSQL loader."""
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        self.connection = None
        self.pool = None
        
    def connect(self) -> bool:
        """Establish connection to PostgreSQL."""
        if not PSYCOPG2_AVAILABLE:
            logger.error("psycopg2 not available")
            return False
            
        try:
            # Build connection parameters - only include password if provided
            conn_params = {
                'host': self.host,
                'port': self.port,
                'database': self.database,
                'user': self.user
            }
            if self.password:
                conn_params['password'] = self.password
            
            self.connection = psycopg2.connect(**conn_params)
            self.connection.autocommit = False
            logger.info("Connected to PostgreSQL successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect(self):
        """Close connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from PostgreSQL")
        if self.pool:
            self.pool.closeall()
    
    def execute_sql(self, sql_query: str, params: Optional[tuple] = None) -> bool:
        """Execute SQL query."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_query, params)
                self.connection.commit()
                logger.info("SQL query executed successfully")
                return True
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to execute SQL query: {e}")
            logger.error(f"Query: {sql_query[:200]}...")
            return False
    
    def create_database_and_schema(self) -> bool:
        """Create database and schema."""
        logger.info("Creating database and schema...")
        
        # First, try to connect to our database directly
        try:
            test_params = {
                'host': self.host,
                'port': self.port,
                'database': self.database,
                'user': self.user
            }
            if self.password:
                test_params['password'] = self.password
            
            test_conn = psycopg2.connect(**test_params)
            test_conn.close()
            logger.info(f"Database {self.database} already exists and is accessible")
        except psycopg2.OperationalError as e:
            # Database doesn't exist, try to create it
            logger.info(f"Database {self.database} doesn't exist, attempting to create it...")
            try:
                # Connect to default postgres database to create our database
                admin_params = {
                    'host': self.host,
                    'port': self.port,
                    'database': 'postgres',
                    'user': self.user
                }
                if self.password:
                    admin_params['password'] = self.password
                
                admin_conn = psycopg2.connect(**admin_params)
                admin_conn.autocommit = True
                with admin_conn.cursor() as cursor:
                    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.database}'")
                    exists = cursor.fetchone()
                    if not exists:
                        cursor.execute(f"CREATE DATABASE {self.database}")
                        logger.info(f"✅ Created database: {self.database}")
                    else:
                        logger.info(f"Database {self.database} already exists")
                admin_conn.close()
            except Exception as create_error:
                logger.warning(f"Could not create database: {create_error}")
                logger.warning("You may need to create the database manually in pgAdmin or using: CREATE DATABASE smartlogix_transport;")
        except Exception as e:
            logger.warning(f"Unexpected error checking database: {e}")
        
        # Now reconnect to our database
        self.disconnect()
        if not self.connect():
            return False
        
        # Create schema
        queries = [
            f"CREATE SCHEMA IF NOT EXISTS {self.schema}",
            f"SET search_path TO {self.schema}, public"
        ]
        
        for query in queries:
            if not self.execute_sql(query):
                return False
        
        logger.info("Database and schema created successfully")
        return True
    
    def create_tables(self) -> bool:
        """Create tables in PostgreSQL."""
        logger.info("Creating tables...")
        
        # Table creation queries (PostgreSQL syntax)
        table_queries = {
            'delhivery_data': f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.delhivery_data (
                    id BIGSERIAL PRIMARY KEY,
                    order_id VARCHAR(255),
                    data_type VARCHAR(255),
                    trip_creation_time TIMESTAMP,
                    route_schedule_uuid VARCHAR(255),
                    route_type VARCHAR(255),
                    trip_uuid VARCHAR(255),
                    source_center VARCHAR(255),
                    source_name VARCHAR(255),
                    destination_center VARCHAR(255),
                    destination_name VARCHAR(255),
                    od_start_time TIMESTAMP,
                    od_end_time TIMESTAMP,
                    start_scan_to_end_scan DOUBLE PRECISION,
                    is_cutoff BOOLEAN,
                    cutoff_factor INTEGER,
                    cutoff_timestamp TIMESTAMP,
                    actual_distance_to_destination DOUBLE PRECISION,
                    actual_time DOUBLE PRECISION,
                    osrm_time DOUBLE PRECISION,
                    osrm_distance DOUBLE PRECISION,
                    factor DOUBLE PRECISION,
                    segment_actual_time DOUBLE PRECISION,
                    segment_osrm_time DOUBLE PRECISION,
                    segment_osrm_distance DOUBLE PRECISION,
                    segment_factor DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'distance_data': f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.distance_data (
                    id BIGSERIAL PRIMARY KEY,
                    source VARCHAR(255),
                    destination VARCHAR(255),
                    distance_meters INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'order_data': f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.order_data (
                    id BIGSERIAL PRIMARY KEY,
                    order_id VARCHAR(255) UNIQUE NOT NULL,
                    material_id VARCHAR(255),
                    item_id VARCHAR(255),
                    source VARCHAR(255),
                    destination VARCHAR(255),
                    available_time TIMESTAMP,
                    deadline TIMESTAMP,
                    danger_type VARCHAR(255),
                    area DOUBLE PRECISION,
                    weight DOUBLE PRECISION,
                    order_size VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'supply_chain_data': f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.supply_chain_data (
                    id BIGSERIAL PRIMARY KEY,
                    product_type VARCHAR(255),
                    sku VARCHAR(255),
                    price DOUBLE PRECISION,
                    availability INTEGER,
                    number_of_products_sold INTEGER,
                    revenue_generated DOUBLE PRECISION,
                    customer_demographics VARCHAR(255),
                    stock_levels INTEGER,
                    lead_times INTEGER,
                    order_quantities INTEGER,
                    shipping_times INTEGER,
                    shipping_carriers VARCHAR(255),
                    shipping_costs DOUBLE PRECISION,
                    supplier_name VARCHAR(255),
                    location VARCHAR(255),
                    lead_time INTEGER,
                    production_volumes INTEGER,
                    manufacturing_lead_time INTEGER,
                    manufacturing_costs DOUBLE PRECISION,
                    inspection_results VARCHAR(255),
                    defect_rates DOUBLE PRECISION,
                    transportation_modes VARCHAR(255),
                    routes VARCHAR(255),
                    costs DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'weather_data': f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.weather_data (
                    id BIGSERIAL PRIMARY KEY,
                    location VARCHAR(255),
                    date DATE,
                    temperature DOUBLE PRECISION,
                    temperature_2m DOUBLE PRECISION,
                    humidity DOUBLE PRECISION,
                    wind_speed DOUBLE PRECISION,
                    wind_speed_10m DOUBLE PRECISION,
                    precipitation DOUBLE PRECISION,
                    weather_condition VARCHAR(255),
                    weathercode INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'inventory_data': f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.inventory_data (
                    id BIGSERIAL PRIMARY KEY,
                    product_id VARCHAR(255),
                    product_name VARCHAR(255),
                    category VARCHAR(255),
                    current_stock INTEGER,
                    min_stock_level INTEGER,
                    max_stock_level INTEGER,
                    unit_cost DOUBLE PRECISION,
                    supplier VARCHAR(255),
                    location VARCHAR(255),
                    last_updated TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }
        
        for table_name, query in table_queries.items():
            if not self.execute_sql(query):
                logger.error(f"Failed to create table: {table_name}")
                return False
            logger.info(f"Created table: {table_name}")
        
        # Add order_id column to delhivery_data if it doesn't exist (for existing tables)
        self.add_order_id_column_if_missing()
        
        # Add foreign key constraint if it doesn't exist
        self.add_foreign_key_constraint()
        
        logger.info("All tables created successfully")
        return True
    
    def add_order_id_column_if_missing(self) -> bool:
        """Add order_id column to delhivery_data if it doesn't exist."""
        try:
            with self.connection.cursor() as cursor:
                # Check if column exists
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.schema}' 
                    AND table_name = 'delhivery_data' 
                    AND column_name = 'order_id'
                """)
                if cursor.fetchone() is None:
                    # Column doesn't exist, add it
                    cursor.execute(f"""
                        ALTER TABLE {self.schema}.delhivery_data 
                        ADD COLUMN order_id VARCHAR(255)
                    """)
                    self.connection.commit()
                    logger.info("✅ Added order_id column to delhivery_data")
                else:
                    logger.info("order_id column already exists in delhivery_data")
            return True
        except Exception as e:
            logger.warning(f"Could not add order_id column: {e}")
            return False
    
    def add_foreign_key_constraint(self) -> bool:
        """Add foreign key constraint from delhivery_data.order_id to order_data.order_id."""
        try:
            with self.connection.cursor() as cursor:
                # First ensure order_data.order_id has a unique constraint
                cursor.execute(f"""
                    DO $$
                    BEGIN
                        -- Add unique constraint to order_id if it doesn't exist
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint 
                            WHERE conrelid = '{self.schema}.order_data'::regclass
                            AND contype = 'u'
                            AND conname = 'uk_order_id'
                        ) THEN
                            ALTER TABLE {self.schema}.order_data 
                            ADD CONSTRAINT uk_order_id UNIQUE (order_id);
                        END IF;
                    END $$;
                """)
                self.connection.commit()
                
                # Check if foreign key constraint already exists
                cursor.execute(f"""
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_schema = '{self.schema}' 
                    AND table_name = 'delhivery_data' 
                    AND constraint_type = 'FOREIGN KEY'
                    AND constraint_name LIKE '%order%'
                """)
                if cursor.fetchone() is None:
                    # Constraint doesn't exist, add it
                    cursor.execute(f"""
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_constraint 
                                WHERE conname = 'fk_delhivery_order' 
                                AND conrelid = '{self.schema}.delhivery_data'::regclass
                            ) THEN
                                ALTER TABLE {self.schema}.delhivery_data 
                                ADD CONSTRAINT fk_delhivery_order 
                                FOREIGN KEY (order_id) 
                                REFERENCES {self.schema}.order_data(order_id);
                            END IF;
                        END $$;
                    """)
                    self.connection.commit()
                    logger.info("✅ Added foreign key constraint fk_delhivery_order")
                else:
                    logger.info("Foreign key constraint already exists")
            return True
        except Exception as e:
            logger.warning(f"Could not add foreign key constraint: {e}")
            # This is okay if constraint already exists or if there are NULL values
            return True
    
    def load_data_from_csv(self, table_name: str, csv_file_path: str, batch_size: int = 1000) -> bool:
        """Load data from CSV file using COPY or batch insert."""
        logger.info(f"Loading data into {table_name} from {csv_file_path}")
        
        if not os.path.exists(csv_file_path):
            logger.error(f"File not found: {csv_file_path}")
            return False
        
        try:
            # Read CSV in chunks
            chunk_size = batch_size
            total_rows = 0
            
            for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, low_memory=False):
                # Clean column names (remove spaces, special chars, handle duplicates)
                cleaned_cols = []
                col_counts = {}
                for col in chunk.columns:
                    clean_col = col.strip().replace(' ', '_').lower()
                    # Handle special characters and dots
                    clean_col = clean_col.replace('.', '_').replace('-', '_')
                    # Remove any remaining special chars except underscore
                    clean_col = ''.join(c if c.isalnum() or c == '_' else '_' for c in clean_col)
                    # Handle duplicate column names
                    if clean_col in col_counts:
                        col_counts[clean_col] += 1
                        clean_col = f"{clean_col}_{col_counts[clean_col]}"
                    else:
                        col_counts[clean_col] = 0
                    cleaned_cols.append(clean_col)
                
                chunk.columns = cleaned_cols
                
                # Map CSV columns to table columns for specific tables
                if table_name == 'weather_data' and 'time' in chunk.columns:
                    chunk = chunk.rename(columns={'time': 'date'})
                if table_name == 'weather_data' and 'temperature_2m' in chunk.columns:
                    chunk = chunk.rename(columns={'temperature_2m': 'temperature'})
                if table_name == 'weather_data' and 'wind_speed_10m' in chunk.columns:
                    chunk = chunk.rename(columns={'wind_speed_10m': 'wind_speed'})
                if table_name == 'weather_data' and 'weathercode' in chunk.columns:
                    chunk = chunk.rename(columns={'weathercode': 'weather_condition'})
                
                # Map inventory CSV columns to table columns (after cleaning)
                if table_name == 'inventory_data':
                    inventory_mapping = {
                        'sku_id': 'product_id',
                        'product': 'product_name',
                        'product_reference': 'category',  # Use Product Reference as category fallback
                        'product_category_from_phase_1': 'category',
                        'product_category': 'category',
                        'current_stock_quantity': 'current_stock',
                        'lower_maintaining_inventory_level_lmilpi_pc': 'min_stock_level',
                        'lower_maintaining_inventory_level': 'min_stock_level',
                        'upper_maintaining_inventory_level_umilpi_pc': 'max_stock_level',
                        'upper_maintaining_inventory_level': 'max_stock_level',
                        'unit_price': 'unit_cost',
                        'cost_cp_php_pc': 'unit_cost',
                        'store_location_i': 'location',
                        'store_location': 'location',
                        'order_date': 'last_updated'
                    }
                    # Apply mapping
                    for old_col, new_col in inventory_mapping.items():
                        if old_col in chunk.columns:
                            chunk = chunk.rename(columns={old_col: new_col})
                    
                    # If category is still empty but product_reference exists, use it
                    if 'category' not in chunk.columns or chunk['category'].isna().all():
                        if 'product_reference' in chunk.columns:
                            chunk['category'] = chunk['product_reference']
                
                # Remove duplicate columns (keep first occurrence)
                chunk = chunk.loc[:, ~chunk.columns.duplicated()]
                
                # Replace NaN with None for proper NULL handling in PostgreSQL
                chunk = chunk.where(pd.notnull(chunk), None)
                
                # Filter out incomplete rows based on essential columns for route optimization
                # Only keep rows with complete data for analysis and dashboards
                if table_name == 'delhivery_data':
                    # Essential: source, destination, and at least one time/distance metric
                    essential_cols = ['source_center', 'destination_center']
                    chunk = chunk.dropna(subset=essential_cols, how='any')
                    # Also require at least one of: actual_time, osrm_time, actual_distance_to_destination
                    if len(chunk) > 0:
                        chunk = chunk[
                            chunk['actual_time'].notna() | 
                            chunk['osrm_time'].notna() | 
                            chunk['actual_distance_to_destination'].notna()
                        ]
                
                elif table_name == 'distance_data':
                    # Essential: source, destination, distance
                    essential_cols = ['source', 'destination', 'distance_meters']
                    chunk = chunk.dropna(subset=essential_cols, how='any')
                
                elif table_name == 'order_data':
                    # Essential: source, destination, order_id
                    essential_cols = ['source', 'destination', 'order_id']
                    chunk = chunk.dropna(subset=essential_cols, how='any')
                
                elif table_name == 'inventory_data':
                    # Essential: product_id AND current_stock (only keep rows with actual inventory data)
                    essential_cols = ['product_id', 'current_stock']
                    chunk = chunk.dropna(subset=essential_cols, how='any')
                    # Drop rows where current_stock is 0 or negative (not useful for analysis)
                    if 'current_stock' in chunk.columns:
                        chunk = chunk[chunk['current_stock'] > 0]
                
                elif table_name == 'supply_chain_data':
                    # Essential: product_type or sku
                    if 'product_type' in chunk.columns:
                        chunk = chunk.dropna(subset=['product_type'], how='any')
                    elif 'sku' in chunk.columns:
                        chunk = chunk.dropna(subset=['sku'], how='any')
                
                elif table_name == 'weather_data':
                    # Essential: date (location may not be in CSV, check if it exists)
                    essential_cols = ['date']
                    if 'location' in chunk.columns:
                        essential_cols.append('location')
                    chunk = chunk.dropna(subset=essential_cols, how='any')
                
                # Drop completely empty rows
                chunk = chunk.dropna(how='all')
                
                # Skip if chunk is empty after filtering
                if len(chunk) == 0:
                    continue
                
                # Get table columns from database
                with self.connection.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = '{self.schema}' 
                        AND table_name = '{table_name}'
                        AND column_name != 'id'
                        AND column_name != 'created_at'
                        ORDER BY ordinal_position
                    """)
                    table_columns = [row[0] for row in cursor.fetchall()]
                
                # Filter chunk columns to only those that exist in the table
                valid_columns = [col for col in chunk.columns if col in table_columns]
                
                if not valid_columns:
                    logger.warning(f"No matching columns found for {table_name}. CSV columns: {list(chunk.columns)}, Table columns: {table_columns}")
                    continue
                
                # Select only valid columns
                chunk_filtered = chunk[valid_columns].copy()
                
                # Convert DataFrame to list of tuples, handling None values and data types
                values = []
                for _, row in chunk_filtered.iterrows():
                    row_values = []
                    for col, val in zip(valid_columns, row):
                        if pd.isna(val) or val == 'NaN' or val == 'nan' or val == '':
                            row_values.append(None)
                        elif col in ['cutoff_timestamp', 'trip_creation_time', 'od_start_time', 'od_end_time', 'available_time', 'deadline', 'date']:
                            # Handle timestamp/date columns
                            try:
                                if pd.notna(val):
                                    row_values.append(pd.to_datetime(val))
                                else:
                                    row_values.append(None)
                            except:
                                row_values.append(None)
                        else:
                            row_values.append(val)
                    values.append(tuple(row_values))
                
                # Use execute_values for efficient bulk insert
                columns_str = ', '.join(valid_columns)
                with self.connection.cursor() as cursor:
                    execute_values(
                        cursor,
                        f"INSERT INTO {self.schema}.{table_name} ({columns_str}) VALUES %s",
                        values
                    )
                    self.connection.commit()
                
                total_rows += len(chunk)
                logger.info(f"Loaded {total_rows:,} rows into {table_name}...")
            
            logger.info(f"Successfully loaded {total_rows:,} rows into {table_name}")
            return True
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to load data into {table_name}: {e}")
            return False
    
    def create_views(self) -> bool:
        """Create analytics views."""
        logger.info("Creating analytics views...")
        
        views = {
            'transport_summary': f"""
                CREATE OR REPLACE VIEW {self.schema}.transport_summary AS
                SELECT 
                    source_center,
                    destination_center,
                    COUNT(*) as trip_count,
                    AVG(actual_time) as avg_actual_time,
                    AVG(osrm_time) as avg_osrm_time,
                    AVG(factor) as avg_factor,
                    AVG(actual_distance_to_destination) as avg_distance
                FROM {self.schema}.delhivery_data 
                GROUP BY source_center, destination_center
            """,
            
            'order_summary': f"""
                CREATE OR REPLACE VIEW {self.schema}.order_summary AS
                SELECT 
                    source,
                    destination,
                    order_size,
                    COUNT(*) as order_count,
                    SUM(weight) as total_weight,
                    AVG(EXTRACT(EPOCH FROM (deadline - available_time))/3600) as avg_hours_to_deadline
                FROM {self.schema}.order_data 
                GROUP BY source, destination, order_size
            """,
            
            'dashboard_analytics': f"""
                CREATE OR REPLACE VIEW {self.schema}.dashboard_analytics AS
                SELECT 
                    'Total Transport Records' as metric,
                    (SELECT COUNT(*) FROM {self.schema}.delhivery_data)::text as value
                UNION ALL
                SELECT 
                    'Total Orders',
                    (SELECT COUNT(*) FROM {self.schema}.order_data)::text
                UNION ALL
                SELECT 
                    'Total Routes',
                    (SELECT COUNT(DISTINCT CONCAT(source_center, '->', destination_center))::text
                     FROM {self.schema}.delhivery_data)
                UNION ALL
                SELECT 
                    'Average Efficiency',
                    (SELECT ROUND(CAST(AVG(factor) AS NUMERIC), 2)::text
                     FROM {self.schema}.delhivery_data 
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
            'delhivery_data': f"SELECT COUNT(*) FROM {self.schema}.delhivery_data",
            'distance_data': f"SELECT COUNT(*) FROM {self.schema}.distance_data",
            'order_data': f"SELECT COUNT(*) FROM {self.schema}.order_data",
            'supply_chain_data': f"SELECT COUNT(*) FROM {self.schema}.supply_chain_data",
            'weather_data': f"SELECT COUNT(*) FROM {self.schema}.weather_data",
            'inventory_data': f"SELECT COUNT(*) FROM {self.schema}.inventory_data"
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
    """Main function to load data into PostgreSQL."""
    logger.info("PostgreSQL Data Loading Started")
    logger.info("=" * 50)
    
    # Configuration from environment variables
    # Default to current system user (works with peer authentication)
    import getpass
    default_user = os.getenv('USER', getpass.getuser())
    
    config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', '5432')),
        'database': os.getenv('POSTGRES_DATABASE') or os.getenv('POSTGRES_DB', 'smartlogix_transport'),
        'user': os.getenv('POSTGRES_USER', default_user),
        'password': os.getenv('POSTGRES_PASSWORD', ''),
        'schema': os.getenv('POSTGRES_SCHEMA', 'transport_data')
    }
    
    logger.info(f"Connecting as user: {config['user']}")
    
    # Initialize loader
    loader = PostgreSQLDataLoader(**config)
    
    if not loader.connect():
        logger.error("Failed to connect to PostgreSQL")
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
        
        # Step 3: Load data
        # Use coherent data if it exists, otherwise fall back to processed
        if os.path.exists("data/coherent"):
            processed_data_dir = "data/coherent"
            logger.info("Using coherent connected data from data/coherent/")
        else:
            processed_data_dir = "data/processed"
            logger.info("Using processed data from data/processed/")
        
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
        print(f"Database: {config['database']}.{config['schema']}")
        
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

