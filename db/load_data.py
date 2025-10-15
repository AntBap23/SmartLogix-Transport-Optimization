#!/usr/bin/env python3
"""
Data Loading Script for SmartLogix Transport Optimization Database
This script loads CSV data into PostgreSQL database tables.
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import logging
from typing import Optional, Dict, Any
import numpy as np
from datetime import datetime
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Database loader for SmartLogix transport data."""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 5432,
                 database: str = "smartlogix_transport",
                 user: str = "smartlogix_user",
                 password: str = "smartlogix_password"):
        """Initialize database connection."""
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.engine = None
        self.connection = None
        
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            # SQLAlchemy engine for pandas operations
            connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(connection_string)
            
            # Direct psycopg2 connection for custom operations
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            
            logger.info("Database connection established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connections."""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
        logger.info("Database connections closed")
    
    def clean_data(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Clean and validate data before loading."""
        logger.info(f"Cleaning data for {table_name}")
        
        # Replace 'nan' strings with actual NaN
        df = df.replace(['nan', 'NaN', 'null', 'NULL', ''], np.nan)
        
        # Handle specific data types based on table
        if table_name == 'delhivery_data':
            # Convert timestamp columns
            timestamp_cols = ['trip_creation_time', 'od_start_time', 'od_end_time', 'cutoff_timestamp']
            for col in timestamp_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Convert numeric columns
            numeric_cols = ['start_scan_to_end_scan', 'actual_distance_to_destination', 
                           'actual_time', 'osrm_time', 'osrm_distance', 'factor',
                           'segment_actual_time', 'segment_osrm_time', 'segment_osrm_distance', 'segment_factor']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert boolean columns
            if 'is_cutoff' in df.columns:
                df['is_cutoff'] = df['is_cutoff'].map({'True': True, 'False': False, True: True, False: False})
        
        elif table_name == 'order_data':
            # Convert timestamp columns
            timestamp_cols = ['available_time', 'deadline']
            for col in timestamp_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Convert numeric columns
            numeric_cols = ['area', 'weight']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        elif table_name == 'supply_chain_data':
            # Convert numeric columns
            numeric_cols = ['price', 'availability', 'number_of_products_sold', 'revenue_generated',
                           'stock_levels', 'lead_times', 'order_quantities', 'shipping_times',
                           'shipping_costs', 'lead_time', 'production_volumes', 'manufacturing_lead_time',
                           'manufacturing_costs', 'defect_rates', 'costs']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        logger.info(f"Data cleaning completed. Shape: {df.shape}")
        return df
    
    def load_delhivery_data(self, file_path: str) -> bool:
        """Load Delhivery data from CSV."""
        try:
            logger.info(f"Loading Delhivery data from {file_path}")
            df = pd.read_csv(file_path)
            
            # Clean the data
            df = self.clean_data(df, 'delhivery_data')
            
            # Load to database
            df.to_sql('delhivery_data', self.engine, if_exists='append', index=False, method='multi')
            
            logger.info(f"Successfully loaded {len(df)} Delhivery records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load Delhivery data: {e}")
            return False
    
    def load_distance_data(self, file_path: str) -> bool:
        """Load distance data from CSV."""
        try:
            logger.info(f"Loading distance data from {file_path}")
            df = pd.read_csv(file_path)
            
            # Clean the data
            df = self.clean_data(df, 'distance_data')
            
            # Load to database
            df.to_sql('distance_data', self.engine, if_exists='append', index=False, method='multi')
            
            logger.info(f"Successfully loaded {len(df)} distance records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load distance data: {e}")
            return False
    
    def load_order_data(self, file_path: str, order_size: str = 'small') -> bool:
        """Load order data from CSV."""
        try:
            logger.info(f"Loading {order_size} order data from {file_path}")
            df = pd.read_csv(file_path)
            
            # Add order size column
            df['order_size'] = order_size
            
            # Clean the data
            df = self.clean_data(df, 'order_data')
            
            # Load to database
            df.to_sql('order_data', self.engine, if_exists='append', index=False, method='multi')
            
            logger.info(f"Successfully loaded {len(df)} {order_size} order records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load {order_size} order data: {e}")
            return False
    
    def load_supply_chain_data(self, file_path: str) -> bool:
        """Load supply chain data from CSV."""
        try:
            logger.info(f"Loading supply chain data from {file_path}")
            df = pd.read_csv(file_path)
            
            # Clean the data
            df = self.clean_data(df, 'supply_chain_data')
            
            # Load to database
            df.to_sql('supply_chain_data', self.engine, if_exists='append', index=False, method='multi')
            
            logger.info(f"Successfully loaded {len(df)} supply chain records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load supply chain data: {e}")
            return False
    
    def load_excel_inventory_data(self, file_path: str) -> bool:
        """Load inventory data from Excel file."""
        try:
            logger.info(f"Loading inventory data from {file_path}")
            
            # Read Excel file
            excel_file = pd.ExcelFile(file_path)
            
            # Process each sheet
            for sheet_name in excel_file.sheet_names:
                logger.info(f"Processing sheet: {sheet_name}")
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                
                # Add metadata
                df['sheet_name'] = sheet_name
                df['file_name'] = os.path.basename(file_path)
                
                # Clean the data
                df = self.clean_data(df, 'inventory_data')
                
                # Load to database
                df.to_sql('inventory_data', self.engine, if_exists='append', index=False, method='multi')
                
                logger.info(f"Successfully loaded {len(df)} records from sheet {sheet_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load Excel inventory data: {e}")
            return False
    
    def get_data_quality_report(self) -> Dict[str, Any]:
        """Generate data quality report."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT * FROM get_data_quality_metrics()")
                results = cursor.fetchall()
                
                report = {
                    'timestamp': datetime.now().isoformat(),
                    'tables': []
                }
                
                for row in results:
                    report['tables'].append({
                        'table_name': row[0],
                        'total_records': row[1],
                        'null_records': row[2],
                        'quality_score': float(row[3])
                    })
                
                return report
                
        except Exception as e:
            logger.error(f"Failed to generate data quality report: {e}")
            return {}
    
    def clear_tables(self):
        """Clear all data tables (use with caution)."""
        try:
            tables = ['delhivery_data', 'distance_data', 'order_data', 'supply_chain_data', 'inventory_data']
            
            with self.connection.cursor() as cursor:
                for table in tables:
                    cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
                    logger.info(f"Cleared table: {table}")
                
                self.connection.commit()
                logger.info("All tables cleared successfully")
                
        except Exception as e:
            logger.error(f"Failed to clear tables: {e}")

def main():
    """Main function to load all data."""
    # Data file paths
    data_dir = "data/raw"
    files = {
        'delhivery': os.path.join(data_dir, 'delhivery.csv'),
        'distance': os.path.join(data_dir, 'distance.csv'),
        'order_small': os.path.join(data_dir, 'order_small.csv'),
        'order_large': os.path.join(data_dir, 'order_large.csv'),
        'supply_chain': os.path.join(data_dir, 'supply_chain_data.csv'),
        'inventory_1': os.path.join(data_dir, 'Inventory Data.xlsx'),
        'inventory_2': os.path.join(data_dir, 'Model Sample Data - Retail Inventory.xlsx')
    }
    
    # Initialize loader
    loader = DatabaseLoader()
    
    if not loader.connect():
        logger.error("Failed to connect to database. Please check your connection settings.")
        return False
    
    try:
        # Load all data files
        success_count = 0
        total_files = len(files)
        
        # Load Delhivery data
        if os.path.exists(files['delhivery']):
            if loader.load_delhivery_data(files['delhivery']):
                success_count += 1
        
        # Load distance data
        if os.path.exists(files['distance']):
            if loader.load_distance_data(files['distance']):
                success_count += 1
        
        # Load order data
        if os.path.exists(files['order_small']):
            if loader.load_order_data(files['order_small'], 'small'):
                success_count += 1
        
        if os.path.exists(files['order_large']):
            if loader.load_order_data(files['order_large'], 'large'):
                success_count += 1
        
        # Load supply chain data
        if os.path.exists(files['supply_chain']):
            if loader.load_supply_chain_data(files['supply_chain']):
                success_count += 1
        
        # Load Excel inventory data
        if os.path.exists(files['inventory_1']):
            if loader.load_excel_inventory_data(files['inventory_1']):
                success_count += 1
        
        if os.path.exists(files['inventory_2']):
            if loader.load_excel_inventory_data(files['inventory_2']):
                success_count += 1
        
        # Generate data quality report
        logger.info("Generating data quality report...")
        report = loader.get_data_quality_report()
        
        if report:
            logger.info("Data Quality Report:")
            for table in report['tables']:
                logger.info(f"  {table['table_name']}: {table['total_records']} records, "
                          f"{table['quality_score']}% quality score")
        
        logger.info(f"Data loading completed: {success_count}/{total_files} files loaded successfully")
        return success_count == total_files
        
    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        return False
        
    finally:
        loader.disconnect()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
