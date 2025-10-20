#!/usr/bin/env python3
"""
Data Cleaning Script for SmartLogix Transport Optimization - FREE EDITION
This script is optimized for Databricks free edition with file size limitations.
"""

import os
import sys
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataCleanerFreeEdition:
    """Data cleaner optimized for Databricks free edition."""
    
    def __init__(self, raw_data_dir: str = "data/raw", processed_data_dir: str = "data/processed"):
        """Initialize data cleaner for free edition."""
        self.raw_data_dir = raw_data_dir
        self.processed_data_dir = processed_data_dir
        
        # Create processed directory if it doesn't exist
        os.makedirs(self.processed_data_dir, exist_ok=True)
        
        # Free edition limits
        self.max_file_size_mb = 100  # Conservative limit for free edition
        self.max_records_per_file = 50000  # Limit records per file
        
        # Data quality metrics
        self.quality_metrics = {}
    
    def get_file_size_mb(self, file_path: str) -> float:
        """Get file size in MB."""
        return os.path.getsize(file_path) / (1024 * 1024)
    
    def split_large_file(self, df: pd.DataFrame, base_filename: str, max_records: int = None) -> list:
        """Split large DataFrame into smaller files."""
        if max_records is None:
            max_records = self.max_records_per_file
        
        total_records = len(df)
        if total_records <= max_records:
            return [df]
        
        logger.info(f"Splitting {base_filename} into smaller files ({total_records} records)")
        
        chunks = []
        for i in range(0, total_records, max_records):
            chunk = df.iloc[i:i + max_records].copy()
            chunks.append(chunk)
            logger.info(f"Created chunk {len(chunks)}: {len(chunk)} records")
        
        return chunks
    
    def clean_numeric_column(self, series: pd.Series) -> pd.Series:
        """Clean numeric columns by handling various formats."""
        # Replace common non-numeric values
        series = series.replace(['nan', 'NaN', 'null', 'NULL', '', 'N/A', 'n/a'], np.nan)
        
        # Convert to numeric, coercing errors to NaN
        series = pd.to_numeric(series, errors='coerce')
        
        return series
    
    def clean_timestamp_column(self, series: pd.Series) -> pd.Series:
        """Clean timestamp columns."""
        # Replace common non-timestamp values
        series = series.replace(['nan', 'NaN', 'null', 'NULL', '', 'N/A', 'n/a'], np.nan)
        
        # Convert to datetime
        series = pd.to_datetime(series, errors='coerce')
        
        return series
    
    def clean_string_column(self, series: pd.Series) -> pd.Series:
        """Clean string columns."""
        # Replace common null values
        series = series.replace(['nan', 'NaN', 'null', 'NULL', 'N/A', 'n/a'], np.nan)
        
        # Strip whitespace
        series = series.astype(str).str.strip()
        
        # Replace empty strings with NaN
        series = series.replace('', np.nan)
        
        return series
    
    def clean_delhivery_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean Delhivery transport data with free edition optimizations."""
        logger.info("Cleaning Delhivery data for free edition...")
        
        # Create a copy to avoid modifying original
        df_clean = df.copy()
        
        # Clean string columns
        string_cols = ['data_type', 'route_schedule_uuid', 'route_type', 'trip_uuid',
                      'source_center', 'source_name', 'destination_center', 'destination_name']
        for col in string_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_string_column(df_clean[col])
        
        # Clean timestamp columns
        timestamp_cols = ['trip_creation_time', 'od_start_time', 'od_end_time', 'cutoff_timestamp']
        for col in timestamp_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_timestamp_column(df_clean[col])
        
        # Clean numeric columns
        numeric_cols = ['start_scan_to_end_scan', 'actual_distance_to_destination', 
                       'actual_time', 'osrm_time', 'osrm_distance', 'factor',
                       'segment_actual_time', 'segment_osrm_time', 'segment_osrm_distance', 
                       'segment_factor', 'cutoff_factor']
        for col in numeric_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_numeric_column(df_clean[col])
        
        # Clean boolean column
        if 'is_cutoff' in df_clean.columns:
            df_clean['is_cutoff'] = df_clean['is_cutoff'].map({
                'True': True, 'False': False, True: True, False: False,
                'true': True, 'false': False, '1': True, '0': False
            })
        
        # Remove completely empty rows
        df_clean = df_clean.dropna(how='all')
        
        # Calculate quality metrics
        total_records = len(df_clean)
        non_null_trip_uuid = df_clean['trip_uuid'].notna().sum()
        quality_score = (non_null_trip_uuid / total_records * 100) if total_records > 0 else 0
        
        self.quality_metrics['delhivery_data'] = {
            'total_records': total_records,
            'non_null_trip_uuid': non_null_trip_uuid,
            'quality_score': quality_score
        }
        
        logger.info(f"Delhivery data cleaned: {total_records} records, {quality_score:.2f}% quality score")
        return df_clean
    
    def clean_distance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean distance data."""
        logger.info("Cleaning distance data...")
        
        df_clean = df.copy()
        
        # Clean string columns
        string_cols = ['Source', 'Destination']
        for col in string_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_string_column(df_clean[col])
        
        # Clean distance column (handle different column names)
        distance_cols = ['Distance(M)', 'distance_meters', 'distance']
        for col in distance_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_numeric_column(df_clean[col])
                # Convert to integer (meters)
                df_clean[col] = df_clean[col].astype('Int64')
        
        # Standardize column names
        df_clean = df_clean.rename(columns={
            'Source': 'source',
            'Destination': 'destination',
            'Distance(M)': 'distance_meters'
        })
        
        # Remove rows with missing source or destination
        df_clean = df_clean.dropna(subset=['source', 'destination'])
        
        # Calculate quality metrics
        total_records = len(df_clean)
        valid_distance = df_clean['distance_meters'].notna().sum()
        quality_score = (valid_distance / total_records * 100) if total_records > 0 else 0
        
        self.quality_metrics['distance_data'] = {
            'total_records': total_records,
            'valid_distance': valid_distance,
            'quality_score': quality_score
        }
        
        logger.info(f"Distance data cleaned: {total_records} records, {quality_score:.2f}% quality score")
        return df_clean
    
    def clean_order_data(self, df: pd.DataFrame, order_size: str = 'small') -> pd.DataFrame:
        """Clean order data."""
        logger.info(f"Cleaning {order_size} order data...")
        
        df_clean = df.copy()
        
        # Clean string columns
        string_cols = ['Order_ID', 'Material_ID', 'Item_ID', 'Source', 'Destination', 'Danger_Type']
        for col in string_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_string_column(df_clean[col])
        
        # Clean timestamp columns
        timestamp_cols = ['Available_Time', 'Deadline']
        for col in timestamp_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_timestamp_column(df_clean[col])
        
        # Clean numeric columns
        numeric_cols = ['Area', 'Weight']
        for col in numeric_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_numeric_column(df_clean[col])
        
        # Standardize column names
        df_clean = df_clean.rename(columns={
            'Order_ID': 'order_id',
            'Material_ID': 'material_id',
            'Item_ID': 'item_id',
            'Source': 'source',
            'Destination': 'destination',
            'Available_Time': 'available_time',
            'Deadline': 'deadline',
            'Danger_Type': 'danger_type',
            'Area': 'area',
            'Weight': 'weight'
        })
        
        # Add order size
        df_clean['order_size'] = order_size
        
        # Remove rows with missing critical fields
        df_clean = df_clean.dropna(subset=['order_id', 'source', 'destination'])
        
        # Calculate quality metrics
        total_records = len(df_clean)
        valid_orders = df_clean['order_id'].notna().sum()
        quality_score = (valid_orders / total_records * 100) if total_records > 0 else 0
        
        self.quality_metrics[f'order_data_{order_size}'] = {
            'total_records': total_records,
            'valid_orders': valid_orders,
            'quality_score': quality_score
        }
        
        logger.info(f"{order_size.title()} order data cleaned: {total_records} records, {quality_score:.2f}% quality score")
        return df_clean
    
    def process_all_data_free_edition(self) -> Dict[str, Any]:
        """Process all data files with free edition optimizations."""
        logger.info("Starting data cleaning process for FREE EDITION...")
        logger.info(f"File size limit: {self.max_file_size_mb}MB per file")
        logger.info(f"Record limit: {self.max_records_per_file} records per file")
        
        results = {}
        
        # File mappings
        files = {
            'delhivery': 'delhivery.csv',
            'distance': 'distance.csv',
            'order_small': 'order_small.csv',
            'order_large': 'order_large.csv',
            'supply_chain': 'supply_chain_data.csv'
        }
        
        # Process CSV files
        for data_type, filename in files.items():
            file_path = os.path.join(self.raw_data_dir, filename)
            
            if not os.path.exists(file_path):
                logger.warning(f"File not found: {file_path}")
                continue
            
            file_size_mb = self.get_file_size_mb(file_path)
            logger.info(f"Processing {filename}: {file_size_mb:.2f}MB")
            
            try:
                # Load data
                df = pd.read_csv(file_path)
                logger.info(f"Loaded {filename}: {len(df)} records")
                
                # Clean data based on type
                if data_type == 'delhivery':
                    df_clean = self.clean_delhivery_data(df)
                    base_filename = 'delhivery_cleaned'
                elif data_type == 'distance':
                    df_clean = self.clean_distance_data(df)
                    base_filename = 'distance_cleaned'
                elif data_type == 'order_small':
                    df_clean = self.clean_order_data(df, 'small')
                    base_filename = 'order_small_cleaned'
                elif data_type == 'order_large':
                    df_clean = self.clean_order_data(df, 'large')
                    base_filename = 'order_large_cleaned'
                elif data_type == 'supply_chain':
                    df_clean = self.clean_supply_chain_data(df)
                    base_filename = 'supply_chain_cleaned'
                else:
                    continue
                
                # Check if file needs to be split
                if len(df_clean) > self.max_records_per_file:
                    chunks = self.split_large_file(df_clean, base_filename)
                    
                    for i, chunk in enumerate(chunks):
                        output_filename = f"{base_filename}_part_{i+1}.csv"
                        output_path = os.path.join(self.processed_data_dir, output_filename)
                        chunk.to_csv(output_path, index=False)
                        logger.info(f"Saved chunk {i+1}: {output_path} ({len(chunk)} records)")
                    
                    results[data_type] = {
                        'input_file': filename,
                        'output_files': [f"{base_filename}_part_{i+1}.csv" for i in range(len(chunks))],
                        'records_processed': len(df_clean),
                        'chunks_created': len(chunks),
                        'status': 'success'
                    }
                else:
                    # Save as single file
                    output_filename = f"{base_filename}.csv"
                    output_path = os.path.join(self.processed_data_dir, output_filename)
                    df_clean.to_csv(output_path, index=False)
                    logger.info(f"Saved cleaned data: {output_path}")
                    
                    results[data_type] = {
                        'input_file': filename,
                        'output_file': output_filename,
                        'records_processed': len(df_clean),
                        'status': 'success'
                    }
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                results[data_type] = {
                    'input_file': filename,
                    'status': 'error',
                    'error': str(e)
                }
        
        # Create sample weather data
        df_weather = self.create_sample_weather_data()
        output_path = os.path.join(self.processed_data_dir, 'weather_cleaned.csv')
        df_weather.to_csv(output_path, index=False)
        results['weather'] = {
            'output_file': 'weather_cleaned.csv',
            'records_processed': len(df_weather),
            'status': 'success',
            'note': 'Sample data created'
        }
        logger.info(f"Saved sample weather data: {output_path}")
        
        # Create sample inventory data
        df_inventory = self.create_sample_inventory_data()
        output_path = os.path.join(self.processed_data_dir, 'inventory_cleaned.csv')
        df_inventory.to_csv(output_path, index=False)
        results['inventory'] = {
            'output_file': 'inventory_cleaned.csv',
            'records_processed': len(df_inventory),
            'status': 'success',
            'note': 'Sample data created'
        }
        logger.info(f"Saved sample inventory data: {output_path}")
        
        return results
    
    def create_sample_weather_data(self) -> pd.DataFrame:
        """Create sample weather data."""
        sample_data = {
            'location': ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata', 'Hyderabad', 'Pune', 'Ahmedabad'],
            'date': pd.date_range('2023-01-01', periods=8),
            'temperature': [28.5, 32.1, 26.8, 30.2, 27.9, 29.3, 25.7, 31.4],
            'humidity': [75.0, 65.0, 80.0, 70.0, 78.0, 72.0, 82.0, 68.0],
            'wind_speed': [12.3, 8.7, 15.2, 10.5, 9.8, 11.1, 13.6, 7.9],
            'precipitation': [0.0, 0.0, 2.5, 1.2, 0.5, 0.8, 3.1, 0.0],
            'weather_condition': ['Clear', 'Sunny', 'Cloudy', 'Partly Cloudy', 'Clear', 'Overcast', 'Rainy', 'Sunny']
        }
        return pd.DataFrame(sample_data)
    
    def create_sample_inventory_data(self) -> pd.DataFrame:
        """Create sample inventory data."""
        sample_data = {
            'product_id': [f'P{i:03d}' for i in range(1, 21)],
            'product_name': [f'Product {i}' for i in range(1, 21)],
            'category': ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'] * 4,
            'current_stock': np.random.randint(10, 1000, 20),
            'min_stock_level': np.random.randint(5, 50, 20),
            'max_stock_level': np.random.randint(100, 2000, 20),
            'unit_cost': np.random.uniform(10, 500, 20).round(2),
            'supplier': ['Supplier A', 'Supplier B', 'Supplier C'] * 7 + ['Supplier A'],
            'location': ['Warehouse 1', 'Warehouse 2', 'Warehouse 3'] * 7 + ['Warehouse 1'],
            'last_updated': pd.date_range('2023-01-01', periods=20)
        }
        return pd.DataFrame(sample_data)
    
    def clean_supply_chain_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean supply chain data."""
        logger.info("Cleaning supply chain data...")
        
        df_clean = df.copy()
        
        # Clean string columns
        string_cols = ['product_type', 'sku', 'customer_demographics', 'shipping_carriers',
                      'supplier_name', 'location', 'inspection_results', 'transportation_modes', 'routes']
        for col in string_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_string_column(df_clean[col])
        
        # Clean numeric columns
        numeric_cols = ['price', 'availability', 'number_of_products_sold', 'revenue_generated',
                       'stock_levels', 'lead_times', 'order_quantities', 'shipping_times',
                       'shipping_costs', 'lead_time', 'production_volumes', 'manufacturing_lead_time',
                       'manufacturing_costs', 'defect_rates', 'costs']
        for col in numeric_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_numeric_column(df_clean[col])
        
        # Remove completely empty rows
        df_clean = df_clean.dropna(how='all')
        
        # Calculate quality metrics
        total_records = len(df_clean)
        valid_sku = df_clean['sku'].notna().sum()
        quality_score = (valid_sku / total_records * 100) if total_records > 0 else 0
        
        self.quality_metrics['supply_chain_data'] = {
            'total_records': total_records,
            'valid_sku': valid_sku,
            'quality_score': quality_score
        }
        
        logger.info(f"Supply chain data cleaned: {total_records} records, {quality_score:.2f}% quality score")
        return df_clean
    
    def generate_quality_report(self) -> str:
        """Generate data quality report."""
        report = []
        report.append("=" * 60)
        report.append("DATA QUALITY REPORT - FREE EDITION")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"File size limit: {self.max_file_size_mb}MB per file")
        report.append(f"Record limit: {self.max_records_per_file} records per file")
        report.append("")
        
        for table_name, metrics in self.quality_metrics.items():
            report.append(f"Table: {table_name}")
            report.append(f"  Total Records: {metrics['total_records']:,}")
            report.append(f"  Quality Score: {metrics['quality_score']:.2f}%")
            report.append("")
        
        report.append("=" * 60)
        return "\n".join(report)

def main():
    """Main function to run data cleaning for free edition."""
    logger.info("SmartLogix Data Cleaning Process Started - FREE EDITION")
    logger.info("=" * 60)
    
    # Initialize cleaner
    cleaner = DataCleanerFreeEdition()
    
    # Process all data
    results = cleaner.process_all_data_free_edition()
    
    # Generate quality report
    quality_report = cleaner.generate_quality_report()
    print(quality_report)
    
    # Save quality report
    report_path = os.path.join(cleaner.processed_data_dir, 'data_quality_report_free_edition.txt')
    with open(report_path, 'w') as f:
        f.write(quality_report)
    
    # Summary
    logger.info("Data cleaning completed for FREE EDITION!")
    logger.info(f"Processed files saved to: {cleaner.processed_data_dir}")
    logger.info(f"Quality report saved to: {report_path}")
    
    # Print summary
    print("\n" + "=" * 60)
    print("PROCESSING SUMMARY - FREE EDITION")
    print("=" * 60)
    for data_type, result in results.items():
        if result['status'] == 'success':
            if 'output_files' in result:
                print(f"✅ {data_type}: {result['records_processed']:,} records → {len(result['output_files'])} files")
                for file in result['output_files']:
                    print(f"   - {file}")
            else:
                print(f"✅ {data_type}: {result['records_processed']:,} records → {result['output_file']}")
        else:
            print(f"❌ {data_type}: {result['status']} - {result.get('error', 'Unknown error')}")
    
    print(f"\n📋 FREE EDITION NOTES:")
    print(f"- Files are split if they exceed {cleaner.max_records_per_file:,} records")
    print(f"- File size limit: {cleaner.max_file_size_mb}MB per file")
    print(f"- Upload files to Databricks DBFS manually")
    print(f"- Use the free edition SQL commands for table creation")
    
    return results

if __name__ == "__main__":
    results = main()
    sys.exit(0 if all(r['status'] == 'success' for r in results.values()) else 1)
