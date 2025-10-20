#!/usr/bin/env python3
"""
Data Cleaning and Processing Script for SmartLogix Transport Optimization
This script cleans and processes raw CSV data for Databricks SQL loading.
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

class DataCleaner:
    """Data cleaner for SmartLogix transport data."""
    
    def __init__(self, raw_data_dir: str = "data/raw", processed_data_dir: str = "data/processed"):
        """Initialize data cleaner."""
        self.raw_data_dir = raw_data_dir
        self.processed_data_dir = processed_data_dir
        
        # Create processed directory if it doesn't exist
        os.makedirs(self.processed_data_dir, exist_ok=True)
        
        # Data quality metrics
        self.quality_metrics = {}
    
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
        """Clean Delhivery transport data."""
        logger.info("Cleaning Delhivery data...")
        
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
    
    def clean_weather_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean weather data (create sample if not exists)."""
        logger.info("Cleaning weather data...")
        
        if df is None or df.empty:
            # Create sample weather data
            logger.info("No weather data found, creating sample data...")
            sample_data = {
                'location': ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata'],
                'date': pd.date_range('2023-01-01', periods=5),
                'temperature': [28.5, 32.1, 26.8, 30.2, 27.9],
                'humidity': [75.0, 65.0, 80.0, 70.0, 78.0],
                'wind_speed': [12.3, 8.7, 15.2, 10.5, 9.8],
                'precipitation': [0.0, 0.0, 2.5, 1.2, 0.5],
                'weather_condition': ['Clear', 'Sunny', 'Cloudy', 'Partly Cloudy', 'Clear']
            }
            df_clean = pd.DataFrame(sample_data)
        else:
            df_clean = df.copy()
            
            # Clean string columns
            string_cols = ['location', 'weather_condition']
            for col in string_cols:
                if col in df_clean.columns:
                    df_clean[col] = self.clean_string_column(df_clean[col])
            
            # Clean numeric columns
            numeric_cols = ['temperature', 'humidity', 'wind_speed', 'precipitation']
            for col in numeric_cols:
                if col in df_clean.columns:
                    df_clean[col] = self.clean_numeric_column(df_clean[col])
            
            # Clean date column
            if 'date' in df_clean.columns:
                df_clean['date'] = self.clean_timestamp_column(df_clean['date'])
        
        # Calculate quality metrics
        total_records = len(df_clean)
        valid_location = df_clean['location'].notna().sum()
        quality_score = (valid_location / total_records * 100) if total_records > 0 else 0
        
        self.quality_metrics['weather_data'] = {
            'total_records': total_records,
            'valid_location': valid_location,
            'quality_score': quality_score
        }
        
        logger.info(f"Weather data cleaned: {total_records} records, {quality_score:.2f}% quality score")
        return df_clean
    
    def clean_inventory_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean inventory data."""
        logger.info("Cleaning inventory data...")
        
        df_clean = df.copy()
        
        # Clean string columns
        string_cols = ['product_id', 'product_name', 'category', 'supplier', 'location']
        for col in string_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_string_column(df_clean[col])
        
        # Clean numeric columns
        numeric_cols = ['current_stock', 'min_stock_level', 'max_stock_level', 'unit_cost']
        for col in numeric_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_numeric_column(df_clean[col])
        
        # Clean timestamp column
        if 'last_updated' in df_clean.columns:
            df_clean['last_updated'] = self.clean_timestamp_column(df_clean['last_updated'])
        
        # Remove completely empty rows
        df_clean = df_clean.dropna(how='all')
        
        # Calculate quality metrics
        total_records = len(df_clean)
        valid_product_id = df_clean['product_id'].notna().sum()
        quality_score = (valid_product_id / total_records * 100) if total_records > 0 else 0
        
        self.quality_metrics['inventory_data'] = {
            'total_records': total_records,
            'valid_product_id': valid_product_id,
            'quality_score': quality_score
        }
        
        logger.info(f"Inventory data cleaned: {total_records} records, {quality_score:.2f}% quality score")
        return df_clean
    
    def process_all_data(self) -> Dict[str, Any]:
        """Process all data files."""
        logger.info("Starting data cleaning process...")
        
        results = {}
        
        # File mappings
        files = {
            'delhivery': 'delhivery.csv',
            'distance': 'distance.csv',
            'order_small': 'order_small.csv',
            'order_large': 'order_large.csv',
            'supply_chain': 'supply_chain_data.csv',
            'weather': None,  # Will create sample data
            'inventory': None  # Will handle Excel files separately
        }
        
        # Process CSV files
        for data_type, filename in files.items():
            if filename is None:
                continue
                
            file_path = os.path.join(self.raw_data_dir, filename)
            
            if not os.path.exists(file_path):
                logger.warning(f"File not found: {file_path}")
                continue
            
            try:
                # Load data
                df = pd.read_csv(file_path)
                logger.info(f"Loaded {filename}: {len(df)} records")
                
                # Clean data based on type
                if data_type == 'delhivery':
                    df_clean = self.clean_delhivery_data(df)
                    output_file = 'delhivery_cleaned.csv'
                elif data_type == 'distance':
                    df_clean = self.clean_distance_data(df)
                    output_file = 'distance_cleaned.csv'
                elif data_type == 'order_small':
                    df_clean = self.clean_order_data(df, 'small')
                    output_file = 'order_small_cleaned.csv'
                elif data_type == 'order_large':
                    df_clean = self.clean_order_data(df, 'large')
                    output_file = 'order_large_cleaned.csv'
                elif data_type == 'supply_chain':
                    df_clean = self.clean_supply_chain_data(df)
                    output_file = 'supply_chain_cleaned.csv'
                else:
                    continue
                
                # Save cleaned data
                output_path = os.path.join(self.processed_data_dir, output_file)
                df_clean.to_csv(output_path, index=False)
                results[data_type] = {
                    'input_file': filename,
                    'output_file': output_file,
                    'records_processed': len(df_clean),
                    'status': 'success'
                }
                logger.info(f"Saved cleaned data: {output_path}")
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                results[data_type] = {
                    'input_file': filename,
                    'status': 'error',
                    'error': str(e)
                }
        
        # Process Excel files for inventory data
        excel_files = ['Inventory Data.xlsx', 'Model Sample Data - Retail Inventory.xlsx']
        inventory_dfs = []
        
        for excel_file in excel_files:
            file_path = os.path.join(self.raw_data_dir, excel_file)
            if os.path.exists(file_path):
                try:
                    # Read all sheets
                    excel_data = pd.ExcelFile(file_path)
                    for sheet_name in excel_data.sheet_names:
                        df = pd.read_excel(file_path, sheet_name=sheet_name)
                        df['source_file'] = excel_file
                        df['sheet_name'] = sheet_name
                        inventory_dfs.append(df)
                        logger.info(f"Loaded {excel_file} - {sheet_name}: {len(df)} records")
                except Exception as e:
                    logger.error(f"Error processing {excel_file}: {e}")
        
        # Combine and clean inventory data
        if inventory_dfs:
            combined_inventory = pd.concat(inventory_dfs, ignore_index=True)
            df_clean = self.clean_inventory_data(combined_inventory)
            output_path = os.path.join(self.processed_data_dir, 'inventory_cleaned.csv')
            df_clean.to_csv(output_path, index=False)
            results['inventory'] = {
                'input_files': excel_files,
                'output_file': 'inventory_cleaned.csv',
                'records_processed': len(df_clean),
                'status': 'success'
            }
            logger.info(f"Saved cleaned inventory data: {output_path}")
        
        # Create sample weather data
        df_weather = self.clean_weather_data(None)
        output_path = os.path.join(self.processed_data_dir, 'weather_cleaned.csv')
        df_weather.to_csv(output_path, index=False)
        results['weather'] = {
            'output_file': 'weather_cleaned.csv',
            'records_processed': len(df_weather),
            'status': 'success',
            'note': 'Sample data created'
        }
        logger.info(f"Saved sample weather data: {output_path}")
        
        # Combine order data
        order_small_path = os.path.join(self.processed_data_dir, 'order_small_cleaned.csv')
        order_large_path = os.path.join(self.processed_data_dir, 'order_large_cleaned.csv')
        
        if os.path.exists(order_small_path) and os.path.exists(order_large_path):
            df_small = pd.read_csv(order_small_path)
            df_large = pd.read_csv(order_large_path)
            df_combined = pd.concat([df_small, df_large], ignore_index=True)
            
            output_path = os.path.join(self.processed_data_dir, 'orders_cleaned.csv')
            df_combined.to_csv(output_path, index=False)
            results['orders_combined'] = {
                'output_file': 'orders_cleaned.csv',
                'records_processed': len(df_combined),
                'status': 'success',
                'note': 'Combined small and large orders'
            }
            logger.info(f"Saved combined orders data: {output_path}")
        
        return results
    
    def generate_quality_report(self) -> str:
        """Generate data quality report."""
        report = []
        report.append("=" * 60)
        report.append("DATA QUALITY REPORT")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        for table_name, metrics in self.quality_metrics.items():
            report.append(f"Table: {table_name}")
            report.append(f"  Total Records: {metrics['total_records']:,}")
            report.append(f"  Quality Score: {metrics['quality_score']:.2f}%")
            report.append("")
        
        report.append("=" * 60)
        return "\n".join(report)

def main():
    """Main function to run data cleaning."""
    logger.info("SmartLogix Data Cleaning Process Started")
    logger.info("=" * 50)
    
    # Initialize cleaner
    cleaner = DataCleaner()
    
    # Process all data
    results = cleaner.process_all_data()
    
    # Generate quality report
    quality_report = cleaner.generate_quality_report()
    print(quality_report)
    
    # Save quality report
    report_path = os.path.join(cleaner.processed_data_dir, 'data_quality_report.txt')
    with open(report_path, 'w') as f:
        f.write(quality_report)
    
    # Summary
    logger.info("Data cleaning completed!")
    logger.info(f"Processed files saved to: {cleaner.processed_data_dir}")
    logger.info(f"Quality report saved to: {report_path}")
    
    # Print summary
    print("\n" + "=" * 50)
    print("PROCESSING SUMMARY")
    print("=" * 50)
    for data_type, result in results.items():
        if result['status'] == 'success':
            print(f"✅ {data_type}: {result['records_processed']:,} records → {result['output_file']}")
        else:
            print(f"❌ {data_type}: {result['status']} - {result.get('error', 'Unknown error')}")
    
    return results

if __name__ == "__main__":
    results = main()
    sys.exit(0 if all(r['status'] == 'success' for r in results.values()) else 1)
