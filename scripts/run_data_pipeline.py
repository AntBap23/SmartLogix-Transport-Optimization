#!/usr/bin/env python3
"""
Complete Data Pipeline for SmartLogix Transport Optimization
This script runs the complete data processing and loading pipeline.
"""

import os
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_data_cleaning():
    """Run data cleaning process."""
    logger.info("Step 1: Running data cleaning...")
    
    try:
        from data_cleaner import main as clean_data
        results = clean_data()
        
        if all(r['status'] == 'success' for r in results.values()):
            logger.info("✅ Data cleaning completed successfully")
            return True
        else:
            logger.error("❌ Data cleaning failed")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error in data cleaning: {e}")
        return False

def run_databricks_loading():
    """Run Databricks SQL loading."""
    logger.info("Step 2: Loading data into Databricks SQL...")
    
    try:
        from databricks_sql_loader import main as load_data
        success = load_data()
        
        if success:
            logger.info("✅ Databricks SQL loading completed successfully")
            return True
        else:
            logger.error("❌ Databricks SQL loading failed")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error in Databricks loading: {e}")
        return False

def main():
    """Run complete data pipeline."""
    logger.info("SmartLogix Data Pipeline Started")
    logger.info("=" * 60)
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    # Step 1: Data Cleaning
    if not run_data_cleaning():
        logger.error("Pipeline failed at data cleaning step")
        return False
    
    # Step 2: Databricks Loading
    if not run_databricks_loading():
        logger.error("Pipeline failed at Databricks loading step")
        return False
    
    # Success
    logger.info("=" * 60)
    logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    print("\n📋 NEXT STEPS:")
    print("1. Verify data in Databricks SQL workspace")
    print("2. Run analytics queries to explore the data")
    print("3. Create dashboards and visualizations")
    print("4. Implement route optimization algorithms")
    print("5. Build machine learning models for delay prediction")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
