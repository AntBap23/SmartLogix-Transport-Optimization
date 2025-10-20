"""
Databricks Configuration for SmartLogix Transport Optimization
This file contains configuration settings for Databricks workspace.
"""

import os
from typing import Dict, Any

class DatabricksConfig:
    """Configuration class for Databricks workspace."""
    
    def __init__(self):
        """Initialize Databricks configuration."""
        self.workspace_url = os.getenv('DATABRICKS_WORKSPACE_URL', '')
        self.access_token = os.getenv('DATABRICKS_ACCESS_TOKEN', '')
        self.cluster_id = os.getenv('DATABRICKS_CLUSTER_ID', '')
        
        # Data paths
        self.data_path = '/FileStore/shared_uploads/smartlogix/'
        self.table_catalog = 'default'
        self.table_schema = 'smartlogix'
        
        # Spark configuration
        self.spark_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true"
        }
    
    def get_table_name(self, table: str) -> str:
        """Get fully qualified table name."""
        return f"{self.table_catalog}.{self.table_schema}.{table}"
    
    def get_data_path(self, filename: str) -> str:
        """Get full data path for file."""
        return f"{self.data_path}{filename}"
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration."""
        return self.spark_config

# Global configuration instance
config = DatabricksConfig()

# Table names
TABLES = {
    'delhivery_data': config.get_table_name('delhivery_data'),
    'distance_data': config.get_table_name('distance_data'),
    'order_data': config.get_table_name('order_data'),
    'supply_chain_data': config.get_table_name('supply_chain_data'),
    'weather_data': config.get_table_name('weather_data'),
    'inventory_data': config.get_table_name('inventory_data')
}

# Data file paths
DATA_FILES = {
    'delhivery': 'delhivery.csv',
    'distance': 'distance.csv',
    'order_small': 'order_small.csv',
    'order_large': 'order_large.csv',
    'supply_chain': 'supply_chain_data.csv',
    'inventory_1': 'Inventory Data.xlsx',
    'inventory_2': 'Model Sample Data - Retail Inventory.xlsx'
}
