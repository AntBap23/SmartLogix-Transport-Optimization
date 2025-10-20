"""
Configuration Template for SmartLogix Transport Optimization
Copy this file to config.py and update with your actual values.
"""

# Databricks Configuration
DATABRICKS_WORKSPACE_URL = "https://your-workspace.cloud.databricks.com"
DATABRICKS_ACCESS_TOKEN = "your_access_token_here"
DATABRICKS_CLUSTER_ID = "your_cluster_id_here"

# Data Storage Paths
DATABRICKS_DATA_PATH = "/FileStore/shared_uploads/smartlogix/"
DATABRICKS_TABLE_CATALOG = "default"
DATABRICKS_TABLE_SCHEMA = "smartlogix"

# External Database (Optional)
EXTERNAL_DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "smartlogix_transport",
    "user": "smartlogix_user",
    "password": "your_password_here"
}

# Weather API (Optional)
WEATHER_API_KEY = "your_openweather_api_key_here"

# Development Settings
DEBUG = True
LOG_LEVEL = "INFO"

# Table Names
TABLES = {
    'delhivery_data': f"{DATABRICKS_TABLE_CATALOG}.{DATABRICKS_TABLE_SCHEMA}.delhivery_data",
    'distance_data': f"{DATABRICKS_TABLE_CATALOG}.{DATABRICKS_TABLE_SCHEMA}.distance_data",
    'order_data': f"{DATABRICKS_TABLE_CATALOG}.{DATABRICKS_TABLE_SCHEMA}.order_data",
    'supply_chain_data': f"{DATABRICKS_TABLE_CATALOG}.{DATABRICKS_TABLE_SCHEMA}.supply_chain_data",
    'weather_data': f"{DATABRICKS_TABLE_CATALOG}.{DATABRICKS_TABLE_SCHEMA}.weather_data",
    'inventory_data': f"{DATABRICKS_TABLE_CATALOG}.{DATABRICKS_TABLE_SCHEMA}.inventory_data"
}
