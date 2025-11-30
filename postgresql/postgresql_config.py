"""
PostgreSQL Configuration for SmartLogix Transport Optimization
This file contains configuration settings for PostgreSQL database.
"""

import os
from typing import Dict, Any

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not available, will use system environment variables only
    pass

class PostgreSQLConfig:
    """Configuration class for PostgreSQL database."""
    
    def __init__(self):
        """Initialize PostgreSQL configuration."""
        # Database connection settings
        # Default to 'postgres' user (standard PostgreSQL superuser)
        # Or use current system user if postgres doesn't exist
        import getpass
        default_user = os.getenv('USER', getpass.getuser())
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = int(os.getenv('POSTGRES_PORT', '5432'))
        self.database = os.getenv('POSTGRES_DATABASE') or os.getenv('POSTGRES_DB', 'smartlogix_transport')
        self.user = os.getenv('POSTGRES_USER', default_user)
        self.password = os.getenv('POSTGRES_PASSWORD', '')
        
        # Schema settings
        self.schema = os.getenv('POSTGRES_SCHEMA', 'transport_data')
        
        # Connection pool settings
        self.pool_min_connections = int(os.getenv('POSTGRES_POOL_MIN', '1'))
        self.pool_max_connections = int(os.getenv('POSTGRES_POOL_MAX', '10'))
        self.pool_timeout = int(os.getenv('POSTGRES_POOL_TIMEOUT', '30'))
    
    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        if self.password:
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as dictionary."""
        params = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user
        }
        # Only include password if it's provided
        if self.password:
            params['password'] = self.password
        return params
    
    def get_table_name(self, table: str) -> str:
        """Get fully qualified table name."""
        return f"{self.schema}.{table}"

# Global configuration instance
config = PostgreSQLConfig()

# Table names
TABLES = {
    'delhivery_data': config.get_table_name('delhivery_data'),
    'distance_data': config.get_table_name('distance_data'),
    'order_data': config.get_table_name('order_data'),
    'supply_chain_data': config.get_table_name('supply_chain_data'),
    'weather_data': config.get_table_name('weather_data'),
    'inventory_data': config.get_table_name('inventory_data')
}

# Data file paths (relative to project root)
DATA_FILES = {
    'delhivery': 'data/raw/delhivery.csv',
    'distance': 'data/raw/distance.csv',
    'order_small': 'data/raw/order_small.csv',
    'order_large': 'data/raw/order_large.csv',
    'supply_chain': 'data/raw/supply_chain_data.csv',
    'inventory_1': 'data/raw/Inventory Data.xlsx',
    'inventory_2': 'data/raw/Model Sample Data - Retail Inventory.xlsx'
}

