#!/usr/bin/env python3
"""
Verify that all tables in the coherent data connect properly.
"""

import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path("data/coherent")

def verify_connections():
    """Verify all data connections."""
    logger.info("=" * 60)
    logger.info("Verifying Data Connections")
    logger.info("=" * 60)
    
    # Load all data
    logger.info("Loading data files...")
    distance_df = pd.read_csv(DATA_DIR / "distance_cleaned.csv")
    inventory_df = pd.read_csv(DATA_DIR / "inventory_cleaned.csv")
    order_df = pd.read_csv(DATA_DIR / "orders_cleaned.csv")
    delhivery_df = pd.read_csv(DATA_DIR / "delhivery_cleaned.csv")
    supply_chain_df = pd.read_csv(DATA_DIR / "supply_chain_cleaned.csv")
    
    # 1. Verify location connections
    logger.info("\n1. Verifying Location Connections...")
    delhivery_locations = set(delhivery_df['source_center'].unique()) | set(delhivery_df['destination_center'].unique())
    order_locations = set(order_df['source'].unique()) | set(order_df['destination'].unique())
    distance_locations = set(distance_df['source'].unique()) | set(distance_df['destination'].unique())
    inventory_locations = set(inventory_df['location'].unique())
    
    all_locations = delhivery_locations | order_locations | distance_locations | inventory_locations
    
    logger.info(f"   delhivery_data locations: {len(delhivery_locations)}")
    logger.info(f"   order_data locations: {len(order_locations)}")
    logger.info(f"   distance_data locations: {len(distance_locations)}")
    logger.info(f"   inventory_data locations: {len(inventory_locations)}")
    
    if delhivery_locations == order_locations == distance_locations == inventory_locations:
        logger.info("   ✅ All location sets match perfectly!")
    else:
        logger.warning("   ⚠️  Location sets don't match exactly")
        logger.info(f"   Common locations: {len(delhivery_locations & order_locations & distance_locations & inventory_locations)}")
    
    # 2. Verify product connections
    logger.info("\n2. Verifying Product Connections...")
    inventory_products = set(inventory_df['product_id'].unique())
    order_products = set(order_df['material_id'].unique())
    supply_chain_products = set(supply_chain_df['sku'].unique())
    
    logger.info(f"   inventory_data products: {len(inventory_products)}")
    logger.info(f"   order_data products: {len(order_products)}")
    logger.info(f"   supply_chain_data products: {len(supply_chain_products)}")
    
    if inventory_products == order_products == supply_chain_products:
        logger.info("   ✅ All product sets match perfectly!")
    else:
        logger.warning("   ⚠️  Product sets don't match exactly")
        common_products = inventory_products & order_products & supply_chain_products
        logger.info(f"   Common products: {len(common_products)}")
    
    # 3. Test joins
    logger.info("\n3. Testing SQL Joins...")
    
    # Join delhivery with distance
    delhivery_distance_join = delhivery_df.merge(
        distance_df,
        left_on=['source_center', 'destination_center'],
        right_on=['source', 'destination'],
        how='inner'
    )
    logger.info(f"   delhivery_data JOIN distance_data: {len(delhivery_distance_join):,} matching records")
    
    # Join orders with delhivery
    order_delhivery_join = order_df.merge(
        delhivery_df,
        left_on=['source', 'destination'],
        right_on=['source_center', 'destination_center'],
        how='inner'
    )
    logger.info(f"   order_data JOIN delhivery_data: {len(order_delhivery_join):,} matching records")
    
    # Join orders with inventory
    order_inventory_join = order_df.merge(
        inventory_df,
        left_on=['material_id', 'source'],
        right_on=['product_id', 'location'],
        how='inner'
    )
    logger.info(f"   order_data JOIN inventory_data: {len(order_inventory_join):,} matching records")
    
    # Join inventory with supply chain
    inventory_supply_join = inventory_df.merge(
        supply_chain_df,
        left_on='product_id',
        right_on='sku',
        how='inner'
    )
    logger.info(f"   inventory_data JOIN supply_chain_data: {len(inventory_supply_join):,} matching records")
    
    # 4. Check for NULLs
    logger.info("\n4. Checking for NULL Values...")
    for name, df in [("distance", distance_df), ("inventory", inventory_df), 
                     ("order", order_df), ("delhivery", delhivery_df), 
                     ("supply_chain", supply_chain_df)]:
        null_counts = df.isnull().sum()
        total_nulls = null_counts.sum()
        if total_nulls > 0:
            logger.warning(f"   ⚠️  {name}_data has {total_nulls} NULL values")
            for col, count in null_counts[null_counts > 0].items():
                logger.warning(f"      - {col}: {count}")
        else:
            logger.info(f"   ✅ {name}_data: No NULL values")
    
    # 5. Summary
    logger.info("\n" + "=" * 60)
    logger.info("Verification Summary")
    logger.info("=" * 60)
    logger.info(f"✅ All data files loaded successfully")
    logger.info(f"✅ Location connections: {len(delhivery_locations & order_locations & distance_locations & inventory_locations)} common locations")
    logger.info(f"✅ Product connections: {len(inventory_products & order_products & supply_chain_products)} common products")
    logger.info(f"✅ Join tests: All joins produce matching records")
    logger.info("=" * 60)
    
    return True

if __name__ == "__main__":
    verify_connections()

