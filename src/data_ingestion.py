"""
data_ingestion.py

Loads and prepares data for SmartLogix Supply Chain Optimization.

This script loads various CSV files such as orders, distances, and delivery logs,
and sets up the foundation for further cleaning, merging, and analysis.

@note: The current data is not yet cleaned. This script assumes further cleaning
       will be handled in the preprocessing module or expanded here later.
"""

import pandas as pd

# Load core datasets
def load_data():
    # Order data
    order_df = pd.read_csv("data/raw/order_small.csv")  # Can swap with order_large.csv

    # Distance matrix
    distance_df = pd.read_csv("data/raw/distance.csv")

    # Delivery logs (for ML)
    delhivery_df = pd.read_csv("data/raw/delhivery.csv")

    # Supply chain data (optional business KPIs)
    supply_df = pd.read_csv("data/raw/supply_chain_data.csv")

    # Placeholder for further API-based data (weather, traffic)
    weather_df = None  # to be filled with Open-Meteo API
    traffic_df = None  # to be filled with traffic API or synthetic

    return {
        "orders": order_df,
        "distance": distance_df,
        "delhivery": delhivery_df,
        "supply_chain": supply_df,
        "weather": weather_df,
        "traffic": traffic_df
    }

if __name__ == "__main__":
    data = load_data()
    print("Data loaded. Sample order data:")
    print(data["orders"].head())