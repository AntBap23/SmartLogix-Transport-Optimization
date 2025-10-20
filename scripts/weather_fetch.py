"""
weather_fetch.py

This script fetches historical or forecast weather data using the Open-Meteo API
for given latitude, longitude, and date range.

@note: This script assumes no authentication is required (Open-Meteo is free).
"""

import requests
import pandas as pd

def fetch_weather(lat, lon, start_date, end_date):
    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        "&hourly=temperature_2m,precipitation,wind_speed_10m,weathercode"
        "&timezone=America%2FChicago"
    )

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Failed to fetch weather data")

    data = response.json()
    hourly_data = data.get("hourly", {})
    df = pd.DataFrame(hourly_data)
    return df

if __name__ == "__main__":
    # Example: Chicago
    lat, lon = 41.8781, -87.6298
    start_date = "2023-07-01"
    end_date = "2023-07-03"

    df_weather = fetch_weather(lat, lon, start_date, end_date)
    print("Weather data fetched successfully!")
    print(f"Data shape: {df_weather.shape}")
    print("\nFirst 5 rows:")
    print(df_weather.head())
    
    # Save to CSV
    csv_filename = f"weather_data_{start_date}_to_{end_date}.csv"
    df_weather.to_csv(csv_filename, index=False)
    print(f"\nWeather data saved to: {csv_filename}")
    print(f"Total records: {len(df_weather)}")
