# Scripts

This folder contains utility scripts and data processing tools.

## Files

- `data_cleaner.py` - Data cleaning and preprocessing script (full edition)
- `data_cleaner_free_edition.py` - Data cleaning and preprocessing script (free edition)
- `run_data_pipeline.py` - Main data pipeline execution script
- `weather_fetch.py` - Weather data fetching utility using Open-Meteo API

## Usage

Most scripts can be run directly from the project root:

```bash
# Activate virtual environment
source .venv/bin/activate

# Run weather data fetching
python scripts/weather_fetch.py

# Run data pipeline
python scripts/run_data_pipeline.py
```

## Weather Data

The `weather_fetch.py` script fetches historical weather data and saves it to CSV format. Output files are saved to `data/external/`.
