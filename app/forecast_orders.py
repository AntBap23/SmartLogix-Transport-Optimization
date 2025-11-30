"""
Order Forecasting ML Model
Predicts future order volumes using time series analysis and machine learning.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Note: This module does NOT import Streamlit to avoid set_page_config conflicts
# Streamlit functionality is handled in the dashboard app.py file

try:
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False


def load_orders_from_csv(csv_path=None):
    """
    Load orders from CSV file.
    
    Args:
        csv_path: Path to orders CSV. If None, tries default locations.
    
    Returns:
        DataFrame with orders data
    """
    if csv_path is None:
        # Try multiple possible locations (sample data first for deployment)
        possible_paths = [
            Path(__file__).parent.parent / "data" / "coherent" / "orders_cleaned_sample.csv",  # Sample for deployment
            Path(__file__).parent.parent / "data" / "coherent" / "orders_cleaned.csv",  # Full dataset
            Path(__file__).parent.parent / "data" / "optimizer" / "optimizer_orders.csv",  # Optimizer data
        ]
        
        for path in possible_paths:
            if path.exists():
                csv_path = path
                break
        
        if csv_path is None:
            raise FileNotFoundError("Could not find orders CSV file. Expected locations:\n" +
                                  "  - data/coherent/orders_cleaned_sample.csv (for deployment)\n" +
                                  "  - data/coherent/orders_cleaned.csv\n" +
                                  "  - data/optimizer/optimizer_orders.csv")
    
    # Load CSV
    df = pd.read_csv(csv_path)
    
    # Parse datetime columns
    datetime_cols = ['available_time', 'deadline']
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    return df


def prepare_time_series_data(df, frequency='D'):
    """
    Convert orders data to time series (daily/hourly counts).
    
    Args:
        df: Orders DataFrame
        frequency: 'D' for daily, 'H' for hourly
    
    Returns:
        DataFrame with date and order_count columns
    """
    if 'available_time' not in df.columns:
        raise ValueError("DataFrame must have 'available_time' column")
    
    # Remove null timestamps
    df_clean = df[df['available_time'].notna()].copy()
    
    # Set available_time as index and resample
    df_clean = df_clean.set_index('available_time')
    ts_data = df_clean.resample(frequency).size().reset_index()
    ts_data.columns = ['date', 'order_count']
    
    # Sort by date
    ts_data = ts_data.sort_values('date')
    
    return ts_data


def create_features(df):
    """
    Create time-based features for forecasting.
    
    Args:
        df: Time series DataFrame with 'date' and 'order_count' columns
    
    Returns:
        DataFrame with features added
    """
    df = df.copy()
    
    # Extract time features
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['day_of_week'] = df['date'].dt.dayofweek  # 0=Monday, 6=Sunday
    df['day_of_year'] = df['date'].dt.dayofyear
    df['week_of_year'] = df['date'].dt.isocalendar().week
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    
    # Lag features (previous day's order count)
    df['order_count_lag1'] = df['order_count'].shift(1)
    df['order_count_lag7'] = df['order_count'].shift(7)  # Same day last week
    
    # Rolling averages
    df['order_count_ma7'] = df['order_count'].rolling(window=7, min_periods=1).mean()
    df['order_count_ma30'] = df['order_count'].rolling(window=30, min_periods=1).mean()
    
    # Fill NaN values from lag features
    df['order_count_lag1'] = df['order_count_lag1'].fillna(df['order_count'].mean())
    df['order_count_lag7'] = df['order_count_lag7'].fillna(df['order_count'].mean())
    
    return df


def train_forecast_model(ts_data, test_size=0.2):
    """
    Train a forecasting model on time series data.
    
    Args:
        ts_data: Time series DataFrame
        test_size: Proportion of data to use for testing
    
    Returns:
        Dictionary with model, scaler, and metrics
    """
    if not SKLEARN_AVAILABLE:
        # Fallback to simple moving average
        return _simple_forecast(ts_data)
    
    # Create features
    df_features = create_features(ts_data)
    
    # Remove rows with NaN (from rolling averages at start)
    df_features = df_features.dropna()
    
    if len(df_features) < 10:
        return _simple_forecast(ts_data)
    
    # Prepare features and target
    feature_cols = [
        'year', 'month', 'day', 'day_of_week', 'day_of_year', 'week_of_year',
        'is_weekend', 'order_count_lag1', 'order_count_lag7',
        'order_count_ma7', 'order_count_ma30'
    ]
    
    X = df_features[feature_cols].values
    y = df_features['order_count'].values
    
    # Split data
    split_idx = int(len(X) * (1 - test_size))
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    if len(X_train) < 5:
        return _simple_forecast(ts_data)
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train model
    model = LinearRegression()
    model.fit(X_train_scaled, y_train)
    
    # Predictions
    y_pred_train = model.predict(X_train_scaled)
    y_pred_test = model.predict(X_test_scaled)
    
    # Metrics
    train_mae = mean_absolute_error(y_train, y_pred_train)
    test_mae = mean_absolute_error(y_test, y_pred_test)
    train_rmse = np.sqrt(mean_squared_error(y_train, y_pred_train))
    test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
    
    return {
        'model': model,
        'scaler': scaler,
        'feature_cols': feature_cols,
        'train_mae': train_mae,
        'test_mae': test_mae,
        'train_rmse': train_rmse,
        'test_rmse': test_rmse,
        'model_type': 'linear_regression'
    }


def _simple_forecast(ts_data):
    """Simple moving average forecast as fallback."""
    df = ts_data.copy()
    
    # Use 7-day moving average
    df['forecast'] = df['order_count'].rolling(window=7, min_periods=1).mean()
    
    # Simple metrics
    mae = np.mean(np.abs(df['order_count'] - df['forecast']))
    rmse = np.sqrt(np.mean((df['order_count'] - df['forecast']) ** 2))
    
    return {
        'model': None,
        'scaler': None,
        'feature_cols': None,
        'train_mae': mae,
        'test_mae': mae,
        'train_rmse': rmse,
        'test_rmse': rmse,
        'model_type': 'moving_average',
        'ts_data': df
    }


def forecast_future(model_result, ts_data, days_ahead=30):
    """
    Generate forecasts for future dates.
    
    Args:
        model_result: Result dictionary from train_forecast_model
        ts_data: Original time series data
        days_ahead: Number of days to forecast
    
    Returns:
        DataFrame with forecasts
    """
    if model_result['model_type'] == 'moving_average':
        # Simple forecast using moving average
        last_value = ts_data['order_count'].tail(7).mean()
        forecast_dates = pd.date_range(
            start=ts_data['date'].max() + timedelta(days=1),
            periods=days_ahead,
            freq='D'
        )
        forecast_df = pd.DataFrame({
            'date': forecast_dates,
            'forecast': [last_value] * days_ahead,
            'order_count': None
        })
        return forecast_df
    
    # Use trained model
    model = model_result['model']
    scaler = model_result['scaler']
    feature_cols = model_result['feature_cols']
    
    # Get last row for lag features
    df_features = create_features(ts_data)
    df_features = df_features.dropna()
    
    if len(df_features) == 0:
        # Fallback
        last_value = ts_data['order_count'].tail(7).mean()
        forecast_dates = pd.date_range(
            start=ts_data['date'].max() + timedelta(days=1),
            periods=days_ahead,
            freq='D'
        )
        forecast_df = pd.DataFrame({
            'date': forecast_dates,
            'forecast': [last_value] * days_ahead,
            'order_count': None
        })
        return forecast_df
    
    last_row = df_features.iloc[-1]
    
    # Generate forecasts
    forecasts = []
    current_date = ts_data['date'].max()
    
    # Use last known values for initial lag features
    last_order_count = last_row['order_count']
    last_order_count_lag7 = last_row['order_count_lag7'] if not pd.isna(last_row['order_count_lag7']) else last_order_count
    
    for i in range(1, days_ahead + 1):
        forecast_date = current_date + timedelta(days=i)
        
        # Create features for this date
        features = {
            'year': forecast_date.year,
            'month': forecast_date.month,
            'day': forecast_date.day,
            'day_of_week': forecast_date.weekday(),
            'day_of_year': forecast_date.timetuple().tm_yday,
            'week_of_year': forecast_date.isocalendar()[1],
            'is_weekend': 1 if forecast_date.weekday() >= 5 else 0,
            'order_count_lag1': last_order_count,
            'order_count_lag7': last_order_count_lag7,
            'order_count_ma7': last_row['order_count_ma7'] if not pd.isna(last_row['order_count_ma7']) else last_order_count,
            'order_count_ma30': last_row['order_count_ma30'] if not pd.isna(last_row['order_count_ma30']) else last_order_count
        }
        
        # Prepare feature vector
        X = np.array([[features[col] for col in feature_cols]])
        X_scaled = scaler.transform(X)
        
        # Predict
        pred = model.predict(X_scaled)[0]
        pred = max(0, pred)  # Ensure non-negative
        
        forecasts.append({
            'date': forecast_date,
            'forecast': pred,
            'order_count': None
        })
        
        # Update for next iteration
        last_order_count = pred
        if i >= 7:
            last_order_count_lag7 = forecasts[i-7]['forecast']
    
    forecast_df = pd.DataFrame(forecasts)
    return forecast_df


def forecast_orders(csv_path=None, days_ahead=30, frequency='D'):
    """
    Main function to forecast orders.
    
    Args:
        csv_path: Path to orders CSV
        days_ahead: Number of days to forecast
        frequency: 'D' for daily, 'H' for hourly
    
    Returns:
        Dictionary with historical data, forecasts, and model metrics
    """
    # Load data
    df = load_orders_from_csv(csv_path)
    
    # Prepare time series
    ts_data = prepare_time_series_data(df, frequency=frequency)
    
    if len(ts_data) < 7:
        raise ValueError("Not enough historical data for forecasting. Need at least 7 data points.")
    
    # Train model
    model_result = train_forecast_model(ts_data)
    
    # Generate forecasts
    forecast_df = forecast_future(model_result, ts_data, days_ahead=days_ahead)
    
    return {
        'historical': ts_data,
        'forecast': forecast_df,
        'model_metrics': {
            'train_mae': model_result['train_mae'],
            'test_mae': model_result['test_mae'],
            'train_rmse': model_result['train_rmse'],
            'test_rmse': model_result['test_rmse'],
            'model_type': model_result['model_type']
        },
        'total_historical_orders': len(df),
        'historical_days': len(ts_data),
        'forecast_days': days_ahead
    }


if __name__ == "__main__":
    # Test the forecasting
    try:
        result = forecast_orders(days_ahead=30)
        print("✅ Forecasting successful!")
        print(f"Historical data points: {result['historical_days']}")
        print(f"Forecast days: {result['forecast_days']}")
        print(f"Model type: {result['model_metrics']['model_type']}")
        print(f"Test MAE: {result['model_metrics']['test_mae']:.2f}")
        print(f"\nFirst 5 forecasts:")
        print(result['forecast'].head())
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

