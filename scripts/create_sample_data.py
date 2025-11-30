"""
Create sample data files for deployment.
This script creates smaller versions of large CSV files for GitHub/deployment.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

def create_sample_orders(input_path, output_path, days=30):
    """Create a sample of orders data (last N days)."""
    print(f"📊 Loading {input_path}...")
    df = pd.read_csv(input_path)
    
    # Parse datetime
    if 'available_time' in df.columns:
        df['available_time'] = pd.to_datetime(df['available_time'], errors='coerce')
        
        # Get last N days
        if df['available_time'].notna().any():
            max_date = df['available_time'].max()
            cutoff_date = max_date - timedelta(days=days)
            sample = df[df['available_time'] >= cutoff_date].copy()
        else:
            # If no valid dates, take last N rows
            sample = df.tail(min(10000, len(df))).copy()
    else:
        # No date column, take sample
        sample = df.sample(min(10000, len(df))).copy()
    
    # Save sample
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sample.to_csv(output_path, index=False)
    
    print(f"✅ Created sample: {len(sample)} rows -> {output_path}")
    print(f"   Original: {len(df)} rows")
    print(f"   Reduction: {len(df) - len(sample)} rows ({100*(1-len(sample)/len(df)):.1f}%)")
    
    return sample

if __name__ == "__main__":
    # Create sample data
    data_dir = Path(__file__).parent.parent / "data" / "coherent"
    
    # Sample orders (last 30 days)
    if (data_dir / "orders_cleaned.csv").exists():
        create_sample_orders(
            data_dir / "orders_cleaned.csv",
            data_dir / "orders_cleaned_sample.csv",
            days=30
        )
    
    print("\n✅ Sample data creation complete!")
    print("💡 Use sample files for deployment, keep full files locally.")

